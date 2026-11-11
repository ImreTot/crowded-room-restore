import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram import Bot
from aiokafka import TopicPartition
from aiohttp import ClientResponseError, ClientSession, web
from aiohttp.test_utils import TestServer

from src import main
from tests.factories import BOT_TOKEN, DownloadSession, make_task


class ProcessingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.task = make_task()
        self.outcomes = []
        self.uploads = []
        self.start_status = 200
        self.finish_status = 200
        self.provider_status = 200
        self.provider_payload = {'text': 'Recognized text'}
        self.telegram = DownloadSession()
        self.bot = Bot(token=BOT_TOKEN, session=self.telegram)
        self.addAsyncCleanup(self.telegram.close)

        async def start(request):
            if request.headers.get('Authorization') != 'Bearer test-service-token':
                raise web.HTTPUnauthorized()
            return web.json_response(self.task, status=self.start_status)

        async def finish(request):
            if request.headers.get('Authorization') != 'Bearer test-service-token':
                raise web.HTTPUnauthorized()
            if self.finish_status != 200:
                return web.json_response({'detail': 'API unavailable'}, status=self.finish_status)
            self.outcomes.append(await request.json())
            return web.json_response({})

        async def transcribe(request):
            if request.headers.get('Authorization') != 'Bearer test-openai-key':
                raise web.HTTPUnauthorized()
            form = await request.post()
            audio = form['file']
            self.uploads.append({'filename': audio.filename, 'content': audio.file.read()})
            return web.json_response(self.provider_payload, status=self.provider_status)

        app = web.Application()
        app.router.add_post('/transcriptions/7/start', start)
        app.router.add_post('/transcriptions/7/finish', finish)
        app.router.add_post('/v1/audio/transcriptions', transcribe)
        self.server = await self.enterAsyncContext(TestServer(app))
        self.api = await self.enterAsyncContext(ClientSession(
            base_url=self.server.make_url('/'),
            headers={'Authorization': 'Bearer test-service-token'},
        ))
        self.provider = await self.enterAsyncContext(ClientSession(
            base_url=self.server.make_url('/v1/'),
            headers={'Authorization': 'Bearer test-openai-key'},
        ))

    async def test_downloaded_audio_is_transcribed_and_result_is_saved(self):
        await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.uploads, [{'filename': 'file_1.ogg', 'content': b'audio-bytes'}])
        self.assertEqual(self.outcomes, [{'result': 'Recognized text'}])

    async def test_original_filename_is_sent_to_provider(self):
        self.task = make_task(telegram_file_name='recording.mp3')

        await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.uploads[0]['filename'], 'recording.mp3')
        self.assertEqual(self.outcomes, [{'result': 'Recognized text'}])

    async def test_empty_transcription_is_saved_as_success(self):
        self.provider_payload = {'text': '  '}

        await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.outcomes, [{'result': ''}])

    async def test_succeeded_task_does_not_download_or_transcribe_audio(self):
        self.task = make_task(status='succeeded')

        await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.telegram.requests, [])
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.outcomes, [])

    async def test_failed_task_does_not_download_or_transcribe_audio(self):
        self.task = make_task(status='failed')

        await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.telegram.requests, [])
        self.assertEqual(self.uploads, [])
        self.assertEqual(self.outcomes, [])

    async def test_download_failure_is_saved_without_calling_provider(self):
        self.telegram.download_error = TimeoutError('Sensitive download details')
        with self.assertLogs(level='ERROR') as logs:
            await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.uploads, [])
        self.assertEqual(self.outcomes, [{'error': 'download failed: TimeoutError'}])
        self.assertNotIn('Sensitive download details', '\n'.join(logs.output))

    async def test_provider_failure_preserves_status_without_sensitive_details(self):
        self.provider_status = 429
        self.provider_payload = {'error': {
            'code': 'credit_balance_exhausted', 'type': 'insufficient_quota',
            'message': 'Sensitive provider details',
        }}
        with self.assertLogs(level='ERROR') as logs:
            await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(self.outcomes, [{
            'error': 'transcription failed: ClientResponseError (HTTP 429)',
        }])
        self.assertNotIn('Sensitive provider details', '\n'.join(logs.output))

    async def test_unavailable_api_prevents_paid_transcription(self):
        self.start_status = 503

        with self.assertRaises(ClientResponseError) as error:
            await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(error.exception.status, 503)
        self.assertEqual(self.telegram.requests, [])
        self.assertEqual(self.uploads, [])

    async def test_result_save_failure_is_not_swallowed(self):
        self.finish_status = 503

        with self.assertRaises(ClientResponseError) as error:
            await main.process_task(7, self.api, self.bot, self.provider)

        self.assertEqual(error.exception.status, 503)
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(self.outcomes, [])

    def worker_environment(self, consumer):
        self.enterContext(patch.dict(os.environ, {
            'TELEGRAM_BOT_TOKEN': BOT_TOKEN,
            'OPENAI_API_KEY': 'test-openai-key',
            'INTERNAL_API_TOKEN': 'test-service-token',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
        }))
        self.enterContext(patch.object(main, 'AIOKafkaConsumer', return_value=consumer))
        self.enterContext(patch.object(main, 'Bot', return_value=self.bot))

        def client(*, base_url, headers, timeout):
            paths = {'http://api:8000': '/', 'https://api.openai.com/v1/': '/v1/'}
            return ClientSession(
                base_url=self.server.make_url(paths[base_url]), headers=headers, timeout=timeout,
            )

        self.enterContext(patch.object(main, 'ClientSession', side_effect=client))

    def kafka_message(self):
        consumer = MagicMock()
        consumer.start = AsyncMock()
        consumer.stop = AsyncMock()
        consumer.commit = AsyncMock()
        consumer.__aiter__.return_value = [SimpleNamespace(
            value=b'7', topic='transcriptions', partition=0, offset=9,
        )]
        return consumer

    async def test_consumer_commits_only_after_outcome_is_saved(self):
        consumer = self.kafka_message()
        self.worker_environment(consumer)

        async def commit(offsets):
            self.assertEqual(self.outcomes, [{'result': 'Recognized text'}])
            self.assertEqual(offsets, {TopicPartition('transcriptions', 0): 10})

        consumer.commit.side_effect = commit

        await main.main()

        consumer.commit.assert_awaited_once()
        consumer.stop.assert_awaited_once()
        self.assertTrue(self.telegram.closed)

    async def test_consumer_does_not_commit_if_result_cannot_be_saved(self):
        self.finish_status = 503
        consumer = self.kafka_message()
        self.worker_environment(consumer)

        with self.assertRaises(ClientResponseError):
            await main.main()

        self.assertEqual(self.outcomes, [])
        consumer.commit.assert_not_awaited()
        consumer.stop.assert_awaited_once()
        self.assertTrue(self.telegram.closed)
