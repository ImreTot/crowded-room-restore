import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from aiogram import Bot
from aiogram.methods import EditMessageText, SendDocument
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from tests.factories import BOT_TOKEN, RecordingTelegramSession, make_message

with patch.dict(os.environ, {'TELEGRAM_BOT_TOKEN': BOT_TOKEN}):
    import bot


class AudioHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.telegram = RecordingTelegramSession()
        self.sender = Bot(token=BOT_TOKEN, session=self.telegram)
        self.addAsyncCleanup(self.telegram.close)
        self.message = make_message(self.sender, voice={
            'file_id': 'voice-id', 'file_unique_id': 'voice-unique-id', 'duration': 1,
        })
        self.created = []
        self.polled = []
        self.status_responses = [(200, {'id': 7, 'status': 'succeeded', 'result': 'Recognized text'})]

        async def create_task(request):
            self.created.append(await request.json())
            return web.json_response({'id': 7, 'status': 'queued'}, status=202)

        async def read_status(request):
            self.polled.append((request.match_info['task_id'], dict(request.query)))
            status, payload = self.status_responses.pop(0)
            return web.json_response(payload, status=status)

        app = web.Application()
        app.router.add_post('/transcriptions', create_task)
        app.router.add_get('/transcriptions/{task_id}', read_status)
        server = await self.enterAsyncContext(TestServer(app))
        self.client = await self.enterAsyncContext(ClientSession(base_url=server.make_url('/')))
        self.enterContext(patch.object(bot, 'sleep', new=AsyncMock()))

    def result_message(self):
        messages = [item for item in self.telegram.requests if isinstance(item, EditMessageText)]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].chat_id, self.message.chat.id)
        return messages[0].text

    async def test_voice_is_queued_and_result_is_delivered_to_sender(self):
        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.created, [{
            'telegram_id': 123, 'telegram_chat_id': 123, 'telegram_message_id': 5,
            'telegram_file_id': 'voice-id', 'telegram_file_name': None,
        }])
        self.assertEqual(self.polled, [('7', {'telegram_id': '123'})])
        self.assertEqual(self.result_message(), 'Recognized text')

    async def test_audio_filename_is_sent_to_api(self):
        message = make_message(self.sender, audio={
            'file_id': 'audio-id', 'file_unique_id': 'audio-unique-id',
            'duration': 1, 'file_name': 'recording.mp3',
        })

        await bot.handle_audio(message, self.client)

        self.assertEqual(self.created[0]['telegram_file_id'], 'audio-id')
        self.assertEqual(self.created[0]['telegram_file_name'], 'recording.mp3')
        self.assertEqual(self.result_message(), 'Recognized text')

    async def test_result_is_delivered_after_processing(self):
        self.status_responses = [
            (200, {'id': 7, 'status': 'processing'}),
            (200, {'id': 7, 'status': 'succeeded', 'result': 'Done'}),
        ]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.result_message(), 'Done')

    async def test_empty_result_notifies_user(self):
        self.status_responses = [(200, {'id': 7, 'status': 'succeeded', 'result': ''})]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.result_message(), 'Не удалось распознать речь в этом аудио.')

    async def test_long_result_is_delivered_as_utf8_document(self):
        text = 'Я' * 4097
        self.status_responses = [(200, {'id': 7, 'status': 'succeeded', 'result': text})]

        await bot.handle_audio(self.message, self.client)

        documents = [item for item in self.telegram.requests if isinstance(item, SendDocument)]
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0].chat_id, self.message.chat.id)
        self.assertEqual(documents[0].document.filename, 'transcription.txt')
        self.assertEqual(documents[0].document.data.decode('utf-8'), text)
        self.assertEqual(self.result_message(), '✅ Готово. Расшифровка отправлена файлом.')

    async def test_result_at_message_limit_is_delivered_as_text(self):
        text = 'a' * 4096
        self.status_responses = [(200, {'id': 7, 'status': 'succeeded', 'result': text})]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.result_message(), text)
        self.assertFalse(any(isinstance(item, SendDocument) for item in self.telegram.requests))

    async def test_failed_task_notifies_user(self):
        self.status_responses = [(200, {'id': 7, 'status': 'failed'})]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(
            self.result_message(), '❌ Не удалось расшифровать аудио. Попробуйте ещё раз позже.',
        )

    async def test_temporary_api_failure_does_not_lose_result(self):
        self.status_responses = [
            (503, {'detail': 'API restarting'}),
            (200, {'id': 7, 'status': 'succeeded', 'result': 'Done'}),
        ]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.result_message(), 'Done')
        self.assertEqual(len(self.created), 1)

    async def test_successful_poll_resets_failure_budget(self):
        self.status_responses = [
            (503, {'detail': 'API restarting'}),
            (503, {'detail': 'API restarting'}),
            (200, {'id': 7, 'status': 'processing'}),
            (503, {'detail': 'API restarting'}),
            (200, {'id': 7, 'status': 'succeeded', 'result': 'Done'}),
        ]

        await bot.handle_audio(self.message, self.client)

        self.assertEqual(self.result_message(), 'Done')

    async def test_persistent_api_failure_notifies_user(self):
        self.status_responses = [(503, {'detail': 'API unavailable'})] * 3
        async with asyncio.timeout(5):
            with self.assertLogs(level='ERROR'):
                await bot.handle_audio(self.message, self.client)

        self.assertEqual(len(self.polled), 3)
        self.assertEqual(
            self.result_message(), '❌ Не удалось получить расшифровку. Попробуйте ещё раз позже.',
        )

    async def test_authentication_failure_is_not_retried(self):
        self.status_responses = [(401, {'detail': 'Invalid service token'})]
        async with asyncio.timeout(5):
            with self.assertLogs(level='ERROR'):
                await bot.handle_audio(self.message, self.client)

        self.assertEqual(len(self.polled), 1)
        self.assertEqual(
            self.result_message(), '❌ Не удалось получить расшифровку. Попробуйте ещё раз позже.',
        )

    async def test_missing_service_token_stops_startup(self):
        with patch.dict(os.environ, {'INTERNAL_API_TOKEN': '  '}):
            with self.assertRaisesRegex(ValueError, 'INTERNAL_API_TOKEN is not set'):
                await bot.main()
