from io import BytesIO
import unittest

from aiohttp import ClientResponseError, ClientSession, web
from aiohttp.test_utils import TestServer

from src.transcription import transcribe_audio


class TranscriptionClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.payload = {'text': '  Recognized text\n'}
        self.status = 200
        self.received = None

        async def receive(request):
            form = await request.post()
            audio = form['file']
            self.received = {
                'model': form['model'],
                'filename': audio.filename,
                'audio': audio.file.read(),
                'authorization': request.headers['Authorization'],
            }
            return web.json_response(
                self.payload, status=self.status,
                headers={'x-request-id': 'test-request-id'},
            )

        app = web.Application()
        app.router.add_post('/v1/audio/transcriptions', receive)
        self.server = await self.enterAsyncContext(TestServer(app))
        self.client = await self.enterAsyncContext(ClientSession(
            base_url=self.server.make_url('/v1/'),
            headers={'Authorization': 'Bearer test-key'},
        ))

    async def test_uploads_multipart_audio_and_returns_text(self):
        with BytesIO(b'audio-bytes') as audio:
            text = await transcribe_audio(self.client, audio, 'voice.ogg')
        self.assertEqual(text, 'Recognized text')
        self.assertEqual(self.received, {
            'model': 'gpt-transcribe',
            'filename': 'voice.ogg',
            'audio': b'audio-bytes',
            'authorization': 'Bearer test-key',
        })

    async def test_whitespace_only_transcription_returns_empty_text(self):
        self.payload = {'text': ' \n '}
        with BytesIO(b'audio-bytes') as audio:
            text = await transcribe_audio(self.client, audio, 'voice.ogg')
        self.assertEqual(text, '')

    async def test_http_error_is_propagated(self):
        self.payload = {'error': {'message': 'Invalid API key'}}
        self.status = 401
        with BytesIO(b'audio-bytes') as audio:
            with self.assertRaises(ClientResponseError) as raised:
                await transcribe_audio(self.client, audio, 'voice.ogg')
        self.assertEqual(raised.exception.status, 401)

    async def test_quota_and_rate_limit_codes_are_logged_without_response_message(self):
        for code, error_type in (
            ('credit_balance_exhausted', 'insufficient_quota'),
            ('rate_limit_exceeded', 'rate_limit_error'),
        ):
            with self.subTest(code=code):
                self.status = 429
                self.payload = {'error': {
                    'code': code,
                    'type': error_type,
                    'message': 'Sensitive response details',
                }}
                with self.assertLogs(level='ERROR') as logs:
                    with BytesIO(b'audio-bytes') as audio:
                        with self.assertRaises(ClientResponseError) as raised:
                            await transcribe_audio(self.client, audio, 'voice.ogg')
                self.assertEqual(raised.exception.status, 429)
                self.assertIn(code, logs.output[0])
                self.assertIn(error_type, logs.output[0])
                self.assertIn('test-request-id', logs.output[0])
                self.assertNotIn('Sensitive response details', logs.output[0])
