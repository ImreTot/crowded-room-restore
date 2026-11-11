import os
import unittest
from unittest.mock import patch

from aiogram import Bot
from aiogram.methods import SendMessage
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from tests.factories import BOT_TOKEN, RecordingTelegramSession, make_message

with patch.dict(os.environ, {'TELEGRAM_BOT_TOKEN': BOT_TOKEN}):
    from bot import register_user


class RegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.telegram = RecordingTelegramSession()
        self.sender = Bot(token=BOT_TOKEN, session=self.telegram)
        self.addAsyncCleanup(self.telegram.close)
        self.message = make_message(self.sender)
        self.registered = []
        self.handled = []
        self.status = 200

        async def register(request):
            self.registered.append(await request.json())
            return web.json_response({}, status=self.status)

        app = web.Application()
        app.router.add_post('/users', register)
        server = await self.enterAsyncContext(TestServer(app))
        client = await self.enterAsyncContext(ClientSession(base_url=server.make_url('/')))
        self.data = {'api_client': client}

    async def handler(self, message, data):
        self.handled.append(message)
        return 'handled'

    async def test_user_without_username_is_registered_before_handler(self):
        async def handler(message, data):
            self.assertEqual(self.registered, [{
                'telegram_id': 123, 'telegram_username': None, 'is_bot': False,
            }])
            return 'handled'

        result = await register_user(handler, self.message, self.data)

        self.assertEqual(result, 'handled')
        self.assertEqual(self.telegram.requests, [])

    async def test_api_error_stops_processing_and_notifies_sender(self):
        self.status = 503
        with self.assertLogs(level='ERROR'):
            await register_user(self.handler, self.message, self.data)

        self.assertEqual(self.handled, [])
        self.assertEqual(len(self.telegram.requests), 1)
        notification = self.telegram.requests[0]
        self.assertIsInstance(notification, SendMessage)
        self.assertEqual(notification.chat_id, self.message.chat.id)
        self.assertEqual(
            notification.text, 'Не удалось сохранить данные пользователя. Попробуйте ещё раз позже.',
        )

    async def test_message_without_sender_reaches_handler_without_registration(self):
        message = make_message(self.sender, from_user=None)

        result = await register_user(self.handler, message, self.data)

        self.assertEqual(result, 'handled')
        self.assertEqual(self.handled, [message])
        self.assertEqual(self.registered, [])
