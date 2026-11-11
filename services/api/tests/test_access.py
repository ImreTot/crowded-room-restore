import os
import unittest
from unittest.mock import patch

import httpx

from src.db.database import get_db
from src.main import app, lifespan


class AccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch.object(app.state, 'api_token', 'test-service-token', create=True))

        async def unexpected_database():
            self.fail('Rejected requests and health checks must not access the database')

        self.enterContext(patch.dict(app.dependency_overrides, {get_db: unexpected_database}))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url='http://test',
        ))

    async def test_health_is_public(self):
        response = await self.client.get('/health')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    async def test_data_routes_reject_missing_or_invalid_service_token(self):
        routes = (
            ('GET', '/transcriptions?telegram_id=123', None),
            ('GET', '/transcriptions/7?telegram_id=123', None),
            ('POST', '/users', {'telegram_id': 123}),
            ('POST', '/transcriptions', {
                'telegram_id': 123, 'telegram_chat_id': 123,
                'telegram_message_id': 5, 'telegram_file_id': 'voice-id',
            }),
            ('POST', '/transcriptions/7/start', None),
            ('POST', '/transcriptions/7/finish', {'result': 'Done'}),
        )
        for method, path, payload in routes:
            for headers in ({}, {'Authorization': 'Bearer wrong-token'}):
                with self.subTest(method=method, path=path, headers=headers):
                    response = await self.client.request(method, path, json=payload, headers=headers)

                    self.assertEqual(response.status_code, 401)
                    self.assertEqual(response.json(), {'detail': 'Invalid service token'})
                    self.assertEqual(response.headers['www-authenticate'], 'Bearer')

    async def test_old_users_stub_is_removed(self):
        response = await self.client.get('/users')

        self.assertEqual(response.status_code, 405)

    async def test_missing_service_token_prevents_startup(self):
        with patch.dict(os.environ, {'INTERNAL_API_TOKEN': '', 'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092'}):
            with self.assertRaisesRegex(ValueError, 'INTERNAL_API_TOKEN is not set'):
                async with lifespan(app):
                    self.fail('Startup should fail without a service token')
