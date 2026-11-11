import unittest
from unittest.mock import AsyncMock, patch

import httpx
from aiokafka.errors import KafkaError
from sqlalchemy import func, select

from src.db.database import get_db
from src.main import app
from src.models import Transcription, TranscriptionStatus
from tests.factories import make_task, make_user, rollback_connection, session_for, task_payload


class TaskPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.connection = await self.enterAsyncContext(rollback_connection())

        async def database():
            async with session_for(self.connection) as session:
                yield session

        self.enterContext(patch.dict(app.dependency_overrides, {get_db: database}))
        self.enterContext(patch.object(app.state, 'api_token', 'test-service-token', create=True))
        self.producer = AsyncMock()
        self.enterContext(patch.object(app.state, 'producer', self.producer, create=True))
        self.client = await self.enterAsyncContext(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url='http://test',
            headers={'Authorization': 'Bearer test-service-token'},
        ))
        self.user = await make_user(self.connection)
        self.payload = task_payload(self.user)

    async def saved_task(self, task_id):
        async with session_for(self.connection) as session:
            task = await session.get(Transcription, task_id)
            self.assertIsNotNone(task)
            return task

    async def test_create_saves_task_and_publishes_its_id(self):
        response = await self.client.post('/transcriptions', json=self.payload)

        self.assertEqual(response.status_code, 202, response.text)
        task = response.json()
        saved = await self.saved_task(task['id'])
        self.assertEqual(saved.user_id, self.user.id)
        self.assertEqual(saved.status, TranscriptionStatus.QUEUED)
        self.assertEqual(saved.telegram_file_name, 'recording.mp3')
        key = str(task['id']).encode()
        self.producer.send_and_wait.assert_awaited_once_with('transcriptions', key, key=key)

    async def test_repeated_message_does_not_create_or_publish_another_task(self):
        first = await self.client.post('/transcriptions', json=self.payload)
        self.assertEqual(first.status_code, 202, first.text)

        repeated = await self.client.post('/transcriptions', json=self.payload)

        self.assertEqual(repeated.status_code, 202, repeated.text)
        self.assertEqual(repeated.json()['id'], first.json()['id'])
        async with session_for(self.connection) as session:
            count = await session.scalar(
                select(func.count()).select_from(Transcription)
                .where(Transcription.user_id == self.user.id)
            )
        self.assertEqual(count, 1)
        self.producer.send_and_wait.assert_awaited_once()

    async def test_existing_other_user_cannot_read_task(self):
        owner_task = await make_task(self.connection, self.user)
        other = await make_user(self.connection, telegram_id=self.user.telegram_id + 1)
        await make_task(self.connection, other)

        response = await self.client.get(
            f'/transcriptions/{owner_task.id}', params={'telegram_id': other.telegram_id},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {'detail': 'Transcription not found'})

    async def test_history_contains_only_requested_users_tasks(self):
        owner_task = await make_task(self.connection, self.user)
        other = await make_user(self.connection, telegram_id=self.user.telegram_id + 1)
        other_task = await make_task(self.connection, other)

        owner_response = await self.client.get('/transcriptions', params={'telegram_id': self.user.telegram_id})
        other_response = await self.client.get('/transcriptions', params={'telegram_id': other.telegram_id})

        self.assertEqual(owner_response.status_code, 200)
        self.assertEqual(other_response.status_code, 200)
        self.assertEqual([task['id'] for task in owner_response.json()], [owner_task.id])
        self.assertEqual([task['id'] for task in other_response.json()], [other_task.id])

    async def test_history_returns_latest_twenty_tasks(self):
        tasks = [
            await make_task(self.connection, self.user, telegram_message_id=number)
            for number in range(1, 22)
        ]

        response = await self.client.get('/transcriptions', params={'telegram_id': self.user.telegram_id})

        self.assertEqual(response.status_code, 200)
        self.assertEqual([task['id'] for task in response.json()], [task.id for task in tasks[-20:][::-1]])

    async def test_another_user_cannot_reuse_existing_telegram_message(self):
        await make_task(self.connection, self.user)
        other = await make_user(self.connection, telegram_id=self.user.telegram_id + 1)
        payload = task_payload(self.user, telegram_id=other.telegram_id)

        response = await self.client.post('/transcriptions', json=payload)

        self.assertEqual(response.status_code, 404)
        self.producer.send_and_wait.assert_not_awaited()

    async def test_processing_and_result_survive_separate_requests(self):
        task = await make_task(self.connection, self.user)

        started = await self.client.post(f'/transcriptions/{task.id}/start')
        self.assertEqual(started.status_code, 200, started.text)
        self.assertEqual((await self.saved_task(task.id)).status, TranscriptionStatus.PROCESSING)
        finished = await self.client.post(f'/transcriptions/{task.id}/finish', json={'result': 'Done'})

        self.assertEqual(finished.status_code, 200, finished.text)
        saved = await self.saved_task(task.id)
        self.assertEqual(saved.status, TranscriptionStatus.SUCCEEDED)
        self.assertEqual(saved.result, 'Done')
        self.assertIsNone(saved.error)
        self.assertIsNotNone(saved.finished_at)
        received = await self.client.get(f'/transcriptions/{task.id}', params={'telegram_id': self.user.telegram_id})
        self.assertEqual(received.status_code, 200)
        self.assertEqual(received.json()['result'], 'Done')

    async def test_repeated_finish_cannot_overwrite_saved_success(self):
        task = await make_task(self.connection, self.user)
        await self.client.post(f'/transcriptions/{task.id}/start')
        first = await self.client.post(f'/transcriptions/{task.id}/finish', json={'result': 'Done'})
        self.assertEqual(first.status_code, 200, first.text)

        repeated = await self.client.post(f'/transcriptions/{task.id}/finish', json={'error': 'Late failure'})

        self.assertEqual(repeated.status_code, 200, repeated.text)
        saved = await self.saved_task(task.id)
        self.assertEqual(saved.status, TranscriptionStatus.SUCCEEDED)
        self.assertEqual(saved.result, 'Done')
        self.assertIsNone(saved.error)
        self.assertEqual(saved.finished_at, first.json()['finished_at'])

    async def test_failed_task_preserves_error_and_completion_time(self):
        task = await make_task(self.connection, self.user)
        await self.client.post(f'/transcriptions/{task.id}/start')

        response = await self.client.post(f'/transcriptions/{task.id}/finish', json={'error': 'Failed'})

        self.assertEqual(response.status_code, 200, response.text)
        saved = await self.saved_task(task.id)
        self.assertEqual(saved.status, TranscriptionStatus.FAILED)
        self.assertEqual(saved.error, 'Failed')
        self.assertIsNone(saved.result)
        self.assertIsNotNone(saved.finished_at)

    async def test_finish_before_start_does_not_change_task(self):
        task = await make_task(self.connection, self.user)

        response = await self.client.post(f'/transcriptions/{task.id}/finish', json={'result': 'Done'})

        self.assertEqual(response.status_code, 409)
        saved = await self.saved_task(task.id)
        self.assertEqual(saved.status, TranscriptionStatus.QUEUED)
        self.assertIsNone(saved.result)
        self.assertIsNone(saved.finished_at)

    async def test_finish_requires_one_unambiguous_outcome(self):
        task = await make_task(self.connection, self.user)
        for payload in ({}, {'result': 'Done', 'error': 'Failed'}):
            with self.subTest(payload=payload):
                response = await self.client.post(f'/transcriptions/{task.id}/finish', json=payload)
                self.assertEqual(response.status_code, 422)
        self.assertEqual((await self.saved_task(task.id)).status, TranscriptionStatus.QUEUED)

    async def test_publish_failure_is_reported_and_saved(self):
        self.producer.send_and_wait.side_effect = KafkaError('Kafka unavailable')
        with self.assertLogs(level='ERROR'):
            response = await self.client.post('/transcriptions', json=self.payload)

        self.assertEqual(response.status_code, 503)
        async with session_for(self.connection) as session:
            saved = await session.scalar(select(Transcription).where(Transcription.user_id == self.user.id))
        self.assertIsNotNone(saved)
        self.assertEqual(saved.status, TranscriptionStatus.FAILED)
        self.assertEqual(saved.error, 'Could not enqueue transcription')

    async def test_timeout_after_worker_started_returns_accepted_task(self):
        async def delivered_before_timeout(topic, value, key):
            task_id = int(value)
            response = await self.client.post(f'/transcriptions/{task_id}/start')
            self.assertEqual(response.status_code, 200, response.text)
            raise TimeoutError('Kafka acknowledgement delayed after delivery')

        self.producer.send_and_wait.side_effect = delivered_before_timeout

        with self.assertLogs(level='ERROR'):
            response = await self.client.post('/transcriptions', json=self.payload)

        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(response.json()['status'], 'processing')
        self.assertEqual((await self.saved_task(response.json()['id'])).status, TranscriptionStatus.PROCESSING)

    async def test_timeout_after_worker_finished_returns_saved_result(self):
        async def delivered_before_timeout(topic, value, key):
            task_id = int(value)
            started = await self.client.post(f'/transcriptions/{task_id}/start')
            self.assertEqual(started.status_code, 200, started.text)
            finished = await self.client.post(f'/transcriptions/{task_id}/finish', json={'result': 'Done'})
            self.assertEqual(finished.status_code, 200, finished.text)
            raise TimeoutError('Kafka acknowledgement delayed after delivery')

        self.producer.send_and_wait.side_effect = delivered_before_timeout

        with self.assertLogs(level='ERROR'):
            response = await self.client.post('/transcriptions', json=self.payload)

        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(response.json()['status'], 'succeeded')
        self.assertEqual(response.json()['result'], 'Done')
        self.assertEqual((await self.saved_task(response.json()['id'])).result, 'Done')
