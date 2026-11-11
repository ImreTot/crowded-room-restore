import unittest
from unittest.mock import patch

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from src.main import upsert_user
from src.models import Transcription, TranscriptionStatus, User
from src.schema import UserCreate
from tests.factories import rollback_connection, session_for


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        connection = await self.enterAsyncContext(rollback_connection())
        self.db = await self.enterAsyncContext(session_for(connection))
        self.telegram_id = 9223372036854775000

    async def test_first_request_creates_user(self):
        user = await upsert_user(UserCreate(telegram_id=self.telegram_id), self.db)
        self.assertIsNotNone(user.id)
        self.assertEqual(user.telegram_id, self.telegram_id)
        self.assertIsNone(user.telegram_username)
        self.assertFalse(user.is_bot)

    async def test_repeated_request_updates_existing_user(self):
        with patch('src.main.time.time', return_value=1000):
            original = await upsert_user(
                UserCreate(telegram_id=self.telegram_id, telegram_username='old_name'),
                self.db,
            )
        original_id = original.id
        with patch('src.main.time.time', return_value=2000):
            updated = await upsert_user(
                UserCreate(
                    telegram_id=self.telegram_id,
                    telegram_username='new_name',
                    is_bot=True,
                ),
                self.db,
            )
        self.assertEqual(updated.id, original_id)
        self.assertEqual(updated.telegram_username, 'new_name')
        self.assertTrue(updated.is_bot)
        self.assertEqual(updated.created_at, 1000)
        self.assertEqual(updated.updated_at, 2000)
        count = await self.db.scalar(
            select(func.count()).select_from(User).where(User.telegram_id == self.telegram_id)
        )
        self.assertEqual(count, 1)

    async def test_removed_username_is_cleared(self):
        await upsert_user(
            UserCreate(telegram_id=self.telegram_id, telegram_username='old_name'),
            self.db,
        )
        updated = await upsert_user(UserCreate(telegram_id=self.telegram_id), self.db)
        self.assertIsNone(updated.telegram_username)

    async def test_transcription_persists_status_and_result(self):
        user = await upsert_user(UserCreate(telegram_id=self.telegram_id), self.db)
        transcription = Transcription(
            user_id=user.id,
            telegram_chat_id=self.telegram_id,
            telegram_message_id=1,
            telegram_file_id='test-file-id',
        )
        self.db.add(transcription)
        await self.db.flush()
        self.assertEqual(transcription.status, TranscriptionStatus.QUEUED)
        transcription.transition_to(TranscriptionStatus.PROCESSING)
        transcription.result = 'Recognized text'
        transcription.transition_to(TranscriptionStatus.SUCCEEDED)
        await self.db.commit()
        transcription_id = transcription.id
        self.db.expunge_all()
        stored = await self.db.get(Transcription, transcription_id)
        self.assertEqual(stored.status, TranscriptionStatus.SUCCEEDED)
        self.assertEqual(stored.result, 'Recognized text')
        self.assertIsNotNone(stored.finished_at)

    async def test_same_telegram_message_cannot_create_two_tasks(self):
        user = await upsert_user(UserCreate(telegram_id=self.telegram_id), self.db)
        for _ in range(2):
            self.db.add(Transcription(
                user_id=user.id,
                telegram_chat_id=self.telegram_id,
                telegram_message_id=1,
                telegram_file_id='test-file-id',
            ))
        with self.assertRaises(IntegrityError):
            await self.db.flush()

    async def test_task_cannot_reference_missing_user(self):
        self.db.add(Transcription(
            user_id=-1,
            telegram_chat_id=self.telegram_id,
            telegram_message_id=1,
            telegram_file_id='test-file-id',
        ))
        with self.assertRaises(IntegrityError):
            await self.db.flush()
