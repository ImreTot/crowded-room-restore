import unittest

from src import schema
from src.models import User


class UserSchemaTests(unittest.TestCase):
    def test_user_without_telegram_username(self):
        user = schema.UserCreate(telegram_id=123)
        self.assertIsNone(user.telegram_username)
        self.assertFalse(user.is_bot)

    def test_response_can_be_created_from_orm_user(self):
        user = User(
            id=1,
            telegram_id=123,
            telegram_username=None,
            is_bot=False,
            created_at=1000,
            updated_at=2000,
        )
        response = schema.User.model_validate(user)
        self.assertEqual(response.id, 1)
        self.assertEqual(response.telegram_id, 123)
        self.assertIsNone(response.telegram_username)
        self.assertEqual(response.created_at, 1000)
        self.assertEqual(response.updated_at, 2000)
