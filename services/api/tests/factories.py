import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from src.models import Transcription, User


@asynccontextmanager
async def rollback_connection():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                yield connection
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


def session_for(connection):
    return AsyncSession(
        bind=connection, expire_on_commit=False, join_transaction_mode='create_savepoint',
    )


async def make_user(connection, telegram_id=4000000000001):
    async with session_for(connection) as session:
        async with session.begin():
            user = User(telegram_id=telegram_id, telegram_username=None, is_bot=False)
            session.add(user)
            await session.flush()
            return user


def task_payload(user, **fields):
    payload = {
        'telegram_id': user.telegram_id,
        'telegram_chat_id': user.telegram_id,
        'telegram_message_id': 1,
        'telegram_file_id': 'test-file',
        'telegram_file_name': 'recording.mp3',
    }
    payload.update(fields)
    return payload


async def make_task(connection, user, **fields):
    values = task_payload(user, **fields)
    values.pop('telegram_id')
    async with session_for(connection) as session:
        async with session.begin():
            task = Transcription(user_id=user.id, **values)
            session.add(task)
            await session.flush()
            return task
