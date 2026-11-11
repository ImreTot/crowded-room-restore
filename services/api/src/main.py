import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from secrets import compare_digest

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from . import schema
from .db.database import engine, get_db
from .models import Transcription, TranscriptionStatus, User


@asynccontextmanager
async def lifespan(app: FastAPI):
    token = os.getenv('INTERNAL_API_TOKEN', '').strip()
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '').strip()
    if not token:
        raise ValueError('INTERNAL_API_TOKEN is not set. Set it in .env before starting the API.')
    if not bootstrap_servers:
        raise ValueError('KAFKA_BOOTSTRAP_SERVERS is not set. Set it in .env before starting the API.')

    app.state.api_token = token
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=True,
        request_timeout_ms=5000,
    )
    try:
        await producer.start()
        app.state.producer = producer
        yield
    finally:
        await producer.stop()
        await engine.dispose()


async def authorize(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(HTTPBearer(auto_error=False)),
) -> None:
    if credentials is None or not compare_digest(
        credentials.credentials.encode(), request.app.state.api_token.encode(),
    ):
        raise HTTPException(
            status_code=401, detail='Invalid service token',
            headers={'WWW-Authenticate': 'Bearer'},
        )


app = FastAPI(lifespan=lifespan)
router = APIRouter(dependencies=[Depends(authorize)])


@app.get('/health')
async def health():
    return {'status': 'ok'}


@router.post('/users', response_model=schema.User)
async def upsert_user(user: schema.UserCreate, db: AsyncSession = Depends(get_db)):
    statement = insert(User).values(**user.model_dump())
    statement = statement.on_conflict_do_update(
        index_elements=[User.telegram_id],
        set_={
            'telegram_username': statement.excluded.telegram_username,
            'is_bot': statement.excluded.is_bot,
            'updated_at': int(time.time()),
        },
    ).returning(User)

    async with db.begin():
        users = await db.scalars(statement.execution_options(populate_existing=True))
        return users.one()


@router.post('/transcriptions', response_model=schema.Transcription, status_code=202)
async def create_transcription(
    task: schema.TranscriptionCreate, request: Request, db: AsyncSession = Depends(get_db),
):
    async with db.begin():
        user_id = await db.scalar(select(User.id).where(User.telegram_id == task.telegram_id))
        if user_id is None:
            raise HTTPException(status_code=404, detail='User not found')
        statement = insert(Transcription).values(
            user_id=user_id, **task.model_dump(exclude={'telegram_id'}),
        ).on_conflict_do_nothing(
            constraint='uq_transcription_telegram_message',
        ).returning(Transcription)
        transcription = (await db.scalars(statement)).one_or_none()
        if transcription is None:
            existing = await db.scalar(select(Transcription).where(
                Transcription.telegram_chat_id == task.telegram_chat_id,
                Transcription.telegram_message_id == task.telegram_message_id,
                Transcription.user_id == user_id,
            ))
            if existing is None:
                raise HTTPException(status_code=404, detail='Transcription not found')
            return existing

    task_id = transcription.id
    try:
        async with asyncio.timeout(5):
            await request.app.state.producer.send_and_wait(
                'transcriptions', str(task_id).encode(), key=str(task_id).encode(),
            )
    except (KafkaError, TimeoutError):
        logging.exception('Failed to publish transcription: task_id=%s', task_id)
        async with db.begin():
            transcription = await db.get(
                Transcription, task_id, with_for_update=True, populate_existing=True,
            )
            if transcription.status != TranscriptionStatus.QUEUED:
                return transcription
            transcription.error = 'Could not enqueue transcription'
            transcription.transition_to(TranscriptionStatus.FAILED)
        raise HTTPException(status_code=503, detail='Task queue unavailable')
    return transcription


@router.get('/transcriptions', response_model=list[schema.Transcription])
async def read_history(telegram_id: int, db: AsyncSession = Depends(get_db)):
    tasks = await db.scalars(
        select(Transcription).join(User).where(User.telegram_id == telegram_id)
        .order_by(Transcription.id.desc()).limit(20)
    )
    return tasks.all()


@router.get('/transcriptions/{task_id}', response_model=schema.Transcription)
async def read_transcription(task_id: int, telegram_id: int, db: AsyncSession = Depends(get_db)):
    task = await db.scalar(select(Transcription).join(User).where(
        Transcription.id == task_id, User.telegram_id == telegram_id,
    ))
    if task is None:
        raise HTTPException(status_code=404, detail='Transcription not found')
    return task


@router.post('/transcriptions/{task_id}/start', response_model=schema.Transcription)
async def start_transcription(task_id: int, db: AsyncSession = Depends(get_db)):
    async with db.begin():
        task = await db.get(Transcription, task_id, with_for_update=True)
        if task is None:
            raise HTTPException(status_code=404, detail='Transcription not found')
        if task.status == TranscriptionStatus.QUEUED:
            task.transition_to(TranscriptionStatus.PROCESSING)
        return task


@router.post('/transcriptions/{task_id}/finish', response_model=schema.Transcription)
async def finish_transcription(
    task_id: int, outcome: schema.TranscriptionFinish, db: AsyncSession = Depends(get_db),
):
    async with db.begin():
        task = await db.get(Transcription, task_id, with_for_update=True)
        if task is None:
            raise HTTPException(status_code=404, detail='Transcription not found')
        if task.status in (TranscriptionStatus.SUCCEEDED, TranscriptionStatus.FAILED):
            return task
        if task.status != TranscriptionStatus.PROCESSING:
            raise HTTPException(status_code=409, detail='Transcription has not started')
        task.result = outcome.result
        task.error = outcome.error
        task.transition_to(
            TranscriptionStatus.FAILED if outcome.error is not None else TranscriptionStatus.SUCCEEDED,
        )
        return task


app.include_router(router)
