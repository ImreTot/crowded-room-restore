import logging
from asyncio import sleep

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command
from aiohttp import ClientError, ClientResponseError, ClientSession, ClientTimeout
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
if not TOKEN:
    raise ValueError('TELEGRAM_BOT_TOKEN is not set. Set it in .env before starting the bot.')

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()


@router.message.outer_middleware()
async def register_user(handler, message: types.Message, data: dict):
    user = message.from_user
    if user is not None:
        try:
            async with data['api_client'].post('/users', json={
                'telegram_id': user.id,
                'telegram_username': user.username,
                'is_bot': user.is_bot,
            }) as response:
                response.raise_for_status()
        except (ClientError, TimeoutError):
            logging.exception('Failed to register Telegram user')
            await message.reply('Не удалось сохранить данные пользователя. Попробуйте ещё раз позже.')
            return
    return await handler(message, data)


@router.message(Command('start'))
async def start_handler(message: types.Message) -> None:
    await message.reply(
        'Привет! 👋\n\n'
        'Отправьте мне голосовое сообщение или аудиофайл, '
        'и я расшифрую его в текст с помощью OpenAI! 🎤➡️📝'
    )


@router.message(F.voice | F.audio)
async def handle_audio(message: types.Message, api_client: ClientSession) -> None:
    media = message.voice or message.audio
    processing_msg = await message.reply('🎧 Аудио получено. Ожидаю расшифровку...')

    try:
        async with api_client.post('/transcriptions', json={
            'telegram_id': message.from_user.id,
            'telegram_chat_id': message.chat.id,
            'telegram_message_id': message.message_id,
            'telegram_file_id': media.file_id,
            'telegram_file_name': message.audio.file_name if message.audio else None,
        }) as response:
            response.raise_for_status()
            task = await response.json()

        poll_failures = 0
        while task['status'] in ('queued', 'processing'):
            await sleep(2)
            try:
                async with api_client.get(
                    f"/transcriptions/{task['id']}",
                    params={'telegram_id': message.from_user.id},
                ) as response:
                    response.raise_for_status()
                    task = await response.json()
            except (ClientError, TimeoutError) as error:
                if isinstance(error, ClientResponseError) and error.status < 500 and error.status != 429:
                    raise
                poll_failures += 1
                if poll_failures >= 3:
                    raise
            else:
                poll_failures = 0

        if task['status'] == 'failed':
            await processing_msg.edit_text('❌ Не удалось расшифровать аудио. Попробуйте ещё раз позже.')
            return
        transcription = task['result']
        if not transcription:
            await processing_msg.edit_text('Не удалось распознать речь в этом аудио.')
        elif len(transcription) > 4096:
            await message.reply_document(types.BufferedInputFile(
                transcription.encode('utf-8'),
                filename='transcription.txt',
            ))
            await processing_msg.edit_text('✅ Готово. Расшифровка отправлена файлом.')
        else:
            await processing_msg.edit_text(transcription)
    except Exception:
        logging.exception('Failed to receive transcription')
        await processing_msg.edit_text('❌ Не удалось получить расшифровку. Попробуйте ещё раз позже.')


@router.message()
async def handle_other_messages(message: types.Message) -> None:
    await message.reply(
        '🤖 Я умею расшифровывать только аудио!\n\n'
        'Отправьте мне:\n'
        '🎤 Голосовое сообщение\n'
        '🎵 Аудиофайл\n\n'
        'И я переведу речь в текст!'
    )


async def main() -> None:
    api_token = os.getenv('INTERNAL_API_TOKEN', '').strip()
    if not api_token:
        raise ValueError('INTERNAL_API_TOKEN is not set. Set it in .env before starting the bot.')

    dp.include_router(router)

    logging.info('Audio transcription bot started')
    await bot.delete_webhook(drop_pending_updates=True)
    async with ClientSession(
        base_url='http://api:8000',
        headers={'Authorization': f'Bearer {api_token}'},
        timeout=ClientTimeout(total=10),
    ) as api_client:
        await dp.start_polling(bot, api_client=api_client)
