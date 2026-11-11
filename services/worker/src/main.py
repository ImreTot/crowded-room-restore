import asyncio
import logging
import os
from io import BytesIO
from pathlib import Path

from aiogram import Bot
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiohttp import ClientResponseError, ClientSession, ClientTimeout
from dotenv import load_dotenv

from .transcription import transcribe_audio


async def process_task(
    task_id: int, api_client: ClientSession, bot: Bot, transcription_client: ClientSession,
) -> None:
    async with api_client.post(f'/transcriptions/{task_id}/start') as response:
        response.raise_for_status()
        task = await response.json()
    if task['status'] in ('succeeded', 'failed'):
        logging.info('Skipping completed transcription: task_id=%s', task_id)
        return

    logging.info('Processing transcription: task_id=%s', task_id)
    stage = 'download'
    try:
        file_info = await bot.get_file(task['telegram_file_id'])
        filename = Path(task['telegram_file_name'] or file_info.file_path).name
        if Path(filename).suffix.lower() == '.oga':
            filename = str(Path(filename).with_suffix('.ogg'))
        with BytesIO() as audio:
            await bot.download_file(file_info.file_path, destination=audio)
            stage = 'transcription'
            text = await transcribe_audio(transcription_client, audio, filename)
    except Exception as error:
        logging.error(
            'Transcription failed: task_id=%s error_type=%s', task_id, type(error).__name__,
        )
        reason = f'{stage} failed: {type(error).__name__}'
        if isinstance(error, ClientResponseError):
            reason += f' (HTTP {error.status})'
        outcome = {'error': reason}
    else:
        outcome = {'result': text}

    async with api_client.post(f'/transcriptions/{task_id}/finish', json=outcome) as response:
        response.raise_for_status()
    logging.info('Transcription outcome saved: task_id=%s', task_id)


async def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    settings = {}
    for name in ('TELEGRAM_BOT_TOKEN', 'OPENAI_API_KEY', 'INTERNAL_API_TOKEN', 'KAFKA_BOOTSTRAP_SERVERS'):
        value = os.getenv(name, '').strip()
        if not value:
            raise ValueError(f'{name} is not set. Set it in .env before starting the worker.')
        settings[name] = value

    bot = Bot(token=settings['TELEGRAM_BOT_TOKEN'])
    consumer = AIOKafkaConsumer(
        'transcriptions',
        bootstrap_servers=settings['KAFKA_BOOTSTRAP_SERVERS'],
        group_id='transcription-workers',
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    async with (
        ClientSession(
            base_url='http://api:8000',
            headers={'Authorization': f"Bearer {settings['INTERNAL_API_TOKEN']}"},
            timeout=ClientTimeout(total=10),
        ) as api_client,
        ClientSession(
            base_url='https://api.openai.com/v1/',
            headers={'Authorization': f"Bearer {settings['OPENAI_API_KEY']}"},
            timeout=ClientTimeout(total=180),
        ) as transcription_client,
    ):
        try:
            await consumer.start()
            async for message in consumer:
                await process_task(int(message.value), api_client, bot, transcription_client)
                await consumer.commit({
                    TopicPartition(message.topic, message.partition): message.offset + 1,
                })
        finally:
            await consumer.stop()
            await bot.session.close()


if __name__ == '__main__':
    asyncio.run(main())
