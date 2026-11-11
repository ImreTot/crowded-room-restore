import logging
from typing import BinaryIO

from aiohttp import ClientSession, FormData


async def transcribe_audio(client: ClientSession, audio: BinaryIO, filename: str) -> str:
    data = FormData()
    data.add_field('model', 'gpt-transcribe')
    data.add_field('file', audio, filename=filename)

    async with client.post('audio/transcriptions', data=data) as response:
        if response.status == 429:
            error = (await response.json())['error']
            logging.error(
                'OpenAI transcription rejected: status=429 code=%s type=%s request_id=%s',
                error.get('code'),
                error.get('type'),
                response.headers.get('x-request-id'),
            )
        response.raise_for_status()
        result = await response.json()
    return result['text'].strip()
