from aiogram.client.session.base import BaseSession
from aiogram.methods import GetFile
from aiogram.types import File

BOT_TOKEN = '123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk'


def make_task(**fields):
    task = {
        'id': 7, 'status': 'processing',
        'telegram_file_id': 'voice-id', 'telegram_file_name': None,
    }
    task.update(fields)
    return task


class DownloadSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.requests = []
        self.file_path = 'voice/file_1.oga'
        self.audio = b'audio-bytes'
        self.download_error = None
        self.closed = False

    async def close(self):
        self.closed = True

    async def make_request(self, bot, method, timeout=None):
        if not isinstance(method, GetFile):
            raise AssertionError(f'Unexpected Telegram method: {type(method).__name__}')
        self.requests.append(method)
        return File(
            file_id=method.file_id, file_unique_id='voice-unique-id', file_path=self.file_path,
        )

    async def stream_content(self, url, **kwargs):
        if self.download_error is not None:
            raise self.download_error
        yield self.audio
