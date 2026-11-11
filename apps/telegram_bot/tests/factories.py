from aiogram.client.session.base import BaseSession
from aiogram.methods import EditMessageText, SendDocument, SendMessage
from aiogram.types import Message

BOT_TOKEN = '123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk'


def make_message(bot, **fields):
    data = {
        'message_id': 5,
        'date': 1700000000,
        'chat': {'id': 123, 'type': 'private'},
        'from_user': {'id': 123, 'first_name': 'Test', 'is_bot': False},
    }
    data.update(fields)
    return Message.model_validate(data, context={'bot': bot})


class RecordingTelegramSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.requests = []

    async def close(self):
        pass

    async def make_request(self, bot, method, timeout=None):
        if not isinstance(method, (SendMessage, EditMessageText, SendDocument)):
            raise AssertionError(f'Unexpected Telegram method: {type(method).__name__}')
        self.requests.append(method)
        return make_message(
            bot, message_id=100, chat={'id': method.chat_id, 'type': 'private'},
            text=getattr(method, 'text', None),
        )

    async def stream_content(self, url, **kwargs):
        yield b'test-audio'
