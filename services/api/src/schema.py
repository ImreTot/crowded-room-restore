from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from .models import TranscriptionStatus


class UserCreate(BaseModel):
    telegram_id: int
    telegram_username: str | None = None
    is_bot: bool = False


class User(UserCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: int
    updated_at: int


class TranscriptionCreate(BaseModel):
    telegram_id: int
    telegram_chat_id: int
    telegram_message_id: int
    telegram_file_id: str
    telegram_file_name: str | None = None


class Transcription(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: int
    telegram_chat_id: int
    telegram_message_id: int
    telegram_file_id: str
    telegram_file_name: str | None
    status: TranscriptionStatus
    result: str | None
    error: str | None
    finished_at: int | None
    created_at: int
    updated_at: int


class TranscriptionFinish(BaseModel):
    result: str | None = None
    error: str | None = None

    @model_validator(mode='after')
    def require_result_or_error(self) -> Self:
        if (self.result is None) == (self.error is None):
            raise ValueError('Provide exactly one of result or error')
        return self
