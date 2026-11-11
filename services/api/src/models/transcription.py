import time
from enum import StrEnum

from sqlalchemy import BigInteger, Enum, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base_models import Base


class TranscriptionStatus(StrEnum):
    QUEUED = 'queued'
    PROCESSING = 'processing'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class Transcription(Base):
    __tablename__ = 'transcription'
    __table_args__ = (
        UniqueConstraint(
            'telegram_chat_id',
            'telegram_message_id',
            name='uq_transcription_telegram_message',
        ),
    )

    user_id: Mapped[int] = mapped_column(ForeignKey('user.id'), index=True)
    telegram_chat_id: Mapped[int] = mapped_column(BigInteger)
    telegram_message_id: Mapped[int]
    telegram_file_id: Mapped[str] = mapped_column(Text)
    telegram_file_name: Mapped[str | None] = mapped_column(Text)
    status: Mapped[TranscriptionStatus] = mapped_column(
        Enum(
            TranscriptionStatus,
            values_callable=lambda statuses: [status.value for status in statuses],
            native_enum=False,
            create_constraint=True,
            name='transcription_status',
        ),
        default=TranscriptionStatus.QUEUED,
        server_default=TranscriptionStatus.QUEUED.value,
    )
    result: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    finished_at: Mapped[int | None]

    def transition_to(self, status: TranscriptionStatus) -> None:
        allowed = {
            TranscriptionStatus.QUEUED: (
                TranscriptionStatus.PROCESSING,
                TranscriptionStatus.FAILED,
            ),
            TranscriptionStatus.PROCESSING: (
                TranscriptionStatus.SUCCEEDED,
                TranscriptionStatus.FAILED,
            ),
        }
        if status not in allowed.get(self.status, ()):
            raise ValueError(f'Invalid transcription status transition: {self.status} -> {status}')

        self.status = status
        if status in (TranscriptionStatus.SUCCEEDED, TranscriptionStatus.FAILED):
            self.finished_at = int(time.time())
