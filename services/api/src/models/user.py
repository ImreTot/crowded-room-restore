from sqlalchemy import String, BigInteger, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from .base_models import Base


class User(Base):
    __tablename__ = 'user'

    telegram_id: Mapped[int] = mapped_column(
        BigInteger,
        unique=True,
        nullable=False,
        index=True
    )
    telegram_username: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True
    )
    is_bot: Mapped[bool] = mapped_column(
        Boolean,
        default=False
    )
