"""Preserve the original Telegram audio filename.

Revision ID: d92f10b6a784
Revises: c4e78a91d203
"""

from alembic import op
import sqlalchemy as sa

revision = 'd92f10b6a784'
down_revision = 'c4e78a91d203'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('transcription', sa.Column('telegram_file_name', sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column('transcription', 'telegram_file_name')
