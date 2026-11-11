"""Add transcription tasks.

Revision ID: c4e78a91d203
Revises: b58d27d2b31a
"""

from alembic import op
import sqlalchemy as sa

revision = 'c4e78a91d203'
down_revision = 'b58d27d2b31a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'transcription',
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('telegram_chat_id', sa.BigInteger(), nullable=False),
        sa.Column('telegram_message_id', sa.Integer(), nullable=False),
        sa.Column('telegram_file_id', sa.Text(), nullable=False),
        sa.Column(
            'status',
            sa.Enum(
                'queued', 'processing', 'succeeded', 'failed',
                native_enum=False,
                create_constraint=True,
                name='transcription_status',
            ),
            server_default='queued',
            nullable=False,
        ),
        sa.Column('result', sa.Text(), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('finished_at', sa.Integer(), nullable=True),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.Integer(), nullable=False),
        sa.Column('updated_at', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['user.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'telegram_chat_id', 'telegram_message_id',
            name='uq_transcription_telegram_message',
        ),
    )
    op.create_index('ix_transcription_user_id', 'transcription', ['user_id'])


def downgrade() -> None:
    op.drop_index('ix_transcription_user_id', table_name='transcription')
    op.drop_table('transcription')
