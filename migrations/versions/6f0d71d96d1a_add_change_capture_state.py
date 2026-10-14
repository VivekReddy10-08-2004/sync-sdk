"""Add durable state for polling database change capture.

Revision ID: 6f0d71d96d1a
Revises: c1a820d431be
"""

import sqlalchemy as sa
from alembic import op

revision = "6f0d71d96d1a"
down_revision = "c1a820d431be"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "sync_capture_checkpoints",
        sa.Column("entity", sa.String(191), primary_key=True),
        sa.Column("scanned_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "sync_captured_rows",
        sa.Column("entity", sa.String(191), primary_key=True),
        sa.Column("entity_id", sa.String(191), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )


def downgrade():
    op.drop_table("sync_captured_rows")
    op.drop_table("sync_capture_checkpoints")
