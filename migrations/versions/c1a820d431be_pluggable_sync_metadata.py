"""Separate SDK metadata; preserve legacy results, feed cursors, and versions.

Revision ID: c1a820d431be
Revises: 94126bd02e32
"""

import sqlalchemy as sa
from alembic import op

revision = "c1a820d431be"
down_revision = "94126bd02e32"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "sync_clock",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("sequence", sa.Integer(), nullable=False),
    )
    op.create_table(
        "sync_versions",
        sa.Column("entity", sa.String(191), primary_key=True),
        sa.Column("entity_id", sa.String(191), primary_key=True),
        sa.Column("version", sa.Integer(), nullable=False),
    )
    op.create_table(
        "sync_processed_mutations",
        sa.Column("mutation_id", sa.String(36), primary_key=True),
        sa.Column("client_id", sa.String(191), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "sync_change_log",
        sa.Column("sequence", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("entity", sa.String(191), nullable=False),
        sa.Column("entity_id", sa.String(191), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.execute(
        "INSERT INTO sync_change_log SELECT sequence, entity, entity_id, operation, payload, version, created_at FROM change_log"
    )
    op.execute(
        "INSERT INTO sync_processed_mutations SELECT mutation_id, client_id, result, processed_at FROM processed_mutations"
    )
    op.execute(
        "INSERT INTO sync_clock (id, sequence) SELECT 1, COALESCE(MAX(sequence), 0) FROM change_log"
    )
    op.execute(
        "INSERT INTO sync_versions (entity, entity_id, version) "
        "SELECT c.entity, c.entity_id, c.version FROM change_log c "
        "JOIN (SELECT entity, entity_id, MAX(sequence) AS last_sequence "
        "FROM change_log GROUP BY entity, entity_id) latest "
        "ON c.entity = latest.entity AND c.entity_id = latest.entity_id "
        "AND c.sequence = latest.last_sequence"
    )
    op.execute(
        "INSERT INTO sync_versions (entity, entity_id, version) "
        "SELECT r.entity, r.entity_id, r.version FROM records r "
        "WHERE NOT EXISTS (SELECT 1 FROM sync_versions v "
        "WHERE v.entity = r.entity AND v.entity_id = r.entity_id)"
    )


def downgrade():
    # Copy the authoritative feed/results back before restoring the legacy app.
    op.execute("DELETE FROM change_log")
    op.execute(
        "INSERT INTO change_log SELECT sequence, entity, entity_id, operation, payload, version, created_at FROM sync_change_log"
    )
    op.execute("DELETE FROM processed_mutations")
    op.execute(
        "INSERT INTO processed_mutations SELECT mutation_id, client_id, result, processed_at FROM sync_processed_mutations"
    )
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            "SELECT setval(pg_get_serial_sequence('change_log', 'sequence'), "
            "COALESCE((SELECT MAX(sequence) FROM change_log), 1), "
            "EXISTS(SELECT 1 FROM change_log))"
        )
    op.drop_table("sync_change_log")
    op.drop_table("sync_processed_mutations")
    op.drop_table("sync_versions")
    op.drop_table("sync_clock")
