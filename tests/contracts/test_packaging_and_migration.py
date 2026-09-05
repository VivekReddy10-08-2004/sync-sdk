import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from uuid import uuid4

from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

from sync_sdk.adapters.sqlalchemy import sync_metadata


def test_core_import_does_not_load_backend_or_web_packages():
    script = """
import sys
class BlockAdapters:
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'sqlalchemy', 'fastapi', 'httpx', 'app', 'asyncpg', 'pydantic_settings'}:
            raise AssertionError('Unexpected core dependency: ' + fullname)
sys.meta_path.insert(0, BlockAdapters())
import sync_sdk
from sync_sdk.adapters.direct import DirectTransport
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=10)


def test_metadata_compiles_for_sql_dialects():
    # Compile only: this is not a claim of live MySQL/PostgreSQL certification.
    for dialect in (mysql.dialect(), postgresql.dialect(), sqlite.dialect()):
        for table in sync_metadata.sorted_tables:
            assert str(CreateTable(table).compile(dialect=dialect))


async def test_legacy_migration_preserves_cursor_results_and_latest_version(tmp_path):
    path = tmp_path / "legacy.db"
    env = {**os.environ, "SYNC_DATABASE_URL": f"sqlite+aiosqlite:///{path}"}

    async def migrate(revision):
        await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "alembic", "upgrade", revision],
            env=env,
            check=True,
            capture_output=True,
            timeout=30,
        )

    await migrate("94126bd02e32")
    mutation_id = str(uuid4())
    result = {
        "mutation_id": mutation_id,
        "status": "applied",
        "server_version": 1,
        "message": None,
    }
    with sqlite3.connect(path) as db:
        # Old behavior could reset versions on recreation. Preserve latest, not max.
        db.execute(
            "INSERT INTO change_log VALUES (10, 'task', '1', 'delete', '{}', 5, '2026-01-01')"
        )
        db.execute(
            "INSERT INTO change_log VALUES (11, 'task', '1', 'create', '{}', 1, '2026-01-02')"
        )
        db.execute(
            "INSERT INTO change_log VALUES (12, 'task', 'deleted', 'delete', '{}', 3, '2026-01-02')"
        )
        db.execute("INSERT INTO records VALUES ('task', '1', '{}', 1, '2026-01-02')")
        db.execute(
            "INSERT INTO processed_mutations VALUES (?, 'client', ?, '2026-01-02')",
            (mutation_id, json.dumps(result)),
        )
    await migrate("head")
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT sequence FROM sync_clock").fetchone()[0] == 12
        assert (
            db.execute(
                "SELECT version FROM sync_versions WHERE entity_id='1'"
            ).fetchone()[0]
            == 1
        )
        assert (
            db.execute(
                "SELECT version FROM sync_versions WHERE entity_id='deleted'"
            ).fetchone()[0]
            == 3
        )
        assert db.execute("SELECT COUNT(*) FROM sync_change_log").fetchone()[0] == 3
        assert (
            json.loads(
                db.execute("SELECT result FROM sync_processed_mutations").fetchone()[0]
            )
            == result
        )

    # The migrated host can replay an old acknowledgement and continue its feed.
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.adapters.postgres import PostgresRemoteStore
    from sync_sdk import Mutation

    engine = create_async_engine(env["SYNC_DATABASE_URL"])
    try:
        gateway = PostgresRemoteStore(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        replay = Mutation(
            mutation_id=mutation_id,
            client_id="client",
            entity="task",
            entity_id="1",
            operation="create",
            payload={},
            base_version=0,
            created_at=datetime.now(UTC),
        )
        assert (await gateway.apply_mutations([replay]))[0].model_dump(
            mode="json"
        ) == result
        update = replay.model_copy(
            update={"mutation_id": uuid4(), "operation": "update", "base_version": 1}
        )
        assert (await gateway.apply_mutations([update]))[0].server_version == 2
        page = await gateway.get_changes(12)
        assert page.cursor == 13
        assert len(page.changes) == 1
    finally:
        await engine.dispose()
