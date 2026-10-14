"""Run the SQL adapter contract on SQLite and optional live server databases."""

import asyncio
import json
import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sync_sdk import Mutation
from sync_sdk.adapters.sqlalchemy import sync_metadata
from sync_sdk.adapters.sqlalchemy_config import SQLAlchemyConfig
from sync_sdk.telemetry import Attributes, NoOpTelemetry


class MetricCapture(NoOpTelemetry):
    def __init__(self) -> None:
        self.counts: defaultdict[str, list[tuple[int, dict]]] = defaultdict(list)
        self.records: defaultdict[str, list[tuple[float, dict]]] = defaultdict(list)

    def count(
        self, name: str, value: int = 1, attributes: Attributes | None = None
    ) -> None:
        self.counts[name].append((value, dict(attributes or {})))

    def record(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        self.records[name].append((value, dict(attributes or {})))


def mutation(
    entity_id: str,
    *,
    operation: Literal["create", "update", "delete"] = "create",
    base_version: int = 0,
    title: str = "offline task",
) -> Mutation:
    return Mutation(
        mutation_id=uuid4(),
        client_id="database-adapter-test",
        entity="tasks",
        entity_id=entity_id,
        operation=operation,
        base_version=base_version,
        payload={} if operation == "delete" else {"title": title},
        created_at=datetime.now(UTC),
    )


def timing_summary(samples: list[tuple[float, dict]]) -> dict[str, float | int]:
    values = [value for value, _ in samples]
    assert values
    return {
        "count": len(values),
        "total_ms": round(sum(values), 3),
        "mean_ms": round(sum(values) / len(values), 3),
        "max_ms": round(max(values), 3),
    }


def metric_summary(telemetry: MetricCapture, backend: str) -> dict:
    transactions = telemetry.records["sync.storage.transaction.duration"]
    pulls = telemetry.records["sync.storage.pull.duration"]
    health = telemetry.records["sync.storage.health.duration"]
    assert len(transactions) == 4
    assert len(pulls) == 3
    assert len(health) == 1
    for samples in (transactions, pulls, health):
        assert all(attributes["db.system.name"] == backend for _, attributes in samples)

    statuses: defaultdict[str, int] = defaultdict(int)
    for value, attributes in telemetry.counts["sync.gateway.mutations"]:
        statuses[str(attributes["sync.mutation.status"])] += value
    assert statuses == {"applied": 4, "conflict": 1}
    return {
        "backend": backend,
        "mutation_results": dict(statuses),
        "transaction": timing_summary(transactions),
        "pull": timing_summary(pulls),
        "health": timing_summary(health),
    }


async def run_contract(database_url: str) -> dict:
    url = make_url(database_url)
    backend = url.get_backend_name()
    if backend != "sqlite":
        database = url.database or ""
        if "test" not in database.lower():
            raise ValueError(
                "Live adapter tests require a database name containing 'test'"
            )

    engine = create_async_engine(database_url, pool_pre_ping=True)
    metadata = MetaData()
    tasks = Table(
        "sync_adapter_test_tasks",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("title", String(200), nullable=False),
    )
    config = SQLAlchemyConfig()
    config.register_table("tasks", tasks, fields={"title": "title"})
    telemetry = MetricCapture()

    try:
        await config.create_tables(engine)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        gateway = config.build(sessions, telemetry=telemetry)
        capture = config.build_change_capture(sessions)

        # Existing rows form the first durable baseline and are not backfilled.
        async with sessions.begin() as session:
            await session.execute(tasks.insert().values(id=99, title="existing"))
        baseline = await capture.capture_once()
        assert baseline.scanned_rows == 1
        assert baseline.initialized_entities == 1
        assert baseline.captured_changes == 0

        create = mutation("1")
        repeated = await gateway.apply_mutations([create, create])
        assert repeated[0] == repeated[1]
        assert repeated[0].status == "applied"

        first = mutation("1", operation="update", base_version=1, title="first")
        second = mutation("1", operation="update", base_version=1, title="second")
        left, right = await asyncio.gather(
            gateway.apply_mutations([first]), gateway.apply_mutations([second])
        )
        assert sorted([left[0].status, right[0].status]) == ["applied", "conflict"]

        page = await gateway.get_changes(0, limit=1)
        assert page.cursor == 1
        assert page.has_more
        next_page = await gateway.get_changes(page.cursor, limit=10)
        assert [change.version for change in next_page.changes] == [2]

        deleted = await gateway.apply_mutations(
            [mutation("1", operation="delete", base_version=2)]
        )
        assert deleted[0].status == "applied"
        assert deleted[0].server_version == 3
        async with sessions() as session:
            assert [row.id for row in (await session.execute(select(tasks))).all()] == [
                99
            ]

        # Gateway writes update the snapshot transactionally and are not echoed.
        assert (await capture.capture_once()).captured_changes == 0

        async with sessions.begin() as session:
            await session.execute(tasks.insert().values(id=2, title="direct create"))
        assert (await capture.capture_once()).captured_changes == 1
        async with sessions.begin() as session:
            await session.execute(
                tasks.update().where(tasks.c.id == 2).values(title="direct update")
            )
        assert (await capture.capture_once()).captured_changes == 1
        async with sessions.begin() as session:
            await session.execute(tasks.delete().where(tasks.c.id == 2))
        assert (await capture.capture_once()).captured_changes == 1

        captured = await gateway.get_changes(3, limit=10)
        assert [change.operation for change in captured.changes] == [
            "create",
            "update",
            "delete",
        ]
        assert [change.version for change in captured.changes] == [1, 2, 3]
        assert await gateway.health_check()
        return metric_summary(telemetry, backend)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
            await connection.run_sync(sync_metadata.drop_all)
        await engine.dispose()


async def test_sql_database_adapters(tmp_path):
    urls = [f"sqlite+aiosqlite:///{tmp_path / 'adapter.db'}"]
    urls.extend(
        value
        for name in ("SYNC_TEST_POSTGRES_URL", "SYNC_TEST_MYSQL_URL")
        if (value := os.getenv(name))
    )
    report = {"databases": [await run_contract(url) for url in urls]}
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if path := os.getenv("SYNC_TEST_METRICS_PATH"):
        Path(path).write_text(rendered + "\n")
