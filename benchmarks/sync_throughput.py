"""Reproducible gateway/storage throughput benchmark with correctness auditing."""

import argparse
import asyncio
import json
import os
import platform
import statistics
import tempfile
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import Column, Integer, MetaData, String, Table, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sync_sdk import Mutation, SyncGateway
from sync_sdk.adapters.sqlalchemy import sync_metadata
from sync_sdk.adapters.sqlalchemy_config import SQLAlchemyConfig


def percentile(values: list[float], percentage: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentage + 0.999) - 1))
    return ordered[index]


def mutations(start: int, count: int) -> list[Mutation]:
    created_at = datetime.now(UTC)
    return [
        Mutation(
            mutation_id=uuid4(),
            client_id="throughput-benchmark",
            entity="benchmark_items",
            entity_id=str(item),
            operation="create",
            payload={"value": f"value-{item}"},
            base_version=0,
            created_at=created_at,
        )
        for item in range(start, start + count)
    ]


async def apply_batches(
    gateway: SyncGateway,
    items: list[Mutation],
    *,
    batch_size: int,
    concurrency: int,
) -> tuple[float, list[float], int, int, dict[str, int]]:
    batches = [
        items[index : index + batch_size] for index in range(0, len(items), batch_size)
    ]
    queue: asyncio.Queue[list[Mutation]] = asyncio.Queue()
    for batch in batches:
        queue.put_nowait(batch)
    latencies: list[float] = []
    applied_count = 0
    failed_count = 0
    errors: dict[str, int] = {}

    async def worker() -> None:
        nonlocal applied_count, failed_count
        while True:
            try:
                batch = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            started = time.perf_counter()
            try:
                results = await gateway.apply_mutations(batch)
                if len(results) != len(batch) or any(
                    result.status != "applied" for result in results
                ):
                    raise RuntimeError("Benchmark batch was not fully applied")
                applied_count += len(batch)
            except Exception as error:  # noqa: BLE001 - failures are benchmark data
                failed_count += len(batch)
                name = type(error).__name__
                errors[name] = errors.get(name, 0) + 1
            finally:
                latencies.append((time.perf_counter() - started) * 1000)

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return time.perf_counter() - started, latencies, applied_count, failed_count, errors


async def read_feed(gateway: SyncGateway, cursor: int) -> tuple[int, list[int]]:
    sequences: list[int] = []
    while True:
        page = await gateway.get_changes(cursor, limit=1000)
        sequences.extend(change.sequence for change in page.changes)
        cursor = page.cursor
        if not page.has_more:
            return cursor, sequences


async def benchmark(
    gateway: SyncGateway,
    count_rows: Callable[[], Awaitable[int]],
    *,
    backend: str,
    mutation_count: int,
    warmup_count: int,
    batch_size: int,
    concurrency: int,
) -> dict[str, Any]:
    if warmup_count:
        _, _, warmup_applied, _, warmup_errors = await apply_batches(
            gateway,
            mutations(1, warmup_count),
            batch_size=batch_size,
            concurrency=concurrency,
        )
    else:
        warmup_applied, warmup_errors = 0, {}
    cursor, _ = await read_feed(gateway, 0)

    elapsed, latencies, applied_count, failed_count, errors = await apply_batches(
        gateway,
        mutations(warmup_count + 1, mutation_count),
        batch_size=batch_size,
        concurrency=concurrency,
    )
    final_cursor, sequences = await read_feed(gateway, cursor)
    expected_sequences = list(range(cursor + 1, cursor + applied_count + 1))
    row_count = await count_rows()
    expected_rows = warmup_applied + applied_count
    audit = {
        "applied_mutations": len(sequences),
        "business_rows": row_count,
        "feed_is_contiguous": sequences == expected_sequences,
        "final_cursor": final_cursor,
        "passed": len(sequences) == applied_count
        and row_count == expected_rows
        and sequences == expected_sequences,
    }
    if not audit["passed"]:
        raise RuntimeError(f"Post-run correctness audit failed: {audit}")

    return {
        "schema_version": 1,
        "measured_at": datetime.now(UTC).isoformat(),
        "scope": "in-process gateway and storage adapter; excludes HTTP and client networking",
        "environment": {
            "backend": backend,
            "python": platform.python_version(),
            "system": platform.system(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
        },
        "workload": {
            "operation": "independent creates",
            "mutations": mutation_count,
            "warmup_mutations": warmup_count,
            "batch_size": batch_size,
            "concurrency": concurrency,
            "batches": len(latencies),
        },
        "results": {
            "benchmark_passed": audit["passed"] and not errors and not warmup_errors,
            "elapsed_seconds": round(elapsed, 6),
            "attempted_mutations_per_second": round(mutation_count / elapsed, 2),
            "successful_mutations_per_second": round(applied_count / elapsed, 2),
            "applied_mutations": applied_count,
            "failed_mutations": failed_count,
            "batch_errors": errors,
            "warmup_batch_errors": warmup_errors,
            "batch_latency_ms": {
                "mean": round(statistics.mean(latencies), 3),
                "p50": round(percentile(latencies, 0.50), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "p99": round(percentile(latencies, 0.99), 3),
                "max": round(max(latencies), 3),
            },
        },
        "correctness_audit": audit,
    }


async def run_sql(args: argparse.Namespace) -> dict[str, Any]:
    temporary_path: str | None = None
    database_url = args.database_url
    if database_url is None:
        descriptor, temporary_path = tempfile.mkstemp(
            prefix="sync-sdk-benchmark-", suffix=".db"
        )
        os.close(descriptor)
        database_url = f"sqlite+aiosqlite:///{temporary_path}"
    url = make_url(database_url)
    if (
        url.get_backend_name() != "sqlite"
        and "test" not in (url.database or "").lower()
    ):
        raise ValueError("Benchmark database names must contain 'test'")

    engine = create_async_engine(database_url, pool_pre_ping=True)
    metadata = MetaData()
    table = Table(
        "sync_benchmark_items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", String(200), nullable=False),
    )
    config = SQLAlchemyConfig()
    config.register_table("benchmark_items", table, fields={"value": "value"})
    try:
        await config.create_tables(engine)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        gateway = config.build(sessions)

        async def count_rows() -> int:
            async with sessions() as session:
                return int(
                    await session.scalar(select(func.count()).select_from(table)) or 0
                )

        return await benchmark(
            gateway,
            count_rows,
            backend=url.get_backend_name(),
            mutation_count=args.mutations,
            warmup_count=args.warmup,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
        )
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
            await connection.run_sync(sync_metadata.drop_all)
        await engine.dispose()
        if temporary_path is not None:
            Path(temporary_path).unlink(missing_ok=True)


async def run_mongodb(args: argparse.Namespace) -> dict[str, Any]:
    from pymongo import AsyncMongoClient

    from sync_sdk.adapters.mongodb import MongoDBConfig

    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(args.mongodb_url)
    database = client.get_default_database()
    if "test" not in database.name.lower():
        raise ValueError("Benchmark database names must contain 'test'")

    config = MongoDBConfig(metadata_prefix="sync_benchmark_")
    config.register_collection(
        "benchmark_items",
        "sync_benchmark_items",
        fields={"value": "value"},
    )
    try:
        await client.drop_database(database.name)
        await config.create_collections(database)
        gateway = config.build(database)

        async def count_rows() -> int:
            return await database["sync_benchmark_items"].count_documents({})

        return await benchmark(
            gateway,
            count_rows,
            backend="mongodb",
            mutation_count=args.mutations,
            warmup_count=args.warmup,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
        )
    finally:
        await client.drop_database(database.name)
        await client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--database-url",
        help="SQLAlchemy URL for an empty test database; defaults to temporary SQLite",
    )
    target.add_argument(
        "--mongodb-url",
        help="MongoDB URL containing an empty test database",
    )
    parser.add_argument("--mutations", type=int, default=5000)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit unsuccessfully when any measured batch fails",
    )
    args = parser.parse_args()
    if args.mutations < 1 or args.warmup < 0:
        parser.error("mutation counts must be positive")
    if not 1 <= args.batch_size <= 1000 or args.concurrency < 1:
        parser.error("batch size must be 1..1000 and concurrency must be positive")
    return args


async def main() -> None:
    args = parse_args()
    report = await run_mongodb(args) if args.mongodb_url else await run_sql(args)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.output is not None:
        args.output.write_text(rendered)
    if args.fail_on_errors and not report["results"]["benchmark_passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
