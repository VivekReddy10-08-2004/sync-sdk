from datetime import UTC, datetime

import pytest
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    event,
    inspect,
    select,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sync_sdk import MutationRejected, SQLiteStore, SyncClient
from sync_sdk.adapters.direct import DirectTransport
from sync_sdk.adapters.sqlalchemy_config import SQLAlchemyConfig


@pytest.fixture
async def configured(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'configured.db'}")

    @event.listens_for(engine.sync_engine, "connect")
    def foreign_keys(connection, _):
        connection.execute("PRAGMA foreign_keys=ON")

    metadata = MetaData()
    parents = Table(
        "customer_accounts",
        metadata,
        Column("tenant", String(50), primary_key=True),
        Column("number", Integer, primary_key=True),
        Column("name", String(100), nullable=False),
        Column("private_note", String(100), server_default="private"),
    )
    children = Table(
        "account_events",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("tenant", String(50), nullable=False),
        Column("number", Integer, nullable=False),
        Column("occurred_at", DateTime(timezone=True), nullable=False),
        Column("details", JSON, nullable=False),
        Column("priority", Integer, default=3, nullable=False),
        ForeignKeyConstraint(
            ["tenant", "number"],
            ["customer_accounts.tenant", "customer_accounts.number"],
        ),
    )
    unrelated = Table(
        "not_registered", metadata, Column("id", Integer, primary_key=True)
    )
    config = SQLAlchemyConfig()
    # Reverse registration order exercises foreign-key creation ordering.
    child_binding = config.register_table("events", children)
    parent_binding = config.register_table(
        "accounts", parents, fields={"label": "name"}
    )
    async with engine.connect() as connection:
        assert await connection.run_sync(lambda c: inspect(c).get_table_names()) == []
    await config.create_tables(engine)
    await config.create_tables(engine)  # Explicit setup is safe to repeat serially.
    async with engine.connect() as connection:
        names = await connection.run_sync(lambda c: inspect(c).get_table_names())
        assert parents.name in names and children.name in names
        assert unrelated.name not in names
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield config, sessions, parents, children, parent_binding, child_binding
    finally:
        await engine.dispose()


async def test_declared_related_tables_sync_with_composite_keys(configured, tmp_path):
    config, sessions, parents, children, parent, child = configured
    key = parent.encode_key({"tenant": "a", "number": 10})
    assert key == parent.encode_key({"number": 10, "tenant": "a"})
    with SQLiteStore(tmp_path / "local.db", client_id="device") as local:
        sdk = SyncClient(local, transport=DirectTransport(config.build(sessions)))
        sdk.enqueue(
            entity="accounts",
            entity_id=key,
            operation="create",
            payload={"label": "Alice"},
            base_version=0,
        )
        sdk.enqueue(
            entity="events",
            entity_id=child.encode_key(1),
            operation="create",
            payload={
                "tenant": "a",
                "number": 10,
                "occurred_at": "2026-09-04T12:00:00Z",
                "details": [1, {"value": True}],
            },
            base_version=0,
        )
        report = await sdk.sync_once()
        assert (report.pushed, report.pulled) == (2, 2)
        assert local.get_record("events", "1")["payload"]["priority"] == 3
        async with sessions() as session:
            account = (await session.execute(select(parents))).mappings().one()
            assert account["private_note"] == "private"
            row = (await session.execute(select(children))).mappings().one()
            assert row["occurred_at"].replace(tzinfo=UTC) == datetime(
                2026, 9, 4, 12, tzinfo=UTC
            )
            assert row["details"] == [1, {"value": True}]
        sdk.enqueue(
            entity="accounts",
            entity_id=key,
            operation="update",
            payload={"label": "Updated"},
            base_version=1,
        )
        await sdk.sync_once()
        assert local.get_record("accounts", key)["version"] == 2
        # Applications explicitly enqueue related deletes in dependency order.
        sdk.enqueue(entity="events", entity_id="1", operation="delete", base_version=1)
        sdk.enqueue(
            entity="accounts", entity_id=key, operation="delete", base_version=2
        )
        await sdk.sync_once()
        assert local.get_record("accounts", key) is None
        async with sessions() as session:
            assert not (await session.execute(select(parents))).all()
            assert not (await session.execute(select(children))).all()


async def test_generated_validation_rejects_extra_and_bad_types(configured, tmp_path):
    config, sessions, _, _, parent, _ = configured
    with SQLiteStore(tmp_path / "local.db", client_id="device") as local:
        sdk = SyncClient(local, transport=DirectTransport(config.build(sessions)))
        sdk.enqueue(
            entity="accounts",
            entity_id=parent.encode_key({"tenant": "a", "number": 1}),
            operation="create",
            payload={"label": "A", "private_note": "override"},
        )
        sdk.enqueue(
            entity="events",
            entity_id="1",
            operation="create",
            payload={
                "tenant": "a",
                "number": "not-int",
                "occurred_at": "bad-date",
                "details": {},
            },
        )
        await sdk.sync_once()
        assert [r.status for r in local.results()] == ["rejected", "rejected"]
        assert local.cursor == 0
    with pytest.raises(MutationRejected):
        parent.encode_key({"tenant": "a", "number": "01"})


async def test_key_only_association_table(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'association.db'}")
    metadata = MetaData()
    association = Table(
        "membership",
        metadata,
        Column("user_id", Integer, primary_key=True),
        Column("group_id", Integer, primary_key=True),
    )
    config = SQLAlchemyConfig()
    binding = config.register_table("membership", association)
    try:
        await config.create_tables(engine)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        with SQLiteStore(tmp_path / "local.db", client_id="device") as local:
            sdk = SyncClient(local, transport=DirectTransport(config.build(sessions)))
            for operation, version in [("create", 0), ("update", 1), ("delete", 2)]:
                sdk.enqueue(
                    entity="membership",
                    entity_id=binding.encode_key({"group_id": 2, "user_id": 1}),
                    operation=operation,
                    payload={},
                    base_version=version,
                )
            await sdk.sync_once()
            assert [r.server_version for r in local.results()] == [1, 2, 3]
    finally:
        await engine.dispose()


def test_duplicate_registration_is_rejected():
    table = Table("items", MetaData(), Column("id", Integer, primary_key=True))
    config = SQLAlchemyConfig()
    config.register_table("items", table)
    with pytest.raises(ValueError, match="unique"):
        config.register_table("items", table)
    with pytest.raises(ValueError, match="physical table"):
        config.register_table("alias", table)


async def test_foreign_key_failure_rolls_back_batch(configured, tmp_path):
    from sqlalchemy.exc import IntegrityError

    config, sessions, parents, _, parent, _ = configured
    gateway = config.build(sessions)
    with SQLiteStore(tmp_path / "local.db", client_id="device") as local:
        sdk = SyncClient(local, transport=DirectTransport(gateway))
        sdk.enqueue(
            entity="accounts",
            entity_id=parent.encode_key({"tenant": "a", "number": 1}),
            operation="create",
            payload={"label": "A"},
        )
        sdk.enqueue(
            entity="events",
            entity_id="1",
            operation="create",
            payload={
                "tenant": "a",
                "number": 999,
                "occurred_at": "2026-09-04T00:00:00Z",
                "details": {},
            },
        )
        with pytest.raises(IntegrityError):
            await sdk.sync_once()
        assert len(local.pending(10)) == 2
        assert not (await gateway.get_changes(0)).changes
        async with sessions() as session:
            assert not (await session.execute(select(parents))).all()


def test_deferred_foreign_key_is_included_in_schema_setup():
    from sqlalchemy import create_mock_engine

    metadata = MetaData()
    parent = Table("parent", metadata, Column("id", Integer, primary_key=True))
    child = Table(
        "child",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("parent_id", Integer),
        ForeignKeyConstraint(
            ["parent_id"], ["parent.id"], use_alter=True, name="fk_child_parent"
        ),
    )
    Table("unregistered", metadata, Column("id", Integer, primary_key=True))
    statements = []
    mock = create_mock_engine(
        "postgresql://",
        lambda sql, *a, **kw: statements.append(str(sql.compile(dialect=mock.dialect))),
    )
    SQLAlchemyConfig._create_tables(mock, [child, parent])
    assert any(
        "ALTER TABLE child ADD CONSTRAINT fk_child_parent" in sql for sql in statements
    )
    assert not any("unregistered" in sql for sql in statements)
