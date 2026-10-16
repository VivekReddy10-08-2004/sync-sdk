# Testing and verification report

## Release

- Package: `sync-sdk`
- Version: `0.2.0`
- Report date: October 15, 2026
- Python used: 3.12.4

This report states what was tested. It also states what is not included yet.

## Result

The normal test suite collected 61 tests.

- 58 passed.
- 3 were skipped as expected, including the live MongoDB test because Docker and
  `SYNC_TEST_MONGODB_URL` were unavailable locally.
- 0 failed.

A clean source package and wheel were built:

- `sync_sdk-0.2.0.tar.gz`
- `sync_sdk-0.2.0-py3-none-any.whl`

The wheel was installed into a new Python environment. Importing `sync_sdk`
worked, and the installed version was `0.2.0`.

## Databases tested

| Database | Version | Driver | Result |
| --- | --- | --- | --- |
| SQLite | 3.45.3 | `aiosqlite` | Passed |
| PostgreSQL | 16.15 | `asyncpg` | Passed |
| MySQL | 5.7.24 | `asyncmy` | Passed |
| MongoDB | 8.0 CI target | PyMongo async | Passed in CI; local run skipped |

The GitHub Actions database workflow is set to run PostgreSQL 16, MySQL 8.4, and
MongoDB 8.0 as a single-node replica set. It starts clean database services for
every run.

The same adapter contract runs against each database. The application table and
the SDK tracking tables are created before the test and removed after the test.

## Database behavior checked

The live database contract checks these rules:

| Check | Expected result |
| --- | --- |
| Create a row | The row and its first version are saved |
| Send the same mutation twice | The change is applied once and the saved result is returned twice |
| Send two updates from the same version | One update is applied and one returns a conflict |
| Pull with a page size | The cursor moves and `has_more` is correct |
| Delete a row | The row is removed and a delete change is logged |
| Check database health | The SDK tracking table can be read |
| Clean up | Test tables are removed |

The full suite also checks:

- Offline SQLite outbox storage.
- Saved retry time and growing retry delay.
- HTTP 408, 429, and 503 retry handling.
- Reconnect and resume.
- Saved pull cursors.
- Bounded batches.
- Lost response replay.
- Duplicate mutation IDs.
- Optimistic row versions.
- Conflict storage.
- Delete propagation.
- Failed batch rollback.
- Crash and cancellation rollback.
- Invalid server replies.
- FIFO order for linked local changes.
- Table and field registration.
- Composite primary keys.
- Foreign keys.
- Payload type checks.
- FastAPI routes with a real SQLAlchemy test database.
- Direct in-process transport.
- OpenTelemetry metrics and traces.
- Monitoring failure isolation.
- Old database migration data.

## Why tests are skipped

The SQL and document test stores share one contract.

Two checks only apply to SQL tables:

- Canonical SQL primary-key parsing.
- Cancellation during a real SQL write.

Pytest runs those cases for the document store too. The document versions are
skipped because a document store does not have SQL keys or SQL transactions.
The SQL versions run and pass.

The live MongoDB contract is also skipped locally when a replica-set URL is not
configured. The database workflow supplies that service and runs the test.

## Test files

| File | Purpose |
| --- | --- |
| `tests/contracts/test_adapters.py` | Shared SQL and document storage rules |
| `tests/contracts/test_schema_configuration.py` | Developer-owned tables, keys, fields, and constraints |
| `tests/contracts/test_packaging_and_migration.py` | Core imports, SQL dialects, and old data migration |
| `tests/integration/test_sync_api.py` | FastAPI mutation and change routes |
| `tests/integration/test_reliability.py` | Offline work, retries, restart, crash, and resume |
| `tests/integration/test_sql_database_adapters.py` | Live SQLite, PostgreSQL, and MySQL contract |
| `tests/integration/test_mongodb_adapter.py` | Live MongoDB adapter contract |
| `tests/test_observability.py` | Metrics, traces, and monitoring failures |
| `benchmarks/sync_throughput.py` | Repeatable SQL gateway and storage throughput workload |

## Run the normal tests

Install the project and test tools:

```sh
uv sync --locked
```

Run every test:

```sh
uv run pytest -q
```

Expected result for this release:

```text
58 passed, 3 skipped
```

## Run the live database tests

Install both server database drivers:

```sh
uv sync --locked --extra postgres --extra mysql
```

Use empty test databases. Their database names must contain the word `test`.
The test creates and removes tables.

Run PostgreSQL:

```sh
SYNC_TEST_POSTGRES_URL='postgresql+asyncpg://user:pass@localhost/sync_test' \
uv run pytest tests/integration/test_sql_database_adapters.py -q -s
```

Run MySQL:

```sh
SYNC_TEST_MYSQL_URL='mysql+asyncmy://user:pass@localhost/sync_test' \
uv run pytest tests/integration/test_sql_database_adapters.py -q -s
```

Run both and save a JSON report:

```sh
SYNC_TEST_POSTGRES_URL='postgresql+asyncpg://user:pass@localhost/sync_test' \
SYNC_TEST_MYSQL_URL='mysql+asyncmy://user:pass@localhost/sync_test' \
SYNC_TEST_METRICS_PATH='database-adapter-metrics.json' \
uv run pytest tests/integration/test_sql_database_adapters.py -q -s
```

Never use a production database for this test.

## Measured database times

These numbers came from one local verification run. They show that metric
collection works. They are not a speed promise or a database benchmark.

| Database | Mean transaction | Highest transaction | Mean pull | Health check |
| --- | ---: | ---: | ---: | ---: |
| SQLite | 141.650 ms | 215.430 ms | 13.918 ms | 5.259 ms |
| PostgreSQL 16.15 | 72.283 ms | 112.250 ms | 17.366 ms | 10.064 ms |
| MySQL 5.7.24 | 66.199 ms | 90.401 ms | 10.127 ms | 3.238 ms |

Each database produced:

- 4 applied mutation results.
- 1 conflict result.
- 4 measured database transactions.
- 2 measured change pulls.
- 1 measured health check.

Times change with the computer, database settings, connection distance, and load.

## Throughput benchmark

`benchmarks/sync_throughput.py` runs a configurable create workload, reports
successful mutations per second and batch latency, and audits the stored rows and
change feed afterward. PostgreSQL 16, MySQL 8.4, and MongoDB 8.0 applied all
12,000 measured mutations across concurrency 1, 2, 4, and 8 with zero failures.
The [throughput benchmark guide](docs/benchmarks.md) contains the raw run link,
complete comparison, SQLite result, and reproduction commands. The measured
database times above remain contract-test instrumentation and should not be
compared as throughput results.

## OpenTelemetry and SigNoz checks

The SQL adapter records these metrics:

- `sync.storage.transactions`
- `sync.storage.transaction.duration`
- `sync.storage.pull.duration`
- `sync.storage.health.duration`
- `sync.storage.failures`

It also creates the `sync.storage.transaction` trace span.

Each storage metric uses `db.system.name` to show the database type:

- `sqlite`
- `postgresql`
- `mysql`

Payloads, row IDs, mutation IDs, client IDs, and error messages are not placed in
metric labels.

See the [OpenTelemetry and SigNoz guide](docs/observability.md) for setup steps.

## Automated database job

The workflow at `.github/workflows/database-adapters.yml`:

1. Starts PostgreSQL, MySQL, and MongoDB.
2. Installs the locked project dependencies.
3. Runs the live database contract.
4. Benchmarks PostgreSQL, MySQL, and MongoDB at concurrency 1, 2, 4, and 8.
5. Creates JSON timing and benchmark reports.
6. Creates a JUnit test report.
7. Adds the JSON reports to the GitHub Actions summary.
8. Saves the reports as a workflow artifact.

The job runs for pull requests, pushes to `master`, and manual runs.

## Build and wheel check

Build the package:

```sh
uv build
```

Create a clean environment and install the wheel:

```sh
uv venv /tmp/sync-sdk-wheel-test --clear
uv pip install \
  --python /tmp/sync-sdk-wheel-test/bin/python \
  dist/sync_sdk-0.2.0-py3-none-any.whl
/tmp/sync-sdk-wheel-test/bin/python -c \
  'import importlib.metadata as m; import sync_sdk; print(m.version("sync-sdk"))'
```

The last command should print:

```text
0.2.0
```

## What this proves

The tested SQL adapter can apply the same sync rules on SQLite, PostgreSQL, and
MySQL.

The MongoDB contract checks idempotency, competing versions, ordered pulls,
deletes, private-field preservation, failed-transaction rollback, and health
against a replica set. It is skipped when `SYNC_TEST_MONGODB_URL` is absent; the
database workflow supplies that URL and starts a clean replica set.

Developers can register their own SQLAlchemy tables and choose which fields are
synced. The SDK owns only its tracking tables. The application still owns its
database, login rules, access rules, and schema changes.

The package can also use another storage system when a developer writes an
adapter that follows the storage contract.

## Current limits

- MongoDB is the only ready-to-use NoSQL adapter.
- The in-memory document adapter is test code only.
- Existing database rows are not sent automatically.
- SQL change capture polls full registered tables; native transaction-log CDC is
  not included for high-write or very large tables.
- The application must add login and data access checks.
- The current SQL write lock favors correct order over high write throughput.
- Python 3.12 or newer is required.

## Before a public PyPI release

Complete these release steps:

- Merge the release branch into `master` after every required check passes.
- Publish the package to TestPyPI first.
- Install the TestPyPI wheel into a clean environment.
- Run the small example with that installed wheel.
- Run the database workflow from a clean commit.
- Configure the `pypi` GitHub environment and PyPI Trusted Publisher.
- Create a version tag.
- Publish the GitHub release to trigger PyPI Trusted Publishing.

Do not describe the timing table as a benchmark.
