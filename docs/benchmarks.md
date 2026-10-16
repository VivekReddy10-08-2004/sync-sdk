# Throughput benchmarks

The repository includes a repeatable benchmark for the gateway and SQL or MongoDB
storage adapter. It measures independent create mutations in process, so it
excludes HTTP handling, client networking, and synchronization over a real network.

The benchmark reports attempted and successful mutations per second, whole-batch
latency, batch errors, and a post-run correctness audit. The audit checks the
business-row count, change-feed count, cursor, and contiguous sequence numbers.
A run passes only when every batch succeeds and the audit passes.

## Run it

Use an empty test database. For SQL, the benchmark removes its business table and
the SDK metadata tables when it finishes. For MongoDB, it drops the named database
before and after the run. Never point it at a production or shared database. Every
server database name must contain `test`.

Run the local SQLite baseline:

```sh
uv run --extra sqlite python benchmarks/sync_throughput.py \
  --mutations 1000 \
  --warmup 100 \
  --batch-size 100 \
  --concurrency 1 \
  --fail-on-errors \
  --output benchmark-sqlite-c1.json
```

Change `--concurrency` to `8` to exercise concurrent writers. The command emits
JSON to standard output and optionally saves the same report with `--output`.
`--fail-on-errors` returns exit code 2 if a measured or warmup batch fails.

Run the same workload against PostgreSQL:

```sh
uv run --extra postgres python benchmarks/sync_throughput.py \
  --database-url 'postgresql+asyncpg://user:pass@localhost/sync_benchmark_test' \
  --mutations 5000 --warmup 500 --batch-size 100 --concurrency 8 \
  --fail-on-errors --output benchmark-postgresql.json
```

The Database adapters GitHub Actions workflow runs this benchmark against
PostgreSQL 16, MySQL 8.4, and MongoDB 8.0 at concurrency 1, 2, 4, and 8. Each run
must have zero errors and a passing correctness audit. Raw JSON is saved as an
artifact.

For MySQL, install the `mysql` extra and use an `asyncmy` URL:

```sh
uv run --extra mysql python benchmarks/sync_throughput.py \
  --database-url 'mysql+asyncmy://user:pass@localhost/sync_benchmark_test' \
  --mutations 5000 --warmup 500 --batch-size 100 --concurrency 8 \
  --fail-on-errors --output benchmark-mysql.json
```

Run the same workload against a MongoDB replica set or sharded cluster:

```sh
uv run --extra mongodb python benchmarks/sync_throughput.py \
  --mongodb-url 'mongodb://localhost:27017/sync_benchmark_test?replicaSet=rs0' \
  --mutations 5000 --warmup 500 --batch-size 100 --concurrency 8 \
  --fail-on-errors --output benchmark-mongodb.json
```

The MongoDB adapter serializes batches inside one gateway process. This benchmark
therefore measures its current queueing behavior; it does not simulate several
gateway processes contending for the clock document.

Keep the mutation count, batch size, concurrency, database resources, and host
stable when comparing revisions. Run several trials and report the median.

## Production adapter CI result

The [GitHub Actions run](https://github.com/VivekReddy10-08-2004/sync-sdk/actions/runs/38832020345)
measured PostgreSQL 16, MySQL 8.4, and MongoDB 8.0 on October 15, 2026. Each runner
used Linux x86-64, 4 logical CPUs, and Python 3.12.3. Every run used 1,000 measured
creates, 100 warmup creates, and batches of 100.

| Backend | Concurrency | Successful mutations/s | Mean batch latency | p95 batch latency | Failed | Audit |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| PostgreSQL | 1 | 402.09 | 248.691 ms | 264.260 ms | 0 | Passed |
| PostgreSQL | 2 | 409.36 | 464.870 ms | 502.169 ms | 0 | Passed |
| PostgreSQL | 4 | 397.68 | 860.922 ms | 1,035.839 ms | 0 | Passed |
| PostgreSQL | 8 | 368.33 | 1,429.084 ms | 2,468.786 ms | 0 | Passed |
| MySQL | 1 | 348.27 | 287.123 ms | 301.210 ms | 0 | Passed |
| MySQL | 2 | 283.73 | 677.228 ms | 1,098.866 ms | 0 | Passed |
| MySQL | 4 | 336.17 | 1,019.327 ms | 1,234.713 ms | 0 | Passed |
| MySQL | 8 | 347.02 | 1,501.963 ms | 2,321.811 ms | 0 | Passed |
| MongoDB | 1 | 273.51 | 365.602 ms | 371.694 ms | 0 | Passed |
| MongoDB | 2 | 270.17 | 703.119 ms | 749.807 ms | 0 | Passed |
| MongoDB | 4 | 272.78 | 1,248.124 ms | 1,471.585 ms | 0 | Passed |
| MongoDB | 8 | 270.24 | 1,928.515 ms | 2,961.846 ms | 0 | Passed |

All three adapters handled every tested concurrency without mutation failures or
partial writes. Throughput stayed relatively flat while latency rose as writers
waited for the shared clock. MongoDB also serializes writers with an in-process
lock. These single short runs prove reliable in-process contention handling; they
do not prove horizontal write scaling, multi-process MongoDB contention handling,
or sustained-load capacity.

## Preliminary SQLite result

These results were measured on October 14, 2026, using Python 3.12.4 and a
temporary SQLite database on Linux x86-64 with 32 logical CPUs. Each batch held
100 mutations. Batch latency is the duration of the complete 100-mutation call,
not the latency of one mutation.

| Concurrency | Applied | Failed | Successful mutations/s | Mean batch latency | p95 batch latency | Correctness audit |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1,000 | 0 | 53.03 | 1,885.669 ms | 2,168.007 ms | Passed |
| 8 | 300 | 700 | 51.11 | 4,111.258 ms | 5,041.636 ms | Passed |

The eight-writer run produced seven SQLite `OperationalError` batch failures
caused by database locking. The correctness audit still passed for the 300
committed mutations, showing that failed batches did not leave partial rows or
change-feed entries. Because 700 mutations failed, that run does not pass the
benchmark.

This result establishes a reproducible baseline and identifies SQLite write
contention. It does not prove high-throughput scaling. That claim requires
error-free PostgreSQL and MySQL results across increasing concurrency, followed
by sustained and failure-injection runs. MongoDB requires the same tests plus a
multi-process run because its in-process writer lock serializes this workload.

## Evidence required for a scaling claim

Define the required throughput and latency before running the test. On fixed,
production-representative database resources, run concurrency levels 1, 2, 4,
8, and 16 with enough mutations to keep each measured run active for at least
10 minutes. Repeat each level three times and publish the raw JSON reports.

A supported scaling claim should show all of the following:

- zero failed mutations and a passing correctness audit in every reported run;
- median successful throughput and p95/p99 batch latency at each concurrency;
- throughput growth up to the documented saturation point;
- the highest error-free load that still meets the predefined latency target;
- a sustained run at that load and separate recovery tests during database or
  network interruption.

The current harness supplies the load matrix and correctness evidence. Sustained
time-based execution and failure injection still require a dedicated test setup.

## Interpreting the JSON

- `attempted_mutations_per_second` includes failed work and must not be used as
  the throughput result when errors occur.
- `successful_mutations_per_second` counts committed mutations.
- `batch_errors` counts failed batches by exception type; `failed_mutations`
  counts the mutations in those batches.
- `benchmark_passed` requires zero warmup and measured errors plus a successful
  correctness audit.
- `correctness_audit.passed` can remain true after batch failures when every
  successful batch committed fully and every failed batch rolled back fully.
