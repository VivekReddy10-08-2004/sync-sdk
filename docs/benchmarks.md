# Throughput benchmarks

The repository includes a repeatable benchmark for the SQL gateway and storage
adapter. It measures independent create mutations in process, so it excludes
HTTP handling, client networking, and synchronization over a real network.

The benchmark reports attempted and successful mutations per second, whole-batch
latency, batch errors, and a post-run correctness audit. The audit checks the
business-row count, change-feed count, cursor, and contiguous sequence numbers.
A run passes only when every batch succeeds and the audit passes.

## Run it

Use an empty test database. The benchmark removes its business table and the SDK
metadata tables when it finishes. Never point it at a production or shared
database. For PostgreSQL and MySQL, the database name must contain `test`.

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
PostgreSQL 16 at concurrency 1, 2, 4, and 8. Each run must have zero errors and
a passing correctness audit, and its raw JSON report is saved as an artifact.

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

## PostgreSQL 16 CI result

The [GitHub Actions run](https://github.com/VivekReddy10-08-2004/sync-sdk/actions/runs/38650288099)
measured PostgreSQL 16 on October 14, 2026. The runner used Linux x86-64,
4 logical CPUs, and Python 3.12.3. Every run used 1,000 measured creates,
100 warmup creates, and batches of 100.

| Concurrency | Applied | Failed | Successful mutations/s | Mean batch latency | p95 batch latency | Audit |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1,000 | 0 | 409.20 | 244.368 ms | 259.650 ms | Passed |
| 2 | 1,000 | 0 | 407.59 | 466.168 ms | 509.502 ms | Passed |
| 4 | 1,000 | 0 | 401.97 | 849.518 ms | 1,013.146 ms | Passed |
| 8 | 1,000 | 0 | 374.82 | 1,419.920 ms | 2,156.099 ms | Passed |

PostgreSQL handled every tested concurrency without mutation failures or partial
writes. Throughput remained relatively flat and decreased at concurrency 8 while
latency rose as writers waited for the shared clock. This proves reliable
contention handling for this short workload; it does not prove horizontal write
scaling or sustained-load capacity.

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
