# Monitor the pipeline with OpenTelemetry and SigNoz

This feature shows how the sync pipeline is working. OpenTelemetry collects the
data. SigNoz shows it in charts and traces.

Monitoring is optional. Sync still works when it is not installed or when the
monitoring service is down.

## Install it

From this project folder:

```sh
pip install '.[observability]'
```

Add your database and web options to the same command when needed:

```sh
pip install '.[http,fastapi,postgres,observability]'
```

## Connect to SigNoz

For SigNoz Cloud, set these values before starting your app:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT="https://ingest.<region>.signoz.cloud:443"
export OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<your-key>"
```

Replace `<region>` and `<your-key>` with the values shown by SigNoz.

For SigNoz running on the same computer, the usual HTTP collector address is:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
```

If the app runs in a container, `localhost` means that container. Use the SigNoz
collector's service name or network address instead.

The setup follows the
[SigNoz environment variable guide](https://signoz.io/docs/ingestion/opentelemetry-environment-variables/)
and sends data with
[OpenTelemetry OTLP over HTTP](https://opentelemetry.io/docs/languages/python/exporters/).

## Set up the server

Call `setup_signoz()` once when the server process starts:

```python
from fastapi import FastAPI
from sync_sdk.adapters.opentelemetry import setup_signoz
from sync_sdk.integrations.fastapi import create_sync_router

app = FastAPI()
monitoring = setup_signoz(
    "my-sync-server",
    fastapi_app=app,
    service_version="1.0.0",
    environment="production",
)

gateway = config.build(sessions, telemetry=monitoring.telemetry)
app.include_router(create_sync_router(lambda: gateway))
```

`fastapi_app=app` adds the HTTP server spans. The pipeline adds its own spans
inside those HTTP spans.

Call `monitoring.shutdown()` when the process stops. This sends waiting data and
closes the OpenTelemetry workers.

## Set up the client

Call `setup_signoz()` once when the client process starts:

```python
import httpx
from sync_sdk import SQLiteStore, SyncClient
from sync_sdk.adapters.http import HTTPTransport
from sync_sdk.adapters.opentelemetry import setup_signoz

monitoring = setup_signoz(
    "my-sync-client",
    instrument_httpx=True,
    service_version="1.0.0",
    environment="production",
)

with SQLiteStore("device.db", client_id="device-1") as local:
    async with httpx.AsyncClient(base_url="https://api.example.com") as http:
        sync = SyncClient(
            local,
            transport=HTTPTransport(http),
            telemetry=monitoring.telemetry,
        )
        await sync.sync_once()

monitoring.shutdown()
```

`instrument_httpx=True` adds HTTP client spans and sends the trace ID to the
server. This lets SigNoz show one trace from the client sync call through the
server write.

If the client and gateway run in the same process, make one monitoring object and
pass the same `monitoring.telemetry` to both.

## Data sent to SigNoz

Metrics:

| Name | Meaning |
| --- | --- |
| `sync.client.queue.depth` | Changes waiting on the device |
| `sync.client.sync.duration` | Time used by one sync run, in milliseconds |
| `sync.client.mutations` | Results received by the client |
| `sync.client.changes.pulled` | Server changes received by the client |
| `sync.client.retries` | Retries planned by the client |
| `sync.client.retry.delay` | Seconds before the next retry |
| `sync.client.transport.failures` | Temporary connection, timeout, rate-limit, or server failures |
| `sync.client.connection.failures` | Failures caused by a broken connection |
| `sync.client.failures` | Client errors that are not retried |
| `sync.gateway.mutations` | Mutations handled by the server |
| `sync.gateway.conflicts` | Changes blocked by a version conflict |
| `sync.gateway.failed_mutations` | Changes rejected by data checks |
| `sync.gateway.batch.duration` | Time used to handle a group of changes |
| `sync.gateway.batch.failures` | Groups that failed and were undone |
| `sync.gateway.pull.duration` | Time used to read server changes |
| `sync.gateway.pull.failures` | Failed server change reads |
| `sync.gateway.changes.returned` | Changes returned to clients |
| `sync.storage.transactions` | Database write groups |
| `sync.storage.transaction.duration` | Database write-group time in milliseconds |
| `sync.storage.pull.duration` | Database change-read time in milliseconds |
| `sync.storage.health.duration` | Database health-check time in milliseconds |
| `sync.storage.failures` | Failed database writes or reads |

Main trace spans:

| Name | Meaning |
| --- | --- |
| `sync.client.sync` | One full client sync run |
| `sync.client.push` | One group sent to the server |
| `sync.client.pull` | One page read from the server |
| `sync.gateway.apply` | One group handled by the server |
| `sync.gateway.pull` | One page read from the server change list |
| `sync.storage.transaction` | One database write group |

Mutation result events are added to `sync.gateway.apply` spans.

Metric labels use the entity name, operation, result, sync result, failure type,
and database type where needed. The database label is `db.system.name`, such as
`sqlite`, `postgresql`, or `mysql`. Labels do not include payloads, row IDs,
mutation IDs, client IDs, or error messages.

Queue size is reported when a change is added and when sync runs. A stopped client
cannot send a new queue size until it starts again.

## Useful SigNoz views

In SigNoz, open **Metrics Explorer** and search for the metric names above. Start
with these views:

- Latest queue depth by service.
- Retry count over time.
- Connection failures over time.
- 95th percentile client sync time.
- Conflict and rejected change counts.
- Failed gateway groups.

Open **Services** and choose the client or server service name to inspect traces.

Good first alerts are:

- Queue depth stays above zero for too long.
- Retry or connection failure count rises quickly.
- Gateway batch failures are above zero.
- Sync time becomes much higher than normal.
- Conflict count becomes much higher than normal.

Set alert limits from your own normal traffic. A fixed limit that works for one
app may be wrong for another.

## Turn it off

Leave out `telemetry=` to run with no monitoring code:

```python
sync = SyncClient(local, transport=transport)
gateway = config.build(sessions)
```

## Use another monitoring system

The pipeline uses the small `PipelineTelemetry` interface. You can send the same
data to another system by implementing `span()`, `count()`, `record()`, and
`gauge()`.

Monitoring code is wrapped so a monitoring error does not stop data sync.
