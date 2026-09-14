from collections import defaultdict
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from sync_sdk import (
    Entity,
    EntityRegistry,
    Mutation,
    SQLiteStore,
    SyncClient,
    SyncGateway,
)
from sync_sdk.adapters.direct import DirectTransport
from sync_sdk.adapters.opentelemetry import OpenTelemetryTelemetry
from sync_sdk.ports import RetryableError
from tests.contracts.test_adapters import DocumentStore, TaskPayload


class CapturedSpan:
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = dict(attributes or {})
        self.events = []

    def set_attribute(self, name, value):
        self.attributes[name] = value

    def add_event(self, name, attributes=None):
        self.events.append((name, dict(attributes or {})))


class CapturedTelemetry:
    def __init__(self):
        self.spans = []
        self.counts = defaultdict(list)
        self.records = defaultdict(list)
        self.gauges = defaultdict(list)

    @contextmanager
    def span(self, name, attributes=None):
        span = CapturedSpan(name, attributes)
        self.spans.append(span)
        yield span

    def count(self, name, value=1, attributes=None):
        self.counts[name].append((value, dict(attributes or {})))

    def record(self, name, value, attributes=None):
        self.records[name].append((value, dict(attributes or {})))

    def gauge(self, name, value, attributes=None):
        self.gauges[name].append((value, dict(attributes or {})))


class OfflineTransport:
    async def push(self, mutations):
        raise RetryableError("offline", reason="connection")

    async def pull(self, cursor, limit):
        raise AssertionError("pull should not run")


class BrokenSpanTelemetry(CapturedTelemetry):
    @contextmanager
    def span(self, name, attributes=None):
        class BrokenSpan:
            def set_attribute(self, name, value):
                raise RuntimeError("monitor is down")

            def add_event(self, name, attributes=None):
                raise RuntimeError("monitor is down")

        yield BrokenSpan()


class BrokenTelemetry:
    def span(self, name, attributes=None):
        raise RuntimeError("monitor is down")

    def count(self, name, value=1, attributes=None):
        raise RuntimeError("monitor is down")

    def record(self, name, value, attributes=None):
        raise RuntimeError("monitor is down")

    def gauge(self, name, value, attributes=None):
        raise RuntimeError("monitor is down")


def mutation(entity_id="1", *, base_version=0, payload=None):
    return Mutation(
        mutation_id=uuid4(),
        client_id="client",
        entity="tasks",
        entity_id=entity_id,
        operation="create",
        payload={"title": "hello"} if payload is None else payload,
        base_version=base_version,
        created_at=datetime.now(UTC),
    )


def gateway(telemetry):
    return SyncGateway(
        DocumentStore(),
        EntityRegistry([Entity("tasks", TaskPayload)]),
        telemetry=telemetry,
    )


async def test_pipeline_reports_queue_results_conflicts_and_latency(tmp_path):
    telemetry = CapturedTelemetry()
    server = gateway(telemetry)
    with SQLiteStore(tmp_path / "client.db", client_id="client") as local:
        client = SyncClient(
            local,
            transport=DirectTransport(server),
            telemetry=telemetry,
        )
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "first"},
            base_version=0,
        )
        await client.sync_once()
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="update",
            payload={"title": "old"},
            base_version=0,
        )
        await client.sync_once()

        assert [value for value, _ in telemetry.gauges["sync.client.queue.depth"]][
            -1
        ] == 0
        assert telemetry.counts["sync.client.changes.pulled"]
        assert telemetry.counts["sync.gateway.conflicts"] == [
            (1, {"sync.entity": "tasks"})
        ]
        statuses = [
            attributes["sync.mutation.status"]
            for _, attributes in telemetry.counts["sync.gateway.mutations"]
        ]
        assert statuses == ["applied", "conflict"]
        assert telemetry.records["sync.client.sync.duration"]
        assert telemetry.records["sync.gateway.batch.duration"]
        assert {
            "sync.client.sync",
            "sync.client.push",
            "sync.client.pull",
            "sync.gateway.apply",
            "sync.gateway.pull",
        } <= {span.name for span in telemetry.spans}
        gateway_span = next(
            span
            for span in telemetry.spans
            if span.name == "sync.gateway.apply"
            and any(
                event[1].get("sync.mutation.status") == "conflict"
                for event in span.events
            )
        )
        event_attributes = gateway_span.events[0][1]
        assert "mutation_id" not in event_attributes
        assert "entity_id" not in event_attributes
        assert "payload" not in event_attributes


async def test_retry_and_connection_failure_are_reported(tmp_path):
    telemetry = CapturedTelemetry()
    with SQLiteStore(tmp_path / "client.db", client_id="client") as local:
        client = SyncClient(
            local,
            transport=OfflineTransport(),
            telemetry=telemetry,
            clock=lambda: 100,
            jitter=lambda: 1,
        )
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "offline"},
        )
        report = await client.sync_once()

        assert report.retry_at == 101
        assert telemetry.counts["sync.client.connection.failures"] == [(1, {})]
        assert telemetry.counts["sync.client.transport.failures"] == [
            (1, {"sync.failure.type": "connection"})
        ]
        assert telemetry.counts["sync.client.retries"] == [
            (1, {"sync.failure.type": "connection"})
        ]
        assert telemetry.records["sync.client.retry.delay"] == [(1, {})]
        sync_span = next(
            span for span in telemetry.spans if span.name == "sync.client.sync"
        )
        assert sync_span.attributes["sync.outcome"] == "retry"
        assert sync_span.events == [
            (
                "sync.retry.scheduled",
                {
                    "sync.retry.attempt": 1,
                    "sync.retry.delay": 1,
                    "sync.failure.type": "connection",
                },
            )
        ]


async def test_rejected_mutation_is_reported():
    telemetry = CapturedTelemetry()
    server = gateway(telemetry)
    result = await server.apply_mutations([mutation(payload={"unknown": True})])

    assert result[0].status == "rejected"
    assert telemetry.counts["sync.gateway.failed_mutations"] == [
        (
            1,
            {"sync.entity": "tasks", "sync.mutation.status": "rejected"},
        )
    ]


async def test_broken_monitoring_does_not_stop_sync(tmp_path):
    server = gateway(BrokenTelemetry())
    with SQLiteStore(tmp_path / "client.db", client_id="client") as local:
        client = SyncClient(
            local,
            transport=DirectTransport(server),
            telemetry=BrokenTelemetry(),
        )
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "safe"},
        )
        report = await client.sync_once()

        assert report.pushed == 1
        assert local.pending_count() == 0


async def test_opentelemetry_adapter_emits_traces_and_metrics(tmp_path):
    resource = Resource.create({"service.name": "sync-test"})
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    telemetry = OpenTelemetryTelemetry(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )

    server = gateway(telemetry)
    with SQLiteStore(tmp_path / "client.db", client_id="client") as local:
        client = SyncClient(
            local,
            transport=DirectTransport(server),
            telemetry=telemetry,
        )
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "observed"},
        )
        await client.sync_once()

    tracer_provider.force_flush()
    meter_provider.force_flush()
    span_names = {span.name for span in span_exporter.get_finished_spans()}
    assert {"sync.client.sync", "sync.gateway.apply", "sync.gateway.pull"} <= span_names

    metrics_data = metric_reader.get_metrics_data()
    metric_names = {
        metric.name
        for resource_metric in metrics_data.resource_metrics
        for scope_metric in resource_metric.scope_metrics
        for metric in scope_metric.metrics
    }
    assert {
        "sync.client.queue.depth",
        "sync.client.sync.duration",
        "sync.gateway.mutations",
        "sync.gateway.batch.duration",
    } <= metric_names

    tracer_provider.shutdown()
    meter_provider.shutdown()


async def test_broken_span_methods_do_not_stop_sync(tmp_path):
    telemetry = BrokenSpanTelemetry()
    server = gateway(telemetry)
    with SQLiteStore(tmp_path / "client.db", client_id="client") as local:
        client = SyncClient(
            local,
            transport=DirectTransport(server),
            telemetry=telemetry,
        )
        client.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "safe"},
        )
        assert (await client.sync_once()).pushed == 1
