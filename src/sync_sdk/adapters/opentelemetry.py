"""Send pipeline traces and metrics to OpenTelemetry and SigNoz."""

from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from ..telemetry import Attributes, TelemetrySpan

_DESCRIPTIONS = {
    "sync.client.queue.depth": "Changes waiting in the local queue",
    "sync.client.mutations": "Mutation results received by the client",
    "sync.client.changes.pulled": "Changes received by the client",
    "sync.client.transport.failures": "Temporary send or receive failures",
    "sync.client.connection.failures": "Connection failures while syncing",
    "sync.client.retries": "Retries scheduled by the client",
    "sync.client.failures": "Client sync failures that were not retried",
    "sync.gateway.mutations": "Mutations handled by the gateway",
    "sync.gateway.conflicts": "Mutation conflicts",
    "sync.gateway.failed_mutations": "Rejected mutations",
    "sync.gateway.batch.failures": "Gateway batches that failed",
    "sync.gateway.pull.failures": "Gateway change reads that failed",
    "sync.gateway.changes.returned": "Changes returned by the gateway",
    "sync.client.retry.delay": "Time before the next retry",
    "sync.client.sync.duration": "Time used by one client sync run",
    "sync.gateway.batch.duration": "Time used to handle a mutation batch",
    "sync.gateway.pull.duration": "Time used to read server changes",
}

_UNITS = {
    "sync.client.queue.depth": "{mutation}",
    "sync.client.mutations": "{mutation}",
    "sync.client.changes.pulled": "{change}",
    "sync.client.transport.failures": "{failure}",
    "sync.client.connection.failures": "{failure}",
    "sync.client.retries": "{retry}",
    "sync.client.failures": "{failure}",
    "sync.gateway.mutations": "{mutation}",
    "sync.gateway.conflicts": "{conflict}",
    "sync.gateway.failed_mutations": "{mutation}",
    "sync.gateway.batch.failures": "{failure}",
    "sync.gateway.pull.failures": "{failure}",
    "sync.gateway.changes.returned": "{change}",
    "sync.client.retry.delay": "s",
    "sync.client.sync.duration": "ms",
    "sync.gateway.batch.duration": "ms",
    "sync.gateway.pull.duration": "ms",
}


class OpenTelemetryTelemetry:
    """Write the pipeline's spans and metrics to OpenTelemetry."""

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
    ):
        self.tracer = trace.get_tracer("sync_sdk", tracer_provider=tracer_provider)
        self.meter = metrics.get_meter("sync_sdk", meter_provider=meter_provider)
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._gauges: dict[str, Any] = {}
        self._gauge_values: dict[str, tuple[float, dict[str, Any]]] = {}

    def span(
        self, name: str, attributes: Attributes | None = None
    ) -> AbstractContextManager[TelemetrySpan]:
        return self.tracer.start_as_current_span(
            name, attributes=dict(attributes or {})
        )

    def count(
        self, name: str, value: int = 1, attributes: Attributes | None = None
    ) -> None:
        instrument = self._counters.get(name)
        if instrument is None:
            instrument = self.meter.create_counter(
                name,
                unit=_UNITS.get(name, "1"),
                description=_DESCRIPTIONS.get(name, ""),
            )
            self._counters[name] = instrument
        instrument.add(value, dict(attributes or {}))

    def record(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        instrument = self._histograms.get(name)
        if instrument is None:
            instrument = self.meter.create_histogram(
                name,
                unit=_UNITS.get(name, "1"),
                description=_DESCRIPTIONS.get(name, ""),
            )
            self._histograms[name] = instrument
        instrument.record(value, dict(attributes or {}))

    def gauge(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        self._gauge_values[name] = (value, dict(attributes or {}))
        if name in self._gauges:
            return

        def observe(_options: Any) -> list[Observation]:
            current, current_attributes = self._gauge_values[name]
            return [Observation(current, current_attributes)]

        self._gauges[name] = self.meter.create_observable_gauge(
            name,
            callbacks=[observe],
            unit=_UNITS.get(name, "1"),
            description=_DESCRIPTIONS.get(name, ""),
        )


@dataclass
class OpenTelemetryRuntime:
    """OpenTelemetry objects owned by one app process."""

    telemetry: OpenTelemetryTelemetry
    tracer_provider: TracerProvider
    meter_provider: MeterProvider

    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        traces = self.tracer_provider.force_flush(timeout_millis)
        metrics_ok = self.meter_provider.force_flush(timeout_millis)
        return bool(traces and metrics_ok)

    def shutdown(self) -> None:
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()


def setup_signoz(
    service_name: str,
    *,
    endpoint: str | None = None,
    headers: Mapping[str, str] | None = None,
    service_version: str | None = None,
    environment: str | None = None,
    fastapi_app: Any | None = None,
    instrument_httpx: bool = False,
    export_interval_millis: int = 60_000,
) -> OpenTelemetryRuntime:
    """Set up OTLP export for SigNoz. Call once when a process starts.

    If endpoint and headers are left out, the standard OTEL environment variables
    are used. Give a base HTTP endpoint, without /v1/traces or /v1/metrics.
    """
    if not service_name.strip():
        raise ValueError("service_name cannot be empty")
    if export_interval_millis <= 0:
        raise ValueError("export_interval_millis must be positive")
    if endpoint is not None and endpoint.rstrip("/").endswith(
        ("/v1/traces", "/v1/metrics")
    ):
        raise ValueError("endpoint must be the base OTLP HTTP address")

    resource_values = {"service.name": service_name}
    if service_version:
        resource_values["service.version"] = service_version
    if environment:
        resource_values["deployment.environment.name"] = environment
    resource = Resource.create(resource_values)

    exporter_options: dict[str, Any] = {}
    if headers is not None:
        exporter_options["headers"] = dict(headers)

    trace_options = dict(exporter_options)
    metric_options = dict(exporter_options)
    if endpoint is not None:
        base = endpoint.rstrip("/")
        trace_options["endpoint"] = f"{base}/v1/traces"
        metric_options["endpoint"] = f"{base}/v1/metrics"

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(**trace_options))
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(**metric_options),
                export_interval_millis=export_interval_millis,
            )
        ],
    )

    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(meter_provider)

    if fastapi_app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(
            fastapi_app,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )

    if instrument_httpx:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument(
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )

    return OpenTelemetryRuntime(
        telemetry=OpenTelemetryTelemetry(
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        ),
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
