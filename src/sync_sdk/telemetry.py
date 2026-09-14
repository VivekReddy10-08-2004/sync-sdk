"""Small observability interface used by the sync pipeline."""

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, suppress
from typing import Protocol

AttributeValue = str | bool | int | float
Attributes = Mapping[str, AttributeValue]


class TelemetrySpan(Protocol):
    def set_attribute(self, name: str, value: AttributeValue) -> None: ...

    def add_event(self, name: str, attributes: Attributes | None = None) -> None: ...


class PipelineTelemetry(Protocol):
    """Receives traces and metrics from the client and gateway."""

    def span(
        self, name: str, attributes: Attributes | None = None
    ) -> AbstractContextManager[TelemetrySpan]: ...

    def count(
        self, name: str, value: int = 1, attributes: Attributes | None = None
    ) -> None: ...

    def record(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None: ...

    def gauge(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None: ...


class _NoOpSpan:
    def set_attribute(self, name: str, value: AttributeValue) -> None:
        pass

    def add_event(self, name: str, attributes: Attributes | None = None) -> None:
        pass


class _SafeSpan:
    def __init__(self, span: TelemetrySpan):
        self._span = span

    def set_attribute(self, name: str, value: AttributeValue) -> None:
        with suppress(Exception):
            self._span.set_attribute(name, value)

    def add_event(self, name: str, attributes: Attributes | None = None) -> None:
        with suppress(Exception):
            self._span.add_event(name, attributes)


class NoOpTelemetry:
    """Default telemetry target. It does no work and needs no extra package."""

    @contextmanager
    def span(
        self, name: str, attributes: Attributes | None = None
    ) -> Iterator[_NoOpSpan]:
        yield _NoOpSpan()

    def count(
        self, name: str, value: int = 1, attributes: Attributes | None = None
    ) -> None:
        pass

    def record(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        pass

    def gauge(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        pass


class SafeTelemetry:
    """Stops monitoring errors from stopping data sync."""

    def __init__(self, telemetry: PipelineTelemetry | None = None):
        self._telemetry = telemetry or NoOpTelemetry()

    @contextmanager
    def span(
        self, name: str, attributes: Attributes | None = None
    ) -> Iterator[TelemetrySpan]:
        try:
            manager = self._telemetry.span(name, attributes)
            span = manager.__enter__()
        except Exception:  # noqa: BLE001 -- monitoring must not stop sync
            yield _NoOpSpan()
            return
        try:
            yield _SafeSpan(span)
        except BaseException as error:
            with suppress(Exception):
                manager.__exit__(type(error), error, error.__traceback__)
            raise
        else:
            with suppress(Exception):
                manager.__exit__(None, None, None)

    def count(
        self, name: str, value: int = 1, attributes: Attributes | None = None
    ) -> None:
        with suppress(Exception):
            self._telemetry.count(name, value, attributes)

    def record(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        with suppress(Exception):
            self._telemetry.record(name, value, attributes)

    def gauge(
        self, name: str, value: float, attributes: Attributes | None = None
    ) -> None:
        with suppress(Exception):
            self._telemetry.gauge(name, value, attributes)


def safe_telemetry(telemetry: PipelineTelemetry | None) -> SafeTelemetry:
    if isinstance(telemetry, SafeTelemetry):
        return telemetry
    return SafeTelemetry(telemetry)
