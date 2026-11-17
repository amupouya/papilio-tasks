"""Native Outbox metrics with explicit durable attempt outcomes."""

from collections.abc import Mapping
from typing import Any

from faststream_outbox.metrics.opentelemetry import OpenTelemetryRecorder
from opentelemetry.metrics import MeterProvider


class Recorder(OpenTelemetryRecorder):
    """Retain outcomes omitted by the native bus-metrics opt-out.

    Args:
        meter_provider: Borrowed host provider. Native middleware owns bus
            metrics; this recorder observes internal relay events.
    """

    def __init__(self, meter_provider: MeterProvider) -> None:
        super().__init__(
            meter_provider=meter_provider, include_bus_metrics=False
        )
        self.outcomes = meter_provider.get_meter(
            "papilio_tasks.outbox"
        ).create_counter("papilio_tasks.outbox.outcomes")

    def __call__(self, event: str, tags: Mapping[str, Any]) -> None:
        super().__call__(event, tags)
        if event in {"acked", "nacked_retried", "nacked_terminal"}:
            attributes = {"queue": tags["queue"], "outcome": event}
            if event == "nacked_terminal":
                attributes["reason"] = tags["reason"]
            self.outcomes.add(1, attributes=attributes)
