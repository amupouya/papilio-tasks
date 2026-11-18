"""Optional DLQ SQL observations, independently of host commit."""

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter

from opentelemetry.trace import Span

from papilio_tasks.tools.monitoring import Monitoring


class Observer:
    def __init__(self, monitoring: Monitoring) -> None:
        self.tracer = monitoring.tracer_provider.get_tracer(
            "papilio_tasks.dlq"
        )
        meter = monitoring.meter_provider.get_meter("papilio_tasks.dlq")
        self.operations = meter.create_counter("papilio_tasks.dlq.operations")
        self.duration = meter.create_histogram(
            "papilio_tasks.dlq.operation.duration", unit="s"
        )

    @contextmanager
    def operation(self, operation: str, queue: str | None) -> Iterator[Span]:
        attributes = {"operation": operation}
        if queue is not None:
            attributes["queue"] = queue
        started, status = perf_counter(), "success"
        try:
            with self.tracer.start_as_current_span(
                "papilio_tasks.dlq." + operation, attributes=attributes
            ) as span:
                yield span
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except Exception:
            status = "error"
            raise
        finally:
            labels = {**attributes, "status": status}
            self.operations.add(1, attributes=labels)
            self.duration.record(perf_counter() - started, attributes=labels)
