# pyright: strict

from typing import assert_type

from faststream.nats import NatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider
from taskiq import AsyncBroker

from papilio_tasks.infra.faststream.outbox_monitoring import Recorder
from papilio_tasks.infra.taskiq.monitoring import Middleware
from papilio_tasks.tools.monitoring import Monitoring


def use(
    traces: TracerProvider,
    metrics: MeterProvider,
    worker: AsyncBroker,
    events: NatsBroker,
) -> None:
    monitor = Monitoring(traces, metrics)
    assert_type(monitor.taskiq(worker), Middleware)
    native = monitor.faststream(events, NatsTelemetryMiddleware)
    assert_type(native, NatsTelemetryMiddleware)
    assert_type(monitor.outbox_recorder(), Recorder)
    monitor.taskiq(events)  # expect-error
    monitor.faststream(events, lambda: 42)  # expect-error
    Monitoring(traces, 42)  # expect-error
