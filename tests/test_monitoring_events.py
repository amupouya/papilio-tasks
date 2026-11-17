import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("faststream.rabbit.opentelemetry")

from dishka import Provider, Scope
from faststream.exceptions import AckMessage
from faststream.rabbit import RabbitBroker as NativeRabbit
from faststream.rabbit import TestRabbitBroker
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from monitoring_support import observations as observations
from opentelemetry import trace

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.rabbit import (
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.tools.monitoring import Monitoring
from papilio_tasks.tools.retry import Retry


@pytest.mark.parametrize("outcome", ["success", "error", "cancel", "ack"])
async def test_local_attempts_and_native_delivery_metrics(
    observations, outcome
):
    class Created(RabbitPublisher[str]):
        exchange = RabbitExchange("monitoring")
        routing_key = "orders.created"

    calls = []

    class Reader(RabbitSubscriber[str]):
        publisher = Created
        queue = RabbitQueue("orders")
        retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))

        async def run(self, event: str) -> None:
            calls.append(event)
            if outcome == "ack":
                raise AckMessage()
            if outcome == "cancel":
                raise asyncio.CancelledError()
            if outcome == "error" or len(calls) < 3:
                raise ConnectionError("transient")

    native = NativeRabbit(logger=None)
    config = observations.monitoring
    installed = config.faststream(native, RabbitTelemetryMiddleware)
    assert config.faststream(native, RabbitTelemetryMiddleware) == installed
    with pytest.raises(ValueError, match="already"):
        Monitoring(config.tracer_provider, object()).faststream(
            native, RabbitTelemetryMiddleware
        )
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(Created)
    reg.subscriber(Reader)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    app = create_app(registrar=reg, providers=[provider])
    tracer = config.tracer_provider.get_tracer("test")
    try:
        async with TestRabbitBroker(native):
            with tracer.start_as_current_span("request") as parent:
                if outcome in {"success", "ack"}:
                    await Created.publish(
                        "secret-body", headers={"event_id": "event-42"}
                    )
                else:
                    with pytest.raises(
                        ConnectionError
                        if outcome == "error"
                        else asyncio.CancelledError
                    ):
                        await Created.publish(
                            "secret-body", headers={"event_id": "event-42"}
                        )
                assert trace.get_current_span() is parent
    finally:
        await app.stop()
    attempts = 1 if outcome in {"cancel", "ack"} else 3
    assert len(calls) == attempts
    assert (
        sum(
            p.value
            for p in observations.points("papilio_tasks.subscriber.attempts")
        )
        == attempts
    )
    assert sum(
        p.value
        for p in observations.points("papilio_tasks.subscriber.retries")
    ) == (0 if outcome in {"cancel", "ack"} else 2)
    if outcome == "ack":
        assert not observations.points("papilio_tasks.subscriber.errors")
    # Native outer metrics describe one delivery, not each Tenacity attempt.
    assert (
        sum(p.value for p in observations.points("messaging.process.messages"))
        == 1
    )
    spans = observations.spans.get_finished_spans()
    processed = [
        s
        for s in spans
        if s.attributes.get("papilio_tasks.event_id") == "event-42"
    ]
    assert len(processed) == 1
    assert processed[0].context.trace_id == parent.get_span_context().trace_id
    assert (
        len(
            [
                e
                for e in processed[0].events
                if e.name == "papilio_tasks.subscriber.attempt"
            ]
        )
        == attempts
    )
    assert "secret-body" not in repr(spans)
    for name in (
        "papilio_tasks.subscriber.attempts",
        "messaging.process.messages",
    ):
        assert all(
            "event_id" not in str(p.attributes)
            for p in observations.points(name)
        )
