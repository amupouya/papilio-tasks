import asyncio
import os
from uuid import uuid4

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("faststream.redis")

from dishka import Provider, Scope
from faststream.redis import RedisBroker as NativeRedis
from faststream.redis import StreamSub
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from monitoring_support import observations as observations
from opentelemetry import trace

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.events.subscribers.streams import StreamSubscriber
from papilio_tasks.infra.faststream.brokers.backends.redis import (
    RedisBroker,
)


async def test_actual_redis_stream_trace_propagation(observations):
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires TEST_REDIS_URL")
    name, done = "monitor_" + uuid4().hex, asyncio.Event()
    seen = []

    class Created(StreamPublisher[int]):
        stream = name

    class Reader(StreamSubscriber[int]):
        publisher = Created
        stream = StreamSub(name, group="reader", consumer="one")

        async def run(self, event: int) -> None:
            seen.append(trace.get_current_span().get_span_context())
            done.set()

    native = NativeRedis(url, logger=None)
    observations.monitoring.faststream(native, RedisTelemetryMiddleware)
    reg = StreamRegistrar(RedisBroker(native))
    reg.publisher(Created)
    reg.subscriber(Reader)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    app = create_app(registrar=reg, providers=[provider])
    try:
        await app.start()
        with observations.monitoring.tracer_provider.get_tracer(
            "test"
        ).start_as_current_span("http") as parent:
            await Created.publish(42, headers={"event_id": "redis-event"})
        await asyncio.wait_for(done.wait(), 5)
    finally:
        await app.stop()
    assert seen[0].trace_id == parent.get_span_context().trace_id
    spans = observations.spans.get_finished_spans()
    assert (
        len(
            [
                s
                for s in spans
                if s.attributes.get("papilio_tasks.event_id") == "redis-event"
            ]
        )
        == 1
    )
