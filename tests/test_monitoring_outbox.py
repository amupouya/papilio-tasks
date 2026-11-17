import asyncio
import os
from uuid import uuid4

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("faststream_outbox")
pytest.importorskip("faststream.redis")

from dishka import Provider, Scope
from faststream.redis import RedisBroker as NativeRedis
from faststream.redis import StreamSub
from faststream.redis.opentelemetry import RedisTelemetryMiddleware
from faststream_outbox import OutboxBroker
from faststream_outbox.metrics.prometheus import PrometheusRecorder
from faststream_outbox.opentelemetry import OutboxTelemetryMiddleware
from monitoring_support import observations as observations
from opentelemetry import trace
from prometheus_client import CollectorRegistry
from sqlalchemy.ext.asyncio import async_sessionmaker
from tenacity import AsyncRetrying, stop_after_delay, wait_fixed
from test_outbox import count
from test_outbox import database as database

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.events.subscribers.streams import StreamSubscriber
from papilio_tasks.apps.outbox import Outbox
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker
from papilio_tasks.tools.retry import Retry


@pytest.mark.parametrize("batch", [False, True])
async def test_real_transaction_relay_and_redis_share_trace(
    database, observations, batch
):
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires TEST_REDIS_URL")
    engine, table, dlq, _ = database
    name, done, seen = "monitor_" + uuid4().hex, asyncio.Event(), []

    class Created(StreamPublisher[int]):
        stream = name
        outbox_key = name

    class Reader(StreamSubscriber[int]):
        publisher = Created
        stream = StreamSub(name, group="reader", consumer="one")

        async def run(self, event: int) -> None:
            seen.append(trace.get_current_span().get_span_context())
            if len(seen) == (2 if batch else 1):
                done.set()

    monitoring = observations.monitoring
    target = NativeRedis(url, logger=None)
    monitoring.faststream(target, RedisTelemetryMiddleware)
    registry = StreamRegistrar(RedisBroker(target))
    registry.publisher(Created)
    registry.subscriber(Reader)
    dependencies = Provider(scope=Scope.REQUEST)
    dependencies.provide(Reader)
    app = create_app(registrar=registry, providers=[dependencies])
    native = OutboxBroker(
        engine,
        outbox_table=table,
        dlq_table=dlq,
        metrics_recorder=monitoring.outbox_recorder(),
        logger=None,
    )
    monitoring.faststream(native, OutboxTelemetryMiddleware)
    runtime = Outbox(native, publishers=[Created])
    relay = runtime.create_relay(
        retry=Retry(attempts=2, delay=0, errors=(ConnectionError,))
    )
    tracer = monitoring.tracer_provider.get_tracer("test")
    try:
        await app.start()
        with tracer.start_as_current_span("command") as command:
            async with (
                async_sessionmaker(engine)() as session,
                session.begin(),
            ):
                writer = runtime.writer(session)
                if batch:
                    await writer.append_batch(Created, [1, 2])
                else:
                    await writer.append(Created, 1)
        await relay.start()
        await asyncio.wait_for(done.wait(), 10)
    finally:
        await relay.stop()
        await app.stop()
    assert await count(engine, table) == 0
    source_trace = command.get_span_context().trace_id
    if batch:
        # Native messaging conventions start batch-consumer traces with links
        # to the producer trace; forcing one parent would erase that relation.
        linked = {
            s.context.trace_id
            for s in observations.spans.get_finished_spans()
            if any(link.context.trace_id == source_trace for link in s.links)
        }
        assert all(s.trace_id in linked for s in seen)
    else:
        assert all(s.trace_id == source_trace for s in seen)
    points = observations.points("messaging.process.messages")
    by_system = {}
    for point in points:
        key = point.attributes["messaging.system"]
        by_system[key] = by_system.get(key, 0) + point.value
    # Recorder and middleware must not count the same relay attempt twice.
    assert by_system == {
        "outbox": 2 if batch else 1,
        "redis": 2 if batch else 1,
    }


async def test_native_durable_retry_and_terminal_metrics(
    database, observations
):
    engine, table, dlq, _ = database
    failures = []

    class Broken(StreamPublisher[int]):
        stream = "controlled_failure"
        outbox_key = "controlled_failure"

        @classmethod
        async def publish(cls, event, **options):
            failures.append(event)
            raise ConnectionError("controlled target outage")

    registry = StreamRegistrar(RedisBroker(NativeRedis(logger=None)))
    registry.publisher(Broken)
    app = create_app(registrar=registry)
    monitoring = observations.monitoring
    native = OutboxBroker(
        engine,
        outbox_table=table,
        dlq_table=dlq,
        metrics_recorder=monitoring.outbox_recorder(),
        logger=None,
    )
    monitoring.faststream(native, OutboxTelemetryMiddleware)
    runtime = Outbox(native, publishers=[Broken])
    relay = runtime.create_relay(
        retry=Retry(attempts=2, delay=0, errors=(ConnectionError,)),
        min_fetch_interval=0.01,
        max_fetch_interval=0.02,
    )
    async with async_sessionmaker(engine)() as session, session.begin():
        await runtime.writer(session).append(Broken, 1)

    async def terminal():
        assert await count(engine, dlq) == 1

    try:
        await relay.start()
        await AsyncRetrying(
            stop=stop_after_delay(5), wait=wait_fixed(0.01), reraise=True
        )(terminal)
    finally:
        await relay.stop()
        await app.stop()
    assert failures == [1, 1]
    outcomes = {
        p.attributes["outcome"]: p.value
        for p in observations.points("papilio_tasks.outbox.outcomes")
    }
    assert outcomes == {"nacked_retried": 1, "nacked_terminal": 1}
    assert observations.points("messaging.outbox.dlq_written")[0].value == 1


async def test_native_lag_sampler_freshness_and_cleanup(database):
    engine, table, _, _ = database
    registry = CollectorRegistry()
    recorder = PrometheusRecorder(
        registry=registry,
        include_bus_metrics=False,
        # Native sampling also uses this interval as its database timeout.
        # Allow connection setup on hosted runners instead of imposing 20 ms.
        queue_lag_interval_seconds=1,
    )
    native = OutboxBroker(
        engine, outbox_table=table, metrics_recorder=recorder, logger=None
    )
    started, blocked = asyncio.Event(), asyncio.Event()

    @native.subscriber(
        "lag",
        max_workers=1,
        fetch_batch_size=1,
        min_fetch_interval=0.01,
        max_fetch_interval=0.02,
    )
    async def handler(body: int):
        started.set()
        await blocked.wait()

    async with async_sessionmaker(engine)() as session, session.begin():
        await native.publish_batch(1, 2, 3, queue="lag", session=session)

    def fresh():
        samples = {
            s.name: s.value
            for family in registry.collect()
            for s in family.samples
        }
        assert (
            samples[
                "faststream_outbox_queue_lag_last_success_timestamp_seconds"
            ]
            > 0
        )
        assert samples["faststream_outbox_oldest_due_message_age_seconds"] >= 0

    try:
        await native.start()
        await asyncio.wait_for(started.wait(), 5)
        await AsyncRetrying(
            stop=stop_after_delay(10), wait=wait_fixed(0.05), reraise=True
        )(fresh)
    finally:
        blocked.set()
        await native.stop()
    assert not [
        s
        for family in registry.collect()
        for s in family.samples
        if s.name.startswith("faststream_outbox_queue_lag_")
    ]
    # Sampler shutdown must not dispose the borrowed host pool.
    await count(engine, table)
