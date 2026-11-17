"""NATS-only scheduling, projection, retry source and telemetry execution."""

import asyncio
import os
import subprocess
import sys
from contextlib import AsyncExitStack, aclosing
from uuid import uuid4

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("taskiq_nats")
pytest.importorskip("faststream.nats")

import nats
from dishka import Provider, Scope
from faststream.nats import JStream, PullSub
from faststream.nats import NatsBroker as NativeNats
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from monitoring_support import observations as observations
from taskiq.receiver import Receiver
from taskiq_nats import NATSKeyValueScheduleSource, PullBasedJetStreamBroker

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.jetstream import JetPublisher
from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
from papilio_tasks.apps.events.subscribers.jetstream import JetSubscriber
from papilio_tasks.apps.projections.application import (
    create_broker as projection_broker,
)
from papilio_tasks.apps.projections.backends.nats import NatsDirect
from papilio_tasks.apps.projections.registry.nats import (
    NatsRegistrar as Projects,
)
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.backends.nats import (
    JetStreamBroker,
    NatsScheduler,
)
from papilio_tasks.apps.schedulers.registry.nats import NatsRegistrar
from papilio_tasks.apps.taskiq import RetryRuntime
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.retry import Retry


@pytest.fixture
def nats_url():
    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    return url


async def test_nats_jobs_projections_shared_kv_retry_and_monitoring(
    nats_url, observations
):
    prefix = "monitor_" + uuid4().hex
    calls = []
    policy = Retry(attempts=2, delay=0, errors=(ConnectionError,))

    class Job(NatsScheduler):
        retry = policy

        async def run(self, value: int) -> None:
            calls.append(("job", value))
            if calls.count(("job", value)) == 1:
                raise ConnectionError("transient")

    class Project(NatsDirect[int, int]):
        retry = policy

        def __init__(self):
            super().__init__()

        async def read(self, value: int) -> int:
            return value

        async def write(self, data: int) -> int:
            calls.append(("project", data))
            if calls.count(("project", data)) == 1:
                raise ConnectionError("transient")
            return data

    def transport(suffix):
        return JetStreamBroker(
            PullBasedJetStreamBroker(
                nats_url,
                subject=prefix + "." + suffix,
                stream_name=prefix + "_" + suffix,
                durable=prefix + "_" + suffix,
                pull_consume_timeout=1,
            )
        )

    jobs, projects = (
        NatsRegistrar(transport("jobs")),
        Projects(transport("projects")),
    )
    jobs.include(Job, name="orders.job")
    projects.include(Project, name="orders.project")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Job)
    provider.provide(Project)
    source = NATSKeyValueScheduleSource(
        nats_url, bucket_name=prefix + "_plans"
    )
    runtime = RetryRuntime(source=source)
    first = create_broker(registrar=jobs, providers=[provider], retry=runtime)
    second = projection_broker(
        registrar=projects, providers=[provider], retry=runtime
    )
    beat = runtime.create_beat(brokers=[first, second])
    first.is_worker_process = second.is_worker_process = True
    observations.monitoring.taskiq(first)
    observations.monitoring.taskiq(second)
    try:
        async with AsyncExitStack() as stack, asyncio.timeout(15):
            await asyncio.gather(first.startup(), second.startup())
            streams = await asyncio.gather(
                stack.enter_async_context(aclosing(first.listen())),
                stack.enter_async_context(aclosing(second.listen())),
            )
            tracer = observations.monitoring.tracer_provider.get_tracer("test")
            with tracer.start_as_current_span("request") as parent:
                handles = await asyncio.gather(
                    Job.enqueue(41), Project.enqueue(42)
                )
            messages = await asyncio.gather(
                *(anext(stream) for stream in streams)
            )
            await asyncio.gather(
                Receiver(first).callback(messages[0]),
                Receiver(second).callback(messages[1]),
            )
            # A fresh native client observes persisted schedules without Redis.
            reopened = NATSKeyValueScheduleSource(
                nats_url, bucket_name=prefix + "_plans"
            )
            await reopened.startup()
            try:
                plans = await reopened.get_schedules()
            finally:
                await reopened.shutdown()
            assert {plan.task_id for plan in plans} == {
                handle.task_id for handle in handles
            }
            await asyncio.gather(
                *(beat.on_ready(beat.sources[0], plan) for plan in plans)
            )
            assert await source.get_schedules() == []
            messages = await asyncio.gather(
                *(anext(stream) for stream in streams)
            )
            await asyncio.gather(
                Receiver(first).callback(messages[0]),
                Receiver(second).callback(messages[1]),
            )
        assert calls.count(("job", 41)) == calls.count(("project", 42)) == 2
        attempts = [
            span
            for span in observations.spans.get_finished_spans()
            if span.name.startswith("execute/")
        ]
        assert len(attempts) == 4
        assert all(
            span.context.trace_id == parent.get_span_context().trace_id
            for span in attempts
        )
        assert sum(p.value for p in observations.points("task_success")) == 2
        assert sum(p.value for p in observations.points("task_errors")) == 2
    finally:
        await asyncio.gather(first.shutdown(), second.shutdown())
        client = await nats.connect(nats_url)
        try:
            js = client.jetstream()
            await asyncio.gather(
                js.delete_key_value(prefix + "_plans"),
                js.delete_stream(prefix + "_jobs"),
                js.delete_stream(prefix + "_projects"),
            )
        finally:
            await client.close()


async def test_real_nats_events_local_retry_and_trace(nats_url, observations):
    name, done, calls = "monitor_" + uuid4().hex, asyncio.Event(), []

    class Created(JetPublisher[int]):
        subject = name + ".created"
        stream = JStream(name)

    class Reader(JetSubscriber[int]):
        publisher = Created
        durable = name
        pull_sub = PullSub(timeout=0.2)
        retry = Retry(attempts=2, delay=0, errors=(ConnectionError,))

        async def run(self, event: int) -> None:
            calls.append(event)
            if len(calls) == 1:
                raise ConnectionError("transient")
            done.set()

    native = NativeNats(nats_url, logger=None)
    observations.monitoring.faststream(native, NatsTelemetryMiddleware)
    registry = JetRegistrar(NatsBroker(native))
    registry.publisher(Created)
    registry.subscriber(Reader)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    app = create_app(registrar=registry, providers=[provider])
    try:
        await app.start()
        tracer = observations.monitoring.tracer_provider.get_tracer("test")
        with tracer.start_as_current_span("request") as parent:
            await Created.publish(42, headers={"event_id": "nats-event"})
        await asyncio.wait_for(done.wait(), 5)
    finally:
        await app.stop()
        client = await nats.connect(nats_url)
        try:
            await client.jetstream().delete_stream(name)
        finally:
            await client.close()
    assert calls == [42, 42]
    spans = [
        span
        for span in observations.spans.get_finished_spans()
        if span.attributes.get("papilio_tasks.event_id") == "nats-event"
    ]
    assert len(spans) == 1
    assert spans[0].context.trace_id == parent.get_span_context().trace_id
    assert (
        sum(
            p.value
            for p in observations.points("papilio_tasks.subscriber.attempts")
        )
        == 2
    )


def test_nats_monitoring_import_and_registration_never_import_redis():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'redis', 'taskiq_redis'}:
            raise AssertionError('Unexpected Redis dependency: ' + fullname)
sys.meta_path.insert(0, Block())
from taskiq_nats import PullBasedJetStreamBroker, NATSKeyValueScheduleSource
from faststream.nats import NatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from papilio_tasks.tools.monitoring import Monitoring
from papilio_tasks.apps.taskiq import RetryRuntime
from papilio_tasks.apps.schedulers.registry.nats import NatsRegistrar
import papilio_tasks.apps.projections.registry.nats
monitor = Monitoring(TracerProvider(), MeterProvider())
monitor.taskiq(PullBasedJetStreamBroker('nats://localhost:4222'))
monitor.faststream(NatsBroker(logger=None), NatsTelemetryMiddleware)
RetryRuntime(source=NATSKeyValueScheduleSource('nats://localhost:4222'))
""",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
