"""Shared native retry dispatch and the distinct Redis retention contracts."""

import asyncio
import os
from uuid import uuid4

import pytest
from dishka import Provider, Scope
from redis.asyncio import Redis
from taskiq.receiver import Receiver

from papilio_tasks.apps.projections.application import (
    create_broker as projection_broker,
)
from papilio_tasks.apps.projections.backends.redis import RedisDirect
from papilio_tasks.apps.projections.registry.redis import (
    RedisRegistrar as Projections,
)
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.backends.redis import (
    RedisScheduler,
    RedisStreamBroker,
)
from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar
from papilio_tasks.apps.taskiq import RetryRuntime
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource
from papilio_tasks.tools.retry import Retry

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_REDIS_URL"), reason="No test Redis"
)


async def test_shared_source_native_workers_routes_ids_and_schedule_cleanup():
    url = os.environ["TEST_REDIS_URL"]
    prefix = "shared-retry-" + uuid4().hex
    calls = []

    class Job(RedisScheduler):
        retry = Retry(attempts=2, delay=0, errors=(ConnectionError,))

        async def run(self, value: int):
            calls.append(("job", value))
            if calls.count(("job", value)) == 1:
                raise ConnectionError("temporary")

    class Project(RedisDirect[int, int]):
        retry = Job.retry

        def __init__(self):
            super().__init__()

        async def read(self, value: int) -> int:
            return value

        async def write(self, data: int) -> int:
            calls.append(("project", data))
            if calls.count(("project", data)) == 1:
                raise ConnectionError("temporary")
            return data

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Job)
    provider.provide(Project)
    jobs = RedisRegistrar(
        RedisStreamBroker(
            url,
            queue_name=prefix + "-jobs",
            consumer_group_name=prefix + "-jobs",
            consumer_id="0",
        )
    )
    projects = Projections(
        RedisStreamBroker(
            url,
            queue_name=prefix + "-projects",
            consumer_group_name=prefix + "-projects",
            consumer_id="0",
        )
    )
    jobs.include(Job)
    projects.include(Project)
    source = RedisSource(url, prefix=prefix + "-plans")
    runtime = RetryRuntime(source=source)
    first = create_broker(registrar=jobs, providers=[provider], retry=runtime)
    second = projection_broker(
        registrar=projects, providers=[provider], retry=runtime
    )
    beat = runtime.create_beat(brokers=[first, second])
    first.is_worker_process = second.is_worker_process = True
    streams = [first.listen(), second.listen()]
    try:
        async with asyncio.timeout(10):
            await asyncio.gather(first.startup(), second.startup())
            handles = await asyncio.gather(
                Job.enqueue(41), Project.enqueue(42)
            )
            initial = await asyncio.gather(
                *(anext(stream) for stream in streams)
            )
            await asyncio.gather(
                Receiver(first).callback(initial[0]),
                Receiver(second).callback(initial[1]),
            )
            plans = await source.list_schedules()
            assert {task.task_id for task in plans} == {
                h.task_id for h in handles
            }
            await asyncio.gather(
                *(beat.on_ready(beat.sources[0], task) for task in plans)
            )
            retried = await asyncio.gather(
                *(anext(stream) for stream in streams)
            )
            await asyncio.gather(
                Receiver(first).callback(retried[0]),
                Receiver(second).callback(retried[1]),
            )
            assert calls.count(("job", 41)) == 2
            assert calls.count(("project", 42)) == 2
            assert await source.list_schedules() == []
            async with Redis.from_url(url) as redis:
                assert [
                    key async for key in redis.scan_iter(prefix + "-plans:*")
                ] == []
    finally:
        await asyncio.gather(*(stream.aclose() for stream in streams))
        await asyncio.gather(first.shutdown(), second.shutdown())
        async with Redis.from_url(url) as redis:
            keys = [key async for key in redis.scan_iter("*" + prefix + "*")]
            if keys:
                await redis.delete(*keys)


async def test_stream_acknowledgement_is_distinct_from_record_retention():
    url = os.environ["TEST_REDIS_URL"]
    name = "retention-" + uuid4().hex
    adapter = RedisStreamBroker(
        url,
        queue_name=name,
        consumer_group_name=name,
        consumer_id="0",
    )

    async def plain(value: int):
        return value

    task = adapter.register(plain, name=name)
    broker = adapter.native
    stream = broker.listen()
    try:
        await broker.startup()
        await task.kiq(1)
        async with asyncio.timeout(5):
            message = await anext(stream)
            await message.ack()
        async with Redis.from_url(url) as redis:
            assert (await redis.xpending(name, name))["pending"] == 0
            assert await redis.xlen(name) == 1
        assert broker.maxlen is None
    finally:
        await stream.aclose()
        await broker.shutdown()
        async with Redis.from_url(url) as redis:
            await redis.delete(name)
