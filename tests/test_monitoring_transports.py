"""Observe real Taskiq transports without requiring a result backend."""

import asyncio
import os
from contextlib import aclosing
from uuid import uuid4

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("taskiq_aio_pika")
pytest.importorskip("taskiq_aio_kafka")
pytest.importorskip("taskiq_redis")

from dishka import Provider, Scope
from kafka.admin import NewTopic
from monitoring_support import observations as observations
from taskiq.receiver import Receiver
from taskiq_aio_kafka import AioKafkaBroker
from taskiq_aio_pika import Exchange, Queue

from papilio_tasks.apps.projections.application import (
    create_broker as projection_broker,
)
from papilio_tasks.apps.projections.backends.kafka import KafkaDirect
from papilio_tasks.apps.projections.backends.rabbit import RabbitDirect
from papilio_tasks.apps.projections.backends.redis import RedisDirect
from papilio_tasks.apps.projections.registry.kafka import (
    KafkaRegistrar as KafkaProjects,
)
from papilio_tasks.apps.projections.registry.rabbit import (
    RabbitRegistrar as RabbitProjects,
)
from papilio_tasks.apps.projections.registry.redis import (
    RedisRegistrar as RedisProjects,
)
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.backends.kafka import (
    KafkaBroker,
    KafkaScheduler,
)
from papilio_tasks.apps.schedulers.backends.rabbit import (
    RabbitBroker,
    RabbitScheduler,
)
from papilio_tasks.apps.schedulers.backends.redis import (
    RedisScheduler,
    RedisStreamBroker,
)
from papilio_tasks.apps.schedulers.registry.kafka import KafkaRegistrar
from papilio_tasks.apps.schedulers.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar


@pytest.mark.parametrize("kind", ["redis", "rabbit", "kafka"])
@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_real_transport_execution_and_monitoring(
    observations, kind, family
):
    url = os.environ.get(f"TEST_{kind.upper()}_URL")
    if not url:
        pytest.skip("Requires isolated " + kind)
    name = "monitor_" + uuid4().hex
    if kind == "redis":
        transport = RedisStreamBroker(
            url, queue_name=name, consumer_group_name=name, consumer_id="0"
        )
        scheduler, direct = RedisScheduler, RedisDirect
        jobs, projects = RedisRegistrar, RedisProjects
    elif kind == "rabbit":
        transport = RabbitBroker(
            url,
            queues=[Queue(name=name)],
            exchange=Exchange(name=name),
            dead_letter_queue=Queue(name=name + "_dead"),
        )
        scheduler, direct = RabbitScheduler, RabbitDirect
        jobs, projects = RabbitRegistrar, RabbitProjects
    else:
        native = AioKafkaBroker(
            url,
            kafka_topic=name,
            kafka_topics=[NewTopic(name, 1, 1)],
            delete_topic_on_shutdown=True,
        )
        native.configure_consumer(group_id=name, auto_offset_reset="earliest")
        transport = KafkaBroker(native)
        scheduler, direct = KafkaScheduler, KafkaDirect
        jobs, projects = KafkaRegistrar, KafkaProjects
    seen = []

    class Job(scheduler):
        async def run(self, value: int) -> None:
            seen.append(value)

    class Project(direct[int, int]):
        def __init__(self):
            super().__init__()

        async def read(self, value: int) -> int:
            return value

        async def write(self, data: int) -> int:
            seen.append(data)
            return data

    cls = Job if family == "scheduler" else Project
    registry = (
        jobs(transport) if family == "scheduler" else projects(transport)
    )
    registry.include(cls, name=f"orders.{family}")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(cls)
    assemble = create_broker if family == "scheduler" else projection_broker
    broker = assemble(registrar=registry, providers=[provider])
    broker.is_worker_process = True
    observations.monitoring.taskiq(broker)
    try:
        await broker.startup()
        tracer = observations.monitoring.tracer_provider.get_tracer("test")
        with tracer.start_as_current_span("request") as parent:
            handle = await cls.enqueue(42)
        async with aclosing(broker.listen()) as messages, asyncio.timeout(20):
            await Receiver(broker).callback(await anext(messages))
        assert seen == [42]
        spans = [
            span
            for span in observations.spans.get_finished_spans()
            if span.name.startswith("execute/")
        ]
        assert len(spans) == 1
        assert spans[0].context.trace_id == parent.get_span_context().trace_id
        assert spans[0].attributes["messaging.message.id"] == handle.task_id
        assert observations.points("task_success")[0].value == 1
        assert observations.points("worker_active_tasks")[0].value == 0
    finally:
        await broker.shutdown()
