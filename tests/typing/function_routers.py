# pyright: strict, reportUnusedFunction=false
from datetime import datetime
from typing import assert_type

from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import AsyncTaskiqTask
from taskiq.scheduler.created_schedule import CreatedSchedule

from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.outbox import Outbox, OutboxWriter
from papilio_tasks.apps.outbox.decorators import publish as outbox_publish
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers.router import TaskRouter
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource


class Service:
    pass


jobs = TaskRouter()


@jobs.task()
async def plain(value: int, /, *, suffix: str = "!") -> str:
    return str(value) + suffix


@jobs.task(dependency=Service)
async def injected(service: Service, value: int, *, factor: int = 2) -> int:
    return value * factor


async def read(service: Service, value: int, *, page: int = 1) -> list[int]:
    return [value, page]


async def transform(service: Service, values: list[int]) -> str:
    return str(values)


async def write(service: Service, value: str) -> int:
    return len(value)


projections = ProjectionRouter()
projected = projections.projection(
    read=read, transform=transform, write=write, dependency=Service
)
events = NatsRouter()
created = events.publisher(int, subject="created", outbox_key="created.v1")


@events.subscriber("created", dependency=Service)
async def receive(service: Service, event: int) -> None:
    pass


class Command:
    def __init__(self, outbox: OutboxWriter):
        self.outbox = outbox

    @outbox_publish(created, select=lambda value: value)
    async def create(self, value: int) -> int:
        return value


async def usage(
    runtime: Outbox, session: AsyncSession, source: MemorySource, now: datetime
) -> None:
    assert_type(await plain.enqueue(1, suffix="?"), AsyncTaskiqTask[str])
    assert_type(await injected.enqueue(1, factor=3), AsyncTaskiqTask[int])
    assert_type(await projected.enqueue(1, page=2), AsyncTaskiqTask[int])
    assert_type(await plain.at(source, now, 1), CreatedSchedule[str])
    assert_type(await plain.cron(source, "* * * * *", 1), CreatedSchedule[str])
    assert_type(await plain.every(source, 60, 1), CreatedSchedule[str])
    await receive(service=Service(), event=1)
    await receive(Service(), "bad")  # expect-error
    await created.publish(1)
    await runtime.writer(session).append(created, 1)
    await runtime.writer(session).append_batch(created, [1, 2])
    await plain.enqueue("bad")  # expect-error
    await plain.enqueue(1, missing=True)  # expect-error
    await injected.enqueue(Service(), 1)  # expect-error
    await injected.enqueue("bad")  # expect-error
    await projected.enqueue("bad")  # expect-error
    await projected.enqueue(1, page="bad")  # expect-error
    await plain.at(source, now, "bad")  # expect-error
    await created.publish("bad")  # expect-error
    await runtime.writer(session).append(created, "bad")  # expect-error
