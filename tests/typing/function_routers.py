# pyright: strict, reportUnusedFunction=false
from datetime import datetime
from typing import assert_type

from dishka import FromDishka
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from taskiq import AsyncTaskiqTask
from taskiq.scheduler.created_schedule import CreatedSchedule

from papilio_tasks.apps.events.publishers.base import PublisherRoute
from papilio_tasks.apps.events.publishers.nats import publisher
from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.outbox import Outbox, OutboxWriter
from papilio_tasks.apps.outbox.decorators import publish as outbox_publish
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers.router import TaskRouter
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource


class Service:
    pass


class Cache:
    pass


class Payload(BaseModel):
    value: int


jobs = TaskRouter()


@jobs.task(payload=Payload)
async def marked(
    payload: Payload,
    *,
    service: FromDishka[Service],
    cache: FromDishka[Cache],
) -> int:
    return payload.value


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
independent = publisher(Payload, subject="independent")
assert_type(independent, PublisherRoute[Payload])


async def independent_publication_types() -> None:
    await independent.publish(Payload(value=1))
    await independent.publish("wrong")  # expect-error
    publisher(Payload, subject=123)  # expect-error


async def marked_read(
    payload: Payload, *, service: FromDishka[Service]
) -> list[int]:
    return [payload.value]


async def marked_transform(
    data: list[int], *, cache: FromDishka[Cache]
) -> str:
    return str(data)


async def marked_write(data: str, *, service: FromDishka[Service]) -> int:
    return len(data)


native_projection = projections.projection(
    payload=Payload,
    read=marked_read,
    transform=marked_transform,
    write=marked_write,
)


@events.subscriber("native")
async def marked_receive(
    event: int,
    *,
    service: FromDishka[Service],
    cache: FromDishka[Cache],
) -> None:
    pass


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
    message = Payload(value=1)
    assert_type(await marked.enqueue(message), AsyncTaskiqTask[int])
    assert_type(await marked.at(source, now, message), CreatedSchedule[int])
    assert_type(
        await marked.cron(source, "* * * * *", message), CreatedSchedule[int]
    )
    assert_type(await marked.every(source, 60, message), CreatedSchedule[int])
    assert_type(await native_projection.enqueue(message), AsyncTaskiqTask[int])
    await marked_receive(1, service=Service(), cache=Cache())
    await marked.enqueue(1)  # expect-error
    await marked.enqueue(message, Service())  # expect-error
    await marked.enqueue(message, service=Service())  # expect-error
    await marked.at(source, now, 1)  # expect-error
    await native_projection.enqueue(1)  # expect-error
    await marked_receive(1)  # expect-error
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
