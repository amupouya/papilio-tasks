"""Inferred scheduler inputs/results and deliberately rejected calls."""

from typing import assert_type

from pydantic import BaseModel
from taskiq import AsyncTaskiqTask

from papilio_tasks.apps.schedulers import Scheduler
from papilio_tasks.apps.schedulers.backends.kafka import KafkaScheduler
from papilio_tasks.apps.schedulers.backends.nats import NatsScheduler
from papilio_tasks.apps.schedulers.backends.rabbit import RabbitScheduler
from papilio_tasks.apps.schedulers.backends.redis import RedisScheduler


class Report(Scheduler):
    async def run(self, value: int, *, suffix: str = "!") -> str:
        return f"{value}{suffix}"


class Inherited(Report):
    pass


class Positional(Scheduler):
    async def run(self, value: int, /, *, enabled: bool) -> bool:
        return bool(value) and enabled


class Variadic(Scheduler):
    async def run(self, *values: int, **options: str) -> tuple[int, str]:
        return sum(values), options.get("suffix", "")


class Empty(Scheduler):
    async def run(self) -> None:
        pass


class Payload(BaseModel):
    value: int


class TypedPayload(Scheduler):
    async def run(self, payload: Payload) -> int:
        return payload.value


class RabbitReport(RabbitScheduler):
    async def run(self, value: int) -> str:
        return str(value)


class RedisReport(RedisScheduler):
    async def run(self, value: int) -> int:
        return value


class NatsReport(NatsScheduler):
    async def run(self, value: int) -> bool:
        return bool(value)


class KafkaReport(KafkaScheduler):
    async def run(self, value: int) -> list[int]:
        return [value]


async def valid() -> None:
    assert_type(await Report.enqueue(1), AsyncTaskiqTask[str])
    assert_type(
        await Report.enqueue(value=1, suffix="?"), AsyncTaskiqTask[str]
    )
    assert_type(await Inherited.enqueue(1), AsyncTaskiqTask[str])
    assert_type(await Report().enqueue(1), AsyncTaskiqTask[str])
    assert_type(
        await Positional.enqueue(1, enabled=True), AsyncTaskiqTask[bool]
    )
    assert_type(await Variadic.enqueue(), AsyncTaskiqTask[tuple[int, str]])
    assert_type(
        await Variadic.enqueue(1, 2, suffix="!"),
        AsyncTaskiqTask[tuple[int, str]],
    )
    assert_type(await Empty.enqueue(), AsyncTaskiqTask[None])
    assert_type(
        await TypedPayload.enqueue(Payload(value=1)), AsyncTaskiqTask[int]
    )
    assert_type(await RabbitReport.enqueue(1), AsyncTaskiqTask[str])
    assert_type(await RedisReport.enqueue(1), AsyncTaskiqTask[int])
    assert_type(await NatsReport.enqueue(1), AsyncTaskiqTask[bool])
    assert_type(await KafkaReport.enqueue(1), AsyncTaskiqTask[list[int]])
    submitted = await Report.enqueue(1)
    completed = await submitted.wait_result()
    assert_type(completed.return_value, str)


async def invalid() -> None:
    await Report.enqueue("bad")  # error
    await Report.enqueue(1, suffix=2)  # error
    await Report.enqueue()  # error
    await Report.enqueue(1, extra=True)  # error
    await Report.enqueue(1, "!")  # error
    await Report.enqueue(1, value=2)  # error
    await Inherited.enqueue("bad")  # error
    await Positional.enqueue(value=1, enabled=True)  # error
    await Positional.enqueue(1)  # error
    await Variadic.enqueue("bad")  # error
    await Variadic.enqueue(1, suffix=2)  # error
    await Empty.enqueue(1)  # error
    await TypedPayload.enqueue({"value": 1})  # error
    await RabbitReport.enqueue("bad")  # error
    await RedisReport.enqueue("bad")  # error
    await NatsReport.enqueue("bad")  # error
    await KafkaReport.enqueue("bad")  # error
