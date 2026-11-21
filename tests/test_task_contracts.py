import asyncio
from datetime import UTC, datetime

import pytest
from dishka import FromDishka, Provider, Scope
from dishka.exceptions import NoFactoryError
from hook_support import handler
from taskiq import TaskiqMessage, TaskiqMiddleware
from test_function_tasks import Payload, Service, Services, result

from papilio_tasks.apps.projections.application import (
    create_broker as projections,
)
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers import Task, TaskRouter
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.infra.taskiq.retry_runtime import RetryRuntime
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.retry import Retry


async def test_contract_scheduler_zero_input_and_original_handler():
    router, services = TaskRouter(), Services()
    refresh = Task[[], int](name="accounts.refresh")

    @router.task(refresh, schedule=[{"interval": 60}])
    async def run(*, service: FromDishka[Service]) -> int:
        assert not service.closed
        return 7

    original = dict(run.__annotations__)
    broker = create_broker(routers=[router], providers=[services])
    assert services.instances == []
    assert refresh.task().labels["schedule"] == [{"interval": 60}]
    await broker.startup()
    try:
        assert await run(service=Service()) == 7
        assert await result(await refresh.enqueue()) == 7
        assert len(services.instances) == 1 and services.instances[0].closed
        assert run.__annotations__ == original
    finally:
        await broker.shutdown()
    with pytest.raises(RuntimeError, match="not included"):
        await refresh.enqueue()
    # The original router can be assembled again after native shutdown.
    second = create_broker(routers=[router], providers=[services])
    await second.startup()
    try:
        assert await result(await refresh.enqueue()) == 7
    finally:
        await second.shutdown()


async def test_contract_payload_wire_schedules_and_concurrent_scopes():
    router, services, messages = TaskRouter(), Services(), []
    refresh = Task[[Payload, int], int](
        name="accounts.selected", labels={"x": 1}
    )
    barrier = asyncio.Barrier(2)

    class Capture(TaskiqMiddleware):
        async def pre_send(self, message: TaskiqMessage):
            messages.append(message)
            return message

    @router.task(refresh, labels={"x": 2})
    async def run(
        payload: Payload, factor: int, *, service: FromDishka[Service]
    ) -> int:
        await barrier.wait()
        assert not service.closed
        return payload.value * factor

    broker = create_broker(routers=[router], providers=[services])
    broker.add_middlewares(Capture())
    source = MemorySource()
    when = datetime(2030, 1, 1, tzinfo=UTC)
    time = await refresh.at(source, when, Payload(value=3), 2)
    cron = await refresh.cron(source, "* * * * *", Payload(value=4), 3)
    interval = await refresh.every(source, 60, Payload(value=5), 4)
    plans = await asyncio.gather(
        *(
            source.get_schedule(item.schedule_id)
            for item in (time, cron, interval)
        )
    )
    assert [plan.args[1] for plan in plans] == [2, 3, 4]
    assert all("service" not in plan.kwargs for plan in plans)
    assert refresh.labels == {"x": 1}
    assert refresh.task().labels["x"] == 2
    await broker.startup()
    try:
        handles = await asyncio.gather(
            refresh.enqueue(Payload(value=3), 2),
            refresh.enqueue(Payload(value=4), 3),
        )
        assert await asyncio.gather(*(result(item) for item in handles)) == [
            6,
            12,
        ]
        assert len(services.instances) == 2
        assert services.instances[0] is not services.instances[1]
        assert all(item.closed for item in services.instances)
        assert [item.args for item in messages] == [
            [{"value": 3}, 2],
            [{"value": 4}, 3],
        ]
        assert all("service" not in item.kwargs for item in messages)
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("with_input", [False, True])
async def test_projection_contract_scopes_stages_and_publication_hooks(
    with_input,
):
    router, services, phases = ProjectionRouter(), Services(), []
    reference = Task(name="projection.contract")

    async def read(value: int, *, service: FromDishka[Service]) -> int:
        phases.append(("read", service))
        return value

    async def read_all(*, service: FromDishka[Service]) -> int:
        return await read(4, service=service)

    async def transform(value: int) -> str:
        return str(value)

    class Writer:
        pass

    async def write(
        value: str, *, writer: FromDishka[Writer], service: FromDishka[Service]
    ) -> int:
        phases.append(("write", service))
        return int(value) * 2

    async def after_send(event):
        phases.append(("published", event.call.meta["task_name"]))

    class Local(PublishHooks):
        def __init__(self):
            super().__init__(after_send=(handler(after_send),))

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Writer)
    provider.provide(Local)
    task = router.projection(
        reference,
        read=read if with_input else read_all,
        transform=transform,
        write=write,
        publish_hooks=Local,
    )
    broker = projections(routers=[router], providers=[services, provider])
    assert reference.task() is task.task()
    await broker.startup()
    try:
        handle = (
            await reference.enqueue(4)
            if with_input
            else await reference.enqueue()
        )
        assert await result(handle) == 8
        assert ("published", "projection.contract") in phases
        stage_services = [
            item for phase, item in phases if phase in {"read", "write"}
        ]
        assert stage_services[0] is stage_services[1]
        assert len(services.instances) == 1 and stage_services[0].closed
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_contract_retry_preserves_policy_and_fresh_request_scope(family):
    router, services, calls = TaskRouter(), Services(), []
    finished = asyncio.Event()
    refresh = Task[[], None](
        name="retry.contract",
        retry=Retry(attempts=2, delay=0, errors=(ValueError,)),
    )

    async def run(*, service: FromDishka[Service]) -> None:
        calls.append(service)
        if len(calls) == 1:
            raise ValueError("transient")
        finished.set()

    if family == "scheduler":
        router.task(refresh)(run)
        factory = create_broker
    else:
        router = ProjectionRouter()

        async def stage(value: None) -> None:
            pass

        router.projection(refresh, read=run, transform=stage, write=stage)
        factory = projections
    broker = factory(
        routers=[router], providers=[services], retry=RetryRuntime.immediate()
    )
    await broker.startup()
    try:
        await refresh.enqueue()
        await asyncio.wait_for(finished.wait(), 5)
        await broker.wait_all()
        assert calls[0] is not calls[1]
        assert all(item.closed for item in calls)
    finally:
        await broker.shutdown()


def test_contract_invalid_and_duplicate_declarations():
    with pytest.raises(ValueError, match="name"):
        Task()
    reference = Task[[], None](name="only.once")
    router = TaskRouter()

    async def run() -> None:
        pass

    router.task(reference)(run)
    with pytest.raises(ValueError, match="already"):
        TaskRouter().task(reference)(run)
    with pytest.raises(ValueError, match="name"):
        TaskRouter().task(Task(name="stable"), name="different")(run)
    with pytest.raises(TypeError, match="payload|dependency"):
        TaskRouter().task(Task(name="mixed"), payload=Payload)(run)

    async def positional_dependency(
        first: int, service: FromDishka[Service], second: int
    ) -> None:
        pass

    with pytest.raises(TypeError, match="keyword-only"):
        TaskRouter().task(Task(name="positional"))(positional_dependency)


@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_contract_missing_provider_and_cancellation_close_scope(family):
    router, services = TaskRouter(), Services()
    reference = Task[[], None](name="cancelled.contract")

    async def run(*, service: FromDishka[Service]) -> None:
        raise asyncio.CancelledError

    if family == "scheduler":
        router.task(reference)(run)
        factory = create_broker
    else:
        router = ProjectionRouter()

        async def stage(value: None) -> None:
            pass

        router.projection(reference, read=run, transform=stage, write=stage)
        factory = projections
    missing = factory(routers=[router])
    await missing.startup()
    try:
        with pytest.raises(NoFactoryError):
            await result(await reference.enqueue())
    finally:
        await missing.shutdown()
    broker = factory(routers=[router], providers=[services])
    await broker.startup()
    try:
        await reference.enqueue()
        await broker.wait_all()
        assert len(services.instances) == 1 and services.instances[0].closed
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()
