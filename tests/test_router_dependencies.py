import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated

import pytest
from dishka import (
    FromComponent,
    FromDishka,
    Provider,
    Scope,
    make_async_container,
    provide,
)
from dishka.exceptions import NoFactoryError
from faststream.nats import NatsBroker as NativeNats
from faststream.nats import TestNatsBroker
from pydantic import BaseModel
from taskiq import TaskiqMessage, TaskiqMiddleware

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.projections.application import (
    create_broker as projections,
)
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers import TaskRouter
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.registry import Registrar
from papilio_tasks.infra.dishka import Function
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.infra.taskiq.retry_runtime import RetryRuntime
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.retry import Retry


class Input(BaseModel):
    value: int


class Store:
    def __init__(self):
        self.closed = False


class Cache:
    def __init__(self):
        self.closed = False


class Resources(Provider):
    scope = Scope.REQUEST

    def __init__(self, component=None):
        super().__init__(component=component)
        self.instances = []

    @provide
    async def store(self) -> AsyncIterator[Store]:
        item = Store()
        self.instances.append(item)
        try:
            yield item
        finally:
            item.closed = True

    @provide
    async def cache(self) -> AsyncIterator[Cache]:
        item = Cache()
        self.instances.append(item)
        try:
            yield item
        finally:
            item.closed = True


async def value(handle):
    result = await handle.wait_result(timeout=5)
    if result.is_err:
        raise result.error
    return result.return_value


async def test_task_multiple_native_dependencies_payload_and_schedule():
    router, resources, messages = TaskRouter(), Resources(), []

    @router.task(payload=Input)
    async def task(
        data: Input,
        *,
        store: FromDishka[Store],
        cache: FromDishka[Cache],
        same: FromDishka[Store],
    ) -> int:
        assert isinstance(data, Input)
        assert store is same and not store.closed and not cache.closed
        return data.value * 2

    original = dict(task.function.__annotations__)

    class Capture(TaskiqMiddleware):
        async def pre_send(self, message: TaskiqMessage):
            messages.append(message)
            return message

    registry = Registrar()
    registry.broker.native.add_middlewares(Capture())
    broker = create_broker(
        registrar=registry, routers=[router], providers=[resources]
    )
    assert not resources.instances
    assert task.function.__annotations__ == original
    assert set(task.task().original_func.__annotations__) == {
        "data",
        "return",
    }
    source = MemorySource()
    plan = await task.at(
        source, datetime(2030, 1, 1, tzinfo=UTC), Input(value=3)
    )
    scheduled = await source.get_schedule(plan.schedule_id)
    assert len(scheduled.args) == 1 and not scheduled.kwargs
    await broker.startup()
    try:
        assert await value(await task.enqueue(Input(value=3))) == 6
        await broker.wait_all()
        assert messages[0].args == [{"value": 3}]
        assert not messages[0].kwargs
        assert len(resources.instances) == 2
        assert all(item.closed for item in resources.instances)
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()


async def test_native_component_markers_use_existing_provider_components():
    router, main, replica = TaskRouter(), Resources(), Resources("replica")

    @router.task(payload=Input)
    async def task(
        data: Input,
        *,
        primary: FromDishka[Store],
        secondary: Annotated[Store, FromComponent("replica")],
    ) -> int:
        assert primary is not secondary
        return data.value

    broker = create_broker(routers=[router], providers=[main, replica])
    await broker.startup()
    try:
        assert await value(await task.enqueue(Input(value=7))) == 7
        await broker.wait_all()
        assert len(main.instances) == len(replica.instances) == 1
        assert main.instances[0].closed and replica.instances[0].closed
    finally:
        await broker.shutdown()


async def test_projection_stages_have_independent_native_dependencies():
    router, resources, stages = ProjectionRouter(), Resources(), []

    async def read(data: Input, *, store: FromDishka[Store]) -> list[int]:
        stages.append(("read", store))
        return [data.value]

    async def transform(
        data: list[int],
        *,
        cache: FromDishka[Cache],
        store: FromDishka[Store],
    ) -> str:
        assert not cache.closed
        stages.append(("transform", store))
        return str(data[0])

    async def write(data: str, *, store: FromDishka[Store]) -> int:
        stages.append(("write", store))
        return int(data) * 2

    task = router.projection(
        payload=Input, read=read, transform=transform, write=write
    )
    broker = projections(routers=[router], providers=[resources])
    await broker.startup()
    try:
        assert await value(await task.enqueue(Input(value=4))) == 8
        await broker.wait_all()
        assert [name for name, _ in stages] == ["read", "transform", "write"]
        assert stages[0][1] is stages[1][1] is stages[2][1]
        assert len(resources.instances) == 2
        assert all(item.closed for item in resources.instances)
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("family", ["scheduler", "events"])
async def test_native_dependencies_reopen_for_retry_attempts(family):
    resources, calls, done = Resources(), [], asyncio.Event()

    async def work(
        data: Input, *, store: FromDishka[Store], cache: FromDishka[Cache]
    ) -> None:
        calls.append((store, cache))
        if len(calls) == 1:
            raise ConnectionError("transient")
        done.set()

    policy = Retry(attempts=2, delay=0, errors=(ConnectionError,))
    if family == "scheduler":
        router = TaskRouter()
        task = router.task(payload=Input, retry=policy)(work)
        broker = create_broker(
            routers=[router],
            providers=[resources],
            retry=RetryRuntime.immediate(),
        )
        await broker.startup()
        try:
            await task.enqueue(Input(value=1))
            await asyncio.wait_for(done.wait(), 5)
            await broker.wait_all()
        finally:
            await broker.shutdown()
    else:
        router = NatsRouter()
        created = router.publisher(Input, subject="created")
        router.subscriber("created", retry=policy)(work)
        native = NativeNats(logger=None)
        app = create_app(
            registrar=NatsRegistrar(NatsBroker(native)),
            routers=[router],
            providers=[resources],
        )
        try:
            async with TestNatsBroker(native):
                await created.publish(Input(value=1))
        finally:
            await app.stop()
    assert len(calls) == 2
    assert calls[0][0] is not calls[1][0] and calls[0][1] is not calls[1][1]
    assert all(item.closed for item in resources.instances)


async def test_events_dependencies_preceding_payload_and_local_call():
    router, resources, seen = NatsRouter(), Resources(), []
    created = router.publisher(Input, subject="created")

    @router.subscriber("created")
    async def receive(
        store: FromDishka[Store],
        event: Input,
        *,
        cache: FromDishka[Cache],
    ) -> None:
        assert not store.closed and not cache.closed
        seen.append(event.value)

    original = dict(receive.__annotations__)
    native = NativeNats(logger=None)
    app = create_app(
        registrar=NatsRegistrar(NatsBroker(native)),
        routers=[router],
        providers=[resources],
    )
    try:
        async with TestNatsBroker(native):
            await created.publish(Input(value=3))
        await receive(Store(), Input(value=4), cache=Cache())
        assert seen == [3, 4]
        assert receive.__annotations__ == original
        assert all(item.closed for item in resources.instances)
    finally:
        await app.stop()


async def test_missing_native_provider_and_cancellation_cleanup():
    router, resources = TaskRouter(), Resources()

    @router.task(payload=Input)
    async def task(data: Input, *, store: FromDishka[Store]) -> None:
        raise asyncio.CancelledError

    broker = create_broker(routers=[router])
    await broker.startup()
    try:
        with pytest.raises(NoFactoryError):
            await value(await task.enqueue(Input(value=1)))
    finally:
        await broker.shutdown()
    broker = create_broker(routers=[router], providers=[resources])
    await broker.startup()
    try:
        await task.enqueue(Input(value=1))
        await broker.wait_all()
        assert len(resources.instances) == 1 and resources.instances[0].closed
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()


async def test_positional_only_payload_with_native_dependencies():
    router, resources = TaskRouter(), Resources()

    @router.task(payload=Input)
    async def task(data: Input, /, *, store: FromDishka[Store]) -> int:
        assert not store.closed
        return data.value

    broker = create_broker(routers=[router], providers=[resources])
    await broker.startup()
    try:
        assert await value(await task.enqueue(Input(value=3))) == 3
        await broker.wait_all()
        assert resources.instances[0].closed
    finally:
        await broker.shutdown()


def test_payload_contract_and_dependency_errors_are_explicit():
    async def marked(data: Input, *, store: FromDishka[Store]) -> None:
        pass

    with pytest.raises(TypeError, match="payload="):
        TaskRouter().task()(marked)
    with pytest.raises(TypeError, match="payload="):
        TaskRouter().task(payload=int)(marked)

    async def extra(data: Input, extra: int, *, store: FromDishka[Store]):
        pass

    with pytest.raises(TypeError, match="one positional"):
        TaskRouter().task(payload=Input)(extra)
    with pytest.raises(TypeError, match="not dependency="):
        TaskRouter().task(payload=Input, dependency=Store)(marked)

    async def mixed(store: Store, data: Input, *, cache: FromDishka[Cache]):
        pass

    with pytest.raises(TypeError, match="not both"):
        TaskRouter().task(dependency=Store)(mixed)


@pytest.mark.parametrize("placement", ["first", "before"])
async def test_bound_native_payload_arguments_retain_python_validation(
    placement,
):
    calls, resources = [], Resources()
    if placement == "first":

        async def handler(data: Input, *, store: FromDishka[Store]) -> int:
            assert not store.closed
            calls.append(data.value)
            return data.value

    else:

        async def handler(store: FromDishka[Store], data: Input) -> int:
            assert not store.closed
            calls.append(data.value)
            return data.value

    call = Function(handler)
    container = make_async_container(resources)
    try:
        async with container() as scope:
            bound = await call.bind(scope)
            assert await bound(Input(value=1)) == 1
            assert await bound(data=Input(value=2)) == 2
            with pytest.raises(TypeError):
                await bound()
            with pytest.raises(TypeError):
                await bound(Input(value=3), Input(value=4))
            with pytest.raises(TypeError):
                await bound(Input(value=3), data=Input(value=4))
            with pytest.raises(TypeError):
                await bound(unknown=Input(value=5))
            assert calls == [1, 2]
        assert len(resources.instances) == 1 and resources.instances[0].closed
    finally:
        await container.close()
