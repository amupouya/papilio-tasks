import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from importlib import import_module
from textwrap import dedent

import pytest
from dishka import FromDishka, Provider, Scope, provide
from dishka.exceptions import NoFactoryError
from hook_support import handler
from pydantic import BaseModel
from taskiq import TaskiqMessage, TaskiqMiddleware

from papilio_tasks.apps.projections import Hooks
from papilio_tasks.apps.projections.application import (
    create_broker as projections,
)
from papilio_tasks.apps.projections.registry import (
    Registrar as ProjectionRegistrar,
)
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers import Scheduler, TaskRouter
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.registry import Registrar
from papilio_tasks.infra.taskiq.retry_runtime import RetryRuntime
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.retry import Retry


class Payload(BaseModel):
    value: int


class Service:
    def __init__(self):
        self.closed = False


class Services(Provider):
    scope = Scope.REQUEST

    def __init__(self):
        super().__init__()
        self.instances = []

    @provide
    async def service(self) -> AsyncIterator[Service]:
        item = Service()
        self.instances.append(item)
        try:
            yield item
        finally:
            item.closed = True


async def result(task):
    completed = await task.wait_result(timeout=5)
    if completed.is_err:
        raise completed.error
    return completed.return_value


async def test_payload_signature_and_request_lifecycle():
    router, services, messages = TaskRouter(), Services(), []

    class Capture(TaskiqMiddleware):
        async def pre_send(self, message: TaskiqMessage):
            messages.append(message)
            return message

    @router.task(dependency=Service)
    async def refresh(
        service: Service, payload: Payload, *, factor: int = 2
    ) -> int:
        assert not service.closed
        await asyncio.sleep(0)
        return payload.value * factor

    @router.task()
    async def plain(value: int, /, *, offset: int = 1) -> int:
        return value + offset

    original = dict(refresh.function.__annotations__)
    registrar = Registrar()
    registrar.broker.native.add_middlewares(Capture())
    broker = create_broker(
        registrar=registrar, routers=[router, router], providers=[services]
    )
    assert services.instances == []
    assert len(broker.local_task_registry) == 2
    assert "service" not in refresh.task().original_func.__annotations__
    assert refresh.function.__annotations__ == original
    await broker.startup()
    try:
        sent = await asyncio.gather(
            refresh.enqueue(Payload(value=3)),
            refresh.enqueue(Payload(value=4), factor=3),
        )
        assert await asyncio.gather(*(result(task) for task in sent)) == [
            6,
            12,
        ]
        assert await result(await plain.enqueue(8, offset=2)) == 10
        assert all(
            row.args[0] in ({"value": 3}, {"value": 4}, 8) for row in messages
        )
        assert all("service" not in row.kwargs for row in messages)
        assert len(services.instances) == 2
        assert all(item.closed for item in services.instances)
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()
    with pytest.raises(RuntimeError, match="not included"):
        await refresh.enqueue(Payload(value=1))


async def test_function_schedules_and_classes_share_one_broker():
    router = TaskRouter()

    @router.task(name="function", labels={"schedule": [{"cron": "* * * * *"}]})
    async def job(value: int) -> int:
        return value * 2

    class ClassJob(Scheduler):
        async def run(self, value: int) -> int:
            return value + 1

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(ClassJob)
    registry = Registrar()
    registry.include(ClassJob, name="class")
    broker = create_broker(
        registrar=registry, routers=[router], providers=[provider]
    )
    source = MemorySource()
    plan = await job.at(source, datetime(2030, 1, 1, tzinfo=UTC), 3)
    assert (await source.get_schedule(plan.schedule_id)).args == [3]
    await plan.unschedule()
    cron = await job.cron(source, "* * * * *", 4)
    interval = await job.every(source, 60, 5)
    assert (await source.get_schedule(cron.schedule_id)).cron == "* * * * *"
    assert (await source.get_schedule(interval.schedule_id)).interval == 60
    await broker.startup()
    try:
        assert await result(await ClassJob.enqueue(2)) == 3
        assert await result(await job.enqueue(2)) == 4
    finally:
        await broker.shutdown()


async def test_function_retry_uses_existing_runtime_and_new_request_scopes():
    router, services, calls = TaskRouter(), Services(), []
    finished = asyncio.Event()

    @router.task(
        dependency=Service,
        retry=Retry(attempts=2, delay=0, errors=(ValueError,)),
    )
    async def job(service: Service, value: int) -> int:
        calls.append(service)
        if len(calls) == 1:
            raise ValueError("transient")
        finished.set()
        return value

    broker = create_broker(
        routers=[router], providers=[services], retry=RetryRuntime.immediate()
    )
    await broker.startup()
    try:
        await job.enqueue(7)
        await asyncio.wait_for(finished.wait(), 5)
        await broker.wait_all()
        assert calls[0] is not calls[1]
        assert all(item.closed for item in calls)
    finally:
        await broker.shutdown()


async def test_projection_stages_hooks_scope_and_result():
    router, services, phases = ProjectionRouter(), Services(), []

    async def read(service: Service, payload: Payload) -> int:
        phases.append(("read", service))
        return payload.value

    async def transform(service: Service, data: int) -> str:
        phases.append(("transform", service))
        return str(data)

    async def write(service: Service, data: str) -> int:
        phases.append(("write", service))
        return int(data) * 2

    async def observe(event):
        phases.append((event.call.projection, event.result))

    class Local(Hooks[int, str, int]):
        def __init__(self):
            super().__init__(after_write=(handler(observe),))

    services.provide(Local)
    task = router.projection(
        read=read,
        transform=transform,
        write=write,
        dependency=Service,
        name="projection",
        hooks=Local,
    )
    broker = projections(routers=[router], providers=[services])
    await broker.startup()
    try:
        assert await result(await task.enqueue(Payload(value=4))) == 8
        assert [phase for phase, _ in phases] == [
            "read",
            "transform",
            "write",
            "projection",
        ]
        assert phases[0][1] is phases[1][1] is phases[2][1]
        assert phases[0][1].closed
    finally:
        await broker.shutdown()


async def test_projection_failure_stage_and_shared_hooks():
    router, failures = ProjectionRouter(), []

    async def read(value: int) -> int:
        return value

    async def transform(data: int) -> str:
        raise ValueError("convert")

    async def write(data: str) -> int:
        raise AssertionError("write must not run")

    async def failed(event):
        failures.append(event)

    class Shared(Hooks[object, object, object]):
        def __init__(self):
            super().__init__(on_error=(handler(failed),))

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Shared)
    task = router.projection(read=read, transform=transform, write=write)
    broker = projections(
        registrar=ProjectionRegistrar(hooks=Shared),
        routers=[router],
        providers=[provider],
    )
    await broker.startup()
    try:
        with pytest.raises(ValueError, match="convert"):
            await result(await task.enqueue(3))
        assert len(failures) == 1 and failures[0].stage == "transform"
    finally:
        await broker.shutdown()


def test_router_discovery_nested_packages_reexports_and_mixed_classes(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    folder = tmp_path / "function_discovery"
    (folder / "group" / "schedulers").mkdir(parents=True)
    for path in (folder, folder / "group", folder / "group" / "schedulers"):
        (path / "__init__.py").write_text("")
    (folder / "group" / "schedulers" / "tasks.py").write_text(
        dedent("""
        from papilio_tasks.apps.schedulers import Scheduler, TaskRouter
        router = TaskRouter()
        alias = router
        @router.task()
        async def job(value: int) -> int: return value
        class Legacy(Scheduler):
            async def run(self): return 2
    """)
    )
    (folder / "group" / "schedulers" / "__init__.py").write_text(
        "from .tasks import router"
    )
    (folder / "unused.py").write_text("raise AssertionError('unrelated')")
    roots = ["function_discovery", "function_discovery.group"]
    module = import_module("function_discovery.group.schedulers.tasks")
    assert Bootstrapper(roots).routers("schedulers", TaskRouter) == [
        module.router
    ]
    broker = create_broker(modules=roots, routers=[module.router])
    assert set(broker.local_task_registry) == {
        module.job.name,
        module.__name__ + ".Legacy",
    }


def test_duplicate_names_and_invalid_function_dependencies_are_explicit():
    router = TaskRouter()

    @router.task(name="duplicate")
    async def one(value: int):
        pass

    @router.task(name="duplicate")
    async def two(value: int):
        pass

    with pytest.raises(ValueError, match="already registered"):
        create_broker(routers=[router])
    with pytest.raises(TypeError, match="async"):
        TaskRouter().task()(lambda value: value)

    async def bad(value: int, service: FromDishka[Service]):
        pass

    with pytest.raises(TypeError, match="dependency="):
        TaskRouter().task()(bad)
    with pytest.raises(TypeError, match="First argument"):
        TaskRouter().task(dependency=Service)(one.function)


async def test_projection_discovery_mixes_classes_and_functions(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    folder = tmp_path / "projection_functions"
    (folder / "projections").mkdir(parents=True)
    (folder / "__init__.py").write_text("")
    (folder / "projections" / "__init__.py").write_text(
        dedent("""
        from papilio_tasks.apps.projections import Direct
        from papilio_tasks.apps.projections.router import ProjectionRouter
        router = ProjectionRouter()
        async def read(value: int) -> int: return value
        async def convert(data: int) -> str: return str(data)
        async def write(data: str) -> int: return int(data) * 2
        task = router.projection(read=read, transform=convert, write=write)
        class Legacy(Direct[int, int]):
            def __init__(self): super().__init__()
            async def read(self, value: int) -> int: return value
            async def write(self, data: int) -> int: return data + 1
    """)
    )
    module = import_module("projection_functions.projections")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(module.Legacy)
    broker = projections(
        modules=["projection_functions"],
        providers=[provider],
        routers=[module.router],
    )
    assert len(broker.local_task_registry) == 2
    await broker.startup()
    try:
        assert await result(await module.Legacy.enqueue(4)) == 5
        assert await result(await module.task.enqueue(4)) == 8
    finally:
        await broker.shutdown()


async def test_missing_provider_and_cancellation_cleanup():
    router, services = TaskRouter(), Services()

    @router.task(dependency=Service)
    async def job(service: Service) -> None:
        raise asyncio.CancelledError

    broker = create_broker(routers=[router])
    await broker.startup()
    try:
        with pytest.raises(NoFactoryError):
            await result(await job.enqueue())
    finally:
        await broker.shutdown()
    # The declaration can be rebound after the previous app has released it.
    broker = create_broker(routers=[router], providers=[services])
    await broker.startup()
    try:
        # Native cancellation does not save a normal task result.
        handle = await job.enqueue()
        await broker.wait_all()
        assert handle.task_id
        assert len(services.instances) == 1 and services.instances[0].closed
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()


async def test_function_projection_project_and_publication_hook_order():
    phases, writes = [], []
    router = ProjectionRouter()

    async def read(value: int) -> int:
        return value

    async def convert(value: int) -> str:
        return str(value)

    async def write(value: str) -> int:
        writes.append(value)
        return int(value)

    async def shared(event):
        phases.append(("shared", event.call.meta["task_name"]))

    async def local(event):
        phases.append(("local", event.call.meta["task_name"]))

    class Shared(PublishHooks):
        def __init__(self):
            super().__init__(after_send=(handler(shared),))

    class Local(PublishHooks):
        def __init__(self):
            super().__init__(after_send=(handler(local),))

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Shared)
    provider.provide(Local)
    task = router.projection(
        name="projected",
        read=read,
        transform=convert,
        write=write,
        publish_hooks=Local,
    )
    broker = projections(
        routers=[router], providers=[provider], publish_hooks=Shared
    )

    @task.project(select=lambda result: {"value": result})
    async def operation() -> int:
        return 7

    await broker.startup()
    try:
        assert await operation() == 7
        await broker.wait_all()
        assert writes == ["7"]
        assert phases == [("shared", "projected"), ("local", "projected")]
    finally:
        await broker.shutdown()
