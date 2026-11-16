import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope
from taskiq import (
    AsyncBroker,
    ScheduledTask,
    ScheduleSource,
    TaskiqMessage,
    TaskiqScheduler,
)
from taskiq.exceptions import SendTaskError
from taskiq.middlewares import SmartRetryMiddleware

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.projections.application import (
    create_broker as projection_broker,
)
from papilio_tasks.apps.projections.registry import Registrar as Projections
from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.taskiq import RetryRuntime
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.retry import Backoff, Retry


def provider_for(cls):
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(cls)
    return provider


class Job(Scheduler):
    retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))

    async def run(self, value: int) -> int:
        return value


class Project(Direct[int, int]):
    retry = Job.retry

    def __init__(self):
        super().__init__()

    async def read(self, value: int) -> int:
        return value

    async def write(self, data: int) -> int:
        return data


def assemble(runtime):
    first, second = Registrar(), Projections()
    first.include(Job, name="module.job")
    second.include(Project, name="module.project")
    return (
        create_broker(
            registrar=first, providers=[provider_for(Job)], retry=runtime
        ),
        projection_broker(
            registrar=second, providers=[provider_for(Project)], retry=runtime
        ),
    )


def plan(name, id):
    return ScheduledTask(
        task_name=name,
        task_id=id,
        args=[42],
        kwargs={},
        labels={"custom": "preserved"},
        time=datetime.now(UTC),
    )


def test_backoff_contract_and_cap(monkeypatch):
    delay = Backoff(initial=2, multiplier=2, max_delay=7)
    assert [delay.at(n) for n in (1, 2, 3, 4, 100000)] == [2, 4, 7, 7, 7]
    monkeypatch.setattr(
        "papilio_tasks.tools.retry.random.uniform", lambda a, b: b / 2
    )
    assert (
        Backoff(initial=2, multiplier=2, max_delay=7, jitter=True).at(3) == 3.5
    )
    assert Retry(attempts=3, delay=delay, errors=(ValueError,)).delay is delay


@pytest.mark.parametrize(
    "changes",
    [
        {"initial": -1},
        {"initial": True},
        {"multiplier": 0.5},
        {"multiplier": float("inf")},
        {"max_delay": 1},
        {"max_delay": float("nan")},
        {"jitter": "yes"},
    ],
)
def test_invalid_backoff(changes):
    with pytest.raises((ValueError, TypeError)):
        Backoff(**(dict(initial=2, multiplier=2, max_delay=10) | changes))


@pytest.mark.parametrize(
    "key", ["retry_multiplier", "retry_max_delay", "retry_jitter"]
)
def test_strategy_labels_cannot_override_a_fixed_class_policy(key):
    registry = Registrar()
    with pytest.raises(ValueError, match="conflict"):
        registry.include(Job, labels={key: 2})
    assert registry.broker.native.local_task_registry == {}


async def test_shared_source_routes_and_removes_only_successful_sends():
    source = MemorySource()
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    first.kick, second.kick = AsyncMock(), AsyncMock()
    beat = runtime.create_beat(brokers=[first, second])
    assert isinstance(beat, TaskiqScheduler)
    assert len(beat.sources) == 1
    await asyncio.gather(
        *(
            source.add_schedule(plan(name, id))
            for name, id in (
                ("module.job", "job-id"),
                ("module.project", "project-id"),
            )
        )
    )
    await asyncio.gather(
        *(
            beat.on_ready(beat.sources[0], task)
            for task in await source.native.get_schedules()
        )
    )
    assert first.kick.await_args.args[0].task_name == "module.job"
    assert second.kick.await_args.args[0].task_name == "module.project"
    assert second.kick.await_args.args[0].task_id == "project-id"
    assert second.kick.await_args.args[0].labels["custom"] == "preserved"
    assert await source.get_schedules() == []

    failed = plan("module.project", "failed-id")
    await source.add_schedule(failed)
    second.kick.side_effect = ConnectionError("send failed")
    with pytest.raises(SendTaskError):
        await beat.on_ready(
            beat.sources[0], (await source.native.get_schedules())[0]
        )
    assert (
        await source.get_schedule(failed.schedule_id)
    ).task_id == "failed-id"
    await asyncio.gather(first.shutdown(), second.shutdown())


async def test_unknown_or_ambiguous_routes_do_not_delete():
    source = MemorySource()
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    beat = runtime.create_beat(brokers=[first, second])
    unknown = plan("unknown", "unknown-id")
    await source.add_schedule(unknown)
    with pytest.raises(ValueError, match="registered"):
        await beat.on_ready(
            beat.sources[0], (await source.native.get_schedules())[0]
        )
    assert await source.get_schedule(unknown.schedule_id)

    second.local_task_registry["module.job"] = first.find_task("module.job")
    with pytest.raises(ValueError, match="multiple"):
        runtime.create_beat(brokers=[first, second])
    await asyncio.gather(first.shutdown(), second.shutdown())


async def test_shared_lifecycle_opens_once_and_last_owner_closes(monkeypatch):
    source = MemorySource()
    opening, closing = AsyncMock(), AsyncMock()
    monkeypatch.setattr(source.native, "startup", opening)
    monkeypatch.setattr(source.native, "shutdown", closing)
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    first.is_worker_process = second.is_worker_process = True
    await asyncio.gather(
        AsyncBroker.startup(first), AsyncBroker.startup(second)
    )
    opening.assert_awaited_once()
    await AsyncBroker.shutdown(first)
    closing.assert_not_awaited()
    await AsyncBroker.shutdown(second)
    closing.assert_awaited_once()


async def test_client_needs_no_source_and_worker_validates_before_execution():
    registry = Registrar()
    registry.include(Job)
    broker = create_broker(registrar=registry)
    await AsyncBroker.startup(broker)
    broker.is_worker_process = True
    try:
        with pytest.raises(ValueError, match="retry"):
            await AsyncBroker.startup(broker)
    finally:
        await AsyncBroker.shutdown(broker)


@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_immediate_retry_executes_without_source_or_beat(family):
    calls = []

    class Temporary(Scheduler):
        retry = Job.retry

        async def run(self, value: int) -> int:
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("temporary")
            return value

    class TemporaryProject(Project):
        async def write(self, data: int) -> int:
            return await Temporary().run(data)

    cls, registry, factory = (
        (Temporary, Registrar(), create_broker)
        if family == "scheduler"
        else (TemporaryProject, Projections(), projection_broker)
    )
    registry.include(cls)
    broker = factory(
        registrar=registry,
        providers=[provider_for(cls)],
        retry=RetryRuntime.immediate(),
    )
    await broker.startup()
    try:
        completed = await (await cls.enqueue(42)).wait_result(timeout=5)
        assert not completed.is_err and completed.return_value == 42
        assert len(calls) == 3
    finally:
        await broker.shutdown()


def test_immediate_does_not_ignore_positive_delay():
    class Delayed(Job):
        retry = Retry(attempts=2, delay=1, errors=(ValueError,))

    registry = Registrar()
    registry.include(Delayed)
    with pytest.raises(ValueError, match="zero"):
        create_broker(registrar=registry, retry=RetryRuntime.immediate())


async def test_single_attempt_needs_no_retry_source():
    class Once(Job):
        retry = Retry(attempts=1, delay=10, errors=(ValueError,))

    registry = Registrar()
    registry.include(Once)
    broker = create_broker(registrar=registry, providers=[provider_for(Once)])
    await broker.startup()
    try:
        completed = await (await Once.enqueue(42)).wait_result(timeout=5)
        assert completed.return_value == 42
    finally:
        await broker.shutdown()


async def test_source_startup_failure_cleans_up_and_allows_retry(monkeypatch):
    source = MemorySource()
    opening = AsyncMock(side_effect=[ConnectionError("open failed"), None])
    closing = AsyncMock()
    monkeypatch.setattr(source.native, "startup", opening)
    monkeypatch.setattr(source.native, "shutdown", closing)
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    first.is_worker_process = second.is_worker_process = True
    with pytest.raises(ConnectionError):
        await AsyncBroker.startup(first)
    await AsyncBroker.startup(second)
    await asyncio.gather(
        AsyncBroker.shutdown(first), AsyncBroker.shutdown(second)
    )
    assert opening.await_count == 2 and closing.await_count == 2


async def test_both_families_retry_through_one_native_beat():
    calls = []

    class TemporaryJob(Job):
        async def run(self, value: int) -> int:
            calls.append(("job", value))
            if calls.count(("job", value)) == 1:
                raise ConnectionError("temporary")
            return value

    class TemporaryProject(Project):
        async def write(self, data: int) -> int:
            calls.append(("projection", data))
            if calls.count(("projection", data)) == 1:
                raise ConnectionError("temporary")
            return data

    runtime = RetryRuntime(source=MemorySource())
    jobs = Registrar(MemoryBroker(await_inplace=True))
    projects = Projections(MemoryBroker(await_inplace=True))
    jobs.include(TemporaryJob)
    projects.include(TemporaryProject)
    first = create_broker(
        registrar=jobs, providers=[provider_for(TemporaryJob)], retry=runtime
    )
    second = projection_broker(
        registrar=projects,
        providers=[provider_for(TemporaryProject)],
        retry=runtime,
    )
    beat = runtime.create_beat(brokers=[first, second])
    await asyncio.gather(first.startup(), second.startup())
    try:
        handles = await asyncio.gather(
            TemporaryJob.enqueue(42), TemporaryProject.enqueue(43)
        )
        schedules = await beat.sources[0].get_schedules()
        assert len(schedules) == 2
        await asyncio.gather(
            *(beat.on_ready(beat.sources[0], task) for task in schedules)
        )
        assert await beat.sources[0].get_schedules() == []
        results = await asyncio.gather(
            *(handle.get_result() for handle in handles)
        )
        assert [result.return_value for result in results] == [42, 43]
        assert calls.count(("job", 42)) == calls.count(("projection", 43)) == 2
    finally:
        await asyncio.gather(first.shutdown(), second.shutdown())


async def test_native_writable_source_without_redis():
    class Native(ScheduleSource):
        def __init__(self):
            self.items = {}

        async def get_schedules(self):
            return list(self.items.values())

        async def add_schedule(self, task):
            self.items[task.schedule_id] = task

        async def delete_schedule(self, id):
            self.items.pop(id, None)

        async def post_send(self, task):
            await self.delete_schedule(task.schedule_id)

    source = Native()
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    first.kick, second.kick = AsyncMock(), AsyncMock()
    beat = runtime.create_beat(brokers=[first, second])
    await source.add_schedule(plan("module.project", "custom-source"))
    await beat.on_ready(
        beat.sources[0], (await beat.sources[0].get_schedules())[0]
    )
    assert await source.get_schedules() == []
    assert first.kick.await_count == 0 and second.kick.await_count == 1
    await asyncio.gather(first.shutdown(), second.shutdown())


async def test_same_runtime_rejects_two_active_beat_readers():
    runtime = RetryRuntime(source=MemorySource())
    first, second = assemble(runtime)
    one = runtime.create_beat(brokers=[first, second])
    two = runtime.create_beat(brokers=[first, second])
    await one.sources[0].startup()
    try:
        with pytest.raises(ValueError, match="one retry beat"):
            await two.sources[0].startup()
    finally:
        await one.sources[0].shutdown()
    await two.sources[0].startup()
    await two.sources[0].shutdown()
    await asyncio.gather(first.shutdown(), second.shutdown())


async def test_beat_cancels_failed_startup_and_closes_all_owned_resources(
    monkeypatch,
):
    source = MemorySource()
    opening, closing = AsyncMock(), AsyncMock()
    monkeypatch.setattr(source.native, "startup", opening)
    monkeypatch.setattr(source.native, "shutdown", closing)
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def pending():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def fail():
        await started.wait()
        raise ConnectionError("broker startup")

    first.startup, second.startup = pending, fail
    first_close, second_close = AsyncMock(), AsyncMock()
    monkeypatch.setattr(first, "shutdown", first_close)
    monkeypatch.setattr(second, "shutdown", second_close)
    beat = runtime.create_beat(brokers=[first, second])
    # Model native CLI source startup before scheduler.startup.
    await beat.sources[0].startup()
    with pytest.raises(ExceptionGroup):
        await beat.startup()
    assert cancelled.is_set()
    assert first.is_scheduler_process and second.is_scheduler_process
    opening.assert_awaited_once()
    closing.assert_awaited_once()
    first_close.assert_awaited_once()
    second_close.assert_awaited_once()
    await beat.sources[0].shutdown()
    closing.assert_awaited_once()


async def test_beat_shutdown_failure_still_closes_source_and_other_brokers(
    monkeypatch,
):
    source = MemorySource()
    closing = AsyncMock()
    monkeypatch.setattr(source.native, "shutdown", closing)
    runtime = RetryRuntime(source=source)
    first, second = assemble(runtime)
    monkeypatch.setattr(first, "startup", AsyncMock())
    monkeypatch.setattr(second, "startup", AsyncMock())
    beat = runtime.create_beat(brokers=[first, second])
    await beat.startup()
    monkeypatch.setattr(
        first, "shutdown", AsyncMock(side_effect=ValueError("close"))
    )
    other_close = AsyncMock()
    monkeypatch.setattr(second, "shutdown", other_close)
    with pytest.raises(ExceptionGroup):
        await beat.shutdown()
    other_close.assert_awaited_once()
    closing.assert_awaited_once()
    await beat.sources[0].shutdown()
    closing.assert_awaited_once()


async def test_container_failure_still_releases_retry_source(
    monkeypatch,
):
    class Root:
        pass

    async def root():
        yield Root()
        raise ValueError("container cleanup")

    source = MemorySource()
    closing = AsyncMock()
    monkeypatch.setattr(source.native, "shutdown", closing)
    runtime = RetryRuntime(source=source)
    registry = Registrar()
    registry.include(Job)
    provider = provider_for(Job)
    provider.provide(root, provides=Root, scope=Scope.APP)
    broker = create_broker(
        registrar=registry, providers=[provider], retry=runtime
    )
    broker.is_worker_process = True
    await AsyncBroker.startup(broker)
    await broker.state.papilio_container.get(Root)
    with pytest.raises(ExceptionGroup):
        await AsyncBroker.shutdown(broker)
    closing.assert_awaited_once()


def test_invalid_runtime_configuration_and_beat_selection():
    with pytest.raises(TypeError, match="writable"):
        RetryRuntime()
    with pytest.raises(ValueError, match="source"):
        RetryRuntime(source=MemorySource(), mode="immediate")
    with pytest.raises(ValueError, match="beat"):
        RetryRuntime.immediate().create_beat(brokers=[])
    runtime = RetryRuntime(source=MemorySource())
    with pytest.raises(ValueError, match="distinct"):
        runtime.create_beat(brokers=[])
    first, second = assemble(runtime)
    with pytest.raises(ValueError, match="distinct"):
        runtime.create_beat(brokers=[first, first])
    with pytest.raises(ValueError, match="both"):
        create_broker(retry=runtime, retry_source=MemorySource())


async def test_backoff_policy_labels_survive_native_wire_format():
    class Delayed(Job):
        retry = Retry(
            attempts=4,
            delay=Backoff(initial=2, multiplier=2, max_delay=7),
            errors=(ConnectionError,),
        )

    registry = Registrar()
    task = registry.include(Delayed)
    broker = create_broker(
        registrar=registry, retry=RetryRuntime(source=MemorySource())
    )
    middleware = next(
        item
        for item in broker.middlewares
        if isinstance(item, SmartRetryMiddleware)
    )
    message = TaskiqMessage(
        task_id="wire-check",
        task_name=task.task_name,
        labels={
            key: str(value)
            for key, value in task.labels.items()
            if key != "types_of_exceptions"
        },
        args=[42],
        kwargs={},
    )
    assert [middleware.make_delay(message, i) for i in (1, 2, 3)] == [2, 4, 7]
    assert "delay" not in message.labels
    assert "delay" not in task.labels
    await broker.shutdown()
