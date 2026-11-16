"""Caller-owned retry execution and one native beat for a shared source."""

import asyncio
from collections.abc import Iterable, Sequence
from typing import Literal

from taskiq import (
    AsyncBroker,
    ScheduledTask,
    ScheduleSource,
    TaskiqEvents,
    TaskiqScheduler,
    TaskiqState,
)
from taskiq.middlewares import SimpleRetryMiddleware, SmartRetryMiddleware
from taskiq.utils import maybe_awaitable

from papilio_tasks.tools.retry import Backoff, Retry

from .retry import _Retry
from .sources.base import MutableSource
from .sources.contracts.base import MutableSourceContract


def _managed_retry(labels: dict) -> bool:
    return (
        "retry_delay" in labels
        and str(labels.get("retry_on_error", False)).lower() == "true"
        and int(labels.get("max_retries", 1)) > 1
    )


async def _close_brokers(brokers: Sequence[AsyncBroker]) -> None:
    results = await asyncio.gather(
        *(broker.shutdown() for broker in brokers), return_exceptions=True
    )
    errors = [
        result for result in results if isinstance(result, BaseException)
    ]
    if errors:
        raise BaseExceptionGroup("Retry beat shutdown failed", errors)


class RetryRuntime:
    """Share explicit Taskiq retry execution settings across applications.

    Scheduled mode uses a writable source and an explicitly run retry beat.
    Immediate mode republishes zero-delay retries without a source or beat.
    One runtime coordinates source ownership within one process/event loop.
    """

    def __init__(
        self,
        *,
        source: MutableSourceContract | ScheduleSource | None = None,
        mode: Literal["scheduled", "immediate"] = "scheduled",
    ) -> None:
        if mode not in ("scheduled", "immediate"):
            raise ValueError("Unknown retry mode")
        if mode == "immediate" and source is not None:
            raise ValueError("Immediate retry does not use a source")
        if isinstance(source, ScheduleSource):
            if (
                type(source).add_schedule is ScheduleSource.add_schedule
                or type(source).delete_schedule
                is ScheduleSource.delete_schedule
            ):
                raise TypeError("retry source must be writable")
            source = MutableSource(source)
        if mode == "scheduled" and not isinstance(
            source, MutableSourceContract
        ):
            raise TypeError("retry source must be writable")
        if source is not None and not isinstance(
            source.native, ScheduleSource
        ):
            raise TypeError("retry source must expose a native Taskiq source")
        self._source = source
        self._mode: Literal["scheduled", "immediate"] = mode
        self._owners: set[object] = set()
        self._beat_owner: object | None = None
        self._lock = asyncio.Lock()

    @property
    def source(self) -> MutableSourceContract | None:
        return self._source

    @property
    def mode(self) -> Literal["scheduled", "immediate"]:
        return self._mode

    @classmethod
    def immediate(cls) -> "RetryRuntime":
        return cls(mode="immediate")

    def check_policy(self, policy: Retry | None) -> None:
        if policy is None:
            return
        if not isinstance(policy, Retry):
            raise TypeError("retry must be Retry or None")
        if (
            policy.attempts > 1
            and self.mode == "immediate"
            and (isinstance(policy.delay, Backoff) or policy.delay != 0)
        ):
            raise ValueError("Immediate retry requires a fixed zero delay")

    def _check_broker(self, broker: AsyncBroker) -> None:
        if self.mode == "immediate" and any(
            _managed_retry(task.labels)
            and (
                float(task.labels["retry_delay"]) != 0
                or "retry_multiplier" in task.labels
            )
            for task in broker.local_task_registry.values()
        ):
            raise ValueError("Immediate retry requires a fixed zero delay")

    async def _acquire(self, owner: object, *, beat: bool = False) -> None:
        if self.source is None:
            return
        async with self._lock:
            if beat and self._beat_owner not in (None, owner):
                raise ValueError("Run one retry beat per shared runtime")
            if owner in self._owners:
                return
            if not self._owners:
                try:
                    await self.source.native.startup()
                except BaseException as error:
                    try:
                        await self.source.native.shutdown()
                    except BaseException as cleanup:
                        raise BaseExceptionGroup(
                            "Retry source startup and cleanup failed",
                            [error, cleanup],
                        ) from None
                    raise
            self._owners.add(owner)
            if beat:
                self._beat_owner = owner

    async def _release(self, owner: object) -> None:
        if self.source is None:
            return
        async with self._lock:
            if owner not in self._owners:
                return
            self._owners.remove(owner)
            if self._beat_owner is owner:
                self._beat_owner = None
            if not self._owners:
                await self.source.native.shutdown()

    def create_beat(
        self, *, brokers: Sequence[AsyncBroker]
    ) -> TaskiqScheduler:
        """Route one shared source through registered native brokers.

        Run one logical beat for this source. Task names must be unique across
        the selected brokers. Missing destinations leave schedules untouched.
        """
        if self.source is None:
            raise ValueError("Immediate retry does not need a beat")
        if not brokers or len({id(broker) for broker in brokers}) != len(
            brokers
        ):
            raise ValueError("Select distinct retry brokers")
        if any(
            broker.state.get("papilio_retry_runtime") is not self
            for broker in brokers
        ):
            raise ValueError("All retry brokers must use this RetryRuntime")
        return _RetryBeat(self, tuple(brokers))


class _SourceLease(ScheduleSource):
    """Delegate source hooks and share the underlying resource lifecycle."""

    def __init__(self, runtime: RetryRuntime) -> None:
        self.runtime = runtime
        assert runtime.source is not None
        self.native = runtime.source.native

    async def startup(self) -> None:
        await self.runtime._acquire(self, beat=True)

    async def shutdown(self) -> None:
        await self.runtime._release(self)

    async def get_schedules(self) -> list[ScheduledTask]:
        return await self.native.get_schedules()

    async def pre_send(self, task: ScheduledTask) -> None:
        await maybe_awaitable(self.native.pre_send(task))

    async def post_send(self, task: ScheduledTask) -> None:
        await maybe_awaitable(self.native.post_send(task))


class _RetryBeat(TaskiqScheduler):
    def __init__(
        self, runtime: RetryRuntime, brokers: tuple[AsyncBroker, ...]
    ) -> None:
        self._brokers = brokers
        self._dispatchers = [TaskiqScheduler(broker, []) for broker in brokers]
        self._source = _SourceLease(runtime)
        self._started_brokers: tuple[AsyncBroker, ...] = ()
        super().__init__(brokers[0], [self._source])
        self._check_routes()

    def _check_routes(self) -> None:
        names: set[str] = set()
        for broker in self._brokers:
            if names.intersection(broker.local_task_registry):
                raise ValueError(
                    "Task name registered on multiple retry brokers"
                )
            names.update(broker.local_task_registry)

    async def on_ready(
        self, source: ScheduleSource, task: ScheduledTask
    ) -> None:
        destinations = [
            scheduler
            for scheduler in self._dispatchers
            if task.task_name in scheduler.broker.local_task_registry
        ]
        if len(destinations) != 1:
            raise ValueError(
                f"Task must be registered on exactly one retry broker: "
                f"{task.task_name}"
            )
        await destinations[0].on_ready(source, task)

    async def startup(self) -> None:
        if self._started_brokers:
            raise RuntimeError("Retry beat is already started")
        try:
            self._check_routes()
            if any(broker.is_worker_process for broker in self._brokers):
                raise ValueError("Retry beat needs producer brokers")
            for broker in self._brokers:
                broker.is_scheduler_process = True
            await self._source.startup()
            self._started_brokers = self._brokers
            async with asyncio.TaskGroup() as startup:
                for broker in self._brokers:
                    startup.create_task(broker.startup())
        except BaseException as error:
            try:
                await self.shutdown()
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "Retry beat startup and cleanup failed", [error, cleanup]
                ) from None
            raise

    async def shutdown(self) -> None:
        brokers, self._started_brokers = self._started_brokers, ()
        try:
            await _close_brokers(brokers)
        finally:
            await self._source.shutdown()


def prepare_retry(
    broker: AsyncBroker,
    runtime: RetryRuntime | None,
    source: MutableSourceContract | None,
    policies: Iterable[Retry | None],
) -> RetryRuntime | None:
    if runtime is not None and source is not None:
        raise ValueError("Select retry or retry_source, not both")
    if runtime is not None and not isinstance(runtime, RetryRuntime):
        raise TypeError("retry must be a RetryRuntime or None")
    if source is not None:
        runtime = RetryRuntime(source=source)
    for policy in policies:
        if policy is not None and not isinstance(policy, Retry):
            raise TypeError("retry must be Retry or None")
        if runtime is not None:
            runtime.check_policy(policy)
    if runtime is not None:
        runtime._check_broker(broker)
        if any(
            isinstance(m, (SimpleRetryMiddleware, SmartRetryMiddleware))
            for m in broker.middlewares
        ):
            raise ValueError("Broker already has a retry middleware")
    return runtime


def install_retry(broker: AsyncBroker, runtime: RetryRuntime | None) -> None:
    broker.state.papilio_retry_runtime = runtime
    broker.state.papilio_retry_source = runtime.source if runtime else None
    broker.state.papilio_retry_worker_ready = False
    if runtime is not None:
        broker.add_middlewares(
            _Retry(
                schedule_source=(
                    runtime.source.native if runtime.source else None
                ),
                default_retry_label=False,
            )
        )

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def start(state: TaskiqState) -> None:
        if broker.is_scheduler_process:
            return
        if runtime is None:
            if any(
                _managed_retry(task.labels)
                for task in broker.local_task_registry.values()
            ):
                raise ValueError(
                    "Worker retry requires a RetryRuntime or retry_source"
                )
        else:
            runtime._check_broker(broker)
            await runtime._acquire(broker)
        state.papilio_retry_worker_ready = True


async def release_retry(broker: AsyncBroker) -> None:
    """Called in application cleanup even when the DI container close fails."""
    broker.state.papilio_retry_worker_ready = False
    runtime: RetryRuntime | None = broker.state.get("papilio_retry_runtime")
    if runtime is not None:
        await runtime._release(broker)
