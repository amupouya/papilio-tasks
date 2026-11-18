"""Native retry beat dispatch and shared-source leases."""

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING

from taskiq import AsyncBroker, ScheduledTask, ScheduleSource, TaskiqScheduler
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:
    from .retry_runtime import RetryRuntime


async def _close_brokers(brokers: Sequence[AsyncBroker]) -> None:
    results = await asyncio.gather(
        *(broker.shutdown() for broker in brokers), return_exceptions=True
    )
    errors = [
        result for result in results if isinstance(result, BaseException)
    ]
    if errors:
        raise BaseExceptionGroup("Retry beat shutdown failed", errors)


class _SourceLease(ScheduleSource):
    """Delegate source hooks and share the underlying resource lifecycle."""

    def __init__(self, runtime: "RetryRuntime") -> None:
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
        self, runtime: "RetryRuntime", brokers: tuple[AsyncBroker, ...]
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
