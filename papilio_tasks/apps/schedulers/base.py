from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, ClassVar, cast

from taskiq import AsyncTaskiqTask
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.scheduler.created_schedule import CreatedSchedule
from taskiq.scheduler.scheduled_task import CronSpec

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
)
from papilio_tasks.tools.retry import Retry

from .contracts import SchedulerContract, _EnqueueTarget


class Scheduler(SchedulerContract, ABC):
    """An async job with application-owned instances and dependencies."""

    _backend: ClassVar[str] = "memory"
    retry: ClassVar[Retry | None] = None
    schedule: ClassVar[list[dict[str, Any]] | None] = None

    @abstractmethod
    async def run(self, *args: Any, **kwargs: Any) -> Any: ...

    @classmethod
    def task(cls) -> AsyncTaskiqDecoratedTask:
        return bindings.get(cls)

    @classmethod
    async def enqueue[**P, R](
        cls: type[_EnqueueTarget[P, R]], *args: P.args, **kwargs: P.kwargs
    ) -> AsyncTaskiqTask[R]:
        # Registration erases types; run supplies the native result type.
        return cast(AsyncTaskiqTask[R], await cls.task().kiq(*args, **kwargs))

    @classmethod
    async def at(
        cls,
        source: MutableSourceContract,
        when: datetime,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]:
        return await cls.task().schedule_by_time(
            source.native, when, *args, **kwargs
        )

    @classmethod
    async def cron(
        cls,
        source: MutableSourceContract,
        expression: str | CronSpec,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]:
        return await cls.task().schedule_by_cron(
            source.native, expression, *args, **kwargs
        )

    @classmethod
    async def every(
        cls,
        source: MutableSourceContract,
        interval: int | timedelta,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]:
        return await cls.task().schedule_by_interval(
            source.native, interval, *args, **kwargs
        )
