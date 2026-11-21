"""Typed deferred task references over the existing native registration."""

from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from datetime import datetime, timedelta
from functools import wraps
from inspect import Parameter
from typing import Any, cast

from dishka import AsyncContainer, FromDishka
from dishka.integrations.taskiq import inject
from taskiq import AsyncTaskiqTask
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.scheduler.created_schedule import CreatedSchedule
from taskiq.scheduler.scheduled_task import CronSpec

from papilio_tasks.infra.dishka import Function
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.retry import Retry

from . import bindings
from .brokers.contracts.base import BrokerContract
from .retry import retry_labels
from .sources.contracts.base import MutableSourceContract


class Task[**P, R]:
    """A module-owned declaration; assembly supplies the native task binding.

    function exposes the original callable for explicitly supplied local DI.
    enqueue/at/cron/every accept only the serialized payload arguments.
    """

    def __init__(
        self,
        call: Function[Any] | None = None,
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
    ) -> None:
        self.call = call
        selected_name = (
            call.name if name is None and call is not None else name
        )
        if not isinstance(selected_name, str) or not selected_name:
            raise ValueError("Task name is required and cannot be empty")
        self.name = selected_name
        if retry is not None and not isinstance(retry, Retry):
            raise TypeError("retry must be Retry or None")
        self.retry = retry
        self.labels = deepcopy(dict(labels or {}))
        self.publish_hooks: type[PublishHooks] | None = None
        self._implementation: Task[..., R] | None = None
        if call is not None:
            self.__module__ = call.function.__module__
            self.__qualname__ = call.function.__qualname__
        else:
            self.__qualname__ = self.name

    @property
    def function(self) -> Callable[..., Awaitable[Any]]:
        """Expose the original handler for explicitly supplied local DI."""
        if self._implementation is not None:
            return self._implementation.function
        if self.call is None:
            raise RuntimeError("Task has no handler declaration")
        return self.call.function

    def _attach(self, implementation: "Task[..., R]") -> None:
        if self.call is not None or self._implementation is not None:
            raise ValueError(f"Task already has a handler: {self.name}")
        self._implementation = implementation

    def task(self) -> AsyncTaskiqDecoratedTask[Any, R]:
        selected = self._implementation
        return bindings.get(self if selected is None else selected)

    async def enqueue(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> AsyncTaskiqTask[R]:
        if self._implementation is not None:
            return await self._implementation.enqueue(*args, **kwargs)
        return cast(AsyncTaskiqTask[R], await self.task().kiq(*args, **kwargs))

    async def at(
        self,
        source: MutableSourceContract,
        when: datetime,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> CreatedSchedule[R]:
        return await self.task().schedule_by_time(
            source.native, when, *args, **kwargs
        )

    async def cron(
        self,
        source: MutableSourceContract,
        expression: str | CronSpec,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> CreatedSchedule[R]:
        return await self.task().schedule_by_cron(
            source.native, expression, *args, **kwargs
        )

    async def every(
        self,
        source: MutableSourceContract,
        interval: int | timedelta,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> CreatedSchedule[R]:
        return await self.task().schedule_by_interval(
            source.native, interval, *args, **kwargs
        )

    async def invoke(
        self,
        scope: AsyncContainer,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        assert self.call is not None
        function = await self.call.bind(scope)
        return await function(*args, **kwargs)

    def include(self, broker: BrokerContract) -> AsyncTaskiqDecoratedTask:
        if self.call is None:
            raise TypeError(
                "Include the router that declares this Task handler"
            )
        bindings.check(self)
        labels = retry_labels(broker.native, self.retry, deepcopy(self.labels))

        @wraps(self.function)
        async def execute(
            *args: Any, _scope: AsyncContainer, **kwargs: Any
        ) -> R:
            return await self.invoke(_scope, args, kwargs)

        parameters = list(self.call.signature.parameters.values())
        index = len(parameters)
        if parameters and parameters[-1].kind == Parameter.VAR_KEYWORD:
            index -= 1
        parameters.insert(
            index,
            Parameter(
                "_scope",
                Parameter.KEYWORD_ONLY,
                annotation=FromDishka[AsyncContainer],
            ),
        )
        setattr(
            execute,
            "__signature__",
            self.call.signature.replace(parameters=parameters),
        )
        execute.__annotations__ = self.call.hints | {
            "_scope": FromDishka[AsyncContainer]
        }
        injected = inject(execute, patch_module=True)
        # Exclude the DI container from Taskiq's payload conversion.
        injected.__annotations__.pop("dishka_container", None)
        task = broker.register(injected, name=self.name, labels=labels)
        bindings.add(self, task)
        return task
