"""Module-owned function tasks, independent of broker selection."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Concatenate, overload

from papilio_tasks.infra.taskiq.functions import Task
from papilio_tasks.tools.functions import Function
from papilio_tasks.tools.retry import Retry

__all__ = ["Task", "TaskRouter"]


class TaskRouter:
    def __init__(self) -> None:
        self.tasks: list[Task[..., Any]] = []

    @overload
    def task[**P, R](
        self,
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        dependency: None = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Task[P, R]]: ...

    @overload
    def task[D, **P, R](
        self,
        *,
        dependency: type[D],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
    ) -> Callable[[Callable[Concatenate[D, P], Awaitable[R]]], Task[P, R]]: ...

    def task(
        self,
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        dependency: type[Any] | None = None,
    ) -> Callable[..., Task[..., Any]]:
        def decorate(
            function: Callable[..., Awaitable[Any]],
        ) -> Task[..., Any]:
            task = Task(
                Function(function, dependency),
                name=name,
                retry=retry,
                labels=labels,
            )
            self.tasks.append(task)
            return task

        return decorate
