"""Module-owned function tasks, independent of broker selection."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Concatenate, overload

from papilio_tasks.infra.dishka import Function
from papilio_tasks.infra.taskiq.functions import Task
from papilio_tasks.tools.retry import Retry

__all__ = ["Task", "TaskRouter"]


class TaskRouter:
    def __init__(self) -> None:
        self.tasks: list[Task[..., Any]] = []

    @overload
    def task[**P, R, **D](
        self,
        reference: Task[P, R],
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        schedule: Sequence[Mapping[str, Any]] | None = None,
    ) -> Callable[[Callable[D, Awaitable[R]]], Callable[D, Awaitable[R]]]: ...

    @overload
    def task[T, **D, R](
        self,
        *,
        payload: type[T],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        schedule: Sequence[Mapping[str, Any]] | None = None,
        dependency: None = None,
    ) -> Callable[
        [Callable[Concatenate[T, D], Awaitable[R]]], Task[[T], R]
    ]: ...

    @overload
    def task[**P, R](
        self,
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        schedule: Sequence[Mapping[str, Any]] | None = None,
        dependency: None = None,
        payload: None = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Task[P, R]]: ...

    @overload
    def task[D, **P, R](
        self,
        *,
        dependency: type[D],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        schedule: Sequence[Mapping[str, Any]] | None = None,
        payload: None = None,
    ) -> Callable[[Callable[Concatenate[D, P], Awaitable[R]]], Task[P, R]]: ...

    def task[**P, R](
        self,
        reference: Task[P, R] | None = None,
        *,
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        schedule: Sequence[Mapping[str, Any]] | None = None,
        dependency: type[Any] | None = None,
        payload: type[Any] | None = None,
    ) -> Callable[..., Any]:
        def decorate(
            function: Callable[..., Awaitable[Any]],
        ) -> Any:
            selected = dict(labels or {})
            if reference is not None:
                if not isinstance(reference, Task):
                    raise TypeError("reference must be a Task")
                if payload is not None or dependency is not None:
                    raise TypeError(
                        "Task references do not use payload= or dependency="
                    )
                if name is not None and name != reference.name:
                    raise ValueError(
                        "Task reference name cannot be overridden"
                    )
                selected = reference.labels | selected
            if schedule is not None:
                selected["schedule"] = list(schedule)
            if payload is not None and dependency is not None:
                raise TypeError(
                    "Use FromDishka with payload=, not dependency="
                )
            call = Function(
                function,
                dependency,
                keyword_dependencies=reference is not None,
            )
            if payload is not None:
                call.check_payload(payload)
            elif reference is None and call.injected is not None:
                raise TypeError("FromDishka tasks require payload=YourPayload")
            task = Task(
                call,
                name=reference.name if reference is not None else name,
                retry=reference.retry
                if reference is not None and retry is None
                else retry,
                labels=selected,
            )
            if reference is not None:
                reference._attach(task)
            self.tasks.append(task)
            return function if reference is not None else task

        return decorate
