"""Function projections sharing the class pipeline and publication hooks."""

from collections.abc import Awaitable, Callable, Coroutine, Mapping
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, Concatenate, overload

from dishka import AsyncContainer
from taskiq import AsyncTaskiqTask

from papilio_tasks.infra.taskiq.functions import Task
from papilio_tasks.tools.functions import Function
from papilio_tasks.tools.hooks.projection import Hooks
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.retry import Retry

from . import pipeline, publish


class ProjectionTask[**P, T, D, R](Task[P, R]):
    def __init__(
        self,
        read: Function[T],
        transform: Function[D],
        write: Function[R],
        *,
        name: str | None,
        retry: Retry | None,
        labels: Mapping[str, Any] | None,
        hooks: type[Hooks[T, D, R]] | None,
        publish_hooks: type[PublishHooks] | None,
    ) -> None:
        super().__init__(read, name=name, retry=retry, labels=labels)
        self.read, self.transform, self.write = read, transform, write
        self.hooks = hooks
        self.publish_hooks = publish_hooks
        for selected, base in ((hooks, Hooks), (publish_hooks, PublishHooks)):
            if selected is not None and (
                not isinstance(selected, type)
                or not issubclass(selected, base)
            ):
                raise TypeError("Select a provided hook class")
        result = write.hints.get("return", Any)
        self.call.hints["return"] = result
        self.call.signature = self.call.signature.replace(
            return_annotation=result
        )

    async def enqueue(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> AsyncTaskiqTask[R]:
        return await publish.enqueue(self, args, kwargs)

    async def invoke(
        self,
        scope: AsyncContainer,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        shared = self.task().broker.state.get("papilio_projection_hooks")
        # Bind all stages to the same request scope and dependency cache.
        read = await self.read.bind(scope)
        transform = await self.transform.bind(scope)
        write = await self.write.bind(scope)
        return await pipeline.run(
            name=self.name,
            read=read,
            transform=transform,
            write=write,
            local=await scope.get(self.hooks)
            if self.hooks is not None
            else Hooks(),
            shared=await scope.get(shared) if shared is not None else None,
            args=args,
            kwargs=kwargs,
        )

    def project[**Q, S](
        self, *, select: Callable[[S], Mapping[str, Any]]
    ) -> Callable[
        [Callable[Q, Awaitable[S]]], Callable[Q, Coroutine[Any, Any, S]]
    ]:
        def decorate(
            function: Callable[Q, Awaitable[S]],
        ) -> Callable[Q, Coroutine[Any, Any, S]]:
            if not iscoroutinefunction(function):
                raise TypeError("project requires an async function")

            @wraps(function)
            async def wrapped(*args: Q.args, **kwargs: Q.kwargs) -> S:
                result = await function(*args, **kwargs)
                await publish.enqueue(self, (), dict(select(result)))
                return result

            return wrapped

        return decorate


class ProjectionRouter:
    def __init__(self) -> None:
        self.tasks: list[ProjectionTask[..., Any, Any, Any]] = []

    @overload
    def projection[I, **A, T, **B, D, **C, R](
        self,
        *,
        payload: type[I],
        read: Callable[Concatenate[I, A], Awaitable[T]],
        transform: Callable[Concatenate[T, B], Awaitable[D]],
        write: Callable[Concatenate[D, C], Awaitable[R]],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        dependency: None = None,
        hooks: type[Hooks[T, D, R]] | None = None,
        publish_hooks: type[PublishHooks] | None = None,
    ) -> ProjectionTask[[I], T, D, R]: ...

    @overload
    def projection[**P, T, D, R](
        self,
        *,
        read: Callable[P, Awaitable[T]],
        transform: Callable[[T], Awaitable[D]],
        write: Callable[[D], Awaitable[R]],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        dependency: None = None,
        hooks: type[Hooks[T, D, R]] | None = None,
        publish_hooks: type[PublishHooks] | None = None,
        payload: None = None,
    ) -> ProjectionTask[P, T, D, R]: ...

    @overload
    def projection[S, **P, T, D, R](
        self,
        *,
        dependency: type[S],
        read: Callable[Concatenate[S, P], Awaitable[T]],
        transform: Callable[[S, T], Awaitable[D]],
        write: Callable[[S, D], Awaitable[R]],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        hooks: type[Hooks[T, D, R]] | None = None,
        publish_hooks: type[PublishHooks] | None = None,
        payload: None = None,
    ) -> ProjectionTask[P, T, D, R]: ...

    def projection(
        self,
        *,
        read: Callable[..., Awaitable[Any]],
        transform: Callable[..., Awaitable[Any]],
        write: Callable[..., Awaitable[Any]],
        name: str | None = None,
        retry: Retry | None = None,
        labels: Mapping[str, Any] | None = None,
        dependency: type[Any] | None = None,
        hooks: type[Hooks[Any, Any, Any]] | None = None,
        publish_hooks: type[PublishHooks] | None = None,
        payload: type[Any] | None = None,
    ) -> ProjectionTask[..., Any, Any, Any]:
        if payload is not None and dependency is not None:
            raise TypeError("Use FromDishka with payload=, not dependency=")
        reader, converter, writer = (
            Function(read, dependency),
            Function(transform, dependency),
            Function(write, dependency),
        )

        if payload is not None:
            reader.check_payload(payload)
        elif any(
            stage.injected is not None for stage in (reader, converter, writer)
        ):
            raise TypeError(
                "FromDishka projections require payload=YourPayload"
            )
        converter.check_payload()
        writer.check_payload()
        task = ProjectionTask(
            reader,
            converter,
            writer,
            name=name,
            retry=retry,
            labels=labels,
            hooks=hooks,
            publish_hooks=publish_hooks,
        )
        self.tasks.append(task)
        return task
