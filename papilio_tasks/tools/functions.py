"""Async function definitions with an explicit, non-payload dependency."""

from collections.abc import Awaitable, Callable
from inspect import Parameter, Signature, iscoroutinefunction, signature
from typing import Any, get_type_hints

from dishka import AsyncContainer
from dishka.integrations.base import default_parse_dependency


class Function[R]:
    """Keep payload signatures separate from a leading injected service."""

    def __init__(
        self,
        function: Callable[..., Awaitable[R]],
        dependency: type[Any] | None = None,
    ) -> None:
        if not iscoroutinefunction(function):
            raise TypeError("Router handlers must be async functions")
        self.function = function
        self.dependency = dependency
        self.name = f"{function.__module__}.{function.__qualname__}"
        self.hints = get_type_hints(function, include_extras=True)
        selected = signature(function)
        parameters = list(selected.parameters.values())
        if dependency is not None:
            if not isinstance(dependency, type):
                raise TypeError("dependency must be a provided service class")
            if not parameters or parameters[0].kind not in (
                Parameter.POSITIONAL_ONLY,
                Parameter.POSITIONAL_OR_KEYWORD,
            ):
                raise TypeError("The dependency must be the first argument")
            first = parameters.pop(0)
            if self.hints.get(first.name) != dependency:
                raise TypeError("First argument must annotate dependency")
            self.hints.pop(first.name, None)
        if {"_scope", "dishka_container"} & {p.name for p in parameters}:
            raise TypeError("_scope and dishka_container are reserved")
        if any(
            default_parse_dependency(p, self.hints.get(p.name)) is not None
            for p in parameters
        ):
            raise TypeError("Use dependency=Service for function injection")
        self.signature: Signature = selected.replace(
            parameters=[
                p.replace(annotation=self.hints.get(p.name, p.annotation))
                for p in parameters
            ],
            return_annotation=self.hints.get(
                "return", selected.return_annotation
            ),
        )

    async def bind(self, scope: AsyncContainer) -> Callable[..., Awaitable[R]]:
        if self.dependency is None:
            return self.function
        service = await scope.get(self.dependency)

        async def invoke(*args: Any, **kwargs: Any) -> R:
            return await self.function(service, *args, **kwargs)

        return invoke
