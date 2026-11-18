"""Payload signatures and native Dishka injection for router functions."""

from collections.abc import Awaitable, Callable
from inspect import Parameter, Signature, iscoroutinefunction, signature
from typing import Any, get_type_hints

from dishka import AsyncContainer
from dishka.integrations.base import default_parse_dependency, wrap_injection


class Function[R]:
    """Keep serialized arguments separate from injected parameters."""

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
        if {"_scope", "dishka_container"} & selected.parameters.keys():
            raise TypeError("_scope and dishka_container are reserved")
        marked = {
            p.name
            for p in parameters
            if default_parse_dependency(p, self.hints.get(p.name)) is not None
        }
        self.injected: Callable[..., Awaitable[R]] | None = None
        if marked:
            if dependency is not None:
                raise TypeError("Choose FromDishka or dependency=, not both")
            if any(
                p.kind == Parameter.POSITIONAL_ONLY
                for p in parameters
                if p.name in marked
            ):
                raise TypeError(
                    "Injected parameters cannot be positional-only"
                )
            self.injected = wrap_injection(
                func=function,
                is_async=True,
                container_getter=lambda _, kwargs: kwargs["_scope"],
                additional_params=(
                    Parameter("_scope", Parameter.KEYWORD_ONLY),
                ),
            )
            parameters = [p for p in parameters if p.name not in marked]
            self.hints = {
                name: hint
                for name, hint in self.hints.items()
                if name not in marked
            }
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
        self.signature: Signature = selected.replace(
            parameters=[
                p.replace(annotation=self.hints.get(p.name, p.annotation))
                for p in parameters
            ],
            return_annotation=self.hints.get(
                "return", selected.return_annotation
            ),
        )

    def check_payload(self, expected: type[Any] | None = None) -> None:
        """Validate one ordinary payload/data parameter after DI removal."""
        parameters = list(self.signature.parameters.values())
        if len(parameters) != 1 or parameters[0].kind not in (
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise TypeError("Expected one positional payload/data argument")
        if expected is not None and (
            not isinstance(expected, type)
            or parameters[0].annotation != expected
        ):
            raise TypeError("The payload argument must annotate payload=")

    async def bind(self, scope: AsyncContainer) -> Callable[..., Awaitable[R]]:
        if self.injected is not None:
            return self._bind_native(scope)
        if self.dependency is None:
            return self.function
        service = await scope.get(self.dependency)

        async def invoke(*args: Any, **kwargs: Any) -> R:
            return await self.function(service, *args, **kwargs)

        return invoke

    def _bind_native(
        self, scope: AsyncContainer
    ) -> Callable[..., Awaitable[R]]:
        injected = self.injected
        assert injected is not None
        positional = next(iter(self.signature.parameters.values())).kind == (
            Parameter.POSITIONAL_ONLY
        )

        async def invoke(*args: Any, **kwargs: Any) -> R:
            bound = self.signature.bind(*args, **kwargs)
            if positional:
                return await injected(*bound.args, _scope=scope)
            return await injected(**bound.arguments, _scope=scope)

        return invoke
