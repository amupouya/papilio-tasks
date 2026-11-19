from collections.abc import Awaitable, Callable, Coroutine, Mapping
from copy import deepcopy
from functools import wraps
from inspect import iscoroutinefunction
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, cast

from .bindings import get
from .contracts import Sender

if TYPE_CHECKING:
    from ..registry.base import Registrar


class Publisher[T]:
    """Declare an event route; the application owns its runtime sender."""

    outbox_key: ClassVar[str | None] = None

    @classmethod
    async def publish(cls, event: T, **options: Any) -> Any:
        return await get(cls)(event, **options)


class PublisherRoute[T]:
    """A typed publication; the application owns its native sender."""

    def __init__(
        self,
        event_type: type[T],
        *,
        name: str | None = None,
        outbox_key: str | None = None,
        broker_type: type[Any] | None = None,
        options: Mapping[str, Any] | None = None,
        unpack: bool = False,
    ) -> None:
        self.event_type = event_type
        self.outbox_key = outbox_key
        self.broker_type = broker_type
        self.options = MappingProxyType(dict(options or {}))
        self._unpack = unpack
        self.__module__ = event_type.__module__
        self.__qualname__ = (
            name if name is not None else outbox_key or event_type.__qualname__
        )
        if not self.__qualname__:
            raise ValueError("Publisher name cannot be empty")

    async def publish(self, event: T, **options: Any) -> Any:
        return await get(self)(event, **options)

    def include(self, registrar: "Registrar", **options: Any) -> None:
        """Bind once at bootstrap, with explicit native option overrides.

        No connection is opened. Existing app ownership, hooks and shutdown
        apply. Subscriber references use this app's effective options.
        """
        if self.broker_type is None:
            raise ValueError(
                "Include a legacy router publisher through its router"
            )
        if not isinstance(registrar.broker.native, self.broker_type):
            raise ValueError(
                "Publisher and broker must use the same transport"
            )
        selected = deepcopy(dict(self.options))
        selected.update(options)

        def create() -> Sender:
            sender = (
                cast(Any, registrar.broker.native)
                .publisher(**selected)
                .publish
            )
            if not self._unpack:
                return sender

            async def batch(events: Any, **kwargs: Any) -> Any:
                return await sender(*events, **kwargs)

            return batch

        registrar.bind_publisher(self, create, selected)


type Publication[T] = type[Publisher[T]] | PublisherRoute[T]


def publish[T, **P, R](
    publisher: Publication[T],
    *,
    select: Callable[[Any], T],
    **options: Any,
) -> Callable[
    [Callable[P, Awaitable[R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    """Publish selected output after success and return the original result."""

    def decorate(
        func: Callable[P, Awaitable[R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        if not iscoroutinefunction(func):
            raise TypeError("publish requires an async function")

        @wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            result = await func(*args, **kwargs)
            await publisher.publish(select(result), **options)
            return result

        return wrapped

    return decorate
