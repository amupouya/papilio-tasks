from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, ClassVar

from .bindings import get


class Publisher[T]:
    """Declare an event route; the application owns its runtime sender."""

    outbox_key: ClassVar[str | None] = None

    @classmethod
    async def publish(cls, event: T, **options: Any) -> Any:
        return await get(cls)(event, **options)


class PublisherRoute[T]:
    """A typed function-router route with the same publication contract."""

    def __init__(
        self,
        event_type: type[T],
        *,
        name: str | None = None,
        outbox_key: str | None = None,
    ) -> None:
        self.event_type = event_type
        self.outbox_key = outbox_key
        self.__module__ = event_type.__module__
        self.__qualname__ = (
            name if name is not None else outbox_key or event_type.__qualname__
        )
        if not self.__qualname__:
            raise ValueError("Publisher name cannot be empty")

    async def publish(self, event: T, **options: Any) -> Any:
        return await get(self)(event, **options)


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
