"""Method decorators over the constructor-injected self.outbox writer."""

from collections.abc import Awaitable, Callable, Coroutine, Sequence
from copy import deepcopy
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any, Concatenate, Protocol

from papilio_tasks.apps.events.publishers.base import (
    Publication,
)

from .writer import OutboxWriter


class OutboxOwner(Protocol):
    @property
    def outbox(self) -> OutboxWriter: ...


def _decorate[T, R, Owner: OutboxOwner, **P](
    publisher: Publication[T],
    execute: Callable[[OutboxWriter, Publication[T], R], Awaitable[None]],
) -> Callable[
    [Callable[Concatenate[Owner, P], Awaitable[R]]],
    Callable[Concatenate[Owner, P], Coroutine[Any, Any, R]],
]:
    def decorate(
        function: Callable[Concatenate[Owner, P], Awaitable[R]],
    ) -> Callable[Concatenate[Owner, P], Coroutine[Any, Any, R]]:
        if not iscoroutinefunction(function):
            raise TypeError("Outbox decorators require an async method")

        @wraps(function)
        async def wrapped(
            owner: Owner, /, *args: P.args, **kwargs: P.kwargs
        ) -> R:
            writer = owner.outbox
            transaction = writer.check()
            writer.runtime.key(publisher)
            result = await function(owner, *args, **kwargs)
            if writer.check() is not transaction:
                raise RuntimeError(
                    "Outbox method changed its owning transaction"
                )
            await execute(writer, publisher, result)
            return result

        return wrapped

    return decorate


def publish[T, R, Owner: OutboxOwner, **P](
    publisher: Publication[T],
    *,
    select: Callable[[R], T],
    **options: Any,
) -> Callable[
    [Callable[Concatenate[Owner, P], Awaitable[R]]],
    Callable[Concatenate[Owner, P], Coroutine[Any, Any, R]],
]:
    """Append selected output; put this inside the actual owning transaction.

    The owner supplies an injected self.outbox writer bound to the same
    session. No framework lookup, scope, commit or broker call is performed.
    Selection/insert errors propagate so the transaction owner can roll back.
    """
    settings = deepcopy(options)

    async def append(
        writer: OutboxWriter, target: Publication[T], result: R
    ) -> None:
        await writer.append(target, select(result), **settings)

    return _decorate(publisher, append)


def publish_batch[T, R, Owner: OutboxOwner, **P](
    publisher: Publication[T],
    *,
    select: Callable[[R], Sequence[T]],
    **options: Any,
) -> Callable[
    [Callable[Concatenate[Owner, P], Awaitable[R]]],
    Callable[Concatenate[Owner, P], Coroutine[Any, Any, R]],
]:
    """Append a caller-bounded selection in one native bulk insertion."""
    settings = deepcopy(options)

    async def append(
        writer: OutboxWriter, target: Publication[T], result: R
    ) -> None:
        await writer.append_batch(target, select(result), **settings)

    return _decorate(publisher, append)
