"""Shared read/transform/write execution for class and function definitions."""

from collections.abc import Awaitable, Callable
from types import MappingProxyType
from typing import Any

from papilio_tasks.tools.hooks import emit
from papilio_tasks.tools.hooks.projection import (
    Call,
    Data,
    Failure,
    Hooks,
    Stage,
    Written,
)


async def run[T, D, R](
    *,
    name: str,
    read: Callable[..., Awaitable[T]],
    transform: Callable[[T], Awaitable[D]],
    write: Callable[[D], Awaitable[R]],
    local: Hooks[T, D, R],
    shared: Hooks[object, object, object] | None,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> R:
    common: Hooks[object, object, object] = (
        shared if shared is not None else Hooks()
    )
    call = Call(
        projection=name,
        args=args,
        kwargs=MappingProxyType(kwargs.copy()),
    )
    stage: Stage = "read"
    try:
        data = await read(*args, **kwargs)
        stage = "after_read"
        observed = Data(call, data)
        await emit(common.after_read, Data[object](call, data))
        await emit(local.after_read, observed)

        stage = "transform"
        converted = await transform(data)
        stage = "after_transform"
        transformed = Data(call, converted)
        await emit(common.after_transform, Data[object](call, converted))
        await emit(local.after_transform, transformed)

        stage = "write"
        result = await write(converted)
        stage = "after_write"
        written = Written(call, converted, result)
        await emit(
            common.after_write,
            Written[object, object](call, converted, result),
        )
        await emit(local.after_write, written)
        return result
    except Exception as error:
        failure = Failure(call, stage, error)
        try:
            await emit(common.on_error, failure)
            await emit(local.on_error, failure)
        except Exception as hook_error:
            raise error from hook_error
        raise
