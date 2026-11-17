"""Internal declaration-to-task bindings for application assembly."""

from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

_tasks: dict[object, AsyncTaskiqDecoratedTask[Any, Any]] = {}


def check(cls: object) -> None:
    if cls in _tasks:
        name = getattr(cls, "__qualname__", cls)
        raise ValueError(f"Class or function already registered: {name}")


def add(cls: object, task: AsyncTaskiqDecoratedTask[Any, Any]) -> None:
    check(cls)
    _tasks[cls] = task


def get(cls: object) -> AsyncTaskiqDecoratedTask[Any, Any]:
    try:
        return _tasks[cls]
    except KeyError:
        name = getattr(cls, "__qualname__", cls)
        raise RuntimeError(
            f"Class or function is not included: {name}"
        ) from None


def release(broker: AsyncBroker) -> None:
    for cls, task in tuple(_tasks.items()):
        if task.broker is broker:
            del _tasks[cls]
