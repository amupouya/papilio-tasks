"""Assemble schedulers from classes, function routers and providers."""

from collections.abc import Sequence
from typing import Any

from dishka import Provider, make_async_container
from dishka.integrations.taskiq import TaskiqProvider
from taskiq import AsyncBroker

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.application import (
    create_beat as create_beat,
)
from papilio_tasks.infra.taskiq.application import setup_container
from papilio_tasks.infra.taskiq.retry_runtime import (
    RetryRuntime,
    install_retry,
    prepare_retry,
)
from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
)
from papilio_tasks.tools.bootstrap import Bootstrapper

from .base import Scheduler
from .registry import Registrar
from .router import TaskRouter


def create_broker(
    *,
    registrar: Registrar[Any] | None = None,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
    routers: Sequence[TaskRouter] = (),
    retry_source: MutableSourceContract | None = None,
    retry: RetryRuntime | None = None,
) -> AsyncBroker:
    """Register classes/functions and wire per-job Dishka injection.

    Providers own scheduler construction and scope. Assembly performs no
    network I/O. The returned native broker owns application startup/shutdown.
    Configure retry with retry or the compatible retry_source keyword.
    Workers validate required retry resources before processing tasks.
    """
    registry = registrar if registrar is not None else Registrar()
    broker = registry.broker.native
    if "papilio_container" in broker.state:
        raise ValueError("Broker already has a Papilio application")

    try:
        if any(not isinstance(router, TaskRouter) for router in routers):
            raise TypeError("routers must contain TaskRouter instances")
        classes = Bootstrapper(modules).classes("schedulers", Scheduler)
        selected = list(
            dict.fromkeys(
                [
                    *Bootstrapper(modules).routers("schedulers", TaskRouter),
                    *routers,
                ]
            )
        )
        tasks = list(
            dict.fromkeys(task for router in selected for task in router.tasks)
        )
        runtime = prepare_retry(
            broker,
            retry,
            retry_source,
            [*(cls.retry for cls in classes), *(task.retry for task in tasks)],
        )
        container = make_async_container(TaskiqProvider(), *providers)
        for cls in classes:
            registry.include(cls)
        for task in tasks:
            task.include(registry.broker)
        setup_container(broker, container)

        install_retry(broker, runtime)
    except Exception:
        bindings.release(broker)
        raise

    return broker
