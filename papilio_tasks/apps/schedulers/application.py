"""Assemble schedulers from classes, function routers and providers."""

from collections.abc import Sequence
from typing import Any

from dishka import Provider, make_async_container
from dishka.integrations.taskiq import TaskiqProvider, setup_dishka
from taskiq import AsyncBroker, TaskiqEvents, TaskiqScheduler, TaskiqState

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.retry_runtime import (
    RetryRuntime,
    install_retry,
    prepare_retry,
    release_retry,
)
from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
    SourceContract,
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
        setup_dishka(container, broker)
        broker.state.papilio_container = container

        @broker.on_event(
            TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN
        )
        async def close_container(state: TaskiqState) -> None:
            try:
                await container.close()
            finally:
                try:
                    await release_retry(broker)
                finally:
                    bindings.release(broker)

        install_retry(broker, runtime)
    except Exception:
        bindings.release(broker)
        raise

    return broker


def create_beat(
    broker: AsyncBroker, *, sources: Sequence[SourceContract]
) -> TaskiqScheduler:
    """Reuse an assembled broker and caller-selected schedule sources.

    The native scheduler CLI owns source startup/shutdown. Construction opens
    no resources and does not create instances or register schedulers again.
    Explicit sources must include the broker's configured retry source.
    """
    native_sources = [source.native for source in sources]
    if "papilio_retry_source" in broker.state:
        retry_source = broker.state.papilio_retry_source
        if retry_source is not None and not any(
            source is retry_source.native for source in native_sources
        ):
            raise ValueError("Beat sources must include the retry_source")
    return TaskiqScheduler(broker, sources=native_sources)
