"""Assemble Projection tasks with application-owned providers and discovery."""

from collections.abc import Sequence

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
from papilio_tasks.tools.hooks.publish import PublishHooks

from .base import Projection
from .registry import Registrar
from .router import ProjectionRouter


def create_broker(
    *,
    registrar: Registrar | None = None,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
    routers: Sequence[ProjectionRouter] = (),
    retry_source: MutableSourceContract | None = None,
    retry: RetryRuntime | None = None,
    publish_hooks: type[PublishHooks] | None = None,
) -> AsyncBroker:
    """Discover projections and attach one container to the native broker.

    Providers own Projection and hook construction. Assembly opens no network
    resources. Callers own native broker startup/shutdown; shutdown also closes
    the container. Pre-included tasks are retained.
    Configure retry with retry or the compatible retry_source keyword.
    Workers validate required retry resources before processing tasks.
    """
    registry = registrar if registrar is not None else Registrar()
    broker = registry.broker.native
    if "papilio_container" in broker.state:
        raise ValueError("Broker already has a Papilio application")

    try:
        if any(not isinstance(router, ProjectionRouter) for router in routers):
            raise TypeError("routers must contain ProjectionRouter instances")
        if publish_hooks is not None and (
            not isinstance(publish_hooks, type)
            or not issubclass(publish_hooks, PublishHooks)
        ):
            raise TypeError(
                "publish_hooks must be a PublishHooks class or None"
            )
        classes = Bootstrapper(modules).classes("projections", Projection)
        selected = list(
            dict.fromkeys(
                [
                    *Bootstrapper(modules).routers(
                        "projections", ProjectionRouter
                    ),
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
        for cls in classes:
            registry.include(cls)
        for task in tasks:
            task.include(registry.broker)
        container = make_async_container(TaskiqProvider(), *providers)
        setup_container(broker, container)
        broker.state.papilio_publish_hooks = publish_hooks
        broker.state.papilio_projection_hooks = registry.hooks

        install_retry(broker, runtime)
    except Exception:
        bindings.release(broker)
        raise

    return broker
