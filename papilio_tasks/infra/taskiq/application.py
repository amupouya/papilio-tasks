"""Shared native container lifecycle and explicit schedule-source assembly."""

from collections.abc import Sequence

from dishka import AsyncContainer
from dishka.integrations.taskiq import setup_dishka
from taskiq import AsyncBroker, TaskiqEvents, TaskiqScheduler, TaskiqState

from . import bindings
from .retry_runtime import release_retry
from .sources.contracts.base import SourceContract


def setup_container(broker: AsyncBroker, container: AsyncContainer) -> None:
    """Attach the existing container and release owned bindings on shutdown."""
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
