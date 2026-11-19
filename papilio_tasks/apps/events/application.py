from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, cast

from dishka import AsyncContainer, Provider, make_async_container
from dishka_faststream import FastStreamProvider, setup_dishka
from faststream import FastStream

from papilio_tasks.apps.lifecycle import Producer, Producers
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks

from .publishers.base import Publication, Publisher, PublisherRoute
from .registry.base import Registrar
from .routers.base import Router
from .subscribers.base import Subscriber


class Application(FastStream):
    """Own broker and DI lifecycle; connect publishes, start also consumes."""

    def __init__(
        self,
        registrar: Registrar,
        container: AsyncContainer,
        producers: Sequence[Producer] = (),
    ) -> None:
        self.registrar = registrar
        self.container = container
        self._closed = False
        self.producers = Producers(*producers)
        # The native broker protocol only exposes lifecycle operations.
        super().__init__(
            cast(Any, registrar.broker.native), lifespan=self._lifespan
        )

    @asynccontextmanager
    async def _lifespan(self) -> AsyncIterator[None]:
        try:
            yield
        finally:
            # Native shutdown is skipped when startup fails; ownership isn't.
            await self.stop()

    async def connect(self) -> None:
        if self._closed:
            raise RuntimeError("Application is closed")
        await self.registrar.broker.connect()

    async def start(self, **run_extra_options: Any) -> None:
        if self._closed:
            raise RuntimeError("Application is closed")
        try:
            await self.producers.connect()
            await super().start(**run_extra_options)
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await super().stop()
        finally:
            try:
                await self.container.close()
            finally:
                try:
                    self.registrar.close()
                finally:
                    await self.producers.stop()


def create_app(
    *,
    registrar: Registrar,
    providers: Sequence[Provider] = (),
    publishers: Sequence[str | Publication[Any]] = (),
    subscribers: Sequence[str] = (),
    routers: Sequence[Router] = (),
    producers: Sequence[Producer] = (),
    publish_hooks: type[PublishHooks] | None = None,
    subscribe_hooks: type[SubscribeHooks] | None = None,
) -> Application:
    """Discover selected event modules and attach message-scoped DI.

    Paths are package roots containing publishers.py/subscribers.py or matching
    packages. Providers remain explicit. Existing manual registration wins.
    Assembly opens no connections; callers start and stop the returned app.
    """
    Producers(*producers)
    registrar.check_open()
    native = registrar.broker.native
    try:
        if any(not isinstance(router, Router) for router in routers):
            raise TypeError("routers must contain Events router instances")
        for selected, base, name in (
            (publish_hooks, PublishHooks, "publish_hooks"),
            (subscribe_hooks, SubscribeHooks, "subscribe_hooks"),
        ):
            if selected is not None and (
                not isinstance(selected, type)
                or not issubclass(selected, base)
            ):
                raise TypeError(
                    f"{name} must be a {base.__name__} class or None"
                )
        roots = [item for item in publishers if isinstance(item, str)]
        senders = Bootstrapper(roots).classes("publishers", Publisher)
        declarations = Bootstrapper(roots).instances(
            "publishers", PublisherRoute
        )
        for item in publishers:
            if isinstance(item, PublisherRoute):
                declarations.append(item)
            elif isinstance(item, type) and issubclass(item, Publisher):
                senders.append(item)
            elif not isinstance(item, str):
                raise TypeError(
                    "publishers must contain roots or typed publications"
                )
        receivers = Bootstrapper(subscribers).classes(
            "subscribers", Subscriber
        )
        # Merge capabilities before inclusion. A shared/re-exported router is
        # compiled once; producer-only roots must never install consumers.
        plans: dict[Router, tuple[bool, bool]] = {}
        for router in Bootstrapper(roots).routers("publishers", Router):
            plans[router] = (True, False)
        for router in Bootstrapper(subscribers).routers("subscribers", Router):
            plans[router] = (plans.get(router, (False, False))[0], True)
        for router in routers:
            plans[router] = (True, True)
        for cls in senders:
            if not registrar.has_publisher(cls):
                registrar.publisher(cls)
        for publication in dict.fromkeys(declarations):
            if (
                publication.broker_type is not None
                and not registrar.has_publisher(publication)
            ):
                publication.include(registrar)
        for cls in receivers:
            if not registrar.has_subscriber(cls):
                registrar.subscriber(cls)
        for router, (send, receive) in plans.items():
            router.include(registrar, publishers=send, subscribers=receive)
        container = make_async_container(FastStreamProvider(), *providers)
        # The shared lifecycle protocol excludes native DI middleware methods.
        setup_dishka(container, broker=cast(Any, native))
        registrar.container = container
        registrar.publish_hooks = publish_hooks
        registrar.subscribe_hooks = subscribe_hooks
    except Exception:
        registrar.close()
        raise
    registrar.attach()
    return Application(registrar, container, producers)
