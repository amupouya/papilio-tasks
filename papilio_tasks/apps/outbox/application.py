"""Compose modular event routes over a caller-configured OutboxBroker."""

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from faststream import AckPolicy, FastStream
from faststream_outbox import OutboxBroker
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from papilio_tasks.apps.events.publishers.base import Publisher
from papilio_tasks.apps.events.publishers.bindings import get as get_sender
from papilio_tasks.apps.lifecycle import Producer, Producers
from papilio_tasks.infra.faststream.outbox import PolicyRetry, forward
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.retry import Retry

from .writer import OutboxWriter


class RelayApplication(FastStream):
    """Native CLI application owning its declared producer lifetimes."""

    def __init__(
        self,
        broker: OutboxBroker,
        producers: Sequence[Producer],
        publishers: Sequence[type[Publisher[Any]]],
    ) -> None:
        self.producers = Producers(*producers)
        self.publishers = tuple(publishers)
        self._closed = False
        super().__init__(broker, lifespan=self._lifespan)

    @asynccontextmanager
    async def _lifespan(self) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await self.stop()

    async def start(self, **options: Any) -> None:
        if self._closed:
            raise RuntimeError("Outbox relay is closed")
        try:
            await self.producers.connect()
            for publisher in self.publishers:
                get_sender(publisher)
            await super().start(**options)
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
            await self.producers.stop()


class Outbox:
    """Native storage/relay configuration with caller-owned transactions.

    Args:
        broker: Native OutboxBroker, optionally configured with engine, DLQ,
            observability and pool/lifecycle settings. No connection is opened.
        modules: Roots searched through the existing publishers discovery.
        publishers: Additional Publisher classes with stable outbox_key.
    """

    def __init__(
        self,
        broker: OutboxBroker,
        *,
        modules: Sequence[str] = (),
        publishers: Sequence[type[Publisher[Any]]] = (),
    ) -> None:
        if not isinstance(broker, OutboxBroker):
            raise TypeError("Outbox requires a native OutboxBroker")
        self.native = broker
        self._keys: dict[type[Publisher[Any]], str] = {}
        self._routes: dict[str, type[Publisher[Any]]] = {}
        self._sealed = False
        for cls in Bootstrapper(modules).classes("publishers", Publisher):
            if cls.outbox_key is not None:
                self.include(cls)
        for cls in publishers:
            self.include(cls)

    def include(self, publisher: type[Publisher[Any]]) -> None:
        """Register a stable route before relay assembly, idempotently.

        Raises:
            ValueError: Missing/invalid key or collision.
            RuntimeError: Relay already assembled.
        """
        if self._sealed:
            raise RuntimeError(
                "Register Outbox routes before creating the relay"
            )
        if not isinstance(publisher, type) or not issubclass(
            publisher, Publisher
        ):
            raise TypeError("Expected a Publisher class")
        key = publisher.outbox_key
        if not isinstance(key, str) or not key.strip() or len(key) > 255:
            raise ValueError("Publisher requires a stable nonempty outbox_key")
        if key in self._routes and self._routes[key] is not publisher:
            raise ValueError(f"Conflicting outbox_key: {key}")
        if publisher in self._keys and self._keys[publisher] != key:
            raise ValueError("Cannot change a registered outbox_key")
        self._keys[publisher] = key
        self._routes[key] = publisher

    def key(self, publisher: type[Publisher[Any]]) -> str:
        """Return a registered route, rejecting a changed key."""
        key = self._keys.get(publisher)
        if key is None:
            raise ValueError("Publisher is not registered with this Outbox")
        if publisher.outbox_key != key:
            raise ValueError("Cannot change a registered outbox_key")
        return key

    def writer(self, session: AsyncSession | AsyncConnection) -> OutboxWriter:
        """Bind a caller-owned handle; no transaction/connection is opened."""
        return OutboxWriter(self, session)

    def create_relay(
        self,
        *,
        producers: Sequence[Producer] = (),
        retry: Retry | None = None,
        **subscriber_options: Any,
    ) -> RelayApplication:
        """Register native workers and return an unstarted application.

        Args:
            producers: Target producer applications to connect and stop.
            retry: Optional tools policy adapted to native durable retry.
                Omit to use the native default or retry_strategy option.
            subscriber_options: Native worker, lease, polling, retry_strategy,
                max_deliveries and other OutboxBroker.subscriber options.

        Raises:
            ValueError: No routes, conflicting retry or acknowledgement policy.
            RuntimeError: Relay already assembled or broker has subscribers.
                Discard an assembly that fails during native registration.
        """
        if self._sealed or self.native.subscribers:
            raise RuntimeError("Outbox relay requires an unused native broker")
        if not self._routes:
            raise ValueError("Outbox relay requires registered publishers")
        Producers(*producers)
        if retry is not None:
            if not isinstance(retry, Retry):
                raise TypeError("retry must be Retry or None")
            if "retry_strategy" in subscriber_options:
                raise ValueError("Supply retry or retry_strategy, not both")
            subscriber_options["retry_strategy"] = PolicyRetry(retry)
        ack = subscriber_options.setdefault(
            "ack_policy", AckPolicy.NACK_ON_ERROR
        )
        if ack is not AckPolicy.NACK_ON_ERROR:
            raise ValueError("Outbox relay requires ack_policy NACK_ON_ERROR")
        for publisher in self._keys:
            self.key(publisher)
        self._sealed = True
        for key, publisher in self._routes.items():
            self.native.subscriber(key, **subscriber_options)(
                forward(publisher.publish)
            )
        return RelayApplication(self.native, producers, tuple(self._keys))
