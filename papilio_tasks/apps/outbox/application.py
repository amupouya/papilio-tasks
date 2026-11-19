"""Compose modular event routes over a caller-configured OutboxBroker."""

from collections.abc import Sequence
from typing import Any

from faststream import AckPolicy
from faststream_outbox import OutboxBroker
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from papilio_tasks.apps.events.publishers.base import (
    Publication,
    Publisher,
    PublisherRoute,
)
from papilio_tasks.apps.events.routers.base import Router
from papilio_tasks.apps.lifecycle import Producer, Producers
from papilio_tasks.infra.faststream.outbox import PolicyRetry, forward
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.retry import Retry

from .relay import RelayApplication as RelayApplication
from .writer import OutboxWriter


class Outbox:
    """Native storage/relay configuration with caller-owned transactions.

    Args:
        broker: Native OutboxBroker, optionally configured with engine, DLQ,
            observability and pool/lifecycle settings. No connection is opened.
        modules: Roots searched through the existing publishers discovery.
        publishers: Additional typed declarations with stable outbox_key.
        routers: Explicit routers, merged with discovered publisher routers.
    """

    def __init__(
        self,
        broker: OutboxBroker,
        *,
        modules: Sequence[str] = (),
        publishers: Sequence[Publication[Any]] = (),
        routers: Sequence[Router] = (),
    ) -> None:
        if not isinstance(broker, OutboxBroker):
            raise TypeError("Outbox requires a native OutboxBroker")
        if any(not isinstance(router, Router) for router in routers):
            raise TypeError("routers must contain Events router instances")
        self.native = broker
        self._keys: dict[Publication[Any], str] = {}
        self._routes: dict[str, Publication[Any]] = {}
        self._sealed = False
        for cls in Bootstrapper(modules).classes("publishers", Publisher):
            if cls.outbox_key is not None:
                self.include(cls)
        for route in Bootstrapper(modules).instances(
            "publishers", PublisherRoute
        ):
            if route.broker_type is not None and route.outbox_key is not None:
                self.include(route)
        selected = dict.fromkeys(
            [
                *Bootstrapper(modules).routers("publishers", Router),
                *routers,
            ]
        )
        for router in selected:
            for route in router.publishers:
                if route.outbox_key is not None:
                    self.include(route)
        for cls in publishers:
            self.include(cls)

    def include(self, publisher: Publication[Any]) -> None:
        """Register a stable route before relay assembly, idempotently.

        Raises:
            ValueError: Missing/invalid key or collision.
            RuntimeError: Relay already assembled.
        """
        if self._sealed:
            raise RuntimeError(
                "Register Outbox routes before creating the relay"
            )
        if not isinstance(publisher, PublisherRoute) and (
            not isinstance(publisher, type)
            or not issubclass(publisher, Publisher)
        ):
            raise TypeError("Expected a Publisher class or router publisher")
        key = publisher.outbox_key
        if not isinstance(key, str) or not key.strip() or len(key) > 255:
            raise ValueError("Publisher requires a stable nonempty outbox_key")
        if key in self._routes and self._routes[key] is not publisher:
            raise ValueError(f"Conflicting outbox_key: {key}")
        if publisher in self._keys and self._keys[publisher] != key:
            raise ValueError("Cannot change a registered outbox_key")
        self._keys[publisher] = key
        self._routes[key] = publisher

    def key(self, publisher: Publication[Any]) -> str:
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
