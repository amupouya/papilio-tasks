from typing import Any, ClassVar

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from .base import Publisher, PublisherRoute

__all__ = ["ExchangeType", "RabbitExchange", "RabbitPublisher", "publisher"]


class RabbitPublisher[T](Publisher[T]):
    exchange: ClassVar[RabbitExchange]
    routing_key: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    exchange: RabbitExchange | str | None = None,
    routing_key: str = "",
    queue: RabbitQueue | str = "",
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare native exchange routing or default-exchange publication."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=RabbitBroker,
        options=dict(
            exchange=exchange, routing_key=routing_key, queue=queue, **options
        ),
    )
