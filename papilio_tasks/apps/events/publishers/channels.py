from typing import Any, ClassVar

from faststream.redis import PubSub, RedisBroker

from .base import Publisher, PublisherRoute


class ChannelPublisher[T](Publisher[T]):
    channel: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    channel: str | PubSub,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a Redis Pub/Sub route without durability assumptions."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=RedisBroker,
        options=dict(channel=channel, **options),
    )
