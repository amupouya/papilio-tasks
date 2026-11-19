from typing import Any, ClassVar

from faststream.redis import RedisBroker, StreamSub

from .base import Publisher, PublisherRoute


class StreamPublisher[T](Publisher[T]):
    stream: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    stream: str | StreamSub,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a Redis Stream route, retaining native retention options."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=RedisBroker,
        options=dict(stream=stream, **options),
    )
