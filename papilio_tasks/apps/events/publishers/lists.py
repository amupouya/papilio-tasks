from typing import Any, ClassVar

from faststream.redis import ListSub, RedisBroker

from .base import Publisher, PublisherRoute


class ListPublisher[T](Publisher[T]):
    list: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    list: str | ListSub,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a Redis List route, preserving native batch options."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=RedisBroker,
        unpack=isinstance(list, ListSub) and list.batch,
        options=dict(list=list, **options),
    )
