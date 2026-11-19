from typing import Any, ClassVar

from faststream.nats import JStream

from .base import Publisher, PublisherRoute
from .nats import publisher as nats_publisher


class JetPublisher[T](Publisher[T]):
    subject: ClassVar[str]
    stream: ClassVar[JStream]


def publisher[T](
    event_type: type[T],
    *,
    subject: str,
    stream: str | JStream,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a JetStream sender with an explicit native stream."""
    return nats_publisher(
        event_type,
        subject=subject,
        stream=stream,
        name=name,
        outbox_key=outbox_key,
        **options,
    )
