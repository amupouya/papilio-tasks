from typing import Any, ClassVar

from faststream.nats import JStream, NatsBroker

from .base import Publisher, PublisherRoute


class NatsPublisher[T](Publisher[T]):
    subject: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    subject: str,
    stream: str | JStream | None = None,
    headers: dict[str, str] | None = None,
    reply_to: str = "",
    timeout: float | None = None,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a Core NATS or JetStream sender without a consumer router."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=NatsBroker,
        options=dict(
            subject=subject,
            stream=stream,
            headers=headers,
            reply_to=reply_to,
            timeout=timeout,
            **options,
        ),
    )
