from typing import Any, ClassVar

from faststream.kafka import KafkaBroker

from .base import Publisher, PublisherRoute


class KafkaPublisher[T](Publisher[T]):
    topic: ClassVar[str]


def publisher[T](
    event_type: type[T],
    *,
    topic: str,
    key: Any = None,
    partition: int | None = None,
    batch: bool = False,
    name: str | None = None,
    outbox_key: str | None = None,
    **options: Any,
) -> PublisherRoute[T]:
    """Declare a Kafka route; shared producer settings stay on KafkaBroker."""
    return PublisherRoute(
        event_type,
        name=name,
        outbox_key=outbox_key,
        broker_type=KafkaBroker,
        unpack=batch,
        options=dict(
            topic=topic, key=key, partition=partition, batch=batch, **options
        ),
    )
