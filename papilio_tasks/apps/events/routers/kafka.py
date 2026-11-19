"""Function routes over FastStream's native Kafka router."""

from collections.abc import Mapping
from typing import Any

from faststream import AckPolicy
from faststream.kafka import KafkaBroker
from faststream.kafka import KafkaRouter as NativeRouter

from .base import Router


class KafkaRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = KafkaBroker
    default_ack = AckPolicy.ACK_FIRST

    def _subscription(
        self,
        publication: Mapping[str, Any],
        args: tuple[Any, ...],
        options: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if (
            not args
            and not options.get("pattern")
            and not options.get("partitions")
        ):
            args = (self._relative(publication["topic"]),)
        return args, options
