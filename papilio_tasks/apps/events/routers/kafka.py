"""Function routes over FastStream's native Kafka router."""

from faststream import AckPolicy
from faststream.kafka import KafkaBroker
from faststream.kafka import KafkaRouter as NativeRouter

from .base import Router


class KafkaRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = KafkaBroker
    default_ack = AckPolicy.ACK_FIRST
