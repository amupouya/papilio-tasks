"""Function routes over FastStream's native Rabbit router."""

from faststream.rabbit import RabbitBroker
from faststream.rabbit import RabbitRouter as NativeRouter

from .base import Router


class RabbitRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = RabbitBroker
