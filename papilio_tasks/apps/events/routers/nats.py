"""Function routes over FastStream's native Nats router."""

from faststream.nats import NatsBroker
from faststream.nats import NatsRouter as NativeRouter

from .base import Router


class NatsRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = NatsBroker
