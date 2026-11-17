"""Function routes over FastStream's native Redis router."""

from faststream.redis import RedisBroker
from faststream.redis import RedisRouter as NativeRouter

from .base import Router


class RedisRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = RedisBroker
