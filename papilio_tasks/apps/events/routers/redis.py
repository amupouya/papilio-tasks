"""Function routes over FastStream's native Redis router."""

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from faststream.redis import RedisBroker
from faststream.redis import RedisRouter as NativeRouter

from .base import Router


class RedisRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = RedisBroker

    def _subscription(
        self,
        publication: Mapping[str, Any],
        args: tuple[Any, ...],
        options: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if not args and not any(
            key in options for key in ("channel", "stream", "list")
        ):
            key = next(
                key
                for key in ("channel", "stream", "list")
                if publication.get(key) is not None
            )
            target = deepcopy(publication[key])
            if isinstance(target, str):
                target = self._relative(target)
            else:
                target.name = self._relative(target.name)
            options[key] = target
        return args, options
