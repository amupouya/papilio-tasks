"""Function routes over FastStream's native Nats router."""

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from faststream.nats import NatsBroker
from faststream.nats import NatsRouter as NativeRouter

from .base import Router


class NatsRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = NatsBroker

    def _subscription(
        self,
        publication: Mapping[str, Any],
        args: tuple[Any, ...],
        options: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if not args and "subject" not in options:
            options["subject"] = self._relative(publication["subject"])
        if publication.get("stream") is not None:
            options.setdefault("stream", deepcopy(publication["stream"]))
        return args, options
