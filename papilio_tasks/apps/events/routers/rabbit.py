"""Function routes over FastStream's native Rabbit router."""

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit import RabbitRouter as NativeRouter

from .base import Router


class RabbitRouter(Router):
    native_router_type = NativeRouter
    native_broker_type = RabbitBroker

    def _subscription(
        self,
        publication: Mapping[str, Any],
        args: tuple[Any, ...],
        options: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if len(args) < 2:
            options.setdefault("exchange", publication.get("exchange"))
        queue = args[0] if args else options.get("queue")
        if queue is None or queue == "":
            raise ValueError("Rabbit subscriber requires its own queue")
        selected = deepcopy(RabbitQueue.validate(queue))
        if not selected.routing_key:
            target = (
                publication.get("routing_key")
                or RabbitQueue.validate(publication.get("queue", "")).routing()
            )
            if target:
                selected = RabbitQueue(
                    selected.name,
                    durable=selected.durable,
                    exclusive=selected.exclusive,
                    declare=selected.declare,
                    auto_delete=selected.auto_delete,
                    arguments=selected.arguments,
                    timeout=selected.timeout,
                    robust=selected.robust,
                    bind_arguments=selected.bind_arguments,
                    routing_key=self._relative(target),
                )
        if args:
            args = (selected, *args[1:])
        else:
            options["queue"] = selected
        return args, options
