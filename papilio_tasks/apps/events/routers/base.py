"""Delayed native routers sharing Events execution and publication."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from faststream import AckPolicy

from papilio_tasks.tools.functions import Function
from papilio_tasks.tools.retry import Retry

from ..publishers import bindings
from ..publishers.base import PublisherRoute
from ..registry.base import Registrar


@dataclass(frozen=True)
class _Subscriber:
    call: Function[Any]
    name: str
    retry: Retry | None
    args: tuple[Any, ...]
    options: dict[str, Any]


class Router:
    """The concrete transport supplies its native router and broker types.

    Declaration performs no I/O. create_app compiles selected capabilities;
    publisher-only discovery never installs the router's consumers.
    """

    native_router_type: ClassVar[type[Any]]
    native_broker_type: ClassVar[type[Any]]
    default_ack: ClassVar[AckPolicy | None] = None

    def __init__(self, **options: Any) -> None:
        self.options = dict(options)
        self.publishers: dict[PublisherRoute[Any], dict[str, Any]] = {}
        self.subscribers: list[_Subscriber] = []

    def publisher[T](
        self,
        event_type: type[T],
        *,
        name: str | None = None,
        outbox_key: str | None = None,
        **options: Any,
    ) -> PublisherRoute[T]:
        """Declare a typed sender; options are native transport arguments."""
        route = PublisherRoute(event_type, name=name, outbox_key=outbox_key)
        self.publishers[route] = dict(options)
        return route

    def subscriber[F: Callable[..., Awaitable[Any]]](
        self,
        *args: Any,
        name: str | None = None,
        retry: Retry | None = None,
        dependency: type[Any] | None = None,
        **options: Any,
    ) -> Callable[[F], F]:
        def decorate(function: F) -> F:
            call = Function(function, dependency)
            selected = call.name if name is None else name
            if not isinstance(selected, str) or not selected:
                raise ValueError("Subscriber name cannot be empty")
            self.subscribers.append(
                _Subscriber(call, selected, retry, tuple(args), dict(options))
            )
            return function

        return decorate

    def include(
        self,
        registrar: Registrar,
        *,
        publishers: bool = True,
        subscribers: bool = True,
    ) -> None:
        registrar.check_open()
        if not isinstance(registrar.broker.native, self.native_broker_type):
            raise TypeError("Router and broker must use the same transport")
        native = self.native_router_type(**dict(self.options))
        for route, options in self.publishers.items() if publishers else ():
            bindings.check(route)
            sender = native.publisher(**dict(options))
            bindings.add(
                route,
                registrar,
                registrar.sender(route, sender.publish, options),
            )
            registrar._publishers.add(route)
        for item in self.subscribers if subscribers else ():
            options = dict(item.options)
            # Preserve the class Events contract: handlers never auto-reply.
            if options.get("no_reply") is False:
                raise ValueError("Events subscribers require no_reply=True")
            options["no_reply"] = True
            ack = options.get("ack_policy", self.options.get("ack_policy"))
            handler = registrar.function_handler(
                item.call,
                name=item.name,
                retry=item.retry,
                ack_policy=ack,
                default_ack=self.default_ack,
            )
            native.subscriber(*item.args, **options)(handler)
        cast(Any, registrar.broker.native).include_router(native)
