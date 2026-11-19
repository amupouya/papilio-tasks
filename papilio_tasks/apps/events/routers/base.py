"""Delayed native routers sharing Events execution and publication."""

from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from faststream import AckPolicy
from faststream.types import BrokerMiddleware

from papilio_tasks.infra.dishka import Function
from papilio_tasks.tools.retry import Retry

from ..publishers.base import PublisherRoute
from ..registry.base import Registrar


@dataclass(frozen=True)
class _Subscriber:
    call: Function[Any]
    name: str
    retry: Retry | None
    args: tuple[Any, ...]
    options: dict[str, Any]
    publisher: PublisherRoute[Any] | None


class Router:
    """The concrete transport supplies its native router and broker types.

    Declaration performs no I/O. create_app compiles selected capabilities;
    publisher-only discovery never installs the router's consumers.
    """

    native_router_type: ClassVar[type[Any]]
    native_broker_type: ClassVar[type[Any]]
    default_ack: ClassVar[AckPolicy | None] = None

    def __init__(
        self,
        prefix: str = "",
        *,
        dependencies: Iterable[Any] = (),
        middlewares: Sequence[BrokerMiddleware[Any, Any]] = (),
        parser: Callable[..., Any] | None = None,
        decoder: Callable[..., Any] | None = None,
        include_in_schema: bool | None = None,
        ack_policy: AckPolicy | None = None,
        **options: Any,
    ) -> None:
        """Group native subscriber settings; per-handler options override them.

        Connection settings, queue/consumer identities and resource retention
        belong to their native owners. Retry remains explicit per handler.
        """
        self.options: dict[str, Any] = dict(
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            parser=parser,
            decoder=decoder,
            include_in_schema=include_in_schema,
            **options,
        )
        if ack_policy is not None:
            self.options["ack_policy"] = ack_policy
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
        publisher: PublisherRoute[Any] | None = None,
        **options: Any,
    ) -> Callable[[F], F]:
        def decorate(function: F) -> F:
            call = Function(function, dependency)
            selected = call.name if name is None else name
            if not isinstance(selected, str) or not selected:
                raise ValueError("Subscriber name cannot be empty")
            self.subscribers.append(
                _Subscriber(
                    call,
                    selected,
                    retry,
                    tuple(args),
                    dict(options),
                    publisher,
                )
            )
            return function

        return decorate

    def _relative(self, address: str) -> str:
        """Let native prefixing recreate the full publication address."""
        prefix = self.options["prefix"]
        if not address.startswith(prefix):
            raise ValueError(
                "Publisher destination is outside the router prefix"
            )
        return address[len(prefix) :]

    def _subscription(
        self,
        publication: Mapping[str, Any],
        args: tuple[Any, ...],
        options: dict[str, Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        raise NotImplementedError

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
            registrar.bind_publisher(
                route,
                lambda: native.publisher(**dict(options)).publish,
                options,
            )
        for item in self.subscribers if subscribers else ():
            options = dict(item.options)
            args = item.args
            if item.publisher is not None:
                route = item.publisher
                if (
                    not isinstance(route, PublisherRoute)
                    or route.broker_type is not self.native_broker_type
                ):
                    raise ValueError(
                        "Subscriber and publisher must use the same transport"
                    )
                effective = registrar.publisher_options(route)
                publication = dict(
                    route.options if effective is None else effective
                )
                args, options = self._subscription(publication, args, options)
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
            native.subscriber(*args, **options)(handler)
        cast(Any, registrar.broker.native).include_router(native)
