from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from inspect import isabstract, iscoroutinefunction, signature
from types import MappingProxyType
from typing import Any, cast, get_type_hints
from weakref import WeakSet

from dishka import AsyncContainer
from dishka_faststream import FromDishka, inject
from faststream import AckPolicy, ContextRepo, StreamMessage

from papilio_tasks.infra.faststream.brokers.contracts.base import (
    BrokerContract,
)
from papilio_tasks.tools.functions import Function
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import (
    SubscribeCall,
    SubscribeHooks,
)
from papilio_tasks.tools.hooks.subscribe import run as run_hooks
from papilio_tasks.tools.retry import Retry

from ..publishers import bindings
from ..publishers.base import Publication, Publisher
from ..publishers.contracts import Sender
from ..publishers.send import send
from ..subscribers.base import Subscriber

_owners: WeakSet[object] = WeakSet()


class Registrar(ABC):
    def __init__(self, broker: BrokerContract[Any]) -> None:
        self.broker = broker
        self.attached = False
        self.closed = False
        self.container: AsyncContainer | None = None
        self.publish_hooks: type[PublishHooks] | None = None
        self.subscribe_hooks: type[SubscribeHooks] | None = None
        self._publishers: set[Publication[Any]] = set()
        self._handler_names: set[str] = set()
        self._subscribers: set[type[Subscriber[Any]]] = set()

    @abstractmethod
    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None: ...

    @abstractmethod
    def subscriber(
        self, cls: type[Subscriber[Any]], **options: Any
    ) -> None: ...

    def has_publisher(self, cls: type[Publisher[Any]]) -> bool:
        return cls in self._publishers

    def has_subscriber(self, cls: type[Subscriber[Any]]) -> bool:
        return cls in self._subscribers

    def check_open(self) -> None:
        if self.closed or self.attached:
            raise RuntimeError(
                "Register events before creating the application"
            )
        if self.broker.native in _owners:
            raise ValueError("Broker already has an Events application")

    def attach(self) -> None:
        self.check_open()
        _owners.add(self.broker.native)
        self.attached = True

    def sender(
        self,
        cls: Publication[Any],
        native: Sender,
        meta: Mapping[str, object],
    ) -> Sender:
        metadata = MappingProxyType(dict(meta))

        async def execute(event: Any, **options: Any) -> Any:
            return await send(
                cls,
                event,
                options,
                native,
                container=self.container,
                hooks=self.publish_hooks,
                meta=metadata,
            )

        return execute

    def handler(
        self,
        cls: type[Subscriber[Any]],
        *,
        ack_policy: AckPolicy | None = None,
        default_ack: AckPolicy | None = None,
    ) -> Any:
        self.check_open()
        if cls in self._subscribers:
            raise ValueError(
                f"Subscriber already registered: {cls.__qualname__}"
            )
        run = cls.run
        params = list(signature(run).parameters.values())
        if (
            isabstract(cls)
            or not iscoroutinefunction(run)
            or len(params) != 2
            or params[1].name != "event"
            or params[1].kind
            not in (params[1].POSITIONAL_ONLY, params[1].POSITIONAL_OR_KEYWORD)
        ):
            raise TypeError("Subscriber must implement async run(self, event)")
        hints = get_type_hints(run)
        if "event" not in hints:
            raise TypeError("Subscriber.run must annotate event with its DTO")

        async def resolve(
            scope: AsyncContainer,
        ) -> Callable[..., Awaitable[Any]]:
            subscriber = await scope.get(cls)
            return subscriber.run

        return self._handler(
            name=f"{cls.__module__}.{cls.__qualname__}",
            event_type=hints["event"],
            policy=cls.retry,
            resolve=resolve,
            ack_policy=ack_policy,
            default_ack=default_ack,
        )

    def function_handler(
        self,
        call: Function[Any],
        *,
        name: str,
        retry: Retry | None,
        ack_policy: AckPolicy | None = None,
        default_ack: AckPolicy | None = None,
    ) -> Any:
        self.check_open()
        parameters = list(call.signature.parameters.values())
        if len(parameters) != 1 or parameters[0].kind not in (
            parameters[0].POSITIONAL_ONLY,
            parameters[0].POSITIONAL_OR_KEYWORD,
        ):
            raise TypeError(
                "Subscriber functions must accept one event argument"
            )
        if parameters[0].name not in call.hints:
            raise TypeError("Subscriber functions must annotate the event DTO")
        return self._handler(
            name=name,
            event_type=call.hints[parameters[0].name],
            policy=retry,
            resolve=call.bind,
            ack_policy=ack_policy,
            default_ack=default_ack,
        )

    def _handler(
        self,
        *,
        name: str,
        event_type: Any,
        policy: Retry | None,
        resolve: Callable[
            [AsyncContainer], Awaitable[Callable[..., Awaitable[Any]]]
        ],
        ack_policy: AckPolicy | None,
        default_ack: AckPolicy | None,
    ) -> Any:
        if not name:
            raise ValueError("Subscriber name cannot be empty")
        if name in self._handler_names:
            raise ValueError(f"Subscriber name already registered: {name}")

        async def execute(event: Any, *, _scope: AsyncContainer) -> None:
            handler = await resolve(_scope)
            if self.subscribe_hooks is None:
                await handler(event)
                return
            hooks = await _scope.get(self.subscribe_hooks)
            await run_hooks(SubscribeCall(name, event), handler, hooks)

        execute.__annotations__ = {
            "event": event_type,
            "_scope": FromDishka[AsyncContainer],
            "return": None,
        }

        if policy is not None:
            if not isinstance(policy, Retry):
                raise TypeError("Subscriber.retry must be Retry or None")
            selected = (
                ack_policy
                if ack_policy is not None
                else cast(Any, self.broker.native).config.ack_policy
            )
            if not isinstance(selected, AckPolicy):
                selected = default_ack
            if selected in (AckPolicy.ACK_FIRST, AckPolicy.MANUAL):
                raise ValueError(
                    "Subscriber retry requires ack after execution; "
                    "ack_policy ACK_FIRST and MANUAL are incompatible"
                )
            # Ordinary Events imports must not require optional Tenacity.
            try:
                from papilio_tasks.infra.faststream.retry import run as retry
            except ModuleNotFoundError as error:
                if error.name == "tenacity":
                    raise ImportError(
                        "Subscriber retry requires papilio-tasks[events-retry]"
                    ) from error
                raise

            async def retried(
                event: Any, *, _message: StreamMessage, _scope: AsyncContainer
            ) -> None:
                async def attempt(scope: AsyncContainer) -> None:
                    await execute(event, _scope=scope)

                if self.container is None:
                    raise RuntimeError("Subscriber retry requires create_app")
                context = await _scope.get(ContextRepo)
                await retry(
                    policy,
                    self.container,
                    _message,
                    context,
                    attempt,
                    subscriber=name,
                )

            retried.__annotations__ = {
                "event": event_type,
                "_message": FromDishka[StreamMessage],
                "_scope": FromDishka[AsyncContainer],
                "return": None,
            }

            self._handler_names.add(name)
            return inject(retried)
        self._handler_names.add(name)
        return inject(execute)

    def close(self) -> None:
        self.closed = True
        bindings.release(self)
