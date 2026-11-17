"""Enrich native telemetry with stable identity and local attempts."""

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol
from weakref import WeakKeyDictionary

from faststream import BaseMiddleware, ContextRepo, StreamMessage
from faststream.exceptions import HandlerException
from faststream.opentelemetry import TelemetryMiddleware
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.metrics import Counter

from papilio_tasks.tools.monitoring import Monitoring


class Broker(Protocol):
    @property
    def middlewares(self) -> Sequence[Any]: ...

    def add_middleware(self, middleware: Any) -> None: ...


@dataclass(frozen=True)
class Attempts:
    """Bounded counters and trace events without payload retention."""

    started: Counter
    errors: Counter
    retried: Counter

    @contextmanager
    def attempt(self, subscriber: str, number: int) -> Iterator[None]:
        attributes = {"subscriber": subscriber}
        self.started.add(1, attributes=attributes)
        span = trace.get_current_span()
        span.add_event(
            "papilio_tasks.subscriber.attempt",
            {"subscriber": subscriber, "attempt": number},
        )
        try:
            yield
        except HandlerException:
            # Ack/nack/reject are native control signals, not attempt errors.
            raise
        except Exception as error:
            self.errors.add(
                1, attributes=attributes | {"error.type": type(error).__name__}
            )
            raise

    def retry(self, subscriber: str, number: int, delay: float) -> None:
        self.retried.add(1, attributes={"subscriber": subscriber})
        trace.get_current_span().add_event(
            "papilio_tasks.subscriber.retry",
            {"subscriber": subscriber, "attempt": number, "delay": delay},
        )


class _Middleware:
    """Configure native telemetry, identity and local attempt observations."""

    def __init__(
        self, native: Callable[..., BaseMiddleware], monitoring: Monitoring
    ) -> None:
        self.native = native
        meter = monitoring.meter_provider.get_meter(
            "papilio_tasks.subscribers"
        )
        self.attempts = Attempts(
            meter.create_counter("papilio_tasks.subscriber.attempts"),
            meter.create_counter("papilio_tasks.subscriber.errors"),
            meter.create_counter("papilio_tasks.subscriber.retries"),
        )

    def __call__(self, msg: Any, /, *, context: ContextRepo) -> "_Scope":
        return _Scope(
            msg,
            context=context,
            native=self.native(msg, context=context),
            attempts=self.attempts,
        )


class _Scope(BaseMiddleware):
    def __init__(
        self,
        msg: Any,
        *,
        context: ContextRepo,
        native: BaseMiddleware,
        attempts: Attempts,
    ) -> None:
        super().__init__(msg, context=context)
        self.native = native
        self.attempts = attempts

    async def on_receive(self) -> None:
        await self.native.on_receive()

    async def consume_scope(
        self, call_next: Callable[..., Awaitable[Any]], msg: StreamMessage[Any]
    ) -> Any:
        async def observed(message: StreamMessage[Any]) -> Any:
            span = trace.get_current_span()
            # IDs aid trace lookup; they never become metric attributes.
            for header in ("event_id", "correlation_id"):
                value = message.headers.get(header)
                if isinstance(value, str):
                    span.set_attribute(f"papilio_tasks.{header}", value)
            with self.context.scope("_papilio_attempts", self.attempts):
                try:
                    result = await call_next(message)
                except asyncio.CancelledError:
                    span.set_attribute("papilio_tasks.cancelled", True)
                    raise
            return result

        # Native 0.7's exceptional consume path skips detach. Restore through
        # public context APIs; native instrumentation still owns its spans.
        token = otel_context.attach(otel_context.get_current())
        try:
            result = await self.native.consume_scope(observed, msg)
        finally:
            otel_context.detach(token)
        return result

    async def publish_scope(
        self, call_next: Callable[..., Awaitable[Any]], cmd: Any
    ) -> Any:
        token = otel_context.attach(otel_context.get_current())
        try:
            result = await self.native.publish_scope(call_next, cmd)
        finally:
            otel_context.detach(token)
        return result

    async def after_processed(self, *args: Any, **kwargs: Any) -> bool | None:
        return await self.native.after_processed(*args, **kwargs)


_installed: WeakKeyDictionary[Any, tuple[Monitoring, Any, Any]] = (
    WeakKeyDictionary()
)


def instrument[T: Callable[..., Any]](
    broker: Broker, factory: Callable[..., T], monitoring: Monitoring
) -> T:
    """Attach telemetry once per native broker, preserving native resources.

    Args:
        broker: Borrowed native transport broker before startup.
        factory: Native TelemetryMiddleware constructor for this transport.
        monitoring: Borrowed providers shared with other brokers.

    Returns:
        The native telemetry factory. Equivalent repeated installation is
        idempotent; no exporter or HTTP server is started.

    Raises:
        ValueError: A different factory/provider pair is already installed.
    """
    previous = _installed.get(broker)
    if previous is not None:
        configured, selected, native = previous
        if configured != monitoring or selected is not factory:
            raise ValueError("FastStream telemetry is already installed")
        return native
    if any(
        isinstance(item, TelemetryMiddleware) for item in broker.middlewares
    ):
        raise ValueError("Native FastStream telemetry is already installed")
    native = factory(
        tracer_provider=monitoring.tracer_provider,
        meter_provider=monitoring.meter_provider,
        include_messages_counters=True,
    )
    broker.add_middleware(_Middleware(native, monitoring))
    _installed[broker] = monitoring, factory, native
    return native
