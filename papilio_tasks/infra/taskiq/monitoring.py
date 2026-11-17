"""Narrow lifecycle adaptation of Taskiq's native OpenTelemetry middleware."""

from copy import copy
from datetime import UTC, datetime
from typing import Any

from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode, get_current_span
from taskiq import AsyncBroker, TaskiqMessage, TaskiqResult
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

from papilio_tasks.tools.monitoring import Monitoring


class Middleware(OpenTelemetryMiddleware):
    """Observe attempts even when retry intentionally saves no result.

    Native metrics and consumer spans are retained. The native post_save hook
    is skipped for NoResultError, so close at post_execute instead. Producer
    propagation deliberately borrows the current trace without opening a send
    span: Taskiq has no failure hook to close one when broker.kick fails.
    """

    def __init__(self, monitoring: Monitoring) -> None:
        self.monitoring = monitoring
        super().__init__(
            tracer_provider=monitoring.tracer_provider,
            meter_provider=monitoring.meter_provider,
        )

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        inject(message.labels)
        # Taskiq NATS forwards labels as transport headers, which require text.
        message.labels["_taskiq_queue_time"] = str(
            datetime.now(UTC).timestamp()
        )
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        self.n_tasks_sent_counter.add(
            1, attributes={"task_name": message.task_name}
        )

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        result = super().pre_execute(message)
        get_current_span().set_attribute(
            "messaging.message.id", message.task_id
        )
        return result

    def post_execute(
        self, message: TaskiqMessage, result: TaskiqResult[Any]
    ) -> None:
        measured = copy(message)
        measured.labels = message.labels.copy()
        stamp = measured.labels.get("_taskiq_queue_time")
        if stamp is not None:
            try:
                measured.labels["_taskiq_queue_time"] = float(stamp)
            except (ValueError, TypeError):
                # An unavailable/invalid sender clock is not zero queue wait.
                measured.labels.pop("_taskiq_queue_time")
        try:
            super().post_execute(measured, result)
        finally:
            super().post_save(message, result)

    def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        # An attempt failed even when retry is enabled. The native retry flag
        # alone cannot tell whether retry was scheduled or exhausted.
        span = get_current_span()
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, type(exception).__name__))

    def post_save(
        self, message: TaskiqMessage, result: TaskiqResult[Any]
    ) -> None:
        # Execution has ended; result persistence is a separate observation.
        pass


def instrument(broker: AsyncBroker, monitoring: Monitoring) -> Middleware:
    """Attach native telemetry once without changing registration or retries.

    Args:
        broker: Caller-owned native broker, before startup.
        monitoring: Borrowed shared telemetry providers.

    Returns:
        Existing equivalent middleware or the newly installed middleware.

    Raises:
        ValueError: Native telemetry or different providers already installed.
    """
    for existing in broker.middlewares:
        if isinstance(existing, OpenTelemetryMiddleware):
            if (
                isinstance(existing, Middleware)
                and existing.monitoring == monitoring
            ):
                return existing
            raise ValueError("Taskiq telemetry is already installed")
    middleware = Middleware(monitoring)
    broker.add_middlewares(middleware)
    return middleware
