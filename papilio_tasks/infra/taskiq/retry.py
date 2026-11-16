"""Map policies to native labels and Taskiq retry execution."""

from copy import copy
from typing import Any

from taskiq import AsyncBroker, TaskiqMessage
from taskiq.kicker import AsyncKicker
from taskiq.middlewares import SmartRetryMiddleware

from papilio_tasks.tools.retry import Backoff, Retry


class _Retry(SmartRetryMiddleware):
    def make_delay(self, message: TaskiqMessage, retries: int) -> float:
        # Native `delay` also delays RabbitMQ publication, including attempt 1.
        if "retry_delay" in message.labels:
            labels = message.labels
            if "retry_multiplier" in labels:
                jitter = labels.get("retry_jitter", False)
                return Backoff(
                    initial=float(labels["retry_delay"]),
                    multiplier=float(labels["retry_multiplier"]),
                    max_delay=float(labels["retry_max_delay"]),
                    jitter=str(jitter).lower() == "true",
                ).at(retries)
            # Adapt only the calculation; never publish Rabbit's delay label.
            adapted = copy(message)
            adapted.labels = labels | {"delay": labels["retry_delay"]}
            return super().make_delay(adapted, retries)
        return super().make_delay(message, retries)

    async def on_send(
        self,
        kicker: AsyncKicker[Any, Any],
        message: TaskiqMessage,
        delay: float,
    ) -> None:
        if self.schedule_source is None:
            # Explicit immediate mode uses native republication, with no delay.
            await kicker.kiq(*message.args, **message.kwargs)
        else:
            await super().on_send(kicker, message, delay)


def retry_labels(
    broker: AsyncBroker,
    policy: Retry | None,
    labels: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge a class policy without overriding explicit task metadata."""
    metadata = dict(labels or {})
    if policy is None:
        return metadata
    if not isinstance(policy, Retry):
        raise TypeError("retry must be Retry or None")
    if policy.attempts > 1 and broker.state.get(
        "papilio_retry_worker_ready", False
    ):
        runtime = broker.state.get("papilio_retry_runtime")
        if runtime is None:
            raise ValueError("Worker retry requires a RetryRuntime")
        runtime.check_policy(policy)
    settings = {
        "retry_on_error": True,
        "max_retries": policy.attempts,
        "retry_delay": (
            policy.delay.initial
            if isinstance(policy.delay, Backoff)
            else policy.delay
        ),
        "types_of_exceptions": policy.errors,
    }
    if isinstance(policy.delay, Backoff):
        settings.update(
            retry_multiplier=policy.delay.multiplier,
            retry_max_delay=policy.delay.max_delay,
            retry_jitter=policy.delay.jitter,
        )
    elif {
        "retry_multiplier",
        "retry_max_delay",
        "retry_jitter",
    } & metadata.keys():
        raise ValueError("Backoff labels conflict with a fixed retry policy")
    reserved = {"_retries", "delay"} & metadata.keys()
    if reserved:
        names = ", ".join(sorted(reserved))
        raise ValueError(
            f"Retry labels conflict with the class policy: {names}"
        )
    for key, value in settings.items():
        if key in metadata and metadata[key] != value:
            raise ValueError(
                f"Retry label conflicts with the class policy: {key}"
            )
    metadata.update(settings)
    return metadata
