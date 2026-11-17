"""Policy/target adapters; native faststream-outbox owns storage and loops."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from faststream_outbox.annotations import OutboxMessage

from papilio_tasks.tools.retry import Backoff, Retry


@dataclass(frozen=True)
class PolicyRetry:
    policy: Retry

    def get_next_attempt_delay(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
        exception: BaseException | None = None,
    ) -> float | None:
        """Adapt persisted attempt counts and errors to the tools policy."""
        if attempts_count >= self.policy.attempts:
            return None
        if exception is not None and not isinstance(
            exception, self.policy.errors
        ):
            return None
        delay = self.policy.delay
        return (
            delay.at(attempts_count) if isinstance(delay, Backoff) else delay
        )


def forward(
    sender: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[None]]:
    """Build a route-local native subscriber using the selected sender."""

    async def relay(body: dict[str, Any], message: OutboxMessage) -> None:
        options = dict(body["options"])
        options.setdefault("correlation_id", message.correlation_id)
        await sender(body["event"], **options)

    return relay
