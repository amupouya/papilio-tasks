"""Short retries within one delivery; Tenacity owns waiting and iteration."""

from asyncio import sleep
from collections.abc import Awaitable, Callable
from typing import Any

from dishka import AsyncContainer
from faststream import ContextRepo, StreamMessage
from faststream.exceptions import HandlerException
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt

from papilio_tasks.tools.retry import Backoff, Retry


async def run(
    policy: Retry,
    container: AsyncContainer,
    message: StreamMessage[Any],
    context: ContextRepo,
    execute: Callable[[AsyncContainer], Awaitable[None]],
) -> None:
    """Execute attempts in independent request scopes, closing before waiting.

    Args:
        policy: Total attempts, exception filter and delay in seconds.
        container: Borrowed application container, never a message scope.
        message: This delivery's native message.
        context: Native per-task context used by the Dishka integration.
        execute: Subscriber and hook execution inside the fresh scope.

    Raises:
        Exception: The original final or nonmatching error. Native message
            control exceptions and cancellation propagate without retry.
    """

    async def attempt() -> None:
        async with container(
            {
                StreamMessage: message,
                type(message): message,
                ContextRepo: context,
            }
        ) as scope:
            with context.scope("dishka", scope):
                await execute(scope)

    retrying = AsyncRetrying(
        stop=stop_after_attempt(policy.attempts),
        retry=retry_if_exception(
            lambda error: (
                isinstance(error, policy.errors)
                and not isinstance(error, HandlerException)
            )
        ),
        wait=lambda state: (
            policy.delay.at(state.attempt_number)
            if isinstance(policy.delay, Backoff)
            else policy.delay
        ),
        sleep=sleep,
        reraise=True,
    )
    await retrying(attempt)
