"""Durable registration inside an existing caller-owned SQL transaction."""

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    AsyncSessionTransaction,
    AsyncTransaction,
)

from papilio_tasks.apps.events.publishers.base import Publisher

if TYPE_CHECKING:
    from .application import Outbox


class OutboxWriter:
    """Bind a session; never begin, commit, close or publish to a bus."""

    def __init__(
        self, runtime: "Outbox", session: AsyncSession | AsyncConnection
    ) -> None:
        if not isinstance(session, (AsyncSession, AsyncConnection)):
            raise TypeError(
                "OutboxWriter requires AsyncSession or AsyncConnection"
            )
        self.runtime = runtime
        self.session = session

    def check(self) -> AsyncSessionTransaction | AsyncTransaction:
        """Return an active caller-owned transaction."""
        transaction = self.session.get_transaction()
        if transaction is None or not transaction.is_active:
            raise RuntimeError(
                "Outbox registration requires an active transaction"
            )
        return transaction

    @staticmethod
    def _body(event: Any, options: dict[str, Any]) -> dict[str, Any]:
        settings = options.copy()
        headers = dict(settings.get("headers") or {})
        headers.setdefault("event_id", str(uuid4()))
        if not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in headers.items()
        ):
            raise TypeError("Outbox headers must be string pairs")
        if not headers["event_id"]:
            raise ValueError("event_id header must be nonempty")
        settings["headers"] = headers
        # Publication options cross a durable wire boundary; do not stringify
        # arbitrary native objects and silently change their later meaning.
        json.dumps(settings, allow_nan=False)
        return {"event": event, "options": settings}

    async def append[T](
        self, publisher: type[Publisher[T]], event: T, **options: Any
    ) -> int:
        """Insert an event with the caller's writes; return its pending row ID.

        Args:
            publisher: Registered typed declaration with a stable outbox_key.
            event: Payload encoded through the native outbox encoder.
            options: JSON-compatible native target publication options.
                Headers receive a stable event_id unless explicitly supplied.

        Raises:
            RuntimeError: No active transaction.
            ValueError: Unknown/mutated route or invalid event ID/options.
            Exception: Native encoding/SQL errors; the caller owns rollback.
        """
        self.check()
        key = self.runtime.key(publisher)
        body = self._body(event, options)
        identifier = await self.runtime.native.publish(
            body,
            queue=key,
            session=self.session,
            headers=body["options"]["headers"],
            correlation_id=options.get("correlation_id"),
        )
        if identifier is None:
            raise RuntimeError("Native Outbox insert returned no row identity")
        return identifier

    async def append_batch[T](
        self,
        publisher: type[Publisher[T]],
        events: Sequence[T],
        **options: Any,
    ) -> None:
        """Insert independent events in one native batch, each with its own ID.

        Empty selections do no I/O. The caller bounds/materializes the batch
        and owns transaction completion. A list-shaped event passed to append
        remains one event; this method registers several separate events.
        """
        if not events:
            return
        self.check()
        key = self.runtime.key(publisher)
        bodies = [self._body(event, options) for event in events]
        await self.runtime.native.publish_batch(
            *bodies, queue=key, session=self.session
        )
