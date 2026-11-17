"""Observe native Inbox claims without altering storage or transactions."""

import asyncio
from time import perf_counter

from sqlalchemy import Table
from sqlalchemy_inbox import Inbox as NativeInbox
from sqlalchemy_inbox import SessionOrConnection

from papilio_tasks.tools.monitoring import Monitoring


class Inbox(NativeInbox):
    def __init__(
        self,
        session: SessionOrConnection,
        table: Table,
        monitoring: Monitoring,
    ) -> None:
        super().__init__(session, table)
        self.tracer = monitoring.tracer_provider.get_tracer(
            "papilio_tasks.inbox"
        )
        meter = monitoring.meter_provider.get_meter("papilio_tasks.inbox")
        self.claims = meter.create_counter("papilio_tasks.inbox.claims")
        self.duration = meter.create_histogram(
            "papilio_tasks.inbox.claim.duration", unit="s"
        )

    async def claim(self, *, scope: str, message_id: str) -> bool:
        """Observe the native SQL decision before host commit."""
        started, outcome = perf_counter(), "error"
        attributes = {"scope": scope} if isinstance(scope, str) else {}
        try:
            with self.tracer.start_as_current_span(
                "papilio_tasks.inbox.claim", attributes=attributes
            ) as span:
                if isinstance(message_id, str):
                    span.set_attribute("messaging.message.id", message_id)
                claimed = await super().claim(
                    scope=scope, message_id=message_id
                )
                outcome = "claimed" if claimed else "duplicate"
                span.set_attribute("papilio_tasks.inbox.claimed", claimed)
                return claimed
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            labels = {**attributes, "outcome": outcome}
            self.claims.add(1, attributes=labels)
            self.duration.record(perf_counter() - started, attributes=labels)
