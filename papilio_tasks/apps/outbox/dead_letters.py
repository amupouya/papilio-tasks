"""Deliberate management of native Outbox dead letters in host transactions."""

from contextlib import nullcontext
from dataclasses import dataclass, fields
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Table, delete, insert, select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.sql import Executable

if TYPE_CHECKING:
    from papilio_tasks.tools.monitoring import Monitoring


@dataclass(frozen=True, kw_only=True)
class DeadLetter:
    """Native archive metadata; deliveries include lease redeliveries.

    Native DLQ storage does not retain attempts_count. Delivery count is not
    an exact count of failed handler executions.
    """

    id: int
    original_id: int
    queue: str
    deliveries_count: int
    created_at: datetime
    failed_at: datetime
    failure_reason: str
    last_exception: str | None
    timer_id: str | None


@dataclass(frozen=True, kw_only=True)
class DeadLetterDetail(DeadLetter):
    """Metadata plus the exact archived wire bytes and native headers."""

    payload: bytes
    headers: dict[str, str] | None


@dataclass(frozen=True)
class DeadLetterPage:
    items: tuple[DeadLetter, ...]
    next_cursor: int | None


_SUMMARY = tuple(field.name for field in fields(DeadLetter))


class DeadLetters:
    """Inspect/requeue native DLQ rows using caller-owned SQL handles.

    Args:
        session: AsyncSession or AsyncConnection with an active transaction.
        outbox_table: The same native pending table used by the relay.
        dlq_table: Its configured native DLQ table, including custom schemas.
        monitoring: Optional shared host-owned telemetry providers.

    No connection, transaction, retention job or broker is started here. The
    host owns route selection, authorization, commit and rollback. Tables are
    explicit so management never depends on private native broker attributes.
    """

    def __init__(
        self,
        session: AsyncSession | AsyncConnection,
        *,
        outbox_table: Table,
        dlq_table: Table,
        monitoring: "Monitoring | None" = None,
    ) -> None:
        if not isinstance(session, (AsyncSession, AsyncConnection)):
            raise TypeError(
                "DeadLetters requires AsyncSession or AsyncConnection"
            )
        self.session = session
        self.outbox_table = outbox_table
        self.dlq_table = dlq_table
        self._observer = None
        if monitoring is not None:
            # Telemetry is optional; ordinary Outbox installations omit OTel.
            from papilio_tasks.infra.faststream.dlq_monitoring import (
                Observer,
            )

            self._observer = Observer(monitoring)

    async def _execute(
        self,
        statement: Executable,
        *,
        operation: str,
        queue: str | None,
        identifier: int | None = None,
    ) -> list[RowMapping]:
        transaction = self.session.get_transaction()
        if transaction is None or not transaction.is_active:
            raise RuntimeError("DLQ management requires an active transaction")
        scope = (
            self._observer.operation(operation, queue)
            if self._observer is not None
            else nullcontext()
        )
        with scope as span:
            if span is not None and identifier is not None:
                span.set_attribute("papilio_tasks.dlq.id", identifier)
            result = await self.session.execute(statement)
            rows = list(result.mappings())
            if span is not None:
                span.set_attribute("papilio_tasks.dlq.rows", len(rows))
                if operation == "replay" and rows:
                    span.set_attribute(
                        "papilio_tasks.outbox.id", rows[0]["id"]
                    )
            return rows

    def _selection(self, identifier: int, queue: str | None):
        if type(identifier) is not int or identifier <= 0:
            raise ValueError("DLQ ID must be a positive integer")
        predicates = [self.dlq_table.c.id == identifier]
        if queue is not None:
            self._queue(queue)
            predicates.append(self.dlq_table.c.queue == queue)
        return predicates

    @staticmethod
    def _queue(queue: str) -> None:
        if not isinstance(queue, str) or not queue.strip():
            raise ValueError("Queue must be nonempty")

    async def list(
        self,
        *,
        queue: str | None = None,
        after: int = 0,
        limit: int = 50,
    ) -> DeadLetterPage:
        """Read an ID-ordered page without loading archived payloads/headers.

        Pass next_cursor as after for the next page, keeping the same filter.
        The host chooses a positive page size. Concurrent removals do not shift
        offsets; pages use the caller's database isolation rather than claiming
        a frozen multi-request snapshot.
        """
        if type(limit) is not int or limit <= 0:
            raise ValueError("Page limit must be a positive integer")
        if type(after) is not int or after < 0:
            raise ValueError("Page cursor must be a nonnegative integer")
        table = self.dlq_table
        statement = (
            select(*(table.c[name] for name in _SUMMARY))
            .where(table.c.id > after)
            .order_by(table.c.id)
            .limit(limit + 1)
        )
        if queue is not None:
            self._queue(queue)
            statement = statement.where(table.c.queue == queue)
        rows = await self._execute(statement, operation="list", queue=queue)
        items = tuple(DeadLetter(**row) for row in rows[:limit])
        return DeadLetterPage(
            items, items[-1].id if len(rows) > limit else None
        )

    async def get(
        self, identifier: int, *, queue: str | None = None
    ) -> DeadLetterDetail | None:
        """Read exact archive data, or None when absent/outside the filter."""
        rows = await self._execute(
            select(
                *(
                    self.dlq_table.c[name]
                    for name in (*_SUMMARY, "payload", "headers")
                )
            ).where(*self._selection(identifier, queue)),
            operation="get",
            queue=queue,
            identifier=identifier,
        )
        return DeadLetterDetail(**rows[0]) if rows else None

    async def replay(self, identifier: int, *, queue: str) -> int | None:
        """Atomically move one matching DLQ row back to its original queue.

        Returns the new pending row ID, or None if another committed replay
        already removed it, it is absent, or it belongs to a different queue.
        Payload, headers, event/correlation identity and timer_id are copied
        exactly. Pending counters, timestamps and lease state use native fresh
        defaults. No bus publication or payload re-encoding occurs.

        PostgreSQL serializes competing deletes. Both effects roll back with
        the host transaction/savepoint. A native unique timer conflict raises
        IntegrityError; the host must roll back, retaining the archived row.
        After commit, normal native relay polling discovers the new row.
        """
        self._queue(queue)
        columns = ("queue", "payload", "headers", "timer_id")
        archived = (
            delete(self.dlq_table)
            .where(*self._selection(identifier, queue))
            .returning(*(self.dlq_table.c[name] for name in columns))
            .cte("replayed_dead_letter")
        )
        statement = (
            insert(self.outbox_table)
            .from_select(
                list(columns), select(*(archived.c[name] for name in columns))
            )
            .returning(self.outbox_table.c.id)
        )
        rows = await self._execute(
            statement, operation="replay", queue=queue, identifier=identifier
        )
        return rows[0]["id"] if rows else None
