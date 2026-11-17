# pyright: strict

from datetime import timedelta
from typing import assert_type

from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy_inbox import Inbox as NativeInbox

from papilio_tasks.apps.inbox import (
    CleanupResult,
    Inbox,
    InboxCleaner,
    SessionOrConnection,
)
from papilio_tasks.tools.monitoring import Monitoring


async def use(
    session: SessionOrConnection,
    table: Table,
    engine: AsyncEngine,
    monitor: Monitoring,
) -> None:
    assert_type(Inbox(session, table), NativeInbox)
    observed = monitor.inbox(session, table)
    assert_type(observed, NativeInbox)
    assert_type(await observed.claim(scope="orders", message_id="event"), bool)
    cleaner = InboxCleaner(
        engine=engine, table=table, retention=timedelta(days=7)
    )
    assert_type(await cleaner.cleanup(), CleanupResult)
    await observed.claim(scope="orders", message_id=None)  # expect-error
    monitor.inbox(42, table)  # expect-error
