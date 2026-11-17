import asyncio

import pytest

pytest.importorskip("sqlalchemy_inbox")
pytest.importorskip("opentelemetry.sdk")

from monitoring_support import observations as observations
from opentelemetry.trace import StatusCode
from sqlalchemy import event, insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from test_inbox import count
from test_inbox import database as database

from papilio_tasks.apps.inbox import Inbox


async def test_claim_metrics_are_sql_decisions_and_borrow_providers(
    database, observations
):
    engine, table, _ = database
    monitor = observations.monitoring
    tracer = monitor.tracer_provider.get_tracer("consumer-test")
    async with async_sessionmaker(engine)() as session:
        inbox = monitor.inbox(session, table)
        assert isinstance(inbox, Inbox)
        with tracer.start_as_current_span("consume") as parent:
            with pytest.raises(ValueError, match="rollback"):
                async with session.begin():
                    assert await inbox.claim(
                        scope="orders", message_id="stable"
                    )
                    assert not await inbox.claim(
                        scope="orders", message_id="stable"
                    )
                    raise ValueError("rollback")
            async with session.begin():
                assert await inbox.claim(scope="orders", message_id="stable")
    spans = [
        s
        for s in observations.spans.get_finished_spans()
        if s.name == "papilio_tasks.inbox.claim"
    ]
    assert len(spans) == 3
    assert all(
        s.parent.span_id == parent.get_span_context().span_id for s in spans
    )
    assert all(s.attributes["messaging.message.id"] == "stable" for s in spans)
    assert [s.attributes["papilio_tasks.inbox.claimed"] for s in spans] == [
        True,
        False,
        True,
    ]
    points = observations.points("papilio_tasks.inbox.claims")
    assert {p.attributes["outcome"]: p.value for p in points} == {
        "claimed": 2,
        "duplicate": 1,
    }
    assert all(set(p.attributes) == {"scope", "outcome"} for p in points)
    assert (
        sum(
            p.count
            for p in observations.points("papilio_tasks.inbox.claim.duration")
        )
        == 3
    )
    # The successful claim inside a rolled-back host transaction is still a
    # claim observation, never a committed-processing counter.
    assert await count(engine, table) == 1
    with tracer.start_as_current_span("host-still-owns-providers"):
        pass


async def test_claim_validation_error_retains_native_semantics(
    database, observations
):
    engine, table, _ = database
    async with engine.connect() as connection:
        inbox = observations.monitoring.inbox(connection, table)
        with pytest.raises(RuntimeError, match="transaction"):
            await inbox.claim(scope="orders", message_id="event")
    point = observations.points("papilio_tasks.inbox.claims")[0]
    assert point.attributes == {"scope": "orders", "outcome": "error"}
    assert (
        observations.spans.get_finished_spans()[0].status.status_code
        is StatusCode.ERROR
    )
    assert await count(engine, table) == 0


async def test_cancelled_competing_claim_rolls_back_and_closes_span(
    database, observations
):
    engine, table, _ = database
    entered = asyncio.Event()

    async def competing():
        async with engine.begin() as connection:
            await observations.monitoring.inbox(connection, table).claim(
                scope="orders", message_id="event"
            )

    def executing(conn, cursor, statement, parameters, context, many):
        if statement.startswith("INSERT INTO " + table.name):
            entered.set()

    task = None
    async with engine.connect() as connection:
        transaction = await connection.begin()
        assert await Inbox(connection, table).claim(
            scope="orders", message_id="event"
        )
        event.listen(engine.sync_engine, "before_cursor_execute", executing)
        try:
            task = asyncio.create_task(competing())
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            event.remove(
                engine.sync_engine, "before_cursor_execute", executing
            )
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await transaction.rollback()
    assert await count(engine, table) == 0
    assert len(observations.spans.get_finished_spans()) == 1
    assert (
        observations.points("papilio_tasks.inbox.claims")[0].attributes[
            "outcome"
        ]
        == "cancelled"
    )
    async with engine.begin() as connection:
        assert await Inbox(connection, table).claim(
            scope="orders", message_id="event"
        )


async def test_cancelled_business_work_requires_host_rollback(database):
    engine, table, domain = database
    entered = asyncio.Event()

    async def consumer():
        async with engine.begin() as connection:
            assert await Inbox(connection, table).claim(
                scope="orders", message_id="event"
            )
            await connection.execute(insert(domain).values(id=1))
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(consumer())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await count(engine, table) == await count(engine, domain) == 0
