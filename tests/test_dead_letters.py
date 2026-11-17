import asyncio
import subprocess
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("faststream_outbox")

from faststream_outbox import OutboxBroker, make_dlq_table, make_outbox_table
from sqlalchemy import Column, MetaData, String, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema
from test_outbox import count, event
from test_outbox import database as database

from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.outbox import DeadLetters, Outbox
from papilio_tasks.tools.retry import Retry


async def archive(engine, dlq, *, queue="orders.created.v1", **values):
    async with engine.begin() as connection:
        return await connection.scalar(
            insert(dlq)
            .values(
                original_id=99,
                queue=queue,
                payload=b"exact\x00payload\xff",
                headers={"event_id": "stable", "correlation_id": "trace"},
                deliveries_count=3,
                created_at=datetime.now(UTC),
                failure_reason="retry_terminal",
                last_exception="ConnectionError('offline')",
                **values,
            )
            .returning(dlq.c.id)
        )


def manager(handle, table, dlq, **options):
    return DeadLetters(handle, outbox_table=table, dlq_table=dlq, **options)


async def test_pagination_filter_details_and_missing_rows(database):
    engine, table, dlq, _ = database
    first = await archive(engine, dlq)
    other = await archive(engine, dlq, queue="other")
    last = await archive(engine, dlq)
    async with engine.begin() as connection:
        letters = manager(connection, table, dlq)
        page = await letters.list(limit=1, queue="orders.created.v1")
        assert [row.id for row in page.items] == [first]
        assert page.next_cursor == first
        assert page.items[0].deliveries_count == 3
        assert not hasattr(page.items[0], "payload")
        second = await letters.list(
            limit=1, after=page.next_cursor, queue="orders.created.v1"
        )
        assert [row.id for row in second.items] == [last]
        assert second.next_cursor is None
        assert [row.id for row in (await letters.list()).items] == [
            first,
            other,
            last,
        ]
        detail = await letters.get(first)
        assert detail.payload == b"exact\x00payload\xff"
        assert detail.headers["event_id"] == "stable"
        assert detail.last_exception == "ConnectionError('offline')"
        assert detail.original_id == 99
        assert await letters.get(first, queue="other") is None
        assert await letters.get(last + 1) is None
        assert await letters.replay(first, queue="other") is None
        assert await letters.replay(last + 1, queue="other") is None


async def test_replay_exact_bytes_fresh_native_state_and_rollback(database):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq)
    sessions = async_sessionmaker(engine)
    async with sessions() as session:
        letters = manager(session, table, dlq)
        with pytest.raises(RuntimeError, match="active transaction"):
            await letters.list()
        with pytest.raises(RuntimeError, match="active transaction"):
            await letters.get(identifier)
        with pytest.raises(RuntimeError, match="active transaction"):
            await letters.replay(identifier, queue="orders.created.v1")
        with pytest.raises(ValueError, match="rollback"):
            async with session.begin():
                assert await letters.replay(
                    identifier, queue="orders.created.v1"
                )
                assert await count(engine, dlq) == 1
                assert await count(engine, table) == 0
                raise ValueError("rollback")
        assert await count(engine, dlq) == 1
        assert await count(engine, table) == 0
        async with session.begin():
            with pytest.raises(ValueError, match="savepoint"):
                async with session.begin_nested():
                    await letters.replay(identifier, queue="orders.created.v1")
                    raise ValueError("savepoint")
            detail = await letters.get(identifier)
            pending = await letters.replay(
                identifier, queue="orders.created.v1"
            )
        assert await count(engine, dlq) == 0
        assert await count(engine, table) == 1
    async with engine.connect() as connection:
        row = (await connection.execute(select(table))).mappings().one()
    assert row["id"] == pending
    assert row["payload"] == detail.payload
    assert row["headers"] == detail.headers
    assert row["attempts_count"] == row["deliveries_count"] == 0
    assert row["acquired_at"] is row["acquired_token"] is None
    assert row["first_attempt_at"] is row["last_attempt_at"] is None


@pytest.mark.parametrize("rollback", [False, True])
async def test_concurrent_replay_commit_or_rollback(database, rollback):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq)
    entered = asyncio.Event()

    async def competing():
        async with engine.begin() as connection:
            entered.set()
            return await manager(connection, table, dlq).replay(
                identifier, queue="orders.created.v1"
            )

    task = None
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            first = await manager(connection, table, dlq).replay(
                identifier, queue="orders.created.v1"
            )
            task = asyncio.create_task(competing())
            await asyncio.wait_for(entered.wait(), 5)
            assert not task.done()
            if rollback:
                await transaction.rollback()
            else:
                await transaction.commit()
        second = await asyncio.wait_for(task, 5)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert first is not None
    assert (second is not None) == rollback
    assert await count(engine, dlq) == 0
    assert await count(engine, table) == 1


async def test_timer_conflict_keeps_archive(database):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq, timer_id="same")
    native = OutboxBroker(outbox_table=table)
    async with engine.begin() as connection:
        await native.publish(
            "existing",
            queue="orders.created.v1",
            session=connection,
            timer_id="same",
        )
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await manager(connection, table, dlq).replay(
                identifier, queue="orders.created.v1"
            )
    assert await count(engine, dlq) == await count(engine, table) == 1


async def test_terminal_archive_replay_and_native_delivery(
    database, monkeypatch
):
    engine, table, dlq, _ = database
    created = event("orders." + uuid4().hex)
    failed, delivered = asyncio.Event(), asyncio.Event()
    seen, healthy = [], False

    def metric(name, tags):
        if name == "dlq_written":
            failed.set()

    async def sender(body, **options):
        seen.append((body, options))
        if not healthy:
            raise ConnectionError("offline")
        delivered.set()

    monkeypatch.setitem(bindings._senders, created, (object(), sender))
    native = OutboxBroker(
        engine,
        outbox_table=table,
        dlq_table=dlq,
        metrics_recorder=metric,
        logger=None,
    )
    runtime = Outbox(native, publishers=[created])
    relay = runtime.create_relay(
        retry=Retry(attempts=1, delay=0, errors=(ConnectionError,)),
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
    )
    async with engine.begin() as connection:
        await runtime.writer(connection).append(
            created,
            {"id": 1},
            headers={"event_id": "preserved"},
            correlation_id="preserved-correlation",
        )
    try:
        await relay.start()
        await asyncio.wait_for(failed.wait(), 10)
        async with engine.begin() as connection:
            letters = manager(connection, table, dlq)
            page = await letters.list(queue=runtime.key(created))
            assert len(page.items) == 1
            assert page.items[0].failure_reason == "retry_terminal"
            assert page.items[0].deliveries_count == 1
            assert "offline" in page.items[0].last_exception
            healthy = True
            assert await letters.replay(
                page.items[0].id, queue=runtime.key(created)
            )
        await asyncio.wait_for(delivered.wait(), 10)
    finally:
        await relay.stop()
    assert await count(engine, table) == await count(engine, dlq) == 0
    assert len(seen) == 2
    assert seen[0] == seen[1]
    assert seen[1][1]["headers"]["event_id"] == "preserved"
    assert seen[1][1]["correlation_id"] == "preserved-correlation"


@pytest.mark.parametrize("arguments", [{"limit": 0}, {"after": -1}])
async def test_invalid_page_bounds(database, arguments):
    engine, table, dlq, _ = database
    async with engine.begin() as connection:
        with pytest.raises(ValueError):
            await manager(connection, table, dlq).list(**arguments)


async def test_schema_qualified_native_tables(database):
    engine, _, _, _ = database
    schema = "dead_letters_" + uuid4().hex
    metadata = MetaData(schema=schema)
    pending = make_outbox_table(metadata)
    dlq = make_dlq_table(metadata)
    dlq.append_column(Column("operator_note", String))
    async with engine.begin() as connection:
        await connection.execute(CreateSchema(schema))
        await connection.run_sync(metadata.create_all)
    try:
        identifier = await archive(engine, dlq, timer_id="keep-timer")
        async with engine.begin() as connection:
            letters = manager(connection, pending, dlq)
            assert (await letters.get(identifier)).timer_id == "keep-timer"
            new_id = await letters.replay(
                identifier, queue="orders.created.v1"
            )
        async with engine.connect() as connection:
            row = (await connection.execute(select(pending))).mappings().one()
        assert row["id"] == new_id
        assert row["timer_id"] == "keep-timer"
        assert await count(engine, dlq) == 0
    finally:
        async with engine.begin() as connection:
            await connection.execute(DropSchema(schema, cascade=True))


def test_management_imports_without_telemetry_or_transport_backends():
    code = """
import builtins
import sys
original = builtins.__import__
blocked = {
    "opentelemetry", "taskiq", "redis", "nats", "aiokafka", "aio_pika"
}
def guarded(name, *args, **kwargs):
    if name.split(".")[0] in blocked:
        raise ModuleNotFoundError(name)
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from papilio_tasks.apps.outbox import DeadLetters
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession
from faststream_outbox import make_outbox_table, make_dlq_table
metadata = MetaData()
DeadLetters(AsyncSession(), outbox_table=make_outbox_table(metadata),
            dlq_table=make_dlq_table(metadata))
assert not any(name.split(".")[0] in blocked for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
