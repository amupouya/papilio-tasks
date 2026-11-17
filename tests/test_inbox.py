import asyncio
import subprocess
import sys
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy_inbox")

import sqlalchemy_inbox
from sqlalchemy import Column, Integer, MetaData, Table, func, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from papilio_tasks.apps.inbox import Inbox, InboxCleaner, make_inbox_table


@pytest.fixture
async def database(pg_url):
    engine = create_async_engine(pg_url, pool_size=6)
    metadata = MetaData()
    suffix = uuid4().hex[:12]
    table = make_inbox_table(metadata, table_name="inbox_" + suffix)
    domain = Table(
        "effects_" + suffix, metadata, Column("id", Integer, primary_key=True)
    )
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    try:
        yield engine, table, domain
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
        await engine.dispose()


async def count(engine, table):
    async with engine.connect() as connection:
        return await connection.scalar(select(func.count()).select_from(table))


def test_public_exports_are_native():
    assert Inbox is sqlalchemy_inbox.Inbox
    assert InboxCleaner is sqlalchemy_inbox.InboxCleaner
    assert make_inbox_table is sqlalchemy_inbox.make_inbox_table


@pytest.mark.parametrize("session_handle", [True, False])
async def test_claim_and_effect_share_transaction(database, session_handle):
    engine, table, domain = database
    factory = async_sessionmaker(engine) if session_handle else engine.connect
    async with factory() as handle:
        inbox = Inbox(handle, table)
        with pytest.raises(RuntimeError, match="transaction"):
            await inbox.claim(scope="orders.apply.v1", message_id="event")
        async with handle.begin():
            assert await inbox.claim(
                scope="orders.apply.v1", message_id="event"
            )
            await handle.execute(insert(domain).values(id=1))
            assert (
                await count(engine, table) == await count(engine, domain) == 0
            )
        async with handle.begin():
            assert not await inbox.claim(
                scope="orders.apply.v1", message_id="event"
            )
            assert await inbox.claim(
                scope="another.consumer.v1", message_id="event"
            )
        assert await count(engine, table) == 2
        assert await count(engine, domain) == 1


async def test_full_and_savepoint_rollback_allow_retry(database):
    engine, table, domain = database
    async with async_sessionmaker(engine)() as session:
        inbox = Inbox(session, table)
        with pytest.raises(ValueError, match="outer"):
            async with session.begin():
                assert await inbox.claim(scope="orders", message_id="event")
                await session.execute(insert(domain).values(id=1))
                raise ValueError("outer")
        assert await count(engine, table) == await count(engine, domain) == 0
        async with session.begin():
            with pytest.raises(ValueError, match="savepoint"):
                async with session.begin_nested():
                    assert await inbox.claim(
                        scope="orders", message_id="event"
                    )
                    await session.execute(insert(domain).values(id=1))
                    raise ValueError("savepoint")
            assert await inbox.claim(scope="orders", message_id="event")
            await session.execute(insert(domain).values(id=1))
        assert await count(engine, table) == await count(engine, domain) == 1


@pytest.mark.parametrize("rollback", [False, True])
async def test_concurrent_claims_follow_first_transaction(database, rollback):
    engine, table, domain = database
    entered = asyncio.Event()

    async def competing():
        async with engine.begin() as connection:
            entered.set()
            claimed = await Inbox(connection, table).claim(
                scope="orders", message_id="event"
            )
            if claimed:
                await connection.execute(insert(domain).values(id=1))
            return claimed

    task = None
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            assert await Inbox(connection, table).claim(
                scope="orders", message_id="event"
            )
            await connection.execute(insert(domain).values(id=1))
            task = asyncio.create_task(competing())
            await asyncio.wait_for(entered.wait(), 5)
            assert not task.done()
            if rollback:
                await transaction.rollback()
            else:
                await transaction.commit()
        assert await asyncio.wait_for(task, 5) is rollback
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await count(engine, table) == await count(engine, domain) == 1


async def test_database_failure_rolls_back_marker(database):
    engine, table, domain = database
    async with engine.begin() as connection:
        await connection.execute(insert(domain).values(id=1))
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            assert await Inbox(connection, table).claim(
                scope="orders", message_id="event"
            )
            await connection.execute(insert(domain).values(id=1))
    assert await count(engine, table) == 0
    async with engine.begin() as connection:
        assert await Inbox(connection, table).claim(
            scope="orders", message_id="event"
        )


@pytest.mark.parametrize("message_id", [None, ""])
async def test_missing_identity_has_no_fallback(database, message_id):
    engine, table, _ = database
    async with engine.begin() as connection:
        with pytest.raises(ValueError):
            await Inbox(connection, table).claim(
                scope="orders", message_id=message_id
            )
    assert await count(engine, table) == 0


async def test_explicit_native_cleanup_reopens_expired_identity(database):
    engine, table, _ = database
    async with engine.begin() as connection:
        await connection.execute(
            insert(table).values(
                scope="orders",
                message_id="old",
                processed_at=func.now() - timedelta(days=10),
            )
        )
        assert await Inbox(connection, table).claim(
            scope="orders", message_id="retained"
        )
    result = await InboxCleaner(
        engine=engine, table=table, retention=timedelta(days=7), batch_size=1
    ).cleanup()
    assert result.deleted_count == 1
    async with engine.begin() as connection:
        inbox = Inbox(connection, table)
        assert await inbox.claim(scope="orders", message_id="old")
        assert not await inbox.claim(scope="orders", message_id="retained")


@pytest.mark.parametrize("committed", [False, True])
async def test_worker_exit_before_or_after_commit(database, pg_url, committed):
    engine, table, domain = database
    code = """
import asyncio, os, sys
from sqlalchemy import MetaData, Table, insert
from sqlalchemy.ext.asyncio import create_async_engine
from papilio_tasks.apps.inbox import Inbox
async def run():
    engine = create_async_engine(sys.argv[1])
    connection = await engine.connect()
    metadata = MetaData()
    await connection.run_sync(lambda sync: metadata.reflect(sync))
    await connection.rollback()
    transaction = await connection.begin()
    assert await Inbox(connection, metadata.tables[sys.argv[2]]).claim(
        scope="orders", message_id="event")
    await connection.execute(insert(metadata.tables[sys.argv[3]]).values(id=1))
    if sys.argv[4] == "True":
        await transaction.commit()
    os._exit(0)
asyncio.run(run())
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            "-c",
            code,
            pg_url,
            table.name,
            domain.name,
            str(committed),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    async with engine.begin() as connection:
        claimed = await Inbox(connection, table).claim(
            scope="orders", message_id="event"
        )
        assert claimed is not committed
        if claimed:
            await connection.execute(insert(domain).values(id=1))
    assert await count(engine, table) == await count(engine, domain) == 1
