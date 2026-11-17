import asyncio
import subprocess
import sys
from dataclasses import dataclass
from uuid import uuid4

import pytest

pytest.importorskip("faststream_outbox")

from faststream_outbox import (
    OutboxBroker,
    make_dlq_table,
    make_outbox_table,
)
from sqlalchemy import Column, Integer, MetaData, Table, func, insert, select
from sqlalchemy import event as sql_event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.base import Publisher
from papilio_tasks.apps.events.publishers.base import publish as direct_publish
from papilio_tasks.apps.outbox import Outbox
from papilio_tasks.apps.outbox import decorators as outbox
from papilio_tasks.tools.retry import Backoff, Retry


@dataclass
class Event:
    id: int


def event(key="orders.created.v1"):
    class Created(Publisher[Event]):
        outbox_key = key

    return Created


@pytest.fixture
async def database(pg_url):
    engine = create_async_engine(pg_url, pool_size=12, max_overflow=4)
    metadata = MetaData()
    suffix = uuid4().hex[:12]
    table = make_outbox_table(metadata, table_name="outbox_" + suffix)
    dlq = make_dlq_table(metadata, table_name="dlq_" + suffix)
    domain = Table(
        "orders_" + suffix, metadata, Column("id", Integer, primary_key=True)
    )
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    try:
        yield engine, table, dlq, domain
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
        await engine.dispose()


async def count(engine, table):
    async with engine.connect() as connection:
        return await connection.scalar(select(func.count()).select_from(table))


async def test_append_shares_transaction_and_never_publishes(database):
    engine, table, dlq, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table, dlq_table=dlq),
        publishers=[created],
    )
    sessions = async_sessionmaker(engine)
    async with sessions() as session:
        writer = runtime.writer(session)
        with pytest.raises(RuntimeError, match="transaction"):
            await writer.append(created, Event(1))
        async with session.begin():
            await session.execute(insert(domain).values(id=1))
            identifier = await writer.append(
                created, Event(1), headers={"trace": "one"}
            )
            assert isinstance(identifier, int)
            assert await count(engine, table) == 0
            assert await count(engine, domain) == 0
        assert await count(engine, table) == await count(engine, domain) == 1
        with pytest.raises(RuntimeError, match="transaction"):
            await writer.append(created, Event(2))
        with pytest.raises(ValueError, match="rollback"):
            async with session.begin():
                await session.execute(insert(domain).values(id=2))
                await writer.append(created, Event(2))
                raise ValueError("rollback")
        assert await count(engine, table) == await count(engine, domain) == 1


async def test_decorator_result_error_and_nested_rollback(database):
    engine, table, dlq, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    sessions = async_sessionmaker(engine)

    class Commands:
        def __init__(self, writer, session):
            self.outbox, self.session = writer, session

        @outbox.publish(created, select=lambda result: Event(result["id"]))
        async def create(self, identifier):
            await self.session.execute(insert(domain).values(id=identifier))
            return {"id": identifier}

    async with sessions() as session:
        command = Commands(runtime.writer(session), session)
        with pytest.raises(RuntimeError, match="transaction"):
            await command.create(1)
        assert await count(engine, domain) == 0
        with pytest.raises(ValueError):
            async with session.begin():
                assert await command.create(1) == {"id": 1}
                assert await count(engine, table) == 0
                raise ValueError("outer rollback")
        assert await count(engine, table) == await count(engine, domain) == 0
        async with session.begin():
            assert await command.create(2) == {"id": 2}
        assert await count(engine, table) == await count(engine, domain) == 1


async def test_decorator_selection_failure_rolls_back(database):
    engine, table, _, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    error = LookupError("select")

    def selection(result):
        raise error

    class Commands:
        @outbox.publish(created, select=selection)
        async def create(self):
            await session.execute(insert(domain).values(id=1))
            return 1

    async with async_sessionmaker(engine)() as session:
        command = Commands()
        command.outbox = runtime.writer(session)
        with pytest.raises(LookupError) as caught:
            async with session.begin():
                await command.create()
        assert caught.value is error
    assert await count(engine, table) == await count(engine, domain) == 0


async def test_relay_native_retry_preserves_identity_and_options(
    database, monkeypatch
):
    engine, table, dlq, _ = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table, dlq_table=dlq, logger=None),
        publishers=[created],
    )
    calls = []
    done = asyncio.Event()

    async def sender(body, **options):
        calls.append((body, options))
        if len(calls) == 1:
            raise ConnectionError("offline")
        done.set()

    monkeypatch.setitem(bindings._senders, created, (object(), sender))
    async with async_sessionmaker(engine)() as session, session.begin():
        await runtime.writer(session).append(
            created,
            Event(4),
            headers={"trace": "example"},
            correlation_id="c4",
        )
    app = runtime.create_relay(
        retry=Retry(
            attempts=3,
            delay=Backoff(initial=0.02, multiplier=2, max_delay=1),
            errors=(ConnectionError,),
        ),
        min_fetch_interval=0.01,
        max_fetch_interval=0.02,
    )
    try:
        await app.start()
        await asyncio.wait_for(done.wait(), 10)
    finally:
        await app.stop()
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert calls[0][0] == {"id": 4}
    assert calls[0][1]["headers"]["trace"] == "example"
    assert calls[0][1]["headers"]["event_id"]
    assert calls[0][1]["correlation_id"] == "c4"
    assert await count(engine, table) == await count(engine, dlq) == 0
    async with engine.connect() as connection:
        assert await connection.scalar(select(1)) == 1


@pytest.mark.parametrize("matched", [True, False])
async def test_relay_exhaustion_archives_native_dlq(
    database, monkeypatch, matched
):
    engine, table, dlq, _ = database
    created = event()
    terminal = asyncio.Event()

    def metric(name, tags):
        if name == "dlq_written":
            terminal.set()

    native = OutboxBroker(
        engine,
        outbox_table=table,
        dlq_table=dlq,
        metrics_recorder=metric,
        logger=None,
    )
    runtime = Outbox(native, publishers=[created])
    calls = []

    async def sender(body, **options):
        calls.append(body)
        if matched:
            raise ConnectionError("offline")
        raise ValueError("permanent")

    monkeypatch.setitem(bindings._senders, created, (object(), sender))
    async with async_sessionmaker(engine)() as session, session.begin():
        await runtime.writer(session).append(created, Event(1))
    app = runtime.create_relay(
        retry=Retry(attempts=2, delay=0, errors=(ConnectionError,)),
        min_fetch_interval=0.01,
        max_fetch_interval=0.02,
    )
    try:
        await app.start()
        await asyncio.wait_for(terminal.wait(), 10)
    finally:
        await app.stop()
    assert calls == [{"id": 1}] * (2 if matched else 1)
    assert await count(engine, table) == 0
    async with engine.connect() as connection:
        row = (await connection.execute(select(dlq))).mappings().one()
    assert row["failure_reason"] == "retry_terminal"


async def test_connection_handle_and_savepoint_rollback(database):
    engine, table, _, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    async with engine.begin() as connection:
        writer = runtime.writer(connection)
        await connection.execute(insert(domain).values(id=1))
        await writer.append(created, Event(1))
        with pytest.raises(ValueError):
            async with connection.begin_nested():
                await connection.execute(insert(domain).values(id=2))
                await writer.append(created, Event(2))
                raise ValueError("savepoint")
        assert await count(engine, table) == 0
    assert await count(engine, table) == await count(engine, domain) == 1


async def test_batch_decorator_and_empty_selection(database):
    engine, table, _, _ = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )

    class Commands:
        @outbox.publish_batch(created, select=lambda result: result)
        async def create(self, values):
            return values

    async with async_sessionmaker(engine)() as session, session.begin():
        command = Commands()
        command.outbox = runtime.writer(session)
        values = [Event(1), Event(2)]
        assert await command.create(values) is values
        assert await command.create([]) == []
    assert await count(engine, table) == 2


def test_existing_discovery_finds_nested_module_publishers(
    tmp_path, monkeypatch
):
    root = tmp_path / "example_outbox"
    (root / "orders" / "tasks").mkdir(parents=True)
    for path in (root, root / "orders", root / "orders" / "tasks"):
        (path / "__init__.py").write_text("")
    (root / "orders" / "tasks" / "publishers.py").write_text("""
from papilio_tasks.apps.events.publishers.base import Publisher
class Created(Publisher[dict]):
    outbox_key = "orders.created.v1"
class Direct(Publisher[dict]):
    pass
""")
    monkeypatch.syspath_prepend(str(tmp_path))
    native = OutboxBroker(outbox_table=make_outbox_table(MetaData()))
    runtime = Outbox(native, modules=["example_outbox"])
    classes = __import__(
        "example_outbox.orders.tasks.publishers",
        fromlist=["Created", "Direct"],
    )
    assert runtime.key(classes.Created) == "orders.created.v1"
    runtime.include(classes.Created)
    with pytest.raises(ValueError, match="not registered"):
        runtime.key(classes.Direct)
    assert not native.subscribers


async def test_missing_target_fails_before_claiming_any_rows(database):
    engine, table, _, _ = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    async with async_sessionmaker(engine)() as session, session.begin():
        await runtime.writer(session).append(created, Event(1))
    app = runtime.create_relay()
    with pytest.raises(RuntimeError, match="not registered"):
        await app.start()
    await app.stop()
    assert await count(engine, table) == 1


def test_route_collisions_fail_before_registering():
    broker = OutboxBroker(outbox_table=make_outbox_table(MetaData()))
    with pytest.raises(ValueError, match="Conflicting"):
        Outbox(broker, publishers=[event(), event()])
    assert not broker.subscribers


async def test_batch_is_one_insert_with_independent_event_ids(database):
    engine, table, dlq, _ = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    statements = []

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.startswith("INSERT INTO " + table.name):
            statements.append(statement)

    sql_event.listen(engine.sync_engine, "before_cursor_execute", observe)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            await runtime.writer(session).append_batch(
                created, [Event(i) for i in range(20)]
            )
            await runtime.writer(session).append_batch(created, [])
    finally:
        sql_event.remove(engine.sync_engine, "before_cursor_execute", observe)
    assert len(statements) == 1
    assert await count(engine, table) == 20
    received = []
    done = asyncio.Event()

    async def sender(body, **options):
        received.append((body["id"], options["headers"]["event_id"]))
        if len(received) == 20:
            done.set()

    bindings.add(created, runtime, sender)
    other = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    first, second = runtime.create_relay(), other.create_relay()
    try:
        await asyncio.gather(first.start(), second.start())
        await asyncio.wait_for(done.wait(), 10)
    finally:
        await asyncio.gather(first.stop(), second.stop())
        bindings.release(runtime)
    assert sorted(identifier for identifier, _ in received) == list(range(20))
    assert len({identity for _, identity in received}) == 20
    assert await count(engine, table) == await count(engine, dlq) == 0


async def test_native_pending_retry_survives_new_relay_instance(database):
    engine, table, _, _ = database
    created = event()
    scheduled = asyncio.Event()
    calls = []
    runtime = Outbox(
        OutboxBroker(
            engine,
            outbox_table=table,
            logger=None,
            metrics_recorder=lambda name, tags: (
                scheduled.set() if name == "nacked_retried" else None
            ),
        ),
        publishers=[created],
    )

    async def offline(body, **options):
        calls.append(options["headers"]["event_id"])
        raise ConnectionError()

    bindings.add(created, runtime, offline)
    async with async_sessionmaker(engine)() as session, session.begin():
        await runtime.writer(session).append(created, Event(1))
    first = runtime.create_relay(
        retry=Retry(attempts=3, delay=0.1, errors=(ConnectionError,)),
    )
    try:
        await first.start()
        await asyncio.wait_for(scheduled.wait(), 10)
    finally:
        await first.stop()
        bindings.release(runtime)
    assert await count(engine, table) == 1
    async with engine.connect() as connection:
        row = (await connection.execute(select(table))).mappings().one()
    assert row["attempts_count"] == 1
    done = asyncio.Event()

    async def online(body, **options):
        calls.append(options["headers"]["event_id"])
        done.set()

    other = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    bindings.add(created, other, online)
    second = other.create_relay(
        min_fetch_interval=0.01, max_fetch_interval=0.02
    )
    try:
        await second.start()
        await asyncio.wait_for(done.wait(), 10)
    finally:
        await second.stop()
        bindings.release(other)
    assert len(calls) == 2 and calls[0] == calls[1]
    assert await count(engine, table) == 0


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_producer_start_cleans_all_resources(database, cancel):
    engine, table, _, _ = database
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[event()]
    )
    closed = []

    class Producer:
        async def connect(self):
            if cancel:
                raise asyncio.CancelledError()
            raise ConnectionError("startup")

        async def stop(self):
            closed.append(True)

    app = runtime.create_relay(producers=[Producer()])
    # Cancel application ownership, rather than just one TaskGroup child.
    if cancel:

        async def wait():
            await asyncio.Event().wait()

        app.producers.producers[0].connect = wait
        task = asyncio.create_task(app.start())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(ExceptionGroup):
            await app.start()
    await app.stop()
    assert closed == [True]


def test_optional_imports_need_no_outbox_or_tenacity():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'fastapi':
            raise ModuleNotFoundError(fullname, name=fullname)
        blocked = {'tenacity', 'faststream_outbox', 'sqlalchemy', 'papilio'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.tools.retry import Retry
assert Publisher.outbox_key is None and Subscriber.retry is None
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "kind", ["ack", "policy", "duplicate", "changed", "missing"]
)
def test_configuration_failures_are_early(kind):
    created = event()
    native = OutboxBroker(outbox_table=make_outbox_table(MetaData()))
    runtime = Outbox(native, publishers=[created])
    if kind == "ack":

        def operation():
            return runtime.create_relay(ack_policy="manual")
    elif kind == "policy":

        def operation():
            return runtime.create_relay(
                retry=Retry(attempts=2, delay=0, errors=(Exception,)),
                retry_strategy=object(),
            )
    elif kind == "duplicate":

        def operation():
            return runtime.include(event())
    elif kind == "changed":
        created.outbox_key = "changed"

        def operation():
            return runtime.create_relay()
    else:

        def operation():
            return runtime.include(event(None))

    with pytest.raises((ValueError, RuntimeError, TypeError)):
        operation()
    assert not native.subscribers


async def test_direct_publish_stays_separate_from_outbox(database):
    engine, table, _, _ = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    calls = []

    async def sender(body, **options):
        calls.append(body)

    bindings.add(created, runtime, sender)

    @direct_publish(created, select=lambda result: Event(result))
    async def operation():
        return 9

    try:
        assert await operation() == 9
    finally:
        bindings.release(runtime)
    assert calls == [Event(9)]
    assert await count(engine, table) == 0
