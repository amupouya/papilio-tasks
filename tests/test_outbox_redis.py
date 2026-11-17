import asyncio
import os
import sys
from uuid import uuid4

import pytest

pytest.importorskip("faststream_outbox")
pytest.importorskip("faststream.redis")

from dishka import Provider, Scope
from faststream import StreamMessage
from faststream.redis import RedisBroker as NativeRedis
from faststream.redis import StreamSub
from faststream_outbox import OutboxBroker
from sqlalchemy.ext.asyncio import async_sessionmaker
from test_outbox import Event, count
from test_outbox import database as database

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.events.subscribers.streams import StreamSubscriber
from papilio_tasks.apps.outbox import Outbox
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker


async def test_real_redis_delivery_from_native_outbox_relay(database):
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires TEST_REDIS_URL")
    engine, table, _, _ = database
    name = "papilio_outbox_" + uuid4().hex
    seen = []
    delivered = asyncio.Event()

    class Created(StreamPublisher[Event]):
        stream = name
        outbox_key = name

    class Reader(StreamSubscriber[Event]):
        publisher = Created
        stream = StreamSub(name, group="reader", consumer="one")

        def __init__(self, message: StreamMessage):
            self.message = message

        async def run(self, event: Event) -> None:
            seen.append((event, self.message.headers))
            delivered.set()

    consumer_reg = StreamRegistrar(RedisBroker(NativeRedis(url, logger=None)))
    consumer_reg.subscriber(Reader)
    dependencies = Provider(scope=Scope.REQUEST)
    dependencies.provide(Reader)
    consumer = create_app(registrar=consumer_reg, providers=[dependencies])
    producer_reg = StreamRegistrar(RedisBroker(NativeRedis(url, logger=None)))
    producer_reg.publisher(Created)
    producer = create_app(registrar=producer_reg)
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[Created]
    )
    relay = runtime.create_relay(producers=[producer])
    try:
        await consumer.start()
        async with async_sessionmaker(engine)() as session, session.begin():
            await runtime.writer(session).append(
                Created, Event(42), headers={"trace": "test"}
            )
        await relay.start()
        await asyncio.wait_for(delivered.wait(), 10)
    finally:
        await asyncio.gather(relay.stop(), consumer.stop())
    assert seen[0][0] == Event(42)
    assert seen[0][1]["trace"] == "test"
    assert seen[0][1]["event_id"]
    assert await count(engine, table) == 0


async def test_cli_crash_after_publish_reclaims_lease_and_keeps_event_id(
    database, pg_url, tmp_path
):
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires TEST_REDIS_URL")
    engine, table, _, _ = database
    name = "papilio_crash_" + uuid4().hex
    received = []
    first, repeated = asyncio.Event(), asyncio.Event()

    class Created(StreamPublisher[dict]):
        stream = name
        outbox_key = name

    class Reader(StreamSubscriber[dict]):
        publisher = Created
        stream = StreamSub(name, group="reader", consumer="one")

        def __init__(self, message: StreamMessage):
            self.message = message

        async def run(self, event: dict) -> None:
            received.append((event, self.message.headers["event_id"]))
            (first if len(received) == 1 else repeated).set()

    reg = StreamRegistrar(RedisBroker(NativeRedis(url, logger=None)))
    reg.subscriber(Reader)
    dependencies = Provider(scope=Scope.REQUEST)
    dependencies.provide(Reader)
    consumer = create_app(registrar=reg, providers=[dependencies])
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[Created]
    )
    module = tmp_path / "relay_app.py"
    module.write_text("""
import asyncio
import os
from faststream.redis import RedisBroker as NativeRedis
from faststream_outbox import OutboxBroker, make_outbox_table
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.outbox import Outbox
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker

class Created(StreamPublisher[dict]):
    stream = os.environ["PAPILIO_TEST_STREAM"]
    outbox_key = stream

reg = StreamRegistrar(RedisBroker(NativeRedis(os.environ["TEST_REDIS_URL"])))
reg.publisher(Created)
producer = create_app(registrar=reg)
if os.environ["PAPILIO_TEST_CRASH"] == "1":
    original = Created.publish
    async def blocked(body, **options):
        await original(body, **options)
        # Fault injection: accepted by broker, before outbox acknowledgement.
        await asyncio.Event().wait()
    Created.publish = blocked

engine = create_async_engine(os.environ["PAPILIO_TEST_DSN"])
native = OutboxBroker(engine, outbox_table=make_outbox_table(
    MetaData(), table_name=os.environ["PAPILIO_TEST_TABLE"]
))
runtime = Outbox(native, publishers=[Created])
app = runtime.create_relay(
    producers=[producer], lease_ttl_seconds=0.5,
    min_fetch_interval=0.01, max_fetch_interval=0.02,
)
app.after_shutdown(engine.dispose)
""")
    environment = dict(
        os.environ,
        PAPILIO_TEST_DSN=pg_url,
        PAPILIO_TEST_STREAM=name,
        PAPILIO_TEST_TABLE=table.name,
        PAPILIO_TEST_CRASH="1",
    )
    process = None
    with (tmp_path / "relay.log").open("w") as output:

        async def launch():
            return await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "papilio_tasks",
                "events",
                "run",
                "relay_app:app",
                "--app-dir",
                str(tmp_path),
                "--log-level",
                "warning",
                env=environment,
                stdout=output,
                stderr=output,
            )

        try:
            await consumer.start()
            async with (
                async_sessionmaker(engine)() as session,
                session.begin(),
            ):
                await runtime.writer(session).append(Created, {"id": 7})
            process = await launch()
            await asyncio.wait_for(first.wait(), 20)
            process.kill()
            await asyncio.wait_for(process.wait(), 10)
            assert await count(engine, table) == 1
            environment["PAPILIO_TEST_CRASH"] = "0"
            process = await launch()
            await asyncio.wait_for(repeated.wait(), 20)
            process.terminate()
            await asyncio.wait_for(process.wait(), 10)
            assert process.returncode == 0
        finally:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            await consumer.stop()
    assert received[0] == received[1]
    assert await count(engine, table) == 0
