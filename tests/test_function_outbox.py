import asyncio
from importlib import import_module
from textwrap import dedent

import pytest
from faststream.nats import NatsBroker as NativeNats
from faststream.nats import TestNatsBroker
from faststream_outbox import OutboxBroker, make_outbox_table
from sqlalchemy import MetaData, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker
from test_outbox import count
from test_outbox import database as database

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.outbox import Outbox
from papilio_tasks.apps.outbox.decorators import publish
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


async def test_function_publisher_transaction_decorator_and_native_relay(
    database,
):
    engine, table, _, domain = database
    router, seen, done = NatsRouter(), [], asyncio.Event()
    created = router.publisher(
        dict, subject="created", outbox_key="created.v1"
    )

    @router.subscriber("created")
    async def receive(event: dict) -> None:
        seen.append(event)
        done.set()

    native = NativeNats(logger=None)
    app = create_app(
        registrar=NatsRegistrar(NatsBroker(native)), routers=[router]
    )
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table, logger=None), routers=[router]
    )

    class Commands:
        def __init__(self, session):
            self.session = session
            self.outbox = runtime.writer(session)

        @publish(created, select=lambda value: {"id": value})
        async def create(self, value: int) -> int:
            await self.session.execute(insert(domain).values(id=value))
            return value

    relay = runtime.create_relay(
        min_fetch_interval=0.01, max_fetch_interval=0.02
    )
    try:
        async with async_sessionmaker(engine)() as session:
            command = Commands(session)
            with pytest.raises(ValueError, match="rollback"):
                async with session.begin():
                    assert await command.create(1) == 1
                    raise ValueError("rollback")
            assert (
                await count(engine, table) == await count(engine, domain) == 0
            )
            async with session.begin():
                assert await command.create(2) == 2
                assert seen == []
        assert await count(engine, table) == await count(engine, domain) == 1
        async with TestNatsBroker(native):
            await relay.start()
            await asyncio.wait_for(done.wait(), 10)
            await relay.stop()
        assert seen == [{"id": 2}]
        assert await count(engine, table) == 0
        async with engine.connect() as connection:
            assert await connection.scalar(select(1)) == 1
    finally:
        await relay.stop()
        await app.stop()


def test_outbox_discovers_router_publishers_with_stable_keys_and_conflicts(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    folder = tmp_path / "outbox_functions"
    folder.mkdir()
    (folder / "__init__.py").write_text("")
    (folder / "publishers.py").write_text(
        dedent("""
        from papilio_tasks.apps.events.routers.nats import NatsRouter
        router = NatsRouter()
        created = router.publisher(
                dict, subject='created', outbox_key='created.v1'
            )
        direct = router.publisher(dict, subject='direct')
    """)
    )
    module = import_module("outbox_functions.publishers")
    runtime = Outbox(
        OutboxBroker(outbox_table=make_outbox_table(MetaData()), logger=None),
        modules=["outbox_functions"],
        routers=[module.router],
    )
    assert runtime.key(module.created) == "created.v1"
    with pytest.raises(ValueError, match="not registered"):
        runtime.key(module.direct)
    other = module.router.publisher(
        dict, subject="other", outbox_key="created.v1"
    )
    with pytest.raises(ValueError, match="Conflicting"):
        runtime.include(other)
    module.created.outbox_key = "mutated"
    with pytest.raises(ValueError, match="change"):
        runtime.key(module.created)
