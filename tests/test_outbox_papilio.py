from collections.abc import AsyncIterator

import pytest

pytest.importorskip("papilio")
pytest.importorskip("faststream_outbox")

from dishka import Provider, Scope, make_async_container, provide
from faststream_outbox import OutboxBroker
from papilio.infra.db.connection import DBConnection
from papilio.infra.db.tools.decorators import transactional
from papilio.infra.db.uow import PGUnitOfWork
from sqlalchemy import insert
from test_outbox import Event, count, event
from test_outbox import database as database

from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.base import publish as direct_publish
from papilio_tasks.apps.outbox import Outbox, OutboxWriter
from papilio_tasks.apps.outbox import decorators as outbox


async def test_papilio_provider_and_real_nested_transaction_ownership(
    database, pg_url
):
    engine, table, _, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    connection = DBConnection(
        pg_url,
        pool_size=3,
        max_overflow=1,
        pool_timeout=5,
        pool_recycle=60,
        uow_factory=PGUnitOfWork,
    )

    class Commands:
        def __init__(self, unit: PGUnitOfWork, writer: OutboxWriter):
            self.unit, self.outbox = unit, writer

        @transactional
        @outbox.publish(created, select=Event)
        async def create(self, identifier: int) -> int:
            await self.unit.execute(insert(domain).values(id=identifier))
            return identifier

        @transactional
        async def outer(self) -> None:
            await self.create(1)
            assert await count(engine, table) == 0
            raise ValueError("outer rollback")

        @direct_publish(created, select=Event)
        @transactional
        async def direct(self, identifier: int) -> int:
            await self.unit.execute(insert(domain).values(id=identifier))
            return identifier

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def unit(self) -> AsyncIterator[PGUnitOfWork]:
            async with connection.uow() as unit:
                yield unit

        @provide(scope=Scope.REQUEST)
        def writer(self, unit: PGUnitOfWork) -> OutboxWriter:
            return runtime.writer(unit.session)

        commands = provide(Commands, scope=Scope.REQUEST)

    container = make_async_container(Dependencies())
    published = []

    async def sender(body, **options):
        assert await count(engine, domain) == 2
        published.append(body)

    bindings.add(created, runtime, sender)
    try:
        async with container() as scope:
            command = await scope.get(Commands)
            assert command.outbox.session is command.unit.session
            with pytest.raises(ValueError, match="outer rollback"):
                await command.outer()
            assert (
                await count(engine, table) == await count(engine, domain) == 0
            )
            assert await command.create(2) == 2
            assert (
                await count(engine, table) == await count(engine, domain) == 1
            )
            assert await command.direct(3) == 3
            assert published == [Event(3)]
            assert await count(engine, table) == 1
    finally:
        bindings.release(runtime)
        await container.close()
        await connection.dispose()
