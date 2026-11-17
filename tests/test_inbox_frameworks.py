from collections.abc import AsyncIterator
from typing import Annotated

import pytest

pytest.importorskip("sqlalchemy_inbox")
pytest.importorskip("papilio")
pytest.importorskip("fastapi")

from dishka import Provider, Scope, make_async_container, provide
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from papilio.infra.db.connection import DBConnection
from papilio.infra.db.tools.decorators import transactional
from papilio.infra.db.uow import PGUnitOfWork
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_inbox import count
from test_inbox import database as database

from papilio_tasks.apps.inbox import Inbox


async def test_papilio_unit_and_inbox_commit_together(database, pg_url):
    engine, table, domain = database
    connection = DBConnection(
        pg_url,
        pool_size=3,
        max_overflow=1,
        pool_timeout=5,
        pool_recycle=60,
        uow_factory=PGUnitOfWork,
    )

    class Consumer:
        def __init__(self, unit: PGUnitOfWork, inbox: Inbox):
            self.unit, self.inbox = unit, inbox

        @transactional
        async def run(self, fail: bool = False) -> bool:
            if not await self.inbox.claim(scope="orders", message_id="event"):
                return False
            await self.unit.execute(insert(domain).values(id=1))
            if fail:
                raise ValueError("rollback")
            return True

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def unit(self) -> AsyncIterator[PGUnitOfWork]:
            async with connection.uow() as unit:
                yield unit

        @provide(scope=Scope.REQUEST)
        def inbox(self, unit: PGUnitOfWork) -> Inbox:
            return Inbox(unit.session, table)

        consumer = provide(Consumer, scope=Scope.REQUEST)

    container = make_async_container(Dependencies())
    try:
        async with container() as scope:
            command = await scope.get(Consumer)
            with pytest.raises(ValueError, match="rollback"):
                await command.run(fail=True)
            assert (
                await count(engine, table) == await count(engine, domain) == 0
            )
            assert await command.run()
            assert not await command.run()
        assert await count(engine, table) == await count(engine, domain) == 1
    finally:
        await container.close()
        await connection.dispose()


async def test_fastapi_request_session_and_inbox_share_commit(database):
    engine, table, domain = database
    factory = async_sessionmaker(engine)

    async def session():
        async with factory() as handle:
            yield handle

    def inbox(handle: Annotated[AsyncSession, Depends(session)]) -> Inbox:
        return Inbox(handle, table)

    api = FastAPI()

    @api.post("/consume")
    async def consume(
        handle: Annotated[AsyncSession, Depends(session)],
        inbox: Annotated[Inbox, Depends(inbox)],
        fail: bool = False,
    ):
        async with handle.begin():
            if not await inbox.claim(scope="orders", message_id="event"):
                return {"processed": False}
            await handle.execute(insert(domain).values(id=1))
            if fail:
                raise ValueError("rollback")
        return {"processed": True}

    async with AsyncClient(
        transport=ASGITransport(api, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        assert (await client.post("/consume?fail=true")).status_code == 500
        assert await count(engine, table) == await count(engine, domain) == 0
        assert (await client.post("/consume")).json() == {"processed": True}
        assert (await client.post("/consume")).json() == {"processed": False}
    assert await count(engine, table) == await count(engine, domain) == 1
