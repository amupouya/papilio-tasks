from typing import Annotated

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("faststream_outbox")

from fastapi import Depends, FastAPI
from faststream_outbox import OutboxBroker
from httpx import ASGITransport, AsyncClient
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_dead_letters import archive
from test_outbox import Event, count, event
from test_outbox import database as database

from papilio_tasks.apps.outbox import DeadLetters, Outbox, OutboxWriter


async def test_native_fastapi_dependencies_share_transaction(database):
    engine, table, _, domain = database
    created = event()
    runtime = Outbox(
        OutboxBroker(engine, outbox_table=table), publishers=[created]
    )
    sessions = async_sessionmaker(engine)

    async def session():
        async with sessions() as handle:
            yield handle

    async def writer(
        handle: Annotated[AsyncSession, Depends(session)],
    ) -> OutboxWriter:
        return runtime.writer(handle)

    api = FastAPI()

    @api.post("/orders/{identifier}")
    async def register(
        identifier: int,
        handle: Annotated[AsyncSession, Depends(session)],
        outbox: Annotated[OutboxWriter, Depends(writer)],
        fail: bool = False,
    ):
        assert outbox.session is handle
        async with handle.begin():
            await handle.execute(insert(domain).values(id=identifier))
            await outbox.append(created, Event(identifier))
            if fail:
                raise ValueError("rollback")
        return {"id": identifier}

    async with AsyncClient(
        transport=ASGITransport(api, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post("/orders/1")
        assert response.status_code == 200 and response.json() == {"id": 1}
        response = await client.post("/orders/2?fail=true")
        assert response.status_code == 500
    assert await count(engine, table) == await count(engine, domain) == 1
    assert not runtime.native.subscribers


async def test_fastapi_host_owns_dead_letter_replay_transaction(database):
    engine, table, dlq, domain = database
    identifier = await archive(engine, dlq)
    sessions = async_sessionmaker(engine)

    async def session():
        async with sessions() as handle:
            yield handle

    def letters(handle: Annotated[AsyncSession, Depends(session)]):
        return DeadLetters(handle, outbox_table=table, dlq_table=dlq)

    api = FastAPI()

    @api.post("/replay/{identifier}")
    async def replay(
        identifier: int,
        handle: Annotated[AsyncSession, Depends(session)],
        dead: Annotated[DeadLetters, Depends(letters)],
        fail: bool = False,
    ):
        assert dead.session is handle
        async with handle.begin():
            await handle.execute(insert(domain).values(id=9))
            pending_id = await dead.replay(
                identifier, queue="orders.created.v1"
            )
            if fail:
                raise ValueError("rollback")
        return {"pending_id": pending_id}

    async with AsyncClient(
        transport=ASGITransport(api, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        failed = await client.post(f"/replay/{identifier}?fail=true")
        assert failed.status_code == 500
        assert await count(engine, dlq) == 1
        assert await count(engine, table) == await count(engine, domain) == 0
        replayed = await client.post(f"/replay/{identifier}")
        assert replayed.status_code == 200
        assert replayed.json()["pending_id"] is not None
    assert await count(engine, dlq) == 0
    assert await count(engine, table) == await count(engine, domain) == 1
