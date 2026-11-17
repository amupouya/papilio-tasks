import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy_inbox")
pytest.importorskip("faststream.nats")
pytest.importorskip("opentelemetry.sdk")

import nats
from dishka import Provider, Scope, provide
from faststream import StreamMessage
from faststream.nats import JStream, PullSub
from faststream.nats import NatsBroker as NativeNats
from monitoring_support import observations as observations
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_inbox import count
from test_inbox import database as database

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.jetstream import JetPublisher
from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
from papilio_tasks.apps.events.subscribers.jetstream import JetSubscriber
from papilio_tasks.apps.inbox import Inbox
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.retry import Retry


async def test_nats_local_retry_and_redelivery_share_native_inbox(
    database, observations
):
    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    engine, table, domain = database
    name = "inbox_" + uuid4().hex
    committed, duplicate = asyncio.Event(), asyncio.Event()
    sessions, calls = [], []
    factory = async_sessionmaker(engine)

    class Created(JetPublisher[int]):
        subject = name + ".created"
        stream = JStream(name)

    class Reader(JetSubscriber[int]):
        publisher = Created
        durable = name
        pull_sub = PullSub(timeout=0.2)
        retry = Retry(attempts=2, delay=0, errors=(ConnectionError,))

        def __init__(
            self, session: AsyncSession, inbox: Inbox, message: StreamMessage
        ):
            self.session, self.inbox, self.message = session, inbox, message

        async def run(self, event: int) -> None:
            calls.append(event)
            async with self.session.begin():
                if not await self.inbox.claim(
                    scope="orders.apply.v1",
                    message_id=self.message.headers["event_id"],
                ):
                    duplicate.set()
                    return
                await self.session.execute(insert(domain).values(id=event))
                if len(calls) == 1:
                    raise ConnectionError("before commit")
            # Commit completes before handler return/native acknowledgement.
            committed.set()

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def session(self) -> AsyncIterator[AsyncSession]:
            async with factory() as session:
                sessions.append(session)
                yield session

        @provide(scope=Scope.REQUEST)
        def inbox(self, session: AsyncSession) -> Inbox:
            return observations.monitoring.inbox(session, table)

        reader = provide(Reader, scope=Scope.REQUEST)

    registry = JetRegistrar(NatsBroker(NativeNats(url, logger=None)))
    registry.publisher(Created)
    registry.subscriber(Reader)
    app = create_app(registrar=registry, providers=[Dependencies()])
    try:
        await app.start()
        await Created.publish(42, headers={"event_id": "stable-event"})
        await asyncio.wait_for(committed.wait(), 10)
        await Created.publish(42, headers={"event_id": "stable-event"})
        await asyncio.wait_for(duplicate.wait(), 10)
    finally:
        await app.stop()
        client = await nats.connect(url)
        try:
            await client.jetstream().delete_stream(name)
        finally:
            await client.close()
    assert calls == [42, 42, 42]
    assert len({id(session) for session in sessions}) == 3
    assert await count(engine, table) == await count(engine, domain) == 1
    assert {
        p.attributes["outcome"]: p.value
        for p in observations.points("papilio_tasks.inbox.claims")
    } == {"claimed": 2, "duplicate": 1}
