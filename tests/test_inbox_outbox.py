import pytest

pytest.importorskip("sqlalchemy_inbox")
pytest.importorskip("faststream_outbox")

from faststream_outbox import OutboxBroker, make_outbox_table
from sqlalchemy import MetaData, insert
from test_inbox import count
from test_inbox import database as database

from papilio_tasks.apps.events.publishers.base import Publisher
from papilio_tasks.apps.inbox import Inbox
from papilio_tasks.apps.outbox import Outbox


async def test_inbox_business_and_outbox_share_one_commit(database):
    engine, table, domain = database
    metadata = MetaData()
    pending = make_outbox_table(metadata, table_name=table.name + "_out")
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)

    class Applied(Publisher[int]):
        outbox_key = "orders.applied.v1"

    runtime = Outbox(OutboxBroker(outbox_table=pending), publishers=[Applied])

    async def consume(fail=False):
        async with engine.begin() as connection:
            if not await Inbox(connection, table).claim(
                scope="orders.apply.v1", message_id="input-event"
            ):
                return False
            await connection.execute(insert(domain).values(id=1))
            await runtime.writer(connection).append(Applied, 1)
            if fail:
                raise ValueError("rollback")
        return True

    try:
        with pytest.raises(ValueError, match="rollback"):
            await consume(fail=True)
        assert await count(engine, table) == await count(engine, domain) == 0
        assert await count(engine, pending) == 0
        assert await consume()
        assert not await consume()
        assert await count(engine, table) == await count(engine, domain) == 1
        assert await count(engine, pending) == 1
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
