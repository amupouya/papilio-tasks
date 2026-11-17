# pyright: strict

from dataclasses import dataclass
from typing import assert_type

from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from papilio_tasks.apps.events.publishers.base import Publisher
from papilio_tasks.apps.outbox import (
    DeadLetterDetail,
    DeadLetterPage,
    DeadLetters,
    OutboxWriter,
)
from papilio_tasks.apps.outbox.decorators import publish


@dataclass
class Order:
    id: int


class Created(Publisher[Order]):
    outbox_key = "orders.created.v1"


def select_order(result: Order) -> Order:
    return result


class Commands:
    outbox: OutboxWriter

    @publish(Created, select=select_order)
    async def create(self, identifier: int) -> Order:
        return Order(identifier)


async def usage(writer: OutboxWriter, commands: Commands) -> None:
    assert_type(await writer.append(Created, Order(1)), int)
    await writer.append_batch(Created, [Order(1), Order(2)])
    assert_type(await commands.create(1), Order)
    await writer.append(Created, "wrong")  # expect-error
    await writer.append_batch(Created, ["wrong"])  # expect-error
    await commands.create("wrong")  # expect-error


async def dead_letters(
    handle: AsyncSession | AsyncConnection, pending: Table, archived: Table
) -> None:
    letters = DeadLetters(handle, outbox_table=pending, dlq_table=archived)
    assert_type(await letters.list(queue="orders.created.v1"), DeadLetterPage)
    assert_type(await letters.get(1), DeadLetterDetail | None)
    assert_type(await letters.replay(1, queue="orders.created.v1"), int | None)
    await letters.get("wrong")  # expect-error
    await letters.list(after="wrong")  # expect-error
    await letters.replay(1)  # expect-error
    DeadLetters(0, outbox_table=pending, dlq_table=archived)  # expect-error
