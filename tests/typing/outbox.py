# pyright: strict

from dataclasses import dataclass
from typing import assert_type

from papilio_tasks.apps.events.publishers.base import Publisher
from papilio_tasks.apps.outbox import OutboxWriter
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
