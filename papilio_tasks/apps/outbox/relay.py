"""Native Outbox relay application and its producer lifetimes."""

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from faststream import FastStream
from faststream_outbox import OutboxBroker

from papilio_tasks.apps.events.publishers.base import Publication
from papilio_tasks.apps.events.publishers.bindings import get as get_sender
from papilio_tasks.apps.lifecycle import Producer, Producers


class RelayApplication(FastStream):
    """Native CLI application owning its declared producer lifetimes."""

    def __init__(
        self,
        broker: OutboxBroker,
        producers: Sequence[Producer],
        publishers: Sequence[Publication[Any]],
    ) -> None:
        self.producers = Producers(*producers)
        self.publishers = tuple(publishers)
        self._closed = False
        super().__init__(broker, lifespan=self._lifespan)

    @asynccontextmanager
    async def _lifespan(self) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await self.stop()

    async def start(self, **options: Any) -> None:
        if self._closed:
            raise RuntimeError("Outbox relay is closed")
        try:
            await self.producers.connect()
            for publisher in self.publishers:
                get_sender(publisher)
            await super().start(**options)
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await super().stop()
        finally:
            await self.producers.stop()
