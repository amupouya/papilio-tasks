"""Configured Redis scheduler applications."""

from collections.abc import Sequence
from dataclasses import dataclass

from dishka import Provider
from taskiq import ScheduledTask, TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import RedisAsyncResultBackend

from papilio_tasks.apps.lifecycle import Producer, Producers
from papilio_tasks.apps.taskiq import (
    Application,
    RedisSettings,
    RetryRuntime,
    worker_producers,
)
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource
from papilio_tasks.infra.taskiq.sources.base import Source

from .application import create_beat, create_broker
from .backends.redis import RedisStreamBroker
from .registry.redis import RedisRegistrar

__all__ = ["RedisSettings", "SchedulerApplication", "create_app"]


@dataclass(frozen=True)
class SchedulerApplication(Application):
    settings: RedisSettings
    source: RedisSource

    async def stop(self) -> None:
        try:
            await super().stop()
        finally:
            await self.source.native.shutdown()

    async def list_schedules(self) -> list[ScheduledTask]:
        """Read persisted and declared schedules without consuming due jobs."""
        declared = LabelScheduleSource(self.broker)
        await declared.startup()
        labels = await declared.get_schedules()
        stored = await self.source.list_schedules()
        return [*stored, *labels]


def create_app(
    settings: RedisSettings,
    *,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
    producers: Sequence[Producer] = (),
    retry: RetryRuntime | None = None,
) -> SchedulerApplication:
    """Build worker and beat without opening connections."""
    dependencies = Producers(*producers)
    source = RedisSource(settings.url, prefix=settings.schedule_prefix)
    transport = RedisStreamBroker(
        settings.url,
        queue_name=settings.queue_name,
        consumer_group_name=settings.consumer_group,
        consumer_id=settings.consumer_id,
    )
    transport.native.with_result_backend(
        RedisAsyncResultBackend(
            settings.url,
            result_ex_time=settings.result_ttl,
            prefix_str=settings.result_prefix,
        )
    )
    broker = create_broker(
        registrar=RedisRegistrar(transport),
        providers=providers,
        modules=modules,
        retry_source=source if retry is None else None,
        retry=retry,
    )
    worker_producers(broker, dependencies)
    sources = [source, Source(LabelScheduleSource(broker))]
    # An explicit runtime has its own shared retry beat. This beat owns only
    # this application's ordinary schedules and declared defaults.
    scheduler = (
        create_beat(broker, sources=sources)
        if retry is None
        else TaskiqScheduler(broker, sources=[item.native for item in sources])
    )
    return SchedulerApplication(broker, scheduler, settings, source)
