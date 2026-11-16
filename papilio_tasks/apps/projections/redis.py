"""Configured Redis projection applications."""

from collections.abc import Sequence

from dishka import Provider
from taskiq import TaskiqScheduler
from taskiq_redis import RedisAsyncResultBackend

from papilio_tasks.apps.lifecycle import Producer, Producers
from papilio_tasks.apps.taskiq import (
    Application,
    RedisSettings,
    RetryRuntime,
    worker_producers,
)
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource
from papilio_tasks.tools.hooks.publish import PublishHooks

from .application import create_beat, create_broker
from .backends.redis import RedisStreamBroker
from .registry.redis import RedisRegistrar

__all__ = ["RedisSettings", "create_app"]


def create_app(
    settings: RedisSettings,
    *,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
    producers: Sequence[Producer] = (),
    publish_hooks: type[PublishHooks] | None = None,
    retry: RetryRuntime | None = None,
) -> Application:
    """Build a projection worker and retry beat without connecting."""
    dependencies = Producers(*producers)
    source = (
        RedisSource(settings.url, prefix=settings.schedule_prefix)
        if retry is None
        else None
    )
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
        retry_source=source,
        retry=retry,
        publish_hooks=publish_hooks,
    )
    worker_producers(broker, dependencies)
    # A shared runtime's retry source is consumed by its explicit shared beat.
    beat = (
        create_beat(broker, sources=[source])
        if source is not None
        else TaskiqScheduler(broker, sources=[])
    )
    return Application(broker, beat)
