import pytest
from faststream.nats import NatsBroker as NativeNats
from faststream.nats import TestNatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware
from monitoring_support import observations as observations

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.nats import publisher
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.projections.application import (
    create_broker as projects,
)
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers import TaskRouter
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.retry import Retry


@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_function_task_native_monitoring_and_trace_parent(
    observations, family
):
    async def read(value: int) -> int:
        return value

    async def transform(value: int) -> str:
        return str(value)

    async def write(value: str) -> int:
        return int(value)

    if family == "scheduler":
        router = TaskRouter()
        task = router.task(name="function.task")(read)
        broker = create_broker(routers=[router])
    else:
        router = ProjectionRouter()
        task = router.projection(
            name="function.task", read=read, transform=transform, write=write
        )
        broker = projects(routers=[router])
    observations.monitoring.taskiq(broker)
    await broker.startup()
    try:
        tracer = observations.monitoring.tracer_provider.get_tracer("test")
        with tracer.start_as_current_span("request") as parent:
            handle = await task.enqueue(7)
        result = await handle.wait_result(timeout=5)
        assert not result.is_err and result.return_value == 7
        await broker.wait_all()
    finally:
        await broker.shutdown()
    span = next(
        s
        for s in observations.spans.get_finished_spans()
        if s.name == "execute/function.task"
    )
    assert span.parent.span_id == parent.get_span_context().span_id
    assert observations.points("task_success")[0].value == 1


@pytest.mark.parametrize("standalone", [False, True])
async def test_function_event_retry_uses_existing_monitoring(
    observations, standalone
):
    router, calls = NatsRouter(), []
    created = (publisher if standalone else router.publisher)(
        int, subject="created"
    )

    @router.subscriber(
        "created",
        name="function.receive",
        retry=Retry(attempts=2, delay=0, errors=(ConnectionError,)),
    )
    async def receive(event: int) -> None:
        calls.append(event)
        if len(calls) == 1:
            raise ConnectionError("transient")

    native = NativeNats(logger=None)
    observations.monitoring.faststream(native, NatsTelemetryMiddleware)
    app = create_app(
        registrar=NatsRegistrar(NatsBroker(native)),
        routers=[router],
        publishers=[created] if standalone else [],
    )
    try:
        async with TestNatsBroker(native):
            await created.publish(7, headers={"event_id": "stable"})
        points = observations.points("papilio_tasks.subscriber.attempts")
        assert sum(p.value for p in points) == 2
        assert all(
            p.attributes["subscriber"] == "function.receive" for p in points
        )
        assert (
            sum(
                p.value
                for p in observations.points(
                    "papilio_tasks.subscriber.retries"
                )
            )
            == 1
        )
        assert all("event_id" not in p.attributes for p in points)
    finally:
        await app.stop()
