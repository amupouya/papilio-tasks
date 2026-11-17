import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("taskiq.middlewares.opentelemetry_middleware")

from dishka import Provider, Scope
from monitoring_support import observations as observations
from opentelemetry import trace
from taskiq import InMemoryBroker, TaskiqMessage
from taskiq.exceptions import SendTaskError
from taskiq.middlewares import SmartRetryMiddleware

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.projections.application import (
    create_broker as projection_broker,
)
from papilio_tasks.apps.projections.registry import Registrar as Projections
from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.tools.monitoring import Monitoring


@pytest.mark.parametrize("family", ["scheduler", "projection"])
async def test_taskiq_real_execution_correlation_and_borrowed_providers(
    observations, family
):
    class Job(Scheduler):
        async def run(self, value: int) -> int:
            return value * 2

    class Project(Direct[int, int]):
        def __init__(self):
            super().__init__()

        async def read(self, value: int) -> int:
            return value

        async def write(self, data: int) -> int:
            return data * 2

    cls = Job if family == "scheduler" else Project
    registry = Registrar() if family == "scheduler" else Projections()
    registry.include(cls, name=f"orders.{family}")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(cls)
    assemble = create_broker if family == "scheduler" else projection_broker
    broker = assemble(registrar=registry, providers=[provider])
    monitoring = observations.monitoring
    first = monitoring.taskiq(broker)
    assert monitoring.taskiq(broker) is first
    await broker.startup()
    try:
        tracer = monitoring.tracer_provider.get_tracer("test")
        with tracer.start_as_current_span("request") as parent:
            handle = await cls.enqueue(21)
        result = await handle.wait_result(timeout=5)
        assert result.return_value == 42
        await broker.wait_all()
    finally:
        await broker.shutdown()
    spans = observations.spans.get_finished_spans()
    worker = next(s for s in spans if s.name == f"execute/orders.{family}")
    assert worker.parent.span_id == parent.get_span_context().span_id
    assert worker.attributes["messaging.message.id"] == handle.task_id
    assert observations.points("task_success")[0].value == 1
    assert observations.points("worker_active_tasks")[0].value == 0
    # Broker shutdown must not close host-owned shared telemetry providers.
    with tracer.start_as_current_span("after-shutdown"):
        pass
    assert observations.spans.get_finished_spans()[-1].name == "after-shutdown"


async def test_taskiq_retry_closes_every_attempt_without_saved_result(
    observations,
):
    broker = InMemoryBroker()
    broker.with_middlewares(
        SmartRetryMiddleware(default_delay=0, no_result_on_retry=True)
    )
    observations.monitoring.taskiq(broker)
    calls = []

    @broker.task(retry_on_error=True, max_retries=3)
    async def job():
        calls.append(trace.get_current_span().get_span_context().span_id)
        if len(calls) < 3:
            raise ConnectionError("transient")
        return 42

    await broker.startup()
    try:
        result = await (await job.kiq()).wait_result(timeout=5)
        await broker.wait_all()
        assert result.return_value == 42
    finally:
        await broker.shutdown()
    spans = observations.spans.get_finished_spans()
    attempts = [s for s in spans if s.name.startswith("execute/")]
    assert len(attempts) == 3
    assert len(set(calls)) == 3
    assert sum(p.value for p in observations.points("task_errors")) == 2
    assert observations.points("worker_active_tasks")[0].value == 0


async def test_taskiq_concurrent_attempts_do_not_mix_context(observations):
    broker = InMemoryBroker()
    observations.monitoring.taskiq(broker)
    tracer = observations.monitoring.tracer_provider.get_tracer("test")

    @broker.task
    async def job(value: int):
        before = trace.get_current_span().get_span_context()
        await asyncio.sleep(0)
        assert trace.get_current_span().get_span_context() == before
        return value

    async def submit(value):
        with tracer.start_as_current_span(f"parent-{value}"):
            result = await (await job.kiq(value)).wait_result(timeout=5)
        return result

    await broker.startup()
    try:
        results = await asyncio.gather(*(submit(v) for v in range(10)))
        await broker.wait_all()
    finally:
        await broker.shutdown()
    assert sorted(r.return_value for r in results) == list(range(10))
    spans = observations.spans.get_finished_spans()
    attempts = [s for s in spans if s.name.startswith("execute/")]
    assert len(attempts) == 10
    assert len({s.context.trace_id for s in attempts}) == 10


def test_taskiq_conflicting_installation_fails_before_mutation(observations):
    broker = InMemoryBroker()
    monitoring = observations.monitoring
    monitoring.taskiq(broker)
    before = tuple(broker.middlewares)
    with pytest.raises(ValueError, match="already"):
        Monitoring(monitoring.tracer_provider, object()).taskiq(broker)
    assert tuple(broker.middlewares) == before


async def test_failed_send_does_not_leave_a_current_producer_span(
    observations,
):
    broker = InMemoryBroker()
    middleware = observations.monitoring.taskiq(broker)
    tracer = observations.monitoring.tracer_provider.get_tracer("test")

    @broker.task
    async def job():
        pass

    async def unavailable(message):
        raise ConnectionError("broker unavailable")

    broker.kick = unavailable
    with tracer.start_as_current_span("request") as parent:
        with pytest.raises(SendTaskError):
            await job.kiq()
        assert trace.get_current_span() is parent
    # No pending telemetry is retained when native post_send is skipped.
    message = TaskiqMessage(
        task_id="id", task_name="name", labels={}, args=[], kwargs={}
    )
    assert middleware.pre_send(message) is message
