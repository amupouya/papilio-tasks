"""Explicit Docker-stack smoke, separate from normal backend tests."""

import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import pytest

pytest.importorskip("opentelemetry.sdk")

from faststream.rabbit import RabbitBroker, TestRabbitBroker
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from taskiq import InMemoryBroker
from tenacity import AsyncRetrying, stop_after_delay, wait_fixed

from papilio_tasks.infra.faststream.dlq_monitoring import Observer
from papilio_tasks.tools.monitoring import Monitoring

pytestmark = pytest.mark.monitoring_stack


def get_json(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


async def test_packaged_stack_exports_queries_and_provisions_dashboard():
    endpoint = os.environ["TEST_MONITORING_OTLP"]
    prometheus = os.environ["TEST_MONITORING_PROMETHEUS"]
    grafana = os.environ["TEST_MONITORING_GRAFANA"]
    resource = Resource.create(
        {
            "service.name": "papilio-stack-test",
            "service.instance.id": "isolated-test",
        }
    )
    tracer = TracerProvider(resource=resource)
    tracer.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=2),
            schedule_delay_millis=100,
        )
    )
    meter = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(
                    endpoint=endpoint + "/v1/metrics", timeout=2
                ),
                export_interval_millis=1000,
            )
        ],
    )
    monitoring = Monitoring(tracer, meter)
    broker = InMemoryBroker()
    monitoring.taskiq(broker)

    @broker.task(task_name="orders.stack")
    async def job(value: int):
        return value * 2

    async def ready():
        assert (await asyncio.to_thread(get_json, grafana + "/api/health"))[
            "database"
        ] == "ok"

    poll = AsyncRetrying(
        stop=stop_after_delay(90), wait=wait_fixed(1), reraise=True
    )
    try:
        await poll(ready)
        await broker.startup()
        with tracer.get_tracer("test").start_as_current_span(
            "command"
        ) as parent:
            result = await (await job.kiq(21)).wait_result(timeout=5)
        assert result.return_value == 42
        await broker.wait_all()
        await broker.shutdown()
        native = RabbitBroker(logger=None)
        monitoring.faststream(native, RabbitTelemetryMiddleware)

        @native.subscriber("stack")
        async def consume(event: int):
            return None

        async with TestRabbitBroker(native):
            await native.publish(42, queue="stack")
        # Exercise the native recorder seam to verify its wire metric names.
        # Durable SQL retry/DLQ behavior is covered by test_monitoring_outbox.
        recorder = monitoring.outbox_recorder()
        recorder("nacked_retried", {"queue": "stack"})
        recorder(
            "dlq_written",
            {
                "queue": "stack",
                "subscriber": "relay",
                "failure_reason": "retry_terminal",
            },
        )
        # Verify the management telemetry wire/queries here; actual PostgreSQL
        # transaction and concurrency semantics live in test_dead_letters.
        with Observer(monitoring).operation("list", "stack") as operation:
            operation.set_attribute("papilio_tasks.dlq.rows", 0)
        await asyncio.gather(
            asyncio.to_thread(tracer.force_flush),
            asyncio.to_thread(meter.force_flush),
        )
        trace_id = format(parent.get_span_context().trace_id, "032x")

        async def observed():
            payload = await asyncio.to_thread(
                get_json,
                prometheus
                + "/api/v1/query?"
                + urlencode(
                    {
                        "query": (
                            'task_success_total{service_name="papilio-stack-test"}'
                        )
                    }
                ),
            )
            assert payload["data"]["result"]
            metrics = (
                "messaging_process_duration_seconds_count",
                "papilio_tasks_outbox_outcomes_total",
                "messaging_outbox_dlq_written_event_total",
                "papilio_tasks_dlq_operations_total",
                "papilio_tasks_dlq_operation_duration_seconds_count",
            )
            samples = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        get_json,
                        prometheus
                        + "/api/v1/query?"
                        + urlencode(
                            {
                                "query": metric
                                + '{service_name="papilio-stack-test"}'
                            }
                        ),
                    )
                    for metric in metrics
                )
            )
            assert all(sample["data"]["result"] for sample in samples), metrics
            trace_payload = await asyncio.to_thread(
                get_json,
                grafana
                + "/api/datasources/proxy/uid/papilio-tempo/api/traces/"
                + trace_id,
            )
            assert "execute/orders.stack" in json.dumps(trace_payload)

        await poll(observed)
        dashboard = await asyncio.to_thread(
            get_json, grafana + "/api/dashboards/uid/papilio-tasks"
        )
        assert dashboard["dashboard"]["title"] == "Papilio Tasks"
        path = (
            Path(__file__).parents[1]
            / "papilio_tasks"
            / "assets/monitoring/grafana/dashboards/papilio-tasks.json"
        )
        configured = json.loads(path.read_text())
        # Query the actual Prometheus parser for every packaged panel. Panels
        # for unused integrations can be empty; malformed expressions cannot.
        expressions = [
            target["expr"]
            .replace("$__rate_interval", "2m")
            .replace("$service", ".*")
            for panel in configured["panels"]
            for target in panel.get("targets", [])
        ]
        results = await asyncio.gather(
            *(
                asyncio.to_thread(
                    get_json,
                    prometheus
                    + "/api/v1/query?"
                    + urlencode({"query": expression}),
                )
                for expression in expressions
            )
        )
        assert all(result["status"] == "success" for result in results)
    finally:
        await asyncio.gather(
            asyncio.to_thread(tracer.shutdown),
            asyncio.to_thread(meter.shutdown),
        )
