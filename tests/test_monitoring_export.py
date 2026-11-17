import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("opentelemetry.exporter.otlp.proto.http")

from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from taskiq import InMemoryBroker

from papilio_tasks.tools.monitoring import Monitoring


@pytest.mark.parametrize("available", [True, False])
async def test_real_otlp_http_export_and_monitor_outage(available):
    received = []

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(
                (
                    self.path,
                    self.rfile.read(int(self.headers["content-length"])),
                )
            )
            self.send_response(200 if available else 503)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    resource = Resource.create({"service.name": "otlp-test"})
    tracer = TracerProvider(resource=resource)
    tracer.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=0.1),
            max_queue_size=16,
            max_export_batch_size=8,
            schedule_delay_millis=60000,
        )
    )
    meter = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(
                    endpoint=endpoint + "/v1/metrics", timeout=0.1
                ),
                export_interval_millis=60000,
            )
        ],
    )
    monitoring = Monitoring(tracer, meter)
    broker = InMemoryBroker()
    monitoring.taskiq(broker)

    @broker.task
    async def job(value: int):
        return value * 2

    try:
        await broker.startup()
        result = await (await job.kiq(21)).wait_result(timeout=5)
        await broker.wait_all()
        assert result.return_value == 42 and not result.is_err
        await broker.shutdown()
        # Export is host-owned, batched and outside execution/ack/retry paths.
        await asyncio.gather(
            asyncio.to_thread(tracer.force_flush),
            asyncio.to_thread(meter.force_flush),
        )
        paths = {path for path, _ in received}
        assert paths == {"/v1/traces", "/v1/metrics"}
        trace_payload = ExportTraceServiceRequest.FromString(
            next(body for path, body in received if path == "/v1/traces")
        )
        metric_payload = ExportMetricsServiceRequest.FromString(
            next(body for path, body in received if path == "/v1/metrics")
        )
        spans = [
            s
            for r in trace_payload.resource_spans
            for group in r.scope_spans
            for s in group.spans
        ]
        assert len(spans) == 1 and spans[0].name.startswith("execute/")
        names = {
            m.name
            for r in metric_payload.resource_metrics
            for group in r.scope_metrics
            for m in group.metrics
        }
        assert {"task_success", "task_execution_time"} <= names
    finally:
        await asyncio.gather(
            asyncio.to_thread(tracer.shutdown),
            asyncio.to_thread(meter.shutdown),
        )
        await asyncio.to_thread(server.shutdown)
        server.server_close()
        thread.join(timeout=5)
