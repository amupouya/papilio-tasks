from dataclasses import dataclass

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from papilio_tasks.tools.monitoring import Monitoring


@dataclass
class Observations:
    monitoring: Monitoring
    spans: InMemorySpanExporter
    reader: InMemoryMetricReader

    def points(self, name):
        data = self.reader.get_metrics_data()
        return [
            point
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]


@pytest.fixture
def observations():
    resource = Resource.create({"service.name": "monitoring-test"})
    spans, reader = InMemorySpanExporter(), InMemoryMetricReader()
    tracer = TracerProvider(resource=resource)
    tracer.add_span_processor(SimpleSpanProcessor(spans))
    meter = MeterProvider(resource=resource, metric_readers=[reader])
    try:
        yield Observations(Monitoring(tracer, meter), spans, reader)
    finally:
        tracer.shutdown()
        meter.shutdown()
