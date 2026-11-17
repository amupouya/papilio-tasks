"""Optional native telemetry sharing caller-owned providers.

Import this module only when monitoring is enabled. It never configures global
providers, opens an exporter, starts a server or shuts down borrowed resources.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider

if TYPE_CHECKING:
    from taskiq import AsyncBroker

    from papilio_tasks.infra.faststream.monitoring import Broker
    from papilio_tasks.infra.faststream.outbox_monitoring import Recorder
    from papilio_tasks.infra.taskiq.monitoring import Middleware


@dataclass(frozen=True)
class Monitoring:
    """Share explicit providers across independently configured native brokers.

    Args:
        tracer_provider: Host-owned tracing provider, including resource,
            sampling and span processors.
        meter_provider: Host-owned metrics provider, including resource,
            readers, aggregation and cardinality limits.
    """

    tracer_provider: TracerProvider
    meter_provider: MeterProvider

    def taskiq(self, broker: "AsyncBroker") -> "Middleware":
        """Instrument one native scheduler/projection broker before startup.

        Args:
            broker: Borrowed native broker before worker/producer startup.

        Returns:
            The installed middleware. Reusing these providers is idempotent.

        Raises:
            ValueError: Another telemetry configuration is already attached.
        """
        # Taskiq remains optional for Events-only monitoring installations.
        from papilio_tasks.infra.taskiq.monitoring import instrument

        return instrument(broker, self)

    def faststream[T: Callable[..., Any]](
        self, broker: "Broker", factory: Callable[..., T]
    ) -> T:
        """Attach native telemetry with scoped identity observations once.

        Args:
            broker: Native broker before startup, borrowed without lifecycle.
            factory: The selected transport's native TelemetryMiddleware class.

        Returns:
            Native telemetry factory. Identity and local retry observations
            enrich the native processing span inside its execution scope.

        Raises:
            ValueError: A different monitoring configuration is attached.
        """
        # FastStream remains optional for Taskiq-only installations.
        from papilio_tasks.infra.faststream.monitoring import instrument

        return instrument(broker, factory, self)

    def outbox_recorder(self) -> "Recorder":
        """Build the native recorder for internal relay/retry/DLQ metrics.

        Returns:
            Native recorder adapter with bus metrics disabled. Pair it with
            faststream(broker, OutboxTelemetryMiddleware) to avoid counting
            each bus execution twice.
        """
        # PostgreSQL/Outbox dependencies are not required by other monitoring.
        from papilio_tasks.infra.faststream.outbox_monitoring import Recorder

        return Recorder(self.meter_provider)
