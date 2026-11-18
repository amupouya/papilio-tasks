"""Compatibility import for SQL dead-letter observations."""

from papilio_tasks.infra.sqlalchemy.dlq_monitoring import Observer

__all__ = ["Observer"]
