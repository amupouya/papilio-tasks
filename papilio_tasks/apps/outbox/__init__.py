"""Optional transactional event registration and native PostgreSQL relay."""

from .application import Outbox
from .writer import OutboxWriter

__all__ = ["Outbox", "OutboxWriter"]
