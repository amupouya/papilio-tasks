from abc import ABC, abstractmethod
from typing import ClassVar

from papilio_tasks.tools.retry import Retry


class Subscriber[T](ABC):
    """Providers construct subscribers and their dependencies per scope."""

    retry: ClassVar[Retry | None] = None

    @abstractmethod
    async def run(self, event: T) -> None: ...
