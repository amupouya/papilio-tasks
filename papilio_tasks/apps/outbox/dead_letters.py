"""Public compatibility exports for native SQL dead-letter management."""

from papilio_tasks.infra.sqlalchemy.dead_letters import (
    DeadLetter,
    DeadLetterDetail,
    DeadLetterPage,
    DeadLetters,
)

__all__ = ["DeadLetter", "DeadLetterDetail", "DeadLetterPage", "DeadLetters"]
