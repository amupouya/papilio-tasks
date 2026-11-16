"""Immutable per-job retry settings, independent of the task runtime."""

import random
from dataclasses import dataclass
from math import isfinite


def _number(value: object, name: str, minimum: float = 0) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value < minimum
    ):
        raise ValueError(f"{name} must be finite and at least {minimum}")


@dataclass(frozen=True, kw_only=True)
class Backoff:
    """Capped exponential delay; full jitter samples from zero to the cap."""

    initial: float
    multiplier: float
    max_delay: float
    jitter: bool = False

    def __post_init__(self) -> None:
        _number(self.initial, "initial")
        _number(self.multiplier, "multiplier", 1)
        _number(self.max_delay, "max_delay", self.initial)
        if not isinstance(self.jitter, bool):
            raise TypeError("jitter must be a bool")

    def at(self, retry: int) -> float:
        """Return the delay for a one-based retry number."""
        if type(retry) is not int or retry < 1:
            raise ValueError("retry must be a positive integer")
        if self.initial == 0:
            delay = 0.0
        else:
            try:
                delay = min(
                    self.initial * float(self.multiplier) ** (retry - 1),
                    self.max_delay,
                )
            except OverflowError:
                delay = self.max_delay
        return random.uniform(0, delay) if self.jitter else float(delay)


@dataclass(frozen=True, kw_only=True)
class Retry:
    """Total attempts include the initial call; delay is seconds or Backoff."""

    attempts: int
    delay: float | Backoff
    errors: tuple[type[Exception], ...]

    def __post_init__(self) -> None:
        if type(self.attempts) is not int or self.attempts < 1:
            raise ValueError("attempts must be a positive integer")
        if not isinstance(self.delay, Backoff):
            _number(self.delay, "delay")
        if not isinstance(self.errors, tuple) or not self.errors:
            raise ValueError("errors must be a nonempty tuple of exceptions")
        if any(
            not isinstance(error, type) or not issubclass(error, Exception)
            for error in self.errors
        ):
            raise TypeError("errors must contain Exception subclasses")
