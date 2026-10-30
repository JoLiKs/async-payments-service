from dataclasses import dataclass
from typing import Any

from app.messaging.topology import ATTEMPT_HEADER, retry_delay_ms, retry_queue_name


@dataclass(frozen=True, slots=True)
class Retry:
    next_attempt: int
    delay_ms: int
    routing_key: str


@dataclass(frozen=True, slots=True)
class DeadLetter:
    reason: str


def current_attempt(headers: dict[str, Any] | None) -> int:
    try:
        return max(1, int((headers or {}).get(ATTEMPT_HEADER, 1)))
    except (TypeError, ValueError):
        return 1


def decide(attempt: int, *, max_attempts: int, base_delay_ms: int, retryable: bool = True) -> Retry | DeadLetter:
    if not retryable:
        return DeadLetter(reason="non-retryable error")
    if attempt >= max_attempts:
        return DeadLetter(reason=f"exhausted {max_attempts} attempts")
    delay = retry_delay_ms(attempt, base_delay_ms)
    return Retry(next_attempt=attempt + 1, delay_ms=delay, routing_key=retry_queue_name(delay))
