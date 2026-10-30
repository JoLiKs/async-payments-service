import pytest

from app.messaging.retry import DeadLetter, Retry, current_attempt, decide


@pytest.mark.parametrize(
    ("attempt", "delay", "queue"),
    [(1, 1000, "payments.new.retry.1000ms"), (2, 2000, "payments.new.retry.2000ms")],
)
def test_exponential_backoff(attempt: int, delay: int, queue: str) -> None:
    decision = decide(attempt, max_attempts=3, base_delay_ms=1000)
    assert decision == Retry(next_attempt=attempt + 1, delay_ms=delay, routing_key=queue)


def test_dead_letter_after_max_attempts() -> None:
    assert isinstance(decide(3, max_attempts=3, base_delay_ms=1000), DeadLetter)


def test_non_retryable_goes_straight_to_dlq() -> None:
    assert isinstance(decide(1, max_attempts=3, base_delay_ms=1000, retryable=False), DeadLetter)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [(None, 1), ({}, 1), ({"x-attempt": 2}, 2), ({"x-attempt": "3"}, 3), ({"x-attempt": "junk"}, 1)],
)
def test_current_attempt(headers: dict | None, expected: int) -> None:
    assert current_attempt(headers) == expected
