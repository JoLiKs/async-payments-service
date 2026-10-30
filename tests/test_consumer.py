import uuid
from typing import Any

from app.domain.errors import PaymentNotFoundError
from app.messaging.consumer import PaymentMessageHandler


class FakeMessage:
    def __init__(self, headers: dict[str, Any] | None = None) -> None:
        self.headers = headers or {}
        self.acked = False
        self.rejected = False

    async def ack(self) -> None:
        self.acked = True

    async def reject(self, requeue: bool = False) -> None:
        assert requeue is False
        self.rejected = True


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error:
            raise self.error


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[Any, str, dict[str, Any]]] = []

    async def __call__(self, body: Any, routing_key: str, headers: dict[str, Any]) -> None:
        self.published.append((body, routing_key, headers))


def make_handler(processor: FakeProcessor, publisher: RecordingPublisher) -> PaymentMessageHandler:
    return PaymentMessageHandler(processor, publisher, max_attempts=3, base_delay_ms=1000)


BODY = {"payment_id": str(uuid.uuid4())}


async def test_success_is_acked() -> None:
    processor, publisher, msg = FakeProcessor(), RecordingPublisher(), FakeMessage()
    await make_handler(processor, publisher)(BODY, msg)
    assert msg.acked and not msg.rejected
    assert publisher.published == []


async def test_failure_is_republished_to_retry_queue_with_next_attempt() -> None:
    processor, publisher = FakeProcessor(RuntimeError("webhook down")), RecordingPublisher()
    msg = FakeMessage({"x-attempt": 1})
    await make_handler(processor, publisher)(BODY, msg)
    assert msg.acked and not msg.rejected
    [(body, routing_key, headers)] = publisher.published
    assert body == BODY
    assert routing_key == "payments.new.retry.1000ms"
    assert headers["x-attempt"] == 2


async def test_full_retry_chain_ends_in_dlq() -> None:
    processor, publisher = FakeProcessor(RuntimeError("boom")), RecordingPublisher()
    handler = make_handler(processor, publisher)
    headers: dict[str, Any] = {}
    delays: list[str] = []
    while True:
        msg = FakeMessage(headers)
        await handler(BODY, msg)
        if msg.rejected:
            break
        _, routing_key, headers = publisher.published[-1]
        delays.append(routing_key)
    assert len(processor.calls) == 3
    assert delays == ["payments.new.retry.1000ms", "payments.new.retry.2000ms"]


async def test_last_attempt_is_rejected_to_dlq() -> None:
    processor, publisher = FakeProcessor(RuntimeError("boom")), RecordingPublisher()
    msg = FakeMessage({"x-attempt": 3})
    await make_handler(processor, publisher)(BODY, msg)
    assert msg.rejected and not msg.acked
    assert publisher.published == []


async def test_unknown_payment_goes_to_dlq_without_retries() -> None:
    processor, publisher = FakeProcessor(PaymentNotFoundError("x")), RecordingPublisher()
    msg = FakeMessage()
    await make_handler(processor, publisher)(BODY, msg)
    assert msg.rejected and publisher.published == []


async def test_malformed_body_goes_to_dlq() -> None:
    processor, publisher = FakeProcessor(), RecordingPublisher()
    msg = FakeMessage()
    await make_handler(processor, publisher)({"payment_id": "not-a-uuid"}, msg)
    assert msg.rejected and processor.calls == []
