from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.enums import PaymentStatus
from app.services.payments import PaymentService
from app.services.processing import PaymentProcessor
from app.services.webhooks import WebhookDeliveryError
from tests.factories import make_command


class CountingGateway:
    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls = 0

    async def charge(self) -> PaymentStatus:
        self.calls += 1
        return self.result


class FlakyWebhooks:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.sent: list[dict[str, Any]] = []

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        if self.failures:
            self.failures -= 1
            raise WebhookDeliveryError("HTTP 500")
        self.sent.append(payload)


async def test_processing_updates_status_and_sends_webhook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = (await PaymentService(session_factory).create(make_command())).payment
    gateway, webhooks = CountingGateway(PaymentStatus.FAILED), FlakyWebhooks()
    await PaymentProcessor(session_factory, gateway, webhooks).process(payment.id)

    stored = await PaymentService(session_factory).get(payment.id)
    assert stored.status is PaymentStatus.FAILED
    assert stored.processed_at is not None
    assert webhooks.sent[0]["status"] == "failed"


async def test_redelivery_is_a_noop(session_factory: async_sessionmaker[AsyncSession]) -> None:
    payment = (await PaymentService(session_factory).create(make_command())).payment
    gateway, webhooks = CountingGateway(), FlakyWebhooks()
    processor = PaymentProcessor(session_factory, gateway, webhooks)
    await processor.process(payment.id)
    await processor.process(payment.id)
    assert gateway.calls == 1
    assert len(webhooks.sent) == 1


async def test_webhook_retry_does_not_charge_again(session_factory: async_sessionmaker[AsyncSession]) -> None:
    payment = (await PaymentService(session_factory).create(make_command())).payment
    gateway, webhooks = CountingGateway(), FlakyWebhooks(failures=2)
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    for _ in range(2):
        with pytest.raises(WebhookDeliveryError):
            await processor.process(payment.id)
    await processor.process(payment.id)

    assert gateway.calls == 1
    assert len(webhooks.sent) == 1
    stored = await PaymentService(session_factory).get(payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
