import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Payment
from app.db.repositories import PaymentRepository
from app.domain.enums import PaymentStatus
from app.domain.errors import PaymentNotFoundError
from app.services.gateway import PaymentGateway
from app.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)


def webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "metadata": payment.metadata_,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


class PaymentProcessor:

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            repo = PaymentRepository(session)
            payment = await repo.get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            if payment.webhook_delivered_at is not None:
                logger.info("payment %s already fully processed, skipping", payment_id)
                return
            if payment.status is PaymentStatus.PENDING:
                payment.status = await self._gateway.charge()
                payment.processed_at = datetime.now(UTC)
                logger.info("payment %s -> %s", payment_id, payment.status)

        await self._webhooks.send(payment.webhook_url, webhook_payload(payment))

        async with self._session_factory() as session, session.begin():
            payment = await PaymentRepository(session).get(payment_id, for_update=True)
            if payment is not None and payment.webhook_delivered_at is None:
                payment.webhook_delivered_at = datetime.now(UTC)
        logger.info("webhook for payment %s delivered", payment_id)
