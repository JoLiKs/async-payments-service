import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent, Payment
from app.db.repositories import OutboxRepository, PaymentRepository
from app.domain.enums import Currency, PaymentStatus
from app.domain.errors import IdempotencyConflictError, PaymentNotFoundError
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY

PAYMENT_CREATED_EVENT = "payment.created"


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    webhook_url: str

    def fingerprint(self) -> str:
        body = {
            "amount": str(self.amount.normalize()),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": self.webhook_url,
        }
        raw = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create(self, cmd: CreatePaymentCommand) -> CreatePaymentResult:
        fingerprint = cmd.fingerprint()

        existing = await self._find_existing(cmd.idempotency_key, fingerprint)
        if existing is not None:
            return CreatePaymentResult(existing, created=False)

        payment = Payment(
            id=uuid.uuid4(),
            amount=cmd.amount,
            currency=cmd.currency,
            description=cmd.description,
            metadata_=cmd.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=cmd.idempotency_key,
            request_hash=fingerprint,
            webhook_url=cmd.webhook_url,
        )
        try:
            async with self._session_factory() as session, session.begin():
                PaymentRepository(session).add(payment)
                OutboxRepository(session).add(
                    OutboxEvent(
                        aggregate_id=payment.id,
                        event_type=PAYMENT_CREATED_EVENT,
                        routing_key=PAYMENTS_NEW_ROUTING_KEY,
                        payload={"payment_id": str(payment.id)},
                    )
                )
                await session.flush()
                await session.refresh(payment)
        except IntegrityError:
            existing = await self._find_existing(cmd.idempotency_key, fingerprint)
            if existing is None:
                raise
            return CreatePaymentResult(existing, created=False)

        return CreatePaymentResult(payment, created=True)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(str(payment_id))
        return payment

    async def _find_existing(self, key: str, fingerprint: str) -> Payment | None:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get_by_idempotency_key(key)
        if payment is not None and payment.request_hash != fingerprint:
            raise IdempotencyConflictError(key)
        return payment
