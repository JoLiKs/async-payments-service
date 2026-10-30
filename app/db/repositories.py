import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID, *, for_update: bool = False) -> Payment | None:
        stmt = select(Payment).where(Payment.id == payment_id)
        if for_update:
            stmt = stmt.with_for_update()
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == key)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self._session.add(payment)


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.execute(stmt)).scalars().all()

    async def mark_published(self, event_id: uuid.UUID, at: datetime) -> None:
        await self._session.execute(
            update(OutboxEvent).where(OutboxEvent.id == event_id).values(published_at=at)
        )
