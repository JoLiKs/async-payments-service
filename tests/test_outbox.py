from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.services.payments import PaymentService
from tests.factories import make_command


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[tuple[dict[str, Any], str, str]] = []

    async def __call__(self, payload: dict[str, Any], routing_key: str, message_id: str) -> None:
        if self.fail:
            raise ConnectionError("broker unavailable")
        self.messages.append((payload, routing_key, message_id))


async def _events(sf: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with sf() as session:
        return list((await session.execute(select(OutboxEvent))).scalars())


async def test_payment_and_outbox_event_written_together(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    result = await PaymentService(session_factory).create(make_command())
    [event] = await _events(session_factory)
    assert event.aggregate_id == result.payment.id
    assert event.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert event.payload == {"payment_id": str(result.payment.id)}
    assert event.published_at is None


async def test_relay_publishes_and_marks_sent(session_factory: async_sessionmaker[AsyncSession]) -> None:
    service = PaymentService(session_factory)
    for i in range(3):
        await service.create(make_command(f"k{i}"))
    broker = FakeBroker()
    relay = OutboxRelay(session_factory, broker)

    assert await relay.run_once() == 3
    assert await relay.run_once() == 0
    assert len(broker.messages) == 3
    assert all(e.published_at is not None for e in await _events(session_factory))
    ids = {str(e.id) for e in await _events(session_factory)}
    assert {m[2] for m in broker.messages} == ids


async def test_relay_keeps_events_when_broker_is_down(session_factory: async_sessionmaker[AsyncSession]) -> None:
    await PaymentService(session_factory).create(make_command())
    broker = FakeBroker(fail=True)
    relay = OutboxRelay(session_factory, broker)

    assert await relay.run_once() == 0
    [event] = await _events(session_factory)
    assert event.published_at is None
    assert event.attempts == 1
    assert "broker unavailable" in (event.last_error or "")

    broker.fail = False
    assert await relay.run_once() == 1
    [event] = await _events(session_factory)
    assert event.published_at is not None
