import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings, get_settings
from app.db.models import OutboxEvent, Payment
from app.domain.errors import IdempotencyConflictError
from app.main import create_app
from app.services.payments import PaymentService
from tests.factories import make_command


async def _count(sf: async_sessionmaker[AsyncSession], model: type) -> int:
    async with sf() as session:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()


async def test_same_key_returns_same_payment(session_factory: async_sessionmaker[AsyncSession]) -> None:
    service = PaymentService(session_factory)
    first = await service.create(make_command())
    second = await service.create(make_command())
    assert first.created and not second.created
    assert first.payment.id == second.payment.id
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_same_key_different_body_conflicts(session_factory: async_sessionmaker[AsyncSession]) -> None:
    service = PaymentService(session_factory)
    await service.create(make_command())
    with pytest.raises(IdempotencyConflictError):
        await service.create(make_command(amount=Decimal("1")))


async def test_equivalent_amounts_are_not_a_conflict(session_factory: async_sessionmaker[AsyncSession]) -> None:
    service = PaymentService(session_factory)
    first = await service.create(make_command(amount=Decimal("100")))
    second = await service.create(make_command(amount=Decimal("100.00")))
    assert first.payment.id == second.payment.id


async def test_concurrent_requests_create_one_payment(session_factory: async_sessionmaker[AsyncSession]) -> None:
    service = PaymentService(session_factory)
    results = await asyncio.gather(*(service.create(make_command("race")) for _ in range(10)))
    assert len({r.payment.id for r in results}) == 1
    assert sum(r.created for r in results) == 1
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


@pytest.fixture
async def client(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    app.state.payment_service = PaymentService(session_factory)
    app.dependency_overrides[get_settings] = lambda: Settings(api_key="test-key")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


BODY = {
    "amount": "250.00",
    "currency": "USD",
    "description": "test",
    "metadata": {"a": 1},
    "webhook_url": "http://example.test/hook",
}
AUTH = {"X-API-Key": "test-key"}


async def test_api_create_and_replay(client: httpx.AsyncClient) -> None:
    headers = {**AUTH, "Idempotency-Key": "api-1"}
    r1 = await client.post("/api/v1/payments", json=BODY, headers=headers)
    r2 = await client.post("/api/v1/payments", json=BODY, headers=headers)
    assert r1.status_code == r2.status_code == 202
    assert r1.json() == r2.json()
    assert r1.json()["status"] == "pending"
    assert r2.headers["Idempotent-Replayed"] == "true"

    r3 = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=headers)
    assert r3.status_code == 409

    got = await client.get(f"/api/v1/payments/{r1.json()['payment_id']}", headers=AUTH)
    assert got.status_code == 200
    assert got.json()["amount"] == "250.00"
    assert got.json()["metadata"] == {"a": 1}


async def test_api_requires_key_and_idempotency_header(client: httpx.AsyncClient) -> None:
    r = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"})
    assert r.status_code == 401
    r = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000", headers={"X-API-Key": "bad"})
    assert r.status_code == 401
    r = await client.post("/api/v1/payments", json=BODY, headers=AUTH)
    assert r.status_code == 422
    r = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000", headers=AUTH)
    assert r.status_code == 404
