from decimal import Decimal
from typing import Any

from app.domain.enums import Currency
from app.services.payments import CreatePaymentCommand


def make_command(key: str = "key-1", **overrides: Any) -> CreatePaymentCommand:
    data: dict[str, Any] = {
        "idempotency_key": key,
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "http://example.test/hook",
    }
    data.update(overrides)
    return CreatePaymentCommand(**data)
