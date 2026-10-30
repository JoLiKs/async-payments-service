import asyncio
import random
from typing import Protocol

from app.domain.enums import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self) -> PaymentStatus: ...


class EmulatedGateway:

    def __init__(
        self,
        min_seconds: float,
        max_seconds: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min = min_seconds
        self._max = max_seconds
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self) -> PaymentStatus:
        await asyncio.sleep(self._rng.uniform(self._min, self._max))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
