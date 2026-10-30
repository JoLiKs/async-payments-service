import logging
import uuid
from typing import Any, Protocol

from faststream.rabbit import RabbitMessage
from pydantic import BaseModel, ValidationError

from app.domain.errors import PaymentNotFoundError
from app.messaging.retry import DeadLetter, Retry, current_attempt, decide
from app.messaging.topology import ATTEMPT_HEADER

logger = logging.getLogger(__name__)


class PaymentCreated(BaseModel):
    payment_id: uuid.UUID


class Processor(Protocol):
    async def process(self, payment_id: uuid.UUID) -> None: ...


class RetryPublisher(Protocol):
    async def __call__(self, body: Any, routing_key: str, headers: dict[str, Any]) -> None: ...


NON_RETRYABLE: tuple[type[Exception], ...] = (PaymentNotFoundError, ValidationError)


class PaymentMessageHandler:

    def __init__(
        self,
        processor: Processor,
        publish_retry: RetryPublisher,
        *,
        max_attempts: int,
        base_delay_ms: int,
    ) -> None:
        self._processor = processor
        self._publish_retry = publish_retry
        self._max_attempts = max_attempts
        self._base_delay_ms = base_delay_ms

    async def __call__(self, body: Any, message: RabbitMessage) -> None:
        attempt = current_attempt(message.headers)
        try:
            event = PaymentCreated.model_validate(body)
            await self._processor.process(event.payment_id)
        except Exception as exc:
            decision = decide(
                attempt,
                max_attempts=self._max_attempts,
                base_delay_ms=self._base_delay_ms,
                retryable=not isinstance(exc, NON_RETRYABLE),
            )
            await self._on_failure(body, message, attempt, exc, decision)
            return
        await message.ack()

    async def _on_failure(
        self,
        body: Any,
        message: RabbitMessage,
        attempt: int,
        exc: Exception,
        decision: Retry | DeadLetter,
    ) -> None:
        if isinstance(decision, Retry):
            logger.warning(
                "attempt %s/%s failed (%s), retry in %s ms",
                attempt, self._max_attempts, exc, decision.delay_ms,
            )
            headers = {**(message.headers or {}), ATTEMPT_HEADER: decision.next_attempt}
            await self._publish_retry(body, decision.routing_key, headers)
            await message.ack()
        else:
            logger.error("attempt %s failed (%s): %s, sending to DLQ", attempt, exc, decision.reason)
            await message.reject(requeue=False)
