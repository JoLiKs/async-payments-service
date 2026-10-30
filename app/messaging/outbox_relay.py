import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.repositories import OutboxRepository

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def __call__(self, payload: dict[str, Any], routing_key: str, message_id: str) -> None: ...


class OutboxRelay:

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publish: EventPublisher,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publish
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def run_once(self) -> int:
        published = 0
        async with self._session_factory() as session, session.begin():
            repo = OutboxRepository(session)
            for event in await repo.lock_unpublished(self._batch_size):
                try:
                    await self._publish(event.payload, event.routing_key, str(event.id))
                except Exception as exc:
                    logger.warning("failed to publish outbox event %s: %s", event.id, exc)
                    event.attempts += 1
                    event.last_error = str(exc)[:1000]
                    break
                event.attempts += 1
                event.published_at = datetime.now(UTC)
                published += 1
        return published

    async def run_forever(self) -> None:
        logger.info("outbox relay started")
        while True:
            try:
                published = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox relay iteration failed")
                published = 0
            if published < self._batch_size:
                await asyncio.sleep(self._poll_interval)
