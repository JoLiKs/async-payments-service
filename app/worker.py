import asyncio
import contextlib
import logging
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.core.config import get_settings
from app.db.session import make_engine, make_session_factory
from app.messaging.consumer import PaymentMessageHandler
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.topology import (
    ATTEMPT_HEADER,
    declare_topology,
    payments_exchange,
    payments_new_queue,
    retry_exchange,
)
from app.services.gateway import EmulatedGateway
from app.services.processing import PaymentProcessor
from app.services.webhooks import HttpWebhookSender

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

settings = get_settings()
broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)

engine = make_engine(settings.database_url)
session_factory = make_session_factory(engine)
http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)

processor = PaymentProcessor(
    session_factory,
    EmulatedGateway(
        settings.processing_min_seconds, settings.processing_max_seconds, settings.success_rate
    ),
    HttpWebhookSender(http_client),
)


async def publish_retry(body: Any, routing_key: str, headers: dict[str, Any]) -> None:
    await broker.publish(body, exchange=retry_exchange, routing_key=routing_key, headers=headers, persist=True)


async def publish_event(payload: dict[str, Any], routing_key: str, message_id: str) -> None:
    await broker.publish(
        payload,
        exchange=payments_exchange,
        routing_key=routing_key,
        message_id=message_id,
        headers={ATTEMPT_HEADER: 1},
        persist=True,
    )


handler = PaymentMessageHandler(
    processor,
    publish_retry,
    max_attempts=settings.max_attempts,
    base_delay_ms=settings.retry_base_delay_ms,
)


@broker.subscriber(payments_new_queue, payments_exchange, ack_policy=AckPolicy.MANUAL)
async def on_payment_created(body: Any, message: RabbitMessage) -> None:
    await handler(body, message)


relay = OutboxRelay(
    session_factory,
    publish_event,
    batch_size=settings.outbox_batch_size,
    poll_interval=settings.outbox_poll_interval_seconds,
)
_relay_task: asyncio.Task[None] | None = None


@app.after_startup
async def start_relay() -> None:
    global _relay_task
    await declare_topology(
        broker, max_attempts=settings.max_attempts, base_delay_ms=settings.retry_base_delay_ms
    )
    if settings.run_outbox_relay:
        _relay_task = asyncio.create_task(relay.run_forever())


@app.on_shutdown
async def shutdown() -> None:
    if _relay_task is not None:
        _relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _relay_task
    await http_client.aclose()
    await engine.dispose()
