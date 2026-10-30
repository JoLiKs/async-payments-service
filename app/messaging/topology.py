from aio_pika.abc import AbstractRobustQueue
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DLX_EXCHANGE_NAME = "payments.dlx"

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"
DLQ_NAME = "payments.new.dlq"

ATTEMPT_HEADER = "x-attempt"

payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DLQ_NAME,
    },
)

dlq = RabbitQueue(DLQ_NAME, durable=True, routing_key=DLQ_NAME)


def retry_delay_ms(failed_attempt: int, base_delay_ms: int) -> int:
    return base_delay_ms * 2 ** (failed_attempt - 1)


def retry_queue_name(delay_ms: int) -> str:
    return f"{PAYMENTS_NEW_QUEUE_NAME}.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = retry_queue_name(delay_ms)
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def _declare_bound(broker: RabbitBroker, queue: RabbitQueue, exchange: RabbitExchange) -> None:
    declared: AbstractRobustQueue = await broker.declare_queue(queue)
    await declared.bind(exchange.name, routing_key=queue.routing())


async def declare_topology(broker: RabbitBroker, *, max_attempts: int, base_delay_ms: int) -> None:
    for exchange in (payments_exchange, retry_exchange, dlx_exchange):
        await broker.declare_exchange(exchange)
    await _declare_bound(broker, payments_new_queue, payments_exchange)
    await _declare_bound(broker, dlq, dlx_exchange)
    for attempt in range(1, max_attempts):
        await _declare_bound(broker, retry_queue(retry_delay_ms(attempt, base_delay_ms)), retry_exchange)
