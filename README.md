# Async Payments Service

Микросервис асинхронной обработки платежей: принимает платёж по HTTP, гарантированно
публикует событие в RabbitMQ через Outbox, обрабатывает его в consumer'е (эмуляция
платёжного шлюза) и уведомляет клиента webhook'ом с повторными попытками и DLQ.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ
(FastStream), Alembic, Docker / docker-compose, pytest.

## Архитектура

```
 client ──HTTP──▶ api (FastAPI)
                    │  одна транзакция:
                    │  INSERT payments + INSERT outbox
                    ▼
               PostgreSQL ◀──────────────────────────────┐
                    ▲                                    │ статус, processed_at,
                    │ SELECT ... FOR UPDATE SKIP LOCKED  │ webhook_delivered_at
                    │                                    │
 consumer ┌─────────┴──────────┐   publish (confirm)   ┌─┴───────────────────────┐
 process  │  outbox relay      │ ────────────────────▶ │ handler payments.new    │──▶ webhook_url
          │  (фоновая задача)  │   exchange payments   │ (FastStream subscriber) │
          └────────────────────┘                       └─────────────────────────┘
```

Топология RabbitMQ (объявляется consumer'ом при старте, идемпотентно):

```
payments (direct) ──payments.new──▶ [payments.new] ──reject──▶ payments.dlx ──▶ [payments.new.dlq]
      ▲                                   │ ошибка, attempt < 3
      │ TTL истёк (DLX)                   ▼
      └──── [payments.new.retry.1000ms] ◀─┤  payments.retry (direct)
      └──── [payments.new.retry.2000ms] ◀─┘
```

* `payments.new` — основная очередь, `x-dead-letter-exchange=payments.dlx`.
* `payments.new.retry.<N>ms` — очереди задержки без consumer'ов, с `x-message-ttl=N` и
  DLX обратно в `payments`/`payments.new`. Задержка растёт экспоненциально: 1 с, 2 с, ...
* `payments.new.dlq` — сообщения, не обработанные за 3 попытки.

### Структура кода

```
app/
  api/          HTTP-слой: роуты, схемы Pydantic, зависимости (API key)
  services/     бизнес-логика: создание платежа (идемпотентность + outbox),
                обработка платежа, эмулятор шлюза, отправка webhook
  messaging/    топология RabbitMQ, политика retry, обработчик сообщений, outbox relay
  db/           ORM-модели, репозитории, фабрика сессий
  domain/       enum'ы и доменные ошибки
  core/         настройки (pydantic-settings, из env)
  main.py       FastAPI-приложение
  worker.py     FastStream-приложение (consumer + relay)
migrations/     Alembic (таблицы payments и outbox)
tests/          pytest
```

Сервисы не зависят от FastAPI/FastStream: обработчик и relay получают зависимости
(фабрику сессий, шлюз, отправителя webhook, функцию публикации) через конструктор,
поэтому логика тестируется без брокера.

## Запуск

```bash
cp .env.example .env            # при необходимости поменяйте API_KEY
docker compose up --build
```

Поднимаются: `postgres`, `rabbitmq` (UI: http://localhost:15672, guest/guest),
`migrate` (одноразово выполняет `alembic upgrade head`), `api` (http://localhost:8000,
Swagger: `/docs`), `consumer` и демонстрационный `webhook-receiver`
(`POST /ok` → 200, `POST /fail` → 500).

Без Docker:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/payments
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/ API_KEY=change-me
alembic upgrade head
uvicorn app.main:app --port 8000          # API
faststream run app.worker:app             # consumer + outbox relay
python scripts/webhook_receiver.py        # тестовый приёмник webhook на :9000
```

### Настройки (env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | `change-me` | статический ключ для `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://...` | строка подключения к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | брокер |
| `PROCESSING_MIN_SECONDS` / `PROCESSING_MAX_SECONDS` | `2` / `5` | время эмуляции обработки |
| `SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `MAX_ATTEMPTS` | `3` | число попыток до DLQ |
| `RETRY_BASE_DELAY_MS` | `1000` | базовая задержка retry (дальше ×2) |
| `WEBHOOK_TIMEOUT_SECONDS` | `5` | таймаут HTTP-запроса webhook |
| `RUN_OUTBOX_RELAY` | `true` | запускать relay внутри consumer'а |

## Примеры

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:9000/ok"
      }'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"b7107a04-e8e4-428f-b8ca-e19d5f3e49bb","status":"pending","created_at":"2026-10-30T11:47:54.487386Z"}
```

Повтор того же запроса с тем же ключом вернёт тот же платёж (`202`, заголовок
`Idempotent-Replayed: true`). Тот же ключ с другим телом → `409 Conflict`.

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/b7107a04-e8e4-428f-b8ca-e19d5f3e49bb \
  -H "X-API-Key: change-me"
```

```json
{"id":"b7107a04-e8e4-428f-b8ca-e19d5f3e49bb","amount":"1500.00","currency":"RUB",
 "description":"Заказ #42","metadata":{"order_id":42},"status":"succeeded",
 "idempotancy_key":"order-42","webhook_url":"http://webhook-receiver:9000/ok",
 "created_at":"2026-10-30T11:47:54.487386Z","processed_at":"2026-10-30T11:48:06.384193Z"}
```

Webhook (POST на `webhook_url`):

```json
{"payment_id":"b7107a04-...","status":"succeeded","amount":"1500.00","currency":"RUB",
 "metadata":{"order_id":42},"processed_at":"2026-10-30T11:48:06.384193+00:00"}
```

Проверка DLQ: создайте платёж с `"webhook_url": "http://webhook-receiver:9000/fail"`.
В логах consumer'а будут попытки 1/3 и 2/3 (задержки 1 с и 2 с), после третьей
сообщение уйдёт в `payments.new.dlq` (видно в RabbitMQ UI). Статус платежа при этом
сохраняется — webhook не откатывает результат обработки.

Коды ошибок: `401` — нет/неверный `X-API-Key`; `422` — невалидное тело или нет
`Idempotency-Key`; `404` — платёж не найден; `409` — конфликт ключа идемпотентности.

## Проектные решения

### Outbox

`POST /payments` в **одной транзакции** пишет строку в `payments` и событие в `outbox`.
Брокер в HTTP-запросе не участвует: если RabbitMQ недоступен, платёж всё равно
принимается, а событие будет опубликовано позже. Нет ситуации «платёж есть, события нет»
или наоборот.

Relay в цикле берёт пачку неопубликованных строк `SELECT ... FOR UPDATE SKIP LOCKED`
(можно запускать несколько экземпляров), публикует каждую с publisher confirms и
выставляет `published_at` в той же транзакции. При ошибке публикации увеличивается
`attempts`, пишется `last_error`, пачка прерывается до следующего тика. Гарантия —
at-least-once: при падении между confirm и commit событие уйдёт повторно, поэтому
consumer идемпотентен. `outbox.id` передаётся как `message_id`.

**Где живёт relay.** Relay запущен фоновой задачей внутри процесса consumer'а
(`RUN_OUTBOX_RELAY=true`). Причины: ТЗ требует один consumer и compose с четырьмя
сервисами; relay — лёгкий цикл опроса, ему не нужен отдельный контейнер; он использует
то же подключение к RabbitMQ и тот же engine. Благодаря `SKIP LOCKED` при
масштабировании consumer'ов relay'и не мешают друг другу. Если понадобится масштабировать
их отдельно, `OutboxRelay` не зависит от consumer'а и выносится в свой сервис без
изменений (флаг `RUN_OUTBOX_RELAY=false` в consumer'е).

### Идемпотентность

* **API.** `idempotancy_key` — уникальный индекс. Вместе с платежом хранится
  `request_hash` — SHA-256 от нормализованного тела (сумма приводится к каноническому
  виду, JSON сериализуется с сортировкой ключей). Повтор с тем же телом возвращает
  существующий платёж, с другим — `409`. Гонка параллельных запросов с одним ключом
  решается уникальным индексом: проигравший ловит `IntegrityError` и возвращает
  платёж победителя.
* **Consumer.** Обработка повторно безопасна:
  платёж блокируется `SELECT ... FOR UPDATE`; шлюз вызывается только если статус
  `pending`; результат коммитится **до** отправки webhook; после успешной доставки
  выставляется `webhook_delivered_at`, и повторная доставка сообщения — no-op.
  Retry после ошибки webhook не списывает деньги повторно, а только переотправляет
  уведомление.

### Retry и DLQ

* Номер попытки хранится в заголовке `x-attempt`.
* При ошибке, если попытка < `MAX_ATTEMPTS`, сообщение публикуется в exchange
  `payments.retry` с ключом очереди задержки (`payments.new.retry.1000ms`, затем
  `...2000ms`), после чего исходное сообщение ack'ается. По истечении TTL RabbitMQ
  через DLX возвращает его в `payments.new`. Плагин delayed-message не нужен,
  основная очередь не блокируется ожиданием.
* На последней попытке сообщение отклоняется `reject(requeue=False)` и через
  `x-dead-letter-exchange` попадает в `payments.new.dlq` (с заголовком `x-death`).
* Невосстановимые ошибки (битое тело, неизвестный `payment_id`) уходят в DLQ сразу.
* Ошибка 90/10 из эмулятора — это не исключение, а бизнес-результат (`failed`):
  клиент получает webhook со статусом `failed`. Retry касается технических сбоев —
  недоступный webhook, ошибки БД и т.п.

## Тесты

Тесты, работающие с БД, используют реальный PostgreSQL (нужны `FOR UPDATE SKIP LOCKED`,
JSONB, уникальные индексы). В compose создаётся база `payments_test`.

```bash
docker compose up -d postgres
pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/payments_test pytest
```

* `test_idempotancy.py` — повтор ключа, конфликт тела, гонка 10 параллельных запросов,
  HTTP-уровень (202/409/401/422/404).
* `test_outbox.py` — атомарная запись платежа и события, публикация relay'ем без
  дублей, поведение при недоступном брокере.
* `test_processing.py` — смена статуса и webhook, повторная доставка — no-op, retry
  webhook без повторного списания.
* `test_consumer.py`, `test_retry_policy.py` — экспоненциальная задержка, цепочка из
  3 попыток → DLQ, невосстановимые ошибки сразу в DLQ.
