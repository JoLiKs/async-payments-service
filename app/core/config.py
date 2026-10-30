from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "change-me"

    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    success_rate: float = 0.9

    max_attempts: int = 3
    retry_base_delay_ms: int = 1000

    webhook_timeout_seconds: float = 5.0

    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = 100
    run_outbox_relay: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
