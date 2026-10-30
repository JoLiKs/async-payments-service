from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from app.db.session import make_engine, make_session_factory
from app.services.payments import PaymentService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = make_engine(get_settings().database_url)
    app.state.payment_service = PaymentService(make_session_factory(engine))
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Async Payments Service", version="1.0.0", lifespan=lifespan)
    app.include_router(router)

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
