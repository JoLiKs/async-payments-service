import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import Settings, get_settings
from app.services.payments import PaymentService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    if api_key is None or not secrets.compare_digest(api_key, settings.api_key):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


def get_payment_service(request: Request) -> PaymentService:
    return request.app.state.payment_service
