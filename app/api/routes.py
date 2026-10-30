import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.dependencies import get_payment_service, require_api_key
from app.api.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead
from app.db.models import Payment
from app.domain.errors import IdempotencyConflictError, PaymentNotFoundError
from app.services.payments import CreatePaymentCommand, PaymentService

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)])

Service = Annotated[PaymentService, Depends(get_payment_service)]


def _to_read(payment: Payment) -> PaymentRead:
    return PaymentRead(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"model": ErrorResponse}, 401: {"model": ErrorResponse}},
)
async def create_payment(
    body: PaymentCreate,
    response: Response,
    service: Service,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    cmd = CreatePaymentCommand(
        idempotency_key=idempotency_key,
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
    )
    try:
        result = await service.create(cmd)
    except IdempotencyConflictError:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    p = result.payment
    return PaymentAccepted(payment_id=p.id, status=p.status, created_at=p.created_at)


@router.get("/{payment_id}", response_model=PaymentRead, responses={404: {"model": ErrorResponse}})
async def get_payment(payment_id: uuid.UUID, service: Service) -> PaymentRead:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found") from None
    return _to_read(payment)
