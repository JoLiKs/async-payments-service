class DomainError(Exception):
    pass


class IdempotencyConflictError(DomainError):
    pass


class PaymentNotFoundError(DomainError):
    pass
