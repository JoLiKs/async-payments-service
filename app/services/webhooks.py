from typing import Any, Protocol

import httpx


class WebhookDeliveryError(Exception):
    pass


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict[str, Any]) -> None: ...


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        try:
            response = await self._client.post(url, json=payload)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if response.status_code >= 300:
            raise WebhookDeliveryError(f"webhook responded with HTTP {response.status_code}")
