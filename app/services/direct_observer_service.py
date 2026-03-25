from __future__ import annotations

from typing import Any

import httpx


class DirectObserverClient:
    def __init__(self, *, base_url: str, secret: str = "", timeout_sec: float = 3.0) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.secret = secret or ""
        self.timeout_sec = max(0.5, float(timeout_sec))
        self._client: httpx.AsyncClient | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def list_rooms(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        client = self._ensure_client()
        response = await client.get("/api/v1/rooms", headers=self._headers())
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items")
        return items if isinstance(items, list) else []

    async def get_room(self, match_id: str) -> dict[str, Any] | None:
        if not self.enabled or not match_id:
            return None
        client = self._ensure_client()
        response = await client.get(f"/api/v1/rooms/{match_id}", headers=self._headers())
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None

    async def get_snapshot(self, match_id: str) -> dict[str, Any] | None:
        if not self.enabled or not match_id:
            return None
        client = self._ensure_client()
        response = await client.get(f"/api/v1/rooms/{match_id}/snapshot", headers=self._headers())
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout_sec)
        return self._client

    def _headers(self) -> dict[str, str]:
        if not self.secret:
            return {}
        return {"X-RRR-Service-Token": self.secret}
