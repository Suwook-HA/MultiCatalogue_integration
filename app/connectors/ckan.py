"""
CKAN 호환 포털 커넥터.
CKAN Action API v3을 사용한다.
"""

from __future__ import annotations

import httpx

from app.connectors.base import BaseConnector, RawSearchResult


class CKANConnector(BaseConnector):
    """CKAN Action API v3 커넥터"""

    @property
    def _search_url(self) -> str:
        return f"{self.config.base_url.rstrip('/')}/api/3/action/package_search"

    @property
    def _show_url(self) -> str:
        return f"{self.config.base_url.rstrip('/')}/api/3/action/package_show"

    @property
    def _status_url(self) -> str:
        return f"{self.config.base_url.rstrip('/')}/api/3/action/site_read"

    def _headers(self) -> dict:
        headers: dict = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = self.config.api_key
        return headers

    async def search(self, query: str, offset: int = 0, limit: int = 10) -> RawSearchResult:
        params = {"q": query, "start": offset, "rows": limit}
        try:
            async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                response = await client.get(self._search_url, params=params, headers=self._headers())
                response.raise_for_status()
                data = response.json()

            if not data.get("success"):
                return RawSearchResult(
                    portal_id=self.config.id,
                    portal_name=self.config.name,
                    raw_items=[],
                    total=0,
                    error=data.get("error", {}).get("message", "CKAN API 오류"),
                )

            result = data["result"]
            return RawSearchResult(
                portal_id=self.config.id,
                portal_name=self.config.name,
                raw_items=result.get("results", []),
                total=result.get("count", 0),
            )

        except httpx.HTTPStatusError as exc:
            return RawSearchResult(
                portal_id=self.config.id,
                portal_name=self.config.name,
                raw_items=[],
                total=0,
                error=f"HTTP {exc.response.status_code}",
            )
        except Exception as exc:  # noqa: BLE001
            return RawSearchResult(
                portal_id=self.config.id,
                portal_name=self.config.name,
                raw_items=[],
                total=0,
                error=str(exc),
            )

    async def get_dataset(self, dataset_id: str) -> dict:
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(
                self._show_url,
                params={"id": dataset_id},
                headers=self._headers(),
            )
            response.raise_for_status()
            data = response.json()
            return data.get("result", {})

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(self._status_url, headers=self._headers())
                return response.status_code == 200 and response.json().get("success", False)
        except Exception:  # noqa: BLE001
            return False
