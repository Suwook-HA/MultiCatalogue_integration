"""
공공데이터포털 (data.go.kr) 커넥터.
오픈 API를 통해 데이터셋 목록을 검색한다.
"""

from __future__ import annotations

import httpx

from app.connectors.base import BaseConnector, PortalConfig, RawSearchResult

# data.go.kr 오픈 API 검색 엔드포인트
_SEARCH_URL = "https://www.data.go.kr/tcs/dss/selectDataSetList.do"
_DETAIL_URL = "https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do"


class DataGoKrConnector(BaseConnector):
    """공공데이터포털 API 커넥터"""

    async def search(self, query: str, offset: int = 0, limit: int = 10) -> RawSearchResult:
        params = {
            "serviceKey": self.config.api_key,
            "keyword": query,
            "pageNo": (offset // limit) + 1,
            "numOfRows": limit,
            "resultType": "json",
            "type": "FILE",  # 파일 데이터셋 (API/FILE 모두 검색 가능)
        }
        try:
            async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                response = await client.get(_SEARCH_URL, params=params)
                response.raise_for_status()
                data = response.json()

            body = data.get("data", {})
            items = body.get("list", []) or []
            total = body.get("totalCount", 0)

            # API 키 없이도 기본 테스트 가능하도록 에러 처리
            if "error" in data or data.get("resultCode") not in (None, "00", "200"):
                msg = data.get("resultMsg", "알 수 없는 오류")
                return RawSearchResult(
                    portal_id=self.config.id,
                    portal_name=self.config.name,
                    raw_items=[],
                    total=0,
                    error=f"data.go.kr API 오류: {msg}",
                )

            return RawSearchResult(
                portal_id=self.config.id,
                portal_name=self.config.name,
                raw_items=items,
                total=total,
            )

        except httpx.HTTPStatusError as exc:
            return RawSearchResult(
                portal_id=self.config.id,
                portal_name=self.config.name,
                raw_items=[],
                total=0,
                error=f"HTTP {exc.response.status_code}: {exc.response.text[:200]}",
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
        params = {
            "serviceKey": self.config.api_key,
            "publicDataPk": dataset_id,
            "resultType": "json",
        }
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(_DETAIL_URL, params=params)
            response.raise_for_status()
            return response.json()

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(
                    _SEARCH_URL,
                    params={"serviceKey": self.config.api_key, "numOfRows": 1, "resultType": "json"},
                )
                return response.status_code < 500
        except Exception:  # noqa: BLE001
            return False
