"""
DCAT/RDF 엔드포인트 커넥터.
JSON-LD 또는 Turtle/RDF 형식의 DCAT 카탈로그를 파싱한다.
검색은 URL 파라미터 기반 또는 전체 파싱 후 인메모리 필터링으로 처리한다.
"""

from __future__ import annotations

import httpx

from app.connectors.base import BaseConnector, RawSearchResult


class DCATRDFConnector(BaseConnector):
    """DCAT JSON-LD / RDF 엔드포인트 커넥터"""

    async def search(self, query: str, offset: int = 0, limit: int = 10) -> RawSearchResult:
        """
        DCAT 엔드포인트에 검색 요청을 보낸다.
        엔드포인트가 q 파라미터를 지원하면 서버사이드 검색,
        그렇지 않으면 전체 카탈로그를 가져와 인메모리 필터링한다.
        """
        headers = {"Accept": "application/ld+json, application/json;q=0.9"}
        params: dict = {}

        # 많은 DCAT API가 q, query, keyword 파라미터를 지원함
        if query:
            params["q"] = query
        params["offset"] = offset
        params["limit"] = limit

        try:
            async with httpx.AsyncClient(timeout=self.config.timeout) as client:
                response = await client.get(self.config.base_url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

            items, total = self._extract_datasets(data, query, offset, limit)

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

    def _extract_datasets(
        self, data: dict | list, query: str, offset: int, limit: int
    ) -> tuple[list[dict], int]:
        """JSON-LD / 일반 JSON에서 데이터셋 목록을 추출한다."""
        datasets: list[dict] = []

        # 유럽 데이터 포털 스타일: {"result": {"results": [...]}}
        if isinstance(data, dict):
            for key in ("result", "results", "datasets", "items", "data", "records"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    datasets = candidate
                    break
                if isinstance(candidate, dict):
                    for inner in ("results", "datasets", "items"):
                        if isinstance(candidate.get(inner), list):
                            datasets = candidate[inner]
                            break
                    if datasets:
                        break

            # JSON-LD 스타일: {"@graph": [...]}
            if not datasets and "@graph" in data:
                datasets = [
                    item
                    for item in data["@graph"]
                    if item.get("@type") in ("dcat:Dataset", "Dataset")
                ]

            # total 추출
            total = (
                data.get("total")
                or data.get("count")
                or data.get("totalCount")
                or (data.get("result") or {}).get("count")
                or len(datasets)
            )
        elif isinstance(data, list):
            datasets = data
            total = len(data)
        else:
            return [], 0

        # 서버사이드 검색이 없을 경우 인메모리 필터링
        if query and datasets:
            q_lower = query.lower()
            filtered = [
                d
                for d in datasets
                if q_lower in str(d.get("title", "")).lower()
                or q_lower in str(d.get("description", "")).lower()
                or q_lower in str(d.get("dct:title", "")).lower()
                or q_lower in str(d.get("dct:description", "")).lower()
            ]
            # 필터링 결과가 있으면 사용, 없으면 전체 반환 (서버사이드 처리 가정)
            if filtered:
                datasets = filtered
                total = len(filtered)

        return datasets[offset : offset + limit], int(total)

    async def get_dataset(self, dataset_id: str) -> dict:
        url = f"{self.config.base_url.rstrip('/')}/{dataset_id}"
        headers = {"Accept": "application/ld+json, application/json;q=0.9"}
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()

    async def health_check(self) -> bool:
        try:
            headers = {"Accept": "application/ld+json, application/json;q=0.9"}
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(self.config.base_url, headers=headers)
                return response.status_code < 500
        except Exception:  # noqa: BLE001
            return False
