"""
SearchBroker: 복수 포털에 병렬로 검색을 수행하고 결과를 병합한다.

특징:
- asyncio.gather()로 포털별 타임아웃 적용 병렬 요청
- 포털 장애 시 partial result 반환 (전체 실패 방지)
- 결과 병합 후 관련도 순 정렬
- Redis 하이브리드 캐시 적용
"""

from __future__ import annotations

import asyncio
import logging
import os
import re

import yaml

from app.broker.cache import cache_client
from app.broker.deduplicator import deduplicate
from app.connectors.base import BaseConnector, PortalConfig
from app.connectors.ckan import CKANConnector
from app.connectors.data_go_kr import DataGoKrConnector
from app.connectors.dcat_rdf import DCATRDFConnector
from app.models.dcat import (
    DCATDataset,
    FacetValue,
    PortalSearchResult,
    SearchFacets,
    SearchResult,
)
from app.normalizers.base import BaseNormalizer
from app.normalizers.ckan_normalizer import CKANNormalizer
from app.normalizers.data_go_kr_normalizer import DataGoKrNormalizer
from app.normalizers.dcat_normalizer import DCATRDFNormalizer

logger = logging.getLogger(__name__)

_CONNECTOR_MAP: dict[str, type[BaseConnector]] = {
    "data_go_kr": DataGoKrConnector,
    "ckan": CKANConnector,
    "dcat_rdf": DCATRDFConnector,
}

_NORMALIZER_MAP: dict[str, type[BaseNormalizer]] = {
    "data_go_kr": DataGoKrNormalizer,
    "ckan": CKANNormalizer,
    "dcat_rdf": DCATRDFNormalizer,
}

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_env(value: str) -> str:
    """YAML 값의 ${ENV_VAR} 를 환경변수로 치환한다."""
    return _ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), ""), value)


def _load_portals(config_path: str) -> list[PortalConfig]:
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    portals = []
    for item in data.get("portals", []):
        api_key = item.get("api_key", "")
        if isinstance(api_key, str):
            api_key = _resolve_env(api_key)
        portals.append(
            PortalConfig(
                id=item["id"],
                name=item["name"],
                type=item["type"],
                base_url=item["base_url"],
                enabled=item.get("enabled", True),
                timeout=item.get("timeout", 10),
                api_key=api_key,
                description=item.get("description", ""),
            )
        )
    return portals


class SearchBroker:
    """통합 검색 브로커"""

    def __init__(self, portals_yaml: str = "app/config/portals.yaml") -> None:
        self._portals = _load_portals(portals_yaml)
        self._connectors: dict[str, BaseConnector] = {}
        self._normalizers: dict[str, BaseNormalizer] = {}
        self._build_connectors()

    def _build_connectors(self) -> None:
        for portal in self._portals:
            if not portal.enabled:
                continue
            connector_cls = _CONNECTOR_MAP.get(portal.type)
            normalizer_cls = _NORMALIZER_MAP.get(portal.type)
            if connector_cls and normalizer_cls:
                self._connectors[portal.id] = connector_cls(portal)
                self._normalizers[portal.id] = normalizer_cls()
            else:
                logger.warning("지원하지 않는 포털 타입: %s (%s)", portal.type, portal.id)

    def get_portals(self) -> list[PortalConfig]:
        return self._portals

    def get_active_portal_ids(self) -> list[str]:
        return [p.id for p in self._portals if p.enabled]

    async def search(
        self,
        query: str,
        portal_ids: list[str] | None = None,
        page: int = 1,
        size: int = 10,
        # --- 필터 ---
        filter_format: str | None = None,
        filter_theme: str | None = None,
        filter_publisher: str | None = None,
        filter_license: str | None = None,
        modified_after: str | None = None,
        modified_before: str | None = None,
        dedup: bool = True,
    ) -> SearchResult:
        """통합 검색을 수행한다."""
        target_ids = portal_ids or self.get_active_portal_ids()
        target_ids = [pid for pid in target_ids if pid in self._connectors]
        offset = (page - 1) * size

        # 필터가 없을 때만 캐시 사용 (필터 조합은 캐시 키 폭발 방지)
        use_cache = not any(
            [filter_format, filter_theme, filter_publisher, filter_license,
             modified_after, modified_before]
        )

        if use_cache:
            cached = await cache_client.get(query, target_ids, page, size)
            if cached:
                result = SearchResult(**cached)
                result.cached = True
                return result

        # 병렬 검색 (포털별로 더 많이 가져와 필터링 여유분 확보)
        fetch_size = size * 3 if use_cache else size * 5
        tasks = {
            pid: self._search_portal(pid, query, offset, fetch_size)
            for pid in target_ids
        }
        portal_results: list[PortalSearchResult] = await asyncio.gather(*tasks.values())

        # 결과 병합
        all_datasets: list[DCATDataset] = []
        total_raw = 0
        portals_failed: list[str] = []

        for pr in portal_results:
            if pr.error:
                portals_failed.append(pr.portal_id)
                logger.warning("포털 '%s' 검색 실패: %s", pr.portal_id, pr.error)
            else:
                all_datasets.extend(pr.datasets)
                total_raw += pr.total

        # 중복 탐지 & 병합
        if dedup and len(target_ids) > 1:
            all_datasets = deduplicate(all_datasets)

        # 패싯 집계 (필터 적용 전 전체 결과 기준)
        facets = _build_facets(all_datasets)

        # 필터 적용
        all_datasets = _apply_filters(
            all_datasets,
            filter_format=filter_format,
            filter_theme=filter_theme,
            filter_publisher=filter_publisher,
            filter_license=filter_license,
            modified_after=modified_after,
            modified_before=modified_before,
        )

        # 관련도 정렬
        all_datasets = _sort_by_relevance(all_datasets, query)

        # 필터 후 총 건수
        total_filtered = len(all_datasets)
        # 페이지 슬라이싱 (포털에서 이미 offset 적용했으므로 0부터)
        paged = all_datasets[:size]

        search_result = SearchResult(
            query=query,
            total=total_filtered if any([filter_format, filter_theme, filter_publisher,
                                         filter_license, modified_after, modified_before])
                  else total_raw,
            page=page,
            size=size,
            datasets=paged,
            portals_searched=target_ids,
            portals_failed=portals_failed,
            facets=facets,
            cached=False,
        )

        if use_cache:
            await cache_client.set(
                query, target_ids, page, size, search_result.model_dump(mode="json")
            )

        return search_result

    async def get_dataset(self, portal_id: str, dataset_id: str) -> DCATDataset:
        """특정 포털에서 데이터셋 상세 정보를 가져와 DCAT으로 정규화한다."""
        connector = self._connectors.get(portal_id)
        normalizer = self._normalizers.get(portal_id)
        if not connector or not normalizer:
            raise ValueError(f"알 수 없는 포털: {portal_id}")
        portal = next(p for p in self._portals if p.id == portal_id)
        raw = await asyncio.wait_for(
            connector.get_dataset(dataset_id),
            timeout=connector.config.timeout,
        )
        return normalizer.normalize_dataset(raw, portal_id, portal.name)

    async def _search_portal(
        self, portal_id: str, query: str, offset: int, limit: int
    ) -> PortalSearchResult:
        connector = self._connectors[portal_id]
        normalizer = self._normalizers[portal_id]
        try:
            raw = await asyncio.wait_for(
                connector.search(query, offset, limit),
                timeout=connector.config.timeout,
            )
            return normalizer.normalize_search_result(raw)
        except asyncio.TimeoutError:
            portal = next(p for p in self._portals if p.id == portal_id)
            return PortalSearchResult(
                portal_id=portal_id,
                portal_name=portal.name,
                datasets=[],
                total=0,
                error=f"타임아웃 ({connector.config.timeout}초)",
            )
        except Exception as exc:  # noqa: BLE001
            portal = next(p for p in self._portals if p.id == portal_id)
            return PortalSearchResult(
                portal_id=portal_id,
                portal_name=portal.name,
                datasets=[],
                total=0,
                error=str(exc),
            )

    async def health_check(self) -> dict[str, bool]:
        results = {}
        for pid, connector in self._connectors.items():
            try:
                results[pid] = await asyncio.wait_for(connector.health_check(), timeout=5)
            except Exception:  # noqa: BLE001
                results[pid] = False
        return results


def _build_facets(datasets: list[DCATDataset]) -> SearchFacets:
    """전체 데이터셋에서 패싯별 값과 빈도를 집계한다."""
    from collections import Counter

    fmt_counter: Counter[str] = Counter()
    theme_counter: Counter[str] = Counter()
    pub_counter: Counter[str] = Counter()
    lic_counter: Counter[str] = Counter()
    portal_counter: Counter[str] = Counter()

    for ds in datasets:
        for dist in ds.distribution:
            if dist.format:
                fmt_counter[dist.format.upper()] += 1
        for th in ds.theme:
            if th:
                theme_counter[th] += 1
        if ds.publisher and ds.publisher.name:
            pub_counter[ds.publisher.name] += 1
        if ds.license:
            lic_counter[ds.license] += 1
        if ds.source_portal_name:
            portal_counter[ds.source_portal_name] += 1

    def to_facet(counter: Counter[str], limit: int = 20) -> list[FacetValue]:
        return [FacetValue(value=k, count=v) for k, v in counter.most_common(limit)]

    return SearchFacets(
        formats=to_facet(fmt_counter),
        themes=to_facet(theme_counter),
        publishers=to_facet(pub_counter),
        licenses=to_facet(lic_counter),
        portals=to_facet(portal_counter),
    )


def _apply_filters(
    datasets: list[DCATDataset],
    filter_format: str | None,
    filter_theme: str | None,
    filter_publisher: str | None,
    filter_license: str | None,
    modified_after: str | None,
    modified_before: str | None,
) -> list[DCATDataset]:
    """필터 파라미터에 따라 데이터셋을 걸러낸다."""
    result = datasets

    if filter_format:
        fmt_lower = filter_format.lower()
        result = [
            ds for ds in result
            if any(
                (dist.format or "").lower() == fmt_lower
                for dist in ds.distribution
            )
        ]

    if filter_theme:
        th_lower = filter_theme.lower()
        result = [
            ds for ds in result
            if any(th_lower in t.lower() for t in ds.theme)
        ]

    if filter_publisher:
        pub_lower = filter_publisher.lower()
        result = [
            ds for ds in result
            if ds.publisher and pub_lower in (ds.publisher.name or "").lower()
        ]

    if filter_license:
        lic_lower = filter_license.lower()
        result = [
            ds for ds in result
            if ds.license and lic_lower in ds.license.lower()
        ]

    if modified_after:
        result = [
            ds for ds in result
            if ds.modified and str(ds.modified) >= modified_after
        ]

    if modified_before:
        result = [
            ds for ds in result
            if ds.modified and str(ds.modified) <= modified_before
        ]

    return result


def _sort_by_relevance(datasets: list[DCATDataset], query: str) -> list[DCATDataset]:
    """쿼리 관련도 기준으로 데이터셋을 정렬한다 (간단한 휴리스틱)."""
    q = query.lower()

    def score(ds: DCATDataset) -> int:
        s = 0
        title = (ds.title or "").lower()
        desc = (ds.description or "").lower()
        if q in title:
            s += 10
        if q in desc:
            s += 5
        for kw in ds.keyword:
            if q in kw.lower():
                s += 3
        for th in ds.theme:
            if q in th.lower():
                s += 2
        return s

    return sorted(datasets, key=score, reverse=True)


# 싱글턴 인스턴스 (앱 시작 시 초기화)
_broker_instance: SearchBroker | None = None


def get_broker() -> SearchBroker:
    global _broker_instance  # noqa: PLW0603
    if _broker_instance is None:
        _broker_instance = SearchBroker()
    return _broker_instance
