"""추상 BaseNormalizer: 원본 데이터 → DCATDataset 변환 인터페이스"""

from __future__ import annotations

from abc import ABC, abstractmethod

from app.connectors.base import RawSearchResult
from app.models.dcat import DCATDataset, PortalSearchResult


class BaseNormalizer(ABC):
    """포털별 정규화기의 공통 인터페이스"""

    @abstractmethod
    def normalize_dataset(self, raw_item: dict, portal_id: str, portal_name: str) -> DCATDataset:
        """단일 원본 레코드를 DCATDataset으로 변환한다."""

    def normalize_search_result(self, raw_result: RawSearchResult) -> PortalSearchResult:
        """RawSearchResult 전체를 PortalSearchResult로 변환한다."""
        if raw_result.error:
            return PortalSearchResult(
                portal_id=raw_result.portal_id,
                portal_name=raw_result.portal_name,
                datasets=[],
                total=0,
                error=raw_result.error,
            )

        datasets = []
        for item in raw_result.raw_items:
            try:
                dataset = self.normalize_dataset(item, raw_result.portal_id, raw_result.portal_name)
                datasets.append(dataset)
            except Exception as exc:  # noqa: BLE001
                # 단일 레코드 변환 실패가 전체 결과를 망가트리지 않도록 처리
                datasets.append(
                    DCATDataset(
                        title="[정규화 실패]",
                        source_portal=raw_result.portal_id,
                        source_portal_name=raw_result.portal_name,
                        extras={"_normalization_error": str(exc), "_raw": item},
                    )
                )

        return PortalSearchResult(
            portal_id=raw_result.portal_id,
            portal_name=raw_result.portal_name,
            datasets=datasets,
            total=raw_result.total,
        )
