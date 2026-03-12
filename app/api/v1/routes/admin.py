"""
관리자 API: 매핑 제안 조회.
FieldMapper가 confidence 미달로 extras에 보존한 필드의 매핑 후보를 확인한다.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from app.broker.search_broker import get_broker
from app.models.dcat import MappingSuggestion, SearchResult

router = APIRouter()


class MappingSuggestionSummary(MappingSuggestion):
    portal_id: str
    dataset_title: str


@router.get(
    "/admin/mapping-suggestions",
    summary="미매핑 필드 제안 목록 조회",
    description=(
        "최근 검색 결과에서 confidence 미달로 extras에 보존된 필드들의 "
        "DCAT 매핑 후보 목록을 반환합니다. 검토 후 portals.yaml의 known_mappings에 추가하세요."
    ),
)
async def get_mapping_suggestions(
    q: str = Query(..., description="조회 기준 검색 키워드"),
    portals: str | None = Query(None, description="대상 포털 (쉼표 구분)"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0, description="최소 confidence 필터"),
) -> list[MappingSuggestionSummary]:
    broker = get_broker()
    portal_ids = [p.strip() for p in portals.split(",")] if portals else None

    result: SearchResult = await broker.search(query=q, portal_ids=portal_ids, page=1, size=50)

    suggestions: list[MappingSuggestionSummary] = []
    for ds in result.datasets:
        for sug in ds.mapping_suggestions:
            if sug.confidence >= min_confidence:
                suggestions.append(
                    MappingSuggestionSummary(
                        source_field=sug.source_field,
                        source_value=sug.source_value,
                        suggested_dcat=sug.suggested_dcat,
                        confidence=sug.confidence,
                        reason=sug.reason,
                        portal_id=ds.source_portal,
                        dataset_title=ds.title,
                    )
                )

    # confidence 내림차순 정렬
    return sorted(suggestions, key=lambda s: s.confidence, reverse=True)
