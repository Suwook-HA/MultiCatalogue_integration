"""통합 검색 API 엔드포인트"""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.broker.search_broker import get_broker
from app.models.dcat import SearchResult

router = APIRouter()


@router.get(
    "/search",
    response_model=SearchResult,
    summary="통합 메타데이터 검색",
    description=(
        "복수의 데이터 포털에 병렬로 검색을 수행하고 DCAT 표준으로 정규화된 결과를 반환합니다. "
        "포털 장애 시 해당 포털을 제외한 나머지 결과만 반환합니다(partial success). "
        "패싯 집계(형식·주제·제공기관·라이선스)와 교차 포털 중복 탐지를 지원합니다."
    ),
)
async def search_datasets(
    q: str = Query(..., min_length=1, description="검색 키워드"),
    portals: str | None = Query(
        None,
        description="검색 대상 포털 ID 목록 (쉼표 구분, 예: data_go_kr,sample_ckan). 미지정 시 활성 포털 전체",
    ),
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(10, ge=1, le=100, description="페이지당 결과 수"),
    # --- 필터 ---
    format: str | None = Query(
        None, description="배포 형식 필터 (예: CSV, JSON, XML)"
    ),
    theme: str | None = Query(
        None, description="주제 필터 (부분 일치, 예: transport)"
    ),
    publisher: str | None = Query(
        None, description="제공 기관 필터 (부분 일치)"
    ),
    license: str | None = Query(  # noqa: A002
        None, description="라이선스 필터 (부분 일치, 예: CC-BY)"
    ),
    modified_after: str | None = Query(
        None, description="최종 수정일 하한 (ISO 날짜, 예: 2023-01-01)"
    ),
    modified_before: str | None = Query(
        None, description="최종 수정일 상한 (ISO 날짜, 예: 2024-12-31)"
    ),
    dedup: bool = Query(
        True, description="교차 포털 중복 데이터셋 병합 여부"
    ),
) -> SearchResult:
    broker = get_broker()
    portal_ids = [p.strip() for p in portals.split(",")] if portals else None

    result = await broker.search(
        query=q,
        portal_ids=portal_ids,
        page=page,
        size=size,
        filter_format=format,
        filter_theme=theme,
        filter_publisher=publisher,
        filter_license=license,
        modified_after=modified_after,
        modified_before=modified_before,
        dedup=dedup,
    )

    response = JSONResponse(
        content=result.model_dump(mode="json"),
        headers={"X-Cache": "HIT" if result.cached else "MISS"},
    )
    return response  # type: ignore[return-value]
