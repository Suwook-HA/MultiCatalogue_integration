"""
W3C DCAT v2 기반 내부 통합 메타데이터 모델.
모든 외부 포털의 메타데이터는 이 모델로 정규화된다.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DCATDistribution(BaseModel):
    """dcat:Distribution - 데이터셋의 배포 정보"""

    access_url: str | None = None
    download_url: str | None = None
    format: str | None = None
    media_type: str | None = None
    byte_size: int | None = None
    title: str | None = None
    description: str | None = None


class DCATPublisher(BaseModel):
    """foaf:Agent - 데이터 제공 기관"""

    name: str | None = None
    email: str | None = None
    url: str | None = None
    identifier: str | None = None


class DCATSpatial(BaseModel):
    """dct:Location - 공간 범위"""

    uri: str | None = None
    label: str | None = None
    bbox: str | None = None  # WKT format
    geometry: str | None = None


class DCATTemporal(BaseModel):
    """dct:PeriodOfTime - 시간 범위"""

    start: datetime | str | None = None
    end: datetime | str | None = None


class MappingSuggestion(BaseModel):
    """1:1 매핑이 불가한 필드에 대한 DCAT 필드 추천"""

    source_field: str
    source_value: Any
    suggested_dcat: str
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str


class DCATDataset(BaseModel):
    """
    dcat:Dataset - 통합 데이터셋 메타데이터.

    표준 DCAT 필드 외에:
    - extras: 1:1 매핑되지 않은 원본 필드를 손실 없이 보존
    - mapping_suggestions: confidence 미달 필드의 추천 매핑 목록
    - source_portal: 원천 포털 식별자
    """

    # --- 식별 ---
    identifier: str | None = None
    title: str
    description: str | None = None
    landing_page: str | None = None

    # --- 출처 ---
    source_portal: str = ""
    source_portal_name: str = ""
    source_id: str | None = None  # 원본 포털에서의 ID

    # --- 분류 ---
    theme: list[str] = Field(default_factory=list)
    keyword: list[str] = Field(default_factory=list)

    # --- 시간 ---
    issued: datetime | str | None = None
    modified: datetime | str | None = None
    accrual_periodicity: str | None = None  # dct:accrualPeriodicity

    # --- 공간 ---
    spatial: DCATSpatial | None = None
    temporal: DCATTemporal | None = None

    # --- 관계 ---
    publisher: DCATPublisher | None = None
    creator: str | None = None
    contact_point: str | None = None
    language: list[str] = Field(default_factory=list)
    license: str | None = None
    rights: str | None = None

    # --- 배포 ---
    distribution: list[DCATDistribution] = Field(default_factory=list)

    # --- 매핑 확장 ---
    extras: dict[str, Any] = Field(
        default_factory=dict,
        description="1:1 매핑 불가 원본 필드 보존 (손실 방지)",
    )
    mapping_suggestions: list[MappingSuggestion] = Field(
        default_factory=list,
        description="confidence 미달 필드의 DCAT 매핑 후보",
    )
    # --- 중복 탐지 ---
    also_available_at: list[dict[str, str]] = Field(
        default_factory=list,
        description="동일 데이터셋이 다른 포털에도 존재하는 경우 참조 목록 [{portal_id, portal_name, source_id}]",
    )


class PortalSearchResult(BaseModel):
    """단일 포털의 검색 결과"""

    portal_id: str
    portal_name: str
    datasets: list[DCATDataset]
    total: int
    error: str | None = None  # 포털 장애 시 에러 메시지


class FacetValue(BaseModel):
    """패싯 집계 단일 항목"""

    value: str
    count: int


class SearchFacets(BaseModel):
    """검색 결과 패싯 집계"""

    formats: list[FacetValue] = Field(default_factory=list)
    themes: list[FacetValue] = Field(default_factory=list)
    publishers: list[FacetValue] = Field(default_factory=list)
    licenses: list[FacetValue] = Field(default_factory=list)
    portals: list[FacetValue] = Field(default_factory=list)


class SearchResult(BaseModel):
    """통합 검색 결과 (복수 포털 병합)"""

    query: str
    total: int
    page: int
    size: int
    datasets: list[DCATDataset]
    portals_searched: list[str]
    portals_failed: list[str] = Field(default_factory=list)
    facets: SearchFacets = Field(default_factory=SearchFacets)
    cached: bool = False
