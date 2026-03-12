"""
교차 포털 중복 데이터셋 탐지 및 병합.

동일한 데이터셋이 여러 포털에 중복 등록된 경우를 탐지하여
대표 레코드 하나로 병합하고 나머지는 also_available_at으로 참조한다.

탐지 기준:
1. 정규화된 제목 완전 일치
2. difflib.SequenceMatcher 유사도 >= 임계값 (기본 0.85)
"""

from __future__ import annotations

import unicodedata
from difflib import SequenceMatcher

from app.models.dcat import DCATDataset

_DEFAULT_THRESHOLD = 0.85


def _normalize_title(title: str) -> str:
    """비교용 제목 정규화 (소문자, 공백 압축, 유니코드 정규화)."""
    t = unicodedata.normalize("NFKC", title).lower().strip()
    return " ".join(t.split())


def _title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _completeness_score(ds: DCATDataset) -> int:
    """채워진 핵심 필드 수로 레코드 완성도를 평가한다."""
    score = 0
    if ds.description:
        score += 3
    if ds.distribution:
        score += 2
    if ds.publisher:
        score += 2
    if ds.issued:
        score += 1
    if ds.modified:
        score += 1
    if ds.keyword:
        score += 1
    if ds.theme:
        score += 1
    if ds.license:
        score += 1
    if ds.landing_page:
        score += 1
    return score


def deduplicate(
    datasets: list[DCATDataset],
    threshold: float = _DEFAULT_THRESHOLD,
) -> list[DCATDataset]:
    """
    중복 데이터셋을 탐지하여 병합한다.

    - 완성도가 높은 레코드를 대표 레코드로 선택
    - 나머지 레코드는 대표 레코드의 also_available_at에 추가
    - 단일 포털 내 중복은 탐지하지 않음 (포털이 달라야 중복 처리)
    """
    if not datasets:
        return []

    normalized = [_normalize_title(ds.title) for ds in datasets]
    merged: list[bool] = [False] * len(datasets)  # 이미 병합된 레코드 표시
    result: list[DCATDataset] = []

    for i, ds_i in enumerate(datasets):
        if merged[i]:
            continue

        duplicates: list[int] = []  # ds_i와 중복인 인덱스들

        for j in range(i + 1, len(datasets)):
            if merged[j]:
                continue
            # 같은 포털이면 스킵
            if ds_i.source_portal == datasets[j].source_portal:
                continue
            sim = _title_similarity(normalized[i], normalized[j])
            if sim >= threshold:
                duplicates.append(j)

        if not duplicates:
            result.append(ds_i)
            merged[i] = True
            continue

        # 대표 레코드 선택: 완성도 최고 레코드
        group = [i] + duplicates
        best_idx = max(group, key=lambda idx: _completeness_score(datasets[idx]))
        representative = datasets[best_idx].model_copy(deep=True)

        also_at: list[dict[str, str]] = list(representative.also_available_at)

        for idx in group:
            if idx == best_idx:
                continue
            other = datasets[idx]
            merged[idx] = True
            ref: dict[str, str] = {
                "portal_id": other.source_portal,
                "portal_name": other.source_portal_name,
            }
            if other.source_id:
                ref["source_id"] = other.source_id
            if other.landing_page:
                ref["landing_page"] = other.landing_page
            also_at.append(ref)

        representative.also_available_at = also_at
        merged[best_idx] = True
        result.append(representative)

    # 중복 처리되지 않은 레코드는 그대로 추가
    for i, ds in enumerate(datasets):
        if not merged[i]:
            result.append(ds)

    return result
