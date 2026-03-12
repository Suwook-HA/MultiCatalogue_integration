"""
지능형 필드 매핑 엔진.

3단계 파이프라인:
1. known_mappings에서 정확 매핑
2. rapidfuzz 유사도 + 값 패턴 분석으로 자동 추천
3. confidence 미달 시 extras 보존 + MappingSuggestion 기록
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz

from app.config.settings import settings
from app.models.dcat import MappingSuggestion

# DCAT v2의 표준 속성 목록 (매핑 후보)
DCAT_FIELDS = [
    "title",
    "description",
    "identifier",
    "issued",
    "modified",
    "theme",
    "keyword",
    "publisher",
    "creator",
    "contact_point",
    "landing_page",
    "language",
    "license",
    "rights",
    "spatial",
    "temporal",
    "accrual_periodicity",
    "distribution",
]

# 값 패턴 → DCAT 필드 힌트
_VALUE_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (
        re.compile(r"^\d{4}-\d{2}-\d{2}"),
        "issued",
        "날짜 형식(YYYY-MM-DD) 값으로 판단",
    ),
    (
        re.compile(r"^https?://"),
        "landing_page",
        "URL 형식 값으로 판단",
    ),
    (
        re.compile(r"^(매일|daily|weekly|monthly|매주|매월|연간|annual)", re.IGNORECASE),
        "accrual_periodicity",
        "주기성 표현 값으로 판단",
    ),
    (
        re.compile(r"^(ko|en|fr|de|ja|zh)", re.IGNORECASE),
        "language",
        "언어 코드 형식으로 판단",
    ),
]

# 필드명 → DCAT 매핑 힌트 (일반적 동의어)
_FIELD_NAME_HINTS: dict[str, str] = {
    "name": "title",
    "title": "title",
    "label": "title",
    "desc": "description",
    "summary": "description",
    "abstract": "description",
    "overview": "description",
    "date": "issued",
    "created": "issued",
    "published": "issued",
    "updated": "modified",
    "last_modified": "modified",
    "category": "theme",
    "tag": "keyword",
    "tags": "keyword",
    "keywords": "keyword",
    "org": "publisher",
    "organization": "publisher",
    "agency": "publisher",
    "author": "creator",
    "contact": "contact_point",
    "email": "contact_point",
    "url": "landing_page",
    "homepage": "landing_page",
    "lang": "language",
    "license_url": "license",
    "frequency": "accrual_periodicity",
    "update_cycle": "accrual_periodicity",
    "period": "temporal",
    "coverage": "spatial",
    "region": "spatial",
    "location": "spatial",
}


@dataclass
class MappingResult:
    """단일 필드 매핑 결과"""

    dcat_field: str | None  # None이면 extras에 보존
    value: Any
    confidence: float
    suggestion: MappingSuggestion | None = None
    auto_applied: bool = False  # threshold 이상이면 True


class FieldMapper:
    """
    포털 원본 필드를 DCAT 표준 필드로 매핑한다.

    Args:
        known_mappings: 포털별로 미리 확정된 정확 매핑 {source_field: dcat_field}
        threshold: 자동 매핑 적용 최소 confidence (0.0~1.0)
    """

    def __init__(
        self,
        known_mappings: dict[str, str] | None = None,
        threshold: float | None = None,
    ) -> None:
        self.known_mappings: dict[str, str] = known_mappings or {}
        self.threshold = threshold if threshold is not None else settings.field_mapping_threshold

    def map_field(self, source_field: str, value: Any) -> MappingResult:
        """단일 필드를 매핑한다."""
        normalized_key = source_field.lower().strip()

        # 1단계: 정확 매핑
        if normalized_key in self.known_mappings:
            dcat_field = self.known_mappings[normalized_key]
            return MappingResult(
                dcat_field=dcat_field,
                value=value,
                confidence=1.0,
                auto_applied=True,
            )

        # 2단계: 유사도 기반 추천
        suggestion = self._compute_best_match(normalized_key, value)

        if suggestion.confidence >= self.threshold:
            return MappingResult(
                dcat_field=suggestion.suggested_dcat,
                value=value,
                confidence=suggestion.confidence,
                suggestion=suggestion,
                auto_applied=True,
            )

        # 3단계: extras 보존
        return MappingResult(
            dcat_field=None,
            value=value,
            confidence=suggestion.confidence,
            suggestion=suggestion,
            auto_applied=False,
        )

    def _compute_best_match(self, source_field: str, value: Any) -> MappingSuggestion:
        """유사도 + 패턴 분석으로 최적 DCAT 필드를 추천한다."""
        best_field = "description"  # 기본 fallback
        best_score = 0.0
        best_reason = "기본 fallback"

        # 필드명 동의어 힌트 확인
        for hint_key, dcat_field in _FIELD_NAME_HINTS.items():
            score = fuzz.ratio(source_field, hint_key) / 100.0
            if score > best_score:
                best_score = score
                best_field = dcat_field
                best_reason = f"필드명 유사도 매칭 ('{hint_key}' 참조, score={score:.2f})"

        # DCAT 필드명과 직접 비교
        for dcat_field in DCAT_FIELDS:
            score = fuzz.ratio(source_field, dcat_field) / 100.0
            # 언더스코어 없애고 재비교
            score2 = fuzz.ratio(source_field.replace("_", ""), dcat_field.replace("_", "")) / 100.0
            max_score = max(score, score2)
            if max_score > best_score:
                best_score = max_score
                best_field = dcat_field
                best_reason = f"DCAT 필드명 유사도 매칭 (score={max_score:.2f})"

        # 값 패턴 분석으로 보정
        if isinstance(value, str) and value:
            for pattern, dcat_field, reason in _VALUE_PATTERNS:
                if pattern.match(value.strip()):
                    # 패턴 매칭은 강한 신호이므로 score 보정
                    pattern_score = max(best_score, 0.7)
                    if pattern_score >= best_score:
                        best_score = pattern_score
                        best_field = dcat_field
                        best_reason = reason
                    break

        return MappingSuggestion(
            source_field=source_field,
            source_value=value,
            suggested_dcat=best_field,
            confidence=round(min(best_score, 1.0), 4),
            reason=best_reason,
        )

    def map_extras(self, raw_dict: dict, exclude_keys: set[str]) -> tuple[dict, list[MappingSuggestion]]:
        """
        known_mappings에 없는 나머지 필드들을 처리한다.

        Returns:
            (extras_dict, suggestions_list)
            - extras_dict: 보존된 원본 필드
            - suggestions_list: confidence 미달 필드의 매핑 제안
        """
        extras: dict[str, Any] = {}
        suggestions: list[MappingSuggestion] = []

        for key, value in raw_dict.items():
            if key in exclude_keys or key.startswith("_"):
                continue

            result = self.map_field(key, value)

            if not result.auto_applied:
                # extras로 보존
                extras[key] = value
                if result.suggestion:
                    suggestions.append(result.suggestion)

        return extras, suggestions
