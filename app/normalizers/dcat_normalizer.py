"""
DCAT JSON-LD 응답 → 내부 DCATDataset 정규화기.
JSON-LD의 네임스페이스 접두어(dct:, dcat:, foaf:)를 처리한다.
"""

from __future__ import annotations

from app.models.dcat import DCATDataset, DCATDistribution, DCATPublisher, DCATSpatial, DCATTemporal
from app.normalizers.base import BaseNormalizer
from app.normalizers.field_mapper import FieldMapper

# DCAT/JSON-LD 필드 → 내부 DCAT 필드 확정 매핑
_KNOWN_MAPPINGS: dict[str, str] = {
    # Dublin Core Terms
    "dct:title": "title",
    "dct:description": "description",
    "dct:identifier": "identifier",
    "dct:issued": "issued",
    "dct:modified": "modified",
    "dct:publisher": "publisher",
    "dct:creator": "creator",
    "dct:language": "language",
    "dct:license": "license",
    "dct:rights": "rights",
    "dct:spatial": "spatial",
    "dct:temporal": "temporal",
    "dct:accrualPeriodicity": "accrual_periodicity",
    # DCAT
    "dcat:theme": "theme",
    "dcat:keyword": "keyword",
    "dcat:landingPage": "landing_page",
    "dcat:distribution": "distribution",
    "dcat:contactPoint": "contact_point",
    # 단축형
    "title": "title",
    "description": "description",
    "identifier": "identifier",
    "issued": "issued",
    "modified": "modified",
    "publisher": "publisher",
    "theme": "theme",
    "keyword": "keyword",
    "landingPage": "landing_page",
    "distribution": "distribution",
    "language": "language",
    "license": "license",
    "spatial": "spatial",
    "temporal": "temporal",
}

_HANDLED_KEYS = set(_KNOWN_MAPPINGS.keys()) | {"@type", "@id", "@context"}


def _get_str(item: dict, *keys: str) -> str | None:
    """여러 키 후보에서 첫 번째 문자열 값을 반환한다."""
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            return val.strip() or None
        if isinstance(val, dict):
            # {"@value": "...", "@language": "ko"} 처리
            v = val.get("@value") or val.get("value")
            if v:
                return str(v).strip()
        if isinstance(val, list) and val:
            first = val[0]
            if isinstance(first, str):
                return first.strip()
            if isinstance(first, dict):
                v = first.get("@value") or first.get("value")
                if v:
                    return str(v).strip()
    return None


def _get_list(item: dict, *keys: str) -> list[str]:
    """여러 키 후보에서 문자열 리스트를 반환한다."""
    for key in keys:
        val = item.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            return [val] if val else []
        if isinstance(val, list):
            result = []
            for v in val:
                if isinstance(v, str):
                    result.append(v)
                elif isinstance(v, dict):
                    s = v.get("@value") or v.get("value") or v.get("label") or v.get("prefLabel")
                    if s:
                        result.append(str(s))
            return result
    return []


class DCATRDFNormalizer(BaseNormalizer):
    def __init__(self) -> None:
        self._mapper = FieldMapper(known_mappings=_KNOWN_MAPPINGS)

    def normalize_dataset(self, raw_item: dict, portal_id: str, portal_name: str) -> DCATDataset:
        # 배포 정보
        distributions: list[DCATDistribution] = []
        dist_raw = raw_item.get("dcat:distribution") or raw_item.get("distribution") or []
        if isinstance(dist_raw, dict):
            dist_raw = [dist_raw]
        for dist in dist_raw:
            if isinstance(dist, dict):
                distributions.append(
                    DCATDistribution(
                        access_url=_get_str(dist, "dcat:accessURL", "accessURL", "@id"),
                        download_url=_get_str(dist, "dcat:downloadURL", "downloadURL"),
                        format=_get_str(dist, "dct:format", "format"),
                        media_type=_get_str(dist, "dcat:mediaType", "mediaType"),
                        title=_get_str(dist, "dct:title", "title"),
                    )
                )

        # 제공 기관
        pub_raw = raw_item.get("dct:publisher") or raw_item.get("publisher")
        publisher = None
        if isinstance(pub_raw, dict):
            publisher = DCATPublisher(
                name=_get_str(pub_raw, "foaf:name", "name", "rdfs:label"),
                url=pub_raw.get("@id"),
            )
        elif isinstance(pub_raw, str):
            publisher = DCATPublisher(name=pub_raw)

        # 공간/시간 범위
        spatial = None
        sp_raw = raw_item.get("dct:spatial") or raw_item.get("spatial")
        if isinstance(sp_raw, dict):
            spatial = DCATSpatial(
                uri=sp_raw.get("@id"),
                label=_get_str(sp_raw, "rdfs:label", "label"),
            )
        elif isinstance(sp_raw, str):
            spatial = DCATSpatial(label=sp_raw)

        temporal = None
        tm_raw = raw_item.get("dct:temporal") or raw_item.get("temporal")
        if isinstance(tm_raw, dict):
            temporal = DCATTemporal(
                start=_get_str(tm_raw, "dcat:startDate", "startDate", "schema:startDate"),
                end=_get_str(tm_raw, "dcat:endDate", "endDate", "schema:endDate"),
            )

        extras, suggestions = self._mapper.map_extras(raw_item, exclude_keys=_HANDLED_KEYS)

        return DCATDataset(
            identifier=_get_str(raw_item, "dct:identifier", "identifier", "@id"),
            title=_get_str(raw_item, "dct:title", "title") or "(제목 없음)",
            description=_get_str(raw_item, "dct:description", "description"),
            issued=_get_str(raw_item, "dct:issued", "issued"),
            modified=_get_str(raw_item, "dct:modified", "modified"),
            theme=_get_list(raw_item, "dcat:theme", "theme"),
            keyword=_get_list(raw_item, "dcat:keyword", "keyword"),
            publisher=publisher,
            creator=_get_str(raw_item, "dct:creator", "creator"),
            contact_point=_get_str(raw_item, "dcat:contactPoint", "contactPoint"),
            landing_page=_get_str(raw_item, "dcat:landingPage", "landingPage"),
            license=_get_str(raw_item, "dct:license", "license"),
            rights=_get_str(raw_item, "dct:rights", "rights"),
            language=_get_list(raw_item, "dct:language", "language"),
            accrual_periodicity=_get_str(raw_item, "dct:accrualPeriodicity", "accrualPeriodicity"),
            spatial=spatial,
            temporal=temporal,
            distribution=distributions,
            source_portal=portal_id,
            source_portal_name=portal_name,
            source_id=_get_str(raw_item, "@id", "identifier"),
            extras=extras,
            mapping_suggestions=suggestions,
        )
