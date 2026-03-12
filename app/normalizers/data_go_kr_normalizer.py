"""공공데이터포털 (data.go.kr) 응답 → DCAT 정규화기"""

from __future__ import annotations

from app.models.dcat import DCATDataset, DCATDistribution, DCATPublisher
from app.normalizers.base import BaseNormalizer
from app.normalizers.field_mapper import FieldMapper

# data.go.kr API 응답 필드 → DCAT 확정 매핑
_KNOWN_MAPPINGS: dict[str, str] = {
    "dataset_nm": "title",
    "dataset_ko_nm": "title",
    "prcuse_sumry": "description",
    "cate_nm": "theme",
    "registdt": "issued",
    "updt_dt": "modified",
    "mdfcn_dt": "modified",
    "org_nm": "publisher",
    "org_id": "publisher",
    "keyword": "keyword",
    "tag": "keyword",
    "detail_url": "landing_page",
    "file_link": "distribution",
    "download_url": "distribution",
    "lang_nm": "language",
    "license_nm": "license",
}

# 정규화 시 DCAT 필드로 처리된 키 (extras 제외 대상)
_HANDLED_KEYS = set(_KNOWN_MAPPINGS.keys()) | {
    "public_data_pk",
    "dataset_no",
    "type",
    "resultCode",
    "resultMsg",
}


class DataGoKrNormalizer(BaseNormalizer):
    def __init__(self) -> None:
        self._mapper = FieldMapper(known_mappings=_KNOWN_MAPPINGS)

    def normalize_dataset(self, raw_item: dict, portal_id: str, portal_name: str) -> DCATDataset:
        def get(*keys: str) -> str | None:
            for k in keys:
                v = raw_item.get(k)
                if v:
                    return str(v).strip()
            return None

        # 제공 기관
        org_name = get("org_nm", "prvdr_inst_nm")
        publisher = DCATPublisher(name=org_name) if org_name else None

        # 배포 정보
        distributions: list[DCATDistribution] = []
        file_url = get("file_link", "download_url", "fileLink")
        if file_url:
            distributions.append(
                DCATDistribution(
                    download_url=file_url,
                    format=get("file_extsn", "fileExtsn", "ext"),
                )
            )

        # 주제 분류 (문자열 → 리스트)
        theme_raw = get("cate_nm", "category")
        theme = [t.strip() for t in theme_raw.split(",")] if theme_raw else []

        # 키워드
        keyword_raw = get("keyword", "tag")
        keywords = [k.strip() for k in keyword_raw.split(",")] if keyword_raw else []

        # 미매핑 필드 → extras + mapping_suggestions
        extras, suggestions = self._mapper.map_extras(raw_item, exclude_keys=_HANDLED_KEYS)

        return DCATDataset(
            identifier=get("public_data_pk", "dataset_no", "publicDataPk"),
            title=get("dataset_nm", "dataset_ko_nm", "title") or "(제목 없음)",
            description=get("prcuse_sumry", "description", "summary"),
            issued=get("registdt", "reg_dt"),
            modified=get("updt_dt", "mdfcn_dt"),
            theme=theme,
            keyword=keywords,
            publisher=publisher,
            landing_page=get("detail_url", "detailUrl"),
            distribution=distributions,
            language=[get("lang_nm")] if get("lang_nm") else ["ko"],
            license=get("license_nm"),
            source_portal=portal_id,
            source_portal_name=portal_name,
            source_id=get("public_data_pk", "dataset_no"),
            extras=extras,
            mapping_suggestions=suggestions,
        )
