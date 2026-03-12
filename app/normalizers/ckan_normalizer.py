"""CKAN 패키지(데이터셋) 응답 → DCAT 정규화기"""

from __future__ import annotations

from app.models.dcat import DCATDataset, DCATDistribution, DCATPublisher
from app.normalizers.base import BaseNormalizer
from app.normalizers.field_mapper import FieldMapper

# CKAN 필드 → DCAT 확정 매핑
_KNOWN_MAPPINGS: dict[str, str] = {
    "title": "title",
    "name": "identifier",
    "notes": "description",
    "notes_rendered": "description",
    "metadata_created": "issued",
    "metadata_modified": "modified",
    "tags": "keyword",
    "groups": "theme",
    "organization": "publisher",
    "maintainer": "contact_point",
    "maintainer_email": "contact_point",
    "author": "creator",
    "author_email": "creator",
    "url": "landing_page",
    "resources": "distribution",
    "license_id": "license",
    "license_url": "license",
    "language": "language",
}

_HANDLED_KEYS = set(_KNOWN_MAPPINGS.keys()) | {
    "id",
    "type",
    "state",
    "private",
    "isopen",
    "num_resources",
    "num_tags",
    "revision_id",
    "creator_user_id",
}


class CKANNormalizer(BaseNormalizer):
    def __init__(self) -> None:
        self._mapper = FieldMapper(known_mappings=_KNOWN_MAPPINGS)

    def normalize_dataset(self, raw_item: dict, portal_id: str, portal_name: str) -> DCATDataset:
        # 배포 리소스
        distributions: list[DCATDistribution] = []
        for res in raw_item.get("resources", []) or []:
            distributions.append(
                DCATDistribution(
                    access_url=res.get("url"),
                    download_url=res.get("url") if res.get("url_type") == "upload" else None,
                    format=res.get("format"),
                    media_type=res.get("mimetype"),
                    byte_size=res.get("size"),
                    title=res.get("name"),
                    description=res.get("description"),
                )
            )

        # 키워드: CKAN tags는 리스트 of dict
        keywords: list[str] = []
        for tag in raw_item.get("tags", []) or []:
            if isinstance(tag, dict):
                keywords.append(tag.get("display_name") or tag.get("name", ""))
            elif isinstance(tag, str):
                keywords.append(tag)

        # 주제: CKAN groups는 리스트 of dict
        themes: list[str] = []
        for grp in raw_item.get("groups", []) or []:
            if isinstance(grp, dict):
                themes.append(grp.get("display_name") or grp.get("title") or grp.get("name", ""))
            elif isinstance(grp, str):
                themes.append(grp)

        # 제공 기관
        org = raw_item.get("organization") or {}
        publisher = None
        if org:
            publisher = DCATPublisher(
                name=org.get("title") or org.get("name"),
                identifier=org.get("id"),
            )

        extras, suggestions = self._mapper.map_extras(raw_item, exclude_keys=_HANDLED_KEYS)

        # CKAN extras (key-value 배열) 도 보존
        for extra in raw_item.get("extras", []) or []:
            k, v = extra.get("key", ""), extra.get("value", "")
            if k and k not in _HANDLED_KEYS:
                result = self._mapper.map_field(k, v)
                if not result.auto_applied:
                    extras[k] = v
                    if result.suggestion:
                        suggestions.append(result.suggestion)

        return DCATDataset(
            identifier=raw_item.get("id"),
            title=raw_item.get("title") or raw_item.get("name") or "(제목 없음)",
            description=raw_item.get("notes") or raw_item.get("notes_rendered"),
            issued=raw_item.get("metadata_created"),
            modified=raw_item.get("metadata_modified"),
            theme=themes,
            keyword=keywords,
            publisher=publisher,
            creator=raw_item.get("author"),
            contact_point=raw_item.get("maintainer_email") or raw_item.get("maintainer"),
            landing_page=raw_item.get("url"),
            license=raw_item.get("license_url") or raw_item.get("license_id"),
            distribution=distributions,
            source_portal=portal_id,
            source_portal_name=portal_name,
            source_id=raw_item.get("id") or raw_item.get("name"),
            extras=extras,
            mapping_suggestions=suggestions,
        )
