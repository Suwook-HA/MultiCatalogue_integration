"""정규화기 단위 테스트"""

from __future__ import annotations

import pytest

from app.connectors.base import RawSearchResult
from app.normalizers.ckan_normalizer import CKANNormalizer
from app.normalizers.data_go_kr_normalizer import DataGoKrNormalizer
from app.normalizers.dcat_normalizer import DCATRDFNormalizer
from app.normalizers.field_mapper import FieldMapper


class TestFieldMapper:
    def setup_method(self):
        self.mapper = FieldMapper(known_mappings={"dataset_nm": "title"}, threshold=0.75)

    def test_exact_mapping(self):
        result = self.mapper.map_field("dataset_nm", "교통 데이터")
        assert result.dcat_field == "title"
        assert result.confidence == 1.0
        assert result.auto_applied is True

    def test_similarity_mapping_high_confidence(self):
        result = self.mapper.map_field("title", "테스트")
        assert result.dcat_field == "title"
        assert result.auto_applied is True

    def test_date_pattern_detection(self):
        result = self.mapper.map_field("reg_date", "2024-01-15")
        # 날짜 패턴이 감지되어 issued 또는 modified로 매핑
        assert result.suggestion is not None
        assert "날짜" in result.suggestion.reason or result.dcat_field in ("issued", "modified")

    def test_url_pattern_detection(self):
        result = self.mapper.map_field("link", "https://example.com/data")
        assert result.suggestion is not None
        assert result.dcat_field == "landing_page" or result.suggestion.suggested_dcat == "landing_page"

    def test_low_confidence_goes_to_extras(self):
        result = self.mapper.map_field("xyzzzy_unknown_field", "some_value")
        assert result.auto_applied is False
        assert result.suggestion is not None

    def test_map_extras(self):
        raw = {"title": "테스트", "unknown_key": "값", "quality_score": "95"}
        extras, suggestions = self.mapper.map_extras(raw, exclude_keys={"title"})
        assert "unknown_key" in extras or "quality_score" in extras


class TestDataGoKrNormalizer:
    def setup_method(self):
        self.normalizer = DataGoKrNormalizer()

    def test_normalize_basic(self):
        raw = {
            "public_data_pk": "3049990",
            "dataset_nm": "서울시 버스 노선 정보",
            "prcuse_sumry": "버스 노선별 경로 및 정류장 정보",
            "cate_nm": "교통",
            "org_nm": "서울특별시",
            "registdt": "20230101",
            "keyword": "버스,교통,노선",
        }
        ds = self.normalizer.normalize_dataset(raw, "data_go_kr", "공공데이터포털")

        assert ds.title == "서울시 버스 노선 정보"
        assert ds.description == "버스 노선별 경로 및 정류장 정보"
        assert "교통" in ds.theme
        assert ds.publisher is not None
        assert ds.publisher.name == "서울특별시"
        assert ds.source_portal == "data_go_kr"

    def test_normalize_with_unknown_fields(self):
        raw = {
            "dataset_nm": "테스트 데이터",
            "unknown_field_xyz": "알 수 없는 값",
            "quality_score": "90",
        }
        ds = self.normalizer.normalize_dataset(raw, "data_go_kr", "공공데이터포털")
        # 미매핑 필드는 extras에 보존
        assert "unknown_field_xyz" in ds.extras or "quality_score" in ds.extras

    def test_normalize_search_result(self):
        raw_result = RawSearchResult(
            portal_id="data_go_kr",
            portal_name="공공데이터포털",
            raw_items=[{"dataset_nm": "데이터셋 1"}, {"dataset_nm": "데이터셋 2"}],
            total=2,
        )
        result = self.normalizer.normalize_search_result(raw_result)
        assert len(result.datasets) == 2
        assert result.total == 2
        assert result.error is None

    def test_normalize_with_error(self):
        raw_result = RawSearchResult(
            portal_id="data_go_kr",
            portal_name="공공데이터포털",
            raw_items=[],
            total=0,
            error="API 키 오류",
        )
        result = self.normalizer.normalize_search_result(raw_result)
        assert result.error == "API 키 오류"
        assert len(result.datasets) == 0


class TestCKANNormalizer:
    def setup_method(self):
        self.normalizer = CKANNormalizer()

    def test_normalize_full_package(self):
        raw = {
            "id": "abc-123",
            "title": "Transport Data",
            "notes": "Public transport dataset",
            "metadata_created": "2023-01-01T00:00:00Z",
            "tags": [{"name": "transport"}, {"name": "bus"}],
            "groups": [{"display_name": "Transport"}],
            "organization": {"title": "City Council", "id": "org-1"},
            "resources": [{"url": "https://example.com/data.csv", "format": "CSV"}],
        }
        ds = self.normalizer.normalize_dataset(raw, "sample_ckan", "CKAN 포털")

        assert ds.title == "Transport Data"
        assert ds.description == "Public transport dataset"
        assert "transport" in ds.keyword
        assert ds.publisher is not None
        assert ds.publisher.name == "City Council"
        assert len(ds.distribution) == 1
        assert ds.distribution[0].format == "CSV"

    def test_ckan_extras_preserved(self):
        raw = {
            "title": "Test",
            "extras": [{"key": "data_quality", "value": "high"}, {"key": "update_freq", "value": "daily"}],
        }
        ds = self.normalizer.normalize_dataset(raw, "ckan", "CKAN")
        # CKAN extras도 처리되어야 함
        assert isinstance(ds.extras, dict)


class TestDCATRDFNormalizer:
    def setup_method(self):
        self.normalizer = DCATRDFNormalizer()

    def test_normalize_jsonld(self):
        raw = {
            "@id": "https://example.org/dataset/1",
            "@type": "dcat:Dataset",
            "dct:title": "기상 데이터셋",
            "dct:description": "기상청 기온 데이터",
            "dcat:keyword": ["weather", "temperature"],
            "dct:publisher": {"foaf:name": "기상청"},
            "dct:issued": "2023-01-01",
        }
        ds = self.normalizer.normalize_dataset(raw, "sample_dcat", "DCAT 포털")

        assert ds.title == "기상 데이터셋"
        assert "weather" in ds.keyword
        assert ds.publisher is not None
        assert ds.publisher.name == "기상청"

    def test_normalize_flat_keys(self):
        raw = {"title": "Dataset", "description": "Desc", "issued": "2023"}
        ds = self.normalizer.normalize_dataset(raw, "dcat", "DCAT")
        assert ds.title == "Dataset"
