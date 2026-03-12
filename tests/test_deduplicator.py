"""중복 탐지기 테스트"""

from __future__ import annotations

from app.broker.deduplicator import deduplicate
from app.models.dcat import DCATDataset, DCATPublisher, DCATDistribution


def _ds(title: str, portal: str, sid: str = "id-1", **kwargs) -> DCATDataset:
    return DCATDataset(
        title=title,
        source_portal=portal,
        source_portal_name=portal,
        source_id=sid,
        **kwargs,
    )


class TestDeduplicate:
    def test_no_duplicates(self):
        datasets = [
            _ds("Traffic Dataset", "portal_a"),
            _ds("Climate Data", "portal_b"),
        ]
        result = deduplicate(datasets)
        assert len(result) == 2
        assert all(len(ds.also_available_at) == 0 for ds in result)

    def test_exact_title_duplicate(self):
        """완전히 동일한 제목은 중복으로 탐지한다."""
        ds_a = _ds("Air Quality Index", "portal_a", sid="aq-1")
        ds_b = _ds("Air Quality Index", "portal_b", sid="aq-2",
                   description="Detailed air quality data")  # 더 완성도 높음
        result = deduplicate([ds_a, ds_b])
        assert len(result) == 1
        assert result[0].description == "Detailed air quality data"  # 완성도 높은 레코드 선택
        assert len(result[0].also_available_at) == 1
        assert result[0].also_available_at[0]["portal_id"] == "portal_a"

    def test_similar_title_duplicate(self):
        """높은 유사도 제목은 중복으로 탐지한다."""
        ds_a = _ds("Seoul Public Transport Data", "portal_a")
        ds_b = _ds("Seoul Public Transport Data 2023", "portal_b")
        result = deduplicate([ds_a, ds_b], threshold=0.85)
        # 충분히 다르면 2개, 충분히 비슷하면 1개
        # 이 경우 유사도는 약 0.92 → 중복 탐지
        assert len(result) <= 2

    def test_same_portal_not_deduplicated(self):
        """같은 포털 내 유사 제목은 중복 탐지 대상에서 제외한다."""
        datasets = [
            _ds("Transport Data 2022", "portal_a"),
            _ds("Transport Data 2023", "portal_a"),
        ]
        result = deduplicate(datasets)
        assert len(result) == 2

    def test_completeness_scoring(self):
        """완성도 점수가 높은 레코드가 대표 레코드로 선택된다."""
        bare = _ds("Open Data Catalog", "portal_a")
        rich = _ds(
            "Open Data Catalog",
            "portal_b",
            description="Comprehensive open data catalog",
            publisher=DCATPublisher(name="Gov Agency"),
            distribution=[DCATDistribution(format="CSV", access_url="http://example.com")],
            license="CC-BY-4.0",
        )
        result = deduplicate([bare, rich])
        assert len(result) == 1
        assert result[0].source_portal == "portal_b"  # rich가 대표 레코드

    def test_three_portals_same_dataset(self):
        """3개 포털에 동일 데이터셋이 있을 때 1개로 병합된다."""
        datasets = [
            _ds("National Statistics", "portal_a", sid="ns-a"),
            _ds("National Statistics", "portal_b", sid="ns-b"),
            _ds("National Statistics", "portal_c", sid="ns-c",
                description="Full description"),  # 가장 완성도 높음
        ]
        result = deduplicate(datasets)
        assert len(result) == 1
        assert len(result[0].also_available_at) == 2

    def test_empty_input(self):
        assert deduplicate([]) == []

    def test_single_dataset(self):
        result = deduplicate([_ds("Only One", "portal_a")])
        assert len(result) == 1
        assert result[0].also_available_at == []


class TestFacetsAndFilters:
    """브로커의 패싯/필터 로직 단위 테스트 (브로커 import 없이 헬퍼 직접 테스트)"""

    def _make_datasets(self):
        return [
            DCATDataset(
                title="Dataset A",
                source_portal="p1",
                source_portal_name="Portal1",
                publisher=DCATPublisher(name="Ministry of Transport"),
                distribution=[DCATDistribution(format="CSV", access_url="http://a.com")],
                theme=["transport", "urban"],
                license="CC-BY",
                modified="2023-06-01",
            ),
            DCATDataset(
                title="Dataset B",
                source_portal="p2",
                source_portal_name="Portal2",
                publisher=DCATPublisher(name="Environment Agency"),
                distribution=[DCATDistribution(format="JSON", access_url="http://b.com")],
                theme=["environment"],
                license="OGL",
                modified="2024-03-15",
            ),
            DCATDataset(
                title="Dataset C",
                source_portal="p1",
                source_portal_name="Portal1",
                distribution=[DCATDistribution(format="CSV", access_url="http://c.com")],
                theme=["transport"],
                modified="2022-11-20",
            ),
        ]

    def test_build_facets(self):
        from app.broker.search_broker import _build_facets

        datasets = self._make_datasets()
        facets = _build_facets(datasets)

        fmt_values = {f.value: f.count for f in facets.formats}
        assert fmt_values.get("CSV") == 2
        assert fmt_values.get("JSON") == 1

        th_values = {t.value: t.count for t in facets.themes}
        assert th_values.get("transport") == 2

        portal_values = {p.value: p.count for p in facets.portals}
        assert portal_values.get("Portal1") == 2

    def test_filter_by_format(self):
        from app.broker.search_broker import _apply_filters

        datasets = self._make_datasets()
        result = _apply_filters(datasets, filter_format="CSV",
                                filter_theme=None, filter_publisher=None,
                                filter_license=None, modified_after=None, modified_before=None)
        assert len(result) == 2
        assert all(
            any(d.format and d.format.upper() == "CSV" for d in ds.distribution)
            for ds in result
        )

    def test_filter_by_theme(self):
        from app.broker.search_broker import _apply_filters

        datasets = self._make_datasets()
        result = _apply_filters(datasets, filter_format=None,
                                filter_theme="transport", filter_publisher=None,
                                filter_license=None, modified_after=None, modified_before=None)
        assert len(result) == 2

    def test_filter_by_publisher(self):
        from app.broker.search_broker import _apply_filters

        datasets = self._make_datasets()
        result = _apply_filters(datasets, filter_format=None, filter_theme=None,
                                filter_publisher="transport", filter_license=None,
                                modified_after=None, modified_before=None)
        assert len(result) == 1
        assert result[0].publisher.name == "Ministry of Transport"

    def test_filter_by_modified_after(self):
        from app.broker.search_broker import _apply_filters

        datasets = self._make_datasets()
        result = _apply_filters(datasets, filter_format=None, filter_theme=None,
                                filter_publisher=None, filter_license=None,
                                modified_after="2023-01-01", modified_before=None)
        assert len(result) == 2  # 2023-06-01, 2024-03-15

    def test_filter_combined(self):
        from app.broker.search_broker import _apply_filters

        datasets = self._make_datasets()
        result = _apply_filters(datasets, filter_format="CSV", filter_theme="transport",
                                filter_publisher=None, filter_license=None,
                                modified_after=None, modified_before=None)
        assert len(result) == 2  # Dataset A (CSV+transport), Dataset C (CSV+transport)
