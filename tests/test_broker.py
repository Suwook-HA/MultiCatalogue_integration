"""SearchBroker 통합 테스트 (포털 Mock 사용)"""

from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app.broker.search_broker import SearchBroker, _sort_by_relevance
from app.connectors.base import RawSearchResult
from app.models.dcat import DCATDataset


@pytest.fixture
def mock_broker(tmp_path):
    """임시 portals.yaml을 사용하는 브로커"""
    portals_yaml = tmp_path / "portals.yaml"
    portals_yaml.write_text(
        """
portals:
  - id: test_ckan
    name: 테스트 CKAN
    type: ckan
    base_url: https://demo.ckan.org
    enabled: true
    timeout: 5
""",
        encoding="utf-8",
    )
    return SearchBroker(portals_yaml=str(portals_yaml))


@pytest.mark.asyncio
async def test_sort_by_relevance():
    datasets = [
        DCATDataset(title="버스 노선", description="교통 정보", source_portal="a"),
        DCATDataset(title="기상 데이터", description="온도와 날씨 정보", source_portal="b"),
        DCATDataset(title="교통 신호등 데이터", description="교통량 통계", source_portal="c"),
    ]
    sorted_ds = _sort_by_relevance(datasets, "교통")
    assert sorted_ds[0].title in ("버스 노선", "교통 신호등 데이터")


@pytest.mark.asyncio
async def test_broker_partial_failure(mock_broker):
    """한 포털 장애 시 나머지 결과를 반환하는지 확인"""
    with patch.object(
        mock_broker._connectors["test_ckan"],
        "search",
        new_callable=AsyncMock,
        side_effect=Exception("연결 실패"),
    ):
        result = await mock_broker.search("교통", portal_ids=["test_ckan"])

    assert result.portals_failed == ["test_ckan"]
    assert result.datasets == []
    assert result.total == 0


@pytest.mark.asyncio
async def test_broker_successful_search(mock_broker):
    """정상 검색 시 결과가 정규화되어 반환되는지 확인"""
    mock_raw = RawSearchResult(
        portal_id="test_ckan",
        portal_name="테스트 CKAN",
        raw_items=[
            {
                "id": "1",
                "title": "교통 데이터",
                "notes": "서울시 교통량 통계",
                "tags": [],
                "groups": [],
                "resources": [],
            }
        ],
        total=1,
    )

    with patch.object(
        mock_broker._connectors["test_ckan"],
        "search",
        new_callable=AsyncMock,
        return_value=mock_raw,
    ):
        result = await mock_broker.search("교통", portal_ids=["test_ckan"])

    assert result.total == 1
    assert len(result.datasets) == 1
    assert result.datasets[0].title == "교통 데이터"
    assert result.portals_failed == []
