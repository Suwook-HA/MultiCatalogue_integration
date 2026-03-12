"""추상 BaseConnector: 모든 포털 커넥터가 구현해야 할 인터페이스"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class PortalConfig:
    id: str
    name: str
    type: str
    base_url: str
    enabled: bool = True
    timeout: int = 10
    api_key: str = ""
    description: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class RawSearchResult:
    """포털에서 받아온 미정규화 원본 결과"""

    portal_id: str
    portal_name: str
    raw_items: list[dict]
    total: int
    error: str | None = None


class BaseConnector(ABC):
    """모든 포털 커넥터의 공통 인터페이스"""

    def __init__(self, config: PortalConfig) -> None:
        self.config = config

    @abstractmethod
    async def search(self, query: str, offset: int = 0, limit: int = 10) -> RawSearchResult:
        """포털에 검색 요청을 보내고 원본 응답을 반환한다."""

    @abstractmethod
    async def get_dataset(self, dataset_id: str) -> dict:
        """특정 데이터셋의 상세 정보를 반환한다."""

    @abstractmethod
    async def health_check(self) -> bool:
        """포털 연결 상태를 확인한다."""
