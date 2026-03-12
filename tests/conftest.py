import pytest


@pytest.fixture(autouse=True)
def disable_redis(monkeypatch):
    """테스트 중 Redis 연결을 비활성화한다."""
    from app.broker import cache
    monkeypatch.setattr(cache.cache_client, "_client", None)
