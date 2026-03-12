"""
Redis 기반 하이브리드 캐시 레이어.
Redis 장애 시 캐시 없이 graceful하게 동작한다.
"""

from __future__ import annotations

import hashlib
import json
import logging

import redis.asyncio as aioredis

from app.config.settings import settings

logger = logging.getLogger(__name__)


def _make_cache_key(query: str, portals: list[str], page: int, size: int) -> str:
    """결정론적 캐시 키를 생성한다."""
    raw = f"{query.lower().strip()}|{','.join(sorted(portals))}|{page}|{size}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"broker:search:{digest}"


class CacheClient:
    """Redis 캐시 클라이언트 (비동기)"""

    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        try:
            self._client = aioredis.from_url(settings.redis_url, decode_responses=True)
            await self._client.ping()
            logger.info("Redis 연결 성공: %s", settings.redis_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis 연결 실패 (캐시 비활성화): %s", exc)
            self._client = None

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()

    async def get(self, query: str, portals: list[str], page: int, size: int) -> dict | None:
        if not self._client:
            return None
        key = _make_cache_key(query, portals, page, size)
        try:
            raw = await self._client.get(key)
            if raw:
                logger.debug("캐시 히트: %s", key)
                return json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            logger.warning("캐시 조회 실패: %s", exc)
        return None

    async def set(self, query: str, portals: list[str], page: int, size: int, data: dict) -> None:
        if not self._client:
            return
        key = _make_cache_key(query, portals, page, size)
        try:
            await self._client.setex(key, settings.cache_ttl_seconds, json.dumps(data, ensure_ascii=False))
            logger.debug("캐시 저장: %s (TTL=%ds)", key, settings.cache_ttl_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("캐시 저장 실패: %s", exc)

    async def is_connected(self) -> bool:
        if not self._client:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:  # noqa: BLE001
            return False


# 싱글턴 인스턴스
cache_client = CacheClient()
