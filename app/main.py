"""FastAPI 브로커링 시스템 진입점"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.routes import admin, datasets, portals, search
from app.broker.cache import cache_client
from app.broker.search_broker import get_broker
from app.config.settings import settings

logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # 시작 시
    logger.info("데이터 포털 브로커 시작")
    await cache_client.connect()
    get_broker()  # 포털 레지스트리 초기화
    yield
    # 종료 시
    await cache_client.disconnect()
    logger.info("데이터 포털 브로커 종료")


app = FastAPI(
    title="데이터 포털 브로커링 시스템",
    description=(
        "공공데이터포털, CKAN 호환 포털, DCAT/RDF 포털을 통합하여 "
        "단일 DCAT 표준 API로 메타데이터 연합 검색을 제공합니다."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(search.router, prefix="/api/v1", tags=["검색"])
app.include_router(datasets.router, prefix="/api/v1", tags=["데이터셋"])
app.include_router(portals.router, prefix="/api/v1", tags=["포털 관리"])
app.include_router(admin.router, prefix="/api/v1", tags=["관리"])


@app.get("/health", tags=["시스템"])
async def health() -> dict:
    """시스템 및 포털 연결 상태를 반환한다."""
    broker = get_broker()
    portal_health = await broker.health_check()
    redis_ok = await cache_client.is_connected()
    return {
        "status": "ok",
        "redis": "connected" if redis_ok else "disconnected",
        "portals": {pid: ("ok" if ok else "error") for pid, ok in portal_health.items()},
    }
