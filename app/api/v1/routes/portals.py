"""포털 목록 및 상태 조회 API"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.broker.search_broker import get_broker

router = APIRouter()


class PortalInfo(BaseModel):
    id: str
    name: str
    type: str
    base_url: str
    enabled: bool
    description: str


class PortalHealthInfo(PortalInfo):
    healthy: bool | None = None


@router.get("/portals", response_model=list[PortalInfo], summary="등록된 포털 목록 조회")
async def list_portals(enabled_only: bool = False) -> list[PortalInfo]:
    broker = get_broker()
    portals = broker.get_portals()
    result = []
    for p in portals:
        if enabled_only and not p.enabled:
            continue
        result.append(
            PortalInfo(
                id=p.id,
                name=p.name,
                type=p.type,
                base_url=p.base_url,
                enabled=p.enabled,
                description=p.description,
            )
        )
    return result


@router.get("/portals/{portal_id}", response_model=PortalInfo, summary="특정 포털 상세 조회")
async def get_portal(portal_id: str) -> PortalInfo:
    broker = get_broker()
    portal = next((p for p in broker.get_portals() if p.id == portal_id), None)
    if not portal:
        raise HTTPException(status_code=404, detail=f"포털 '{portal_id}'을 찾을 수 없습니다.")
    return PortalInfo(
        id=portal.id,
        name=portal.name,
        type=portal.type,
        base_url=portal.base_url,
        enabled=portal.enabled,
        description=portal.description,
    )
