"""데이터셋 상세 조회 API"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path

from app.broker.search_broker import get_broker
from app.models.dcat import DCATDataset

router = APIRouter()


@router.get(
    "/datasets/{portal_id}/{dataset_id:path}",
    response_model=DCATDataset,
    summary="데이터셋 상세 조회",
    description=(
        "특정 포털의 데이터셋 ID로 전체 메타데이터를 가져와 DCAT 형식으로 반환합니다. "
        "dataset_id에 슬래시(/)가 포함될 수 있으므로 path 타입을 사용합니다."
    ),
)
async def get_dataset(
    portal_id: str = Path(..., description="포털 ID (예: sample_ckan, data_go_kr)"),
    dataset_id: str = Path(..., description="포털 내 데이터셋 식별자"),
) -> DCATDataset:
    broker = get_broker()
    active_ids = broker.get_active_portal_ids()

    if portal_id not in active_ids:
        raise HTTPException(
            status_code=404,
            detail=f"포털 '{portal_id}'을(를) 찾을 수 없습니다. 활성 포털: {active_ids}",
        )

    try:
        dataset = await broker.get_dataset(portal_id, dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"포털 '{portal_id}' 조회 실패: {exc}",
        ) from exc

    return dataset
