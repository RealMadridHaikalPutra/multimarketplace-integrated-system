import uuid
import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from application.dto.requests import (
    CreateMarketplaceRequest,
    UpdateMarketplaceRequest,
    MarketplaceResponse,
    CreateMarketplaceShopRequest,
    UpdateMarketplaceShopRequest,
    MarketplaceShopResponse,
)
from domain.entities.marketplace import Marketplace
from domain.entities.marketplace_shop import MarketplaceShop
from domain.enums import MarketplaceStatus, RecordStatus
from domain.repositories.marketplace_repository import MarketplaceRepository
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from infrastructure.db.database import get_db_session
from infrastructure.repositories.marketplace_repository_impl import MarketplaceRepositoryImpl
from infrastructure.repositories.marketplace_shop_repository_impl import MarketplaceShopRepositoryImpl

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["Marketplace & Shop"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_mp_repo(session: AsyncSession) -> MarketplaceRepository:
    return MarketplaceRepositoryImpl(session)


def _get_shop_repo(session: AsyncSession) -> MarketplaceShopRepository:
    return MarketplaceShopRepositoryImpl(session)


def _marketplace_to_response(m: Marketplace) -> MarketplaceResponse:
    return MarketplaceResponse(
        id=str(m.id),
        code=m.code,
        name=m.name,
        status=m.status.value if hasattr(m.status, "value") else m.status,
        created_at=str(m.created_at) if m.created_at else None,
        updated_at=str(m.updated_at) if m.updated_at else None,
    )


def _shop_to_response(s: MarketplaceShop, mp_code: str = "") -> MarketplaceShopResponse:
    return MarketplaceShopResponse(
        id=str(s.id),
        marketplace_code=mp_code,
        marketplace_id=str(s.marketplace_id),
        external_shop_id=s.external_shop_id,
        shop_name=s.shop_name,
        default_warehouse_id=s.default_warehouse_id,
        status=s.status.value if hasattr(s.status, "value") else s.status,
        created_at=str(s.created_at) if s.created_at else None,
        updated_at=str(s.updated_at) if s.updated_at else None,
    )


# ---------------------------------------------------------------------------
# Marketplace CRUD
# ---------------------------------------------------------------------------


@router.post("/marketplace", response_model=MarketplaceResponse, status_code=201)
async def create_marketplace(
    payload: CreateMarketplaceRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new marketplace (Shopee, TikTok, etc.)."""
    repo = _get_mp_repo(session)

    existing = await repo.get_by_code(payload.code)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Marketplace with code '{payload.code}' already exists",
        )

    marketplace = Marketplace(
        code=payload.code,
        name=payload.name,
        status=MarketplaceStatus(payload.status),
    )
    created = await repo.create(marketplace)
    await session.commit()
    logger.info("Created marketplace: %s (%s)", created.name, created.code)
    return _marketplace_to_response(created)


@router.get("/marketplace", response_model=List[MarketplaceResponse])
async def list_marketplaces(
    session: AsyncSession = Depends(get_db_session),
):
    """List all marketplaces."""
    repo = _get_mp_repo(session)
    marketplaces = await repo.list_all()
    return [_marketplace_to_response(m) for m in marketplaces]


@router.get("/marketplace/shop", response_model=List[MarketplaceShopResponse])
async def list_all_shops(
    session: AsyncSession = Depends(get_db_session),
):
    """List all shops across all marketplaces."""
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)

    shops = await shop_repo.list_all()
    # Build marketplace lookup for codes
    mp_cache: dict = {}
    result: list[MarketplaceShopResponse] = []
    for s in shops:
        if s.marketplace_id not in mp_cache:
            mp = await mp_repo.get_by_id(s.marketplace_id)
            mp_cache[s.marketplace_id] = mp.code if mp else ""
        result.append(_shop_to_response(s, mp_code=mp_cache[s.marketplace_id]))
    return result


@router.get("/marketplace/{marketplace_id}", response_model=MarketplaceResponse)
async def get_marketplace(
    marketplace_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """Get a single marketplace by ID."""
    try:
        mp_uuid = uuid.UUID(marketplace_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid marketplace ID format: '{marketplace_id}'")
    repo = _get_mp_repo(session)
    marketplace = await repo.get_by_id(mp_uuid)
    if not marketplace:
        raise HTTPException(status_code=404, detail="Marketplace not found")
    return _marketplace_to_response(marketplace)


@router.put("/marketplace/{marketplace_id}", response_model=MarketplaceResponse)
async def update_marketplace(
    marketplace_id: str,
    payload: UpdateMarketplaceRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """Update a marketplace (name, status)."""
    try:
        mp_uuid = uuid.UUID(marketplace_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid marketplace ID format: '{marketplace_id}'")
    repo = _get_mp_repo(session)
    marketplace = await repo.get_by_id(mp_uuid)
    if not marketplace:
        raise HTTPException(status_code=404, detail="Marketplace not found")

    if payload.name is not None:
        marketplace.name = payload.name
    if payload.status is not None:
        marketplace.status = MarketplaceStatus(payload.status)

    updated = await repo.update(marketplace)
    await session.commit()
    return _marketplace_to_response(updated)


@router.delete("/marketplace/{marketplace_id}")
async def delete_marketplace(
    marketplace_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """Soft-delete a marketplace (set status to INACTIVE)."""
    try:
        mp_uuid = uuid.UUID(marketplace_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid marketplace ID format: '{marketplace_id}'")
    repo = _get_mp_repo(session)
    marketplace = await repo.get_by_id(mp_uuid)
    if not marketplace:
        raise HTTPException(status_code=404, detail="Marketplace not found")

    marketplace.status = MarketplaceStatus.INACTIVE
    await repo.update(marketplace)
    await session.commit()
    return {"status": "deleted", "id": marketplace_id}


# ---------------------------------------------------------------------------
# Marketplace Shop CRUD
# ---------------------------------------------------------------------------


@router.post("/marketplace/shop", response_model=MarketplaceShopResponse, status_code=201)
async def create_marketplace_shop(
    payload: CreateMarketplaceShopRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """Create a new shop for a marketplace.

    Uses marketplace_code (e.g. 'shopee', 'tiktok') instead of UUID.
    """
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)

    marketplace = await mp_repo.get_by_code(payload.marketplace_code)
    if not marketplace:
        raise HTTPException(
            status_code=404,
            detail=f"Marketplace with code '{payload.marketplace_code}' not found",
        )

    existing = await shop_repo.get_by_external_id(
        marketplace.id, payload.external_shop_id
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Shop with external_id '{payload.external_shop_id}' already exists for this marketplace",
        )

    shop = MarketplaceShop(
        marketplace_id=marketplace.id,
        external_shop_id=payload.external_shop_id,
        shop_name=payload.shop_name,
        default_warehouse_id=payload.default_warehouse_id,
        status=RecordStatus(payload.status),
    )
    created = await shop_repo.create(shop)
    await session.commit()
    logger.info("Created shop: %s for marketplace %s", created.shop_name, marketplace.code)
    return _shop_to_response(created, mp_code=marketplace.code)


@router.get("/marketplace/{marketplace_code}/shops", response_model=List[MarketplaceShopResponse])
async def list_shops_by_marketplace(
    marketplace_code: str,
    session: AsyncSession = Depends(get_db_session),
):
    """List all shops for a marketplace by its code (e.g. 'shopee', 'tiktok')."""
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)

    marketplace = await mp_repo.get_by_code(marketplace_code)
    if not marketplace:
        raise HTTPException(
            status_code=404,
            detail=f"Marketplace with code '{marketplace_code}' not found",
        )

    shops = await shop_repo.list_by_marketplace(marketplace.id)
    return [_shop_to_response(s, mp_code=marketplace.code) for s in shops]


@router.get("/marketplace/shop/{external_shop_id}", response_model=MarketplaceShopResponse)
async def get_marketplace_shop(
    external_shop_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """Get a single shop by external_shop_id."""
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)
    shop = await shop_repo.get_by_id(external_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    mp = await mp_repo.get_by_id(shop.marketplace_id)
    return _shop_to_response(shop, mp_code=mp.code if mp else "")


@router.put("/marketplace/shop/{external_shop_id}", response_model=MarketplaceShopResponse)
async def update_marketplace_shop(
    external_shop_id: str,
    payload: UpdateMarketplaceShopRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """Update a shop (name, status)."""
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)
    shop = await shop_repo.get_by_id(external_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    if payload.shop_name is not None:
        shop.shop_name = payload.shop_name
    # Allow explicit null in update payload to clear the per-shop override
    # (→ revert to global config defaults). Pydantic v2: explicit None is
    # distinguishable from field-not-provided thanks to model_fields_set.
    if "default_warehouse_id" in payload.model_fields_set:
        shop.default_warehouse_id = payload.default_warehouse_id
    if payload.status is not None:
        shop.status = RecordStatus(payload.status)

    updated = await shop_repo.update(shop)
    await session.commit()
    mp = await mp_repo.get_by_id(shop.marketplace_id)
    return _shop_to_response(updated, mp_code=mp.code if mp else "")


@router.delete("/marketplace/shop/{external_shop_id}")
async def delete_marketplace_shop(
    external_shop_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """Soft-delete a shop (set status to INACTIVE)."""
    shop_repo = _get_shop_repo(session)
    shop = await shop_repo.get_by_id(external_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    shop.status = RecordStatus.INACTIVE
    await shop_repo.update(shop)
    await session.commit()
    return {"status": "deleted", "external_shop_id": external_shop_id}
