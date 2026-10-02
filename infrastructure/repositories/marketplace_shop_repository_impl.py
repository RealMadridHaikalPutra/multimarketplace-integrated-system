from typing import List, Optional
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.marketplace_shop import MarketplaceShop
from domain.enums import RecordStatus
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from infrastructure.db.models import MarketplaceShopModel


class MarketplaceShopRepositoryImpl(MarketplaceShopRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, shop: MarketplaceShop) -> MarketplaceShop:
        model = MarketplaceShopModel(
            marketplace_id=shop.marketplace_id,
            external_shop_id=shop.external_shop_id,
            shop_name=shop.shop_name,
            default_warehouse_id=shop.default_warehouse_id,
            status=shop.status.value,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def get_by_id(self, shop_id: str) -> Optional[MarketplaceShop]:
        """Lookup by external_shop_id (or UUID PK when the value parses as one)."""
        lookup_value = shop_id
        try:
            lookup_value = UUID(str(shop_id))
        except Exception:
            pass

        result = await self._session.execute(
            select(MarketplaceShopModel).where(
                (MarketplaceShopModel.id == lookup_value)
                if isinstance(lookup_value, UUID)
                else (MarketplaceShopModel.external_shop_id == lookup_value)
            )
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def get_by_external_id(
        self, marketplace_id: UUID, external_shop_id: str
    ) -> Optional[MarketplaceShop]:
        result = await self._session.execute(
            select(MarketplaceShopModel).where(
                and_(
                    MarketplaceShopModel.marketplace_id == marketplace_id,
                    MarketplaceShopModel.external_shop_id == external_shop_id,
                )
            )
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def list_all(self) -> List[MarketplaceShop]:
        result = await self._session.execute(select(MarketplaceShopModel))
        return [self._to_entity(m) for m in result.scalars().all()]

    async def list_by_marketplace(self, marketplace_id: UUID) -> List[MarketplaceShop]:
        result = await self._session.execute(
            select(MarketplaceShopModel).where(
                MarketplaceShopModel.marketplace_id == marketplace_id
            )
        )
        return [self._to_entity(m) for m in result.scalars().all()]

    async def update(self, shop: MarketplaceShop) -> MarketplaceShop:
        """Persist changes (UNCONDITIONAL overwrite of scalar fields)."""
        result = await self._session.execute(
            select(MarketplaceShopModel).where(MarketplaceShopModel.id == shop.id)
        )
        model = result.scalar_one_or_none()
        if not model:
            raise ValueError(f"MarketplaceShop with id={shop.id} not found")

        model.marketplace_id = shop.marketplace_id
        model.external_shop_id = shop.external_shop_id
        model.shop_name = shop.shop_name
        model.default_warehouse_id = shop.default_warehouse_id
        model.status = shop.status.value
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    @staticmethod
    def _to_entity(model: MarketplaceShopModel) -> MarketplaceShop:
        return MarketplaceShop(
            id=model.id,
            marketplace_id=model.marketplace_id,
            external_shop_id=model.external_shop_id,
            shop_name=model.shop_name,
            default_warehouse_id=model.default_warehouse_id,
            status=RecordStatus(model.status),
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
