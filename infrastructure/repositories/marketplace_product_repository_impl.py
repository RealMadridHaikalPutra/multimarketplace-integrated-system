from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.marketplace_product import MarketplaceProduct
from domain.enums import RecordStatus
from domain.repositories.marketplace_product_repository import (
    MarketplaceProductRepository,
)
from infrastructure.db.models import MarketplaceProductModel


class MarketplaceProductRepositoryImpl(MarketplaceProductRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, product: MarketplaceProduct) -> MarketplaceProduct:
        model = MarketplaceProductModel(
            marketplace_product_id=product.marketplace_product_id,
            shop_id=product.shop_id,
            external_product_id=product.external_product_id,
            product_name=product.product_name,
            product_image=product.product_image,
            category=product.category,
            description=product.description,
            status=product.status.value,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def get_by_id(
        self, shop_id: str, product_id: str
    ) -> Optional[MarketplaceProduct]:
        result = await self._session.execute(
            select(MarketplaceProductModel).where(
                and_(
                    MarketplaceProductModel.shop_id == shop_id,
                    MarketplaceProductModel.marketplace_product_id == product_id,
                )
            )
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def get_by_external_id(
        self, shop_external_id: str, external_product_id: str
    ) -> Optional[MarketplaceProduct]:
        result = await self._session.execute(
            select(MarketplaceProductModel).where(
                and_(
                    MarketplaceProductModel.shop_id == shop_external_id,
                    MarketplaceProductModel.external_product_id == external_product_id,
                )
            )
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def list_by_shop(self, shop_external_id: str) -> List[MarketplaceProduct]:
        result = await self._session.execute(
            select(MarketplaceProductModel).where(
                MarketplaceProductModel.shop_id == shop_external_id
            )
        )
        return [self._to_entity(m) for m in result.scalars().all()]

    async def list_all(self) -> List[MarketplaceProduct]:
        """List ALL products across ALL shops in one query (global cycle)."""
        result = await self._session.execute(select(MarketplaceProductModel))
        return [self._to_entity(m) for m in result.scalars().all()]

    async def update(self, product: MarketplaceProduct) -> MarketplaceProduct:
        result = await self._session.execute(
            select(MarketplaceProductModel).where(
                and_(
                    MarketplaceProductModel.shop_id == product.shop_id,
                    MarketplaceProductModel.external_product_id
                    == product.external_product_id,
                )
            )
        )
        model = result.scalar_one_or_none()
        if not model:
            raise ValueError(
                f"MarketplaceProduct with shop_id={product.shop_id}, "
                f"product_id={product.external_product_id} not found"
            )

        model.product_name = product.product_name
        model.product_image = product.product_image
        model.category = product.category
        model.description = product.description
        model.status = product.status.value
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    @staticmethod
    def _to_entity(model: MarketplaceProductModel) -> MarketplaceProduct:
        return MarketplaceProduct(
            marketplace_product_id=model.marketplace_product_id,
            shop_id=model.shop_id,
            external_product_id=model.external_product_id,
            product_name=model.product_name,
            product_image=model.product_image,
            category=model.category,
            description=model.description,
            status=RecordStatus(model.status),
        )
