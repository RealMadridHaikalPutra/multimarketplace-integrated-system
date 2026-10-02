from typing import List, Optional

from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)
from infrastructure.db.models import MarketplaceProductModel, MarketplaceVariantModel


class MarketplaceVariantRepositoryImpl(MarketplaceVariantRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        model = MarketplaceVariantModel(
            marketplace_variant_id=variant.marketplace_variant_id,
            marketplace_product_id=variant.marketplace_product_id,
            external_variant_id=variant.external_variant_id,
            variant_name=variant.variant_name,
            seller_sku=variant.seller_sku,
            total_reserved_stock=variant.total_reserved_stock,
            total_available_stock=variant.total_available_stock,
            location_id=variant.location_id,
            status=variant.status.value,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return await self._load_entity_by_pk(model.marketplace_variant_id)

    async def get_by_id(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> Optional[MarketplaceVariant]:
        result = await self._session.execute(
            select(MarketplaceVariantModel)
            .join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
            .where(
                and_(
                    MarketplaceProductModel.shop_id == shop_id,
                    MarketplaceProductModel.external_product_id == product_id,
                    MarketplaceVariantModel.external_variant_id == variant_id,
                )
            )
        )
        model = result.scalar_one_or_none()
        return await self._load_entity_by_pk(model.marketplace_variant_id) if model else None

    async def get_by_external_variant_id(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> Optional[MarketplaceVariant]:
        return await self.get_by_id(shop_id, product_id, variant_id)

    async def get_by_pk(
        self, marketplace_variant_id: str
    ) -> Optional[MarketplaceVariant]:
        if not marketplace_variant_id:
            return None
        result = await self._session.execute(
            select(MarketplaceVariantModel).where(
                MarketplaceVariantModel.marketplace_variant_id
                == marketplace_variant_id
            )
        )
        model = result.scalar_one_or_none()
        return await self._load_entity_by_pk(model.marketplace_variant_id) if model else None

    async def list_by_product(
        self, shop_id: str, product_id: str
    ) -> List[MarketplaceVariant]:
        result = await self._session.execute(
            select(MarketplaceVariantModel.marketplace_variant_id)
            .join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
            .where(
                and_(
                    MarketplaceProductModel.shop_id == shop_id,
                    MarketplaceProductModel.external_product_id == product_id,
                )
            )
        )
        return [await self._load_entity_by_pk(row[0]) for row in result.all()]

    async def list_by_shop(self, shop_external_id: str) -> List[MarketplaceVariant]:
        result = await self._session.execute(
            select(
                MarketplaceVariantModel,
                MarketplaceProductModel.shop_id,
                MarketplaceProductModel.external_product_id,
            )
            .join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
            .where(MarketplaceProductModel.shop_id == shop_external_id)
        )
        return [self._row_to_entity(row) for row in result.all()]

    async def list_all(self) -> List[MarketplaceVariant]:
        result = await self._session.execute(
            select(
                MarketplaceVariantModel,
                MarketplaceProductModel.shop_id,
                MarketplaceProductModel.external_product_id,
            ).join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
        )
        return [self._row_to_entity(row) for row in result.all()]

    async def list_by_seller_sku(self, seller_sku: str) -> List[MarketplaceVariant]:
        result = await self._session.execute(
            select(MarketplaceVariantModel.marketplace_variant_id).where(
                MarketplaceVariantModel.seller_sku == seller_sku
            )
        )
        return [await self._load_entity_by_pk(row[0]) for row in result.all()]

    async def list_all_seller_skus(self) -> List[str]:
        result = await self._session.execute(
            select(MarketplaceVariantModel.seller_sku)
            .where(MarketplaceVariantModel.seller_sku.isnot(None))
            .distinct()
        )
        return [str(row[0]) for row in result.all()]

    async def upsert_many(self, variants: List[MarketplaceVariant]) -> int:
        """Batch upsert in ONE PostgreSQL statement (full overwrite on conflict).

        Identity columns (marketplace_product_id, external_variant_id) are
        immutable and never updated.
        """
        if not variants:
            return 0

        values = [
            {
                "marketplace_variant_id": v.marketplace_variant_id,
                "marketplace_product_id": v.marketplace_product_id,
                "external_variant_id": v.external_variant_id,
                "variant_name": v.variant_name,
                "seller_sku": v.seller_sku,
                "total_reserved_stock": v.total_reserved_stock,
                "total_available_stock": v.total_available_stock,
                "location_id": v.location_id,
                "status": v.status.value if hasattr(v.status, "value") else v.status,
            }
            for v in variants
        ]

        stmt = pg_insert(MarketplaceVariantModel).values(values)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=[MarketplaceVariantModel.marketplace_variant_id],
            set_={
                "seller_sku": excluded.seller_sku,
                "status": excluded.status,
                "variant_name": excluded.variant_name,
                "total_reserved_stock": excluded.total_reserved_stock,
                "total_available_stock": excluded.total_available_stock,
                "location_id": excluded.location_id,
            },
        )
        result = await self._session.execute(stmt)
        return result.rowcount or 0

    async def update(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        """Persist changes (UNCONDITIONAL overwrite of scalar fields).

        Callers wanting a partial update MUST mask non-intended fields first.
        """
        if variant.marketplace_variant_id:
            result = await self._session.execute(
                select(MarketplaceVariantModel).where(
                    MarketplaceVariantModel.marketplace_variant_id
                    == variant.marketplace_variant_id
                )
            )
        else:
            result = await self._session.execute(
                select(MarketplaceVariantModel)
                .join(
                    MarketplaceProductModel,
                    MarketplaceVariantModel.marketplace_product_id
                    == MarketplaceProductModel.marketplace_product_id,
                )
                .where(
                    and_(
                        MarketplaceProductModel.shop_id == variant.shop_id,
                        MarketplaceProductModel.external_product_id
                        == variant.external_product_id,
                        MarketplaceVariantModel.external_variant_id
                        == variant.external_variant_id,
                    )
                )
            )
        model = result.scalar_one_or_none()
        if not model:
            raise ValueError(
                f"MarketplaceVariant with variant_id="
                f"{variant.marketplace_variant_id or variant.external_variant_id} not found"
            )

        model.variant_name = variant.variant_name
        model.seller_sku = variant.seller_sku
        model.total_reserved_stock = variant.total_reserved_stock
        model.total_available_stock = variant.total_available_stock
        model.location_id = variant.location_id
        model.status = variant.status.value
        await self._session.flush()
        await self._session.refresh(model)
        return await self._load_entity_by_pk(model.marketplace_variant_id)

    async def soft_delete(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> None:
        await self._session.execute(
            update(MarketplaceVariantModel)
            .where(
                MarketplaceVariantModel.marketplace_variant_id.in_(
                    select(MarketplaceVariantModel.marketplace_variant_id)
                    .join(
                        MarketplaceProductModel,
                        MarketplaceVariantModel.marketplace_product_id
                        == MarketplaceProductModel.marketplace_product_id,
                    )
                    .where(
                        and_(
                            MarketplaceProductModel.shop_id == shop_id,
                            MarketplaceProductModel.external_product_id == product_id,
                            MarketplaceVariantModel.external_variant_id == variant_id,
                        )
                    )
                )
            )
            .values(status="DELETED")
        )
        await self._session.flush()

    @staticmethod
    def _row_to_entity(row) -> MarketplaceVariant:
        model = row[0]
        return MarketplaceVariant(
            marketplace_variant_id=model.marketplace_variant_id,
            marketplace_product_id=model.marketplace_product_id,
            external_variant_id=model.external_variant_id,
            variant_name=model.variant_name,
            seller_sku=model.seller_sku,
            total_reserved_stock=model.total_reserved_stock,
            total_available_stock=model.total_available_stock,
            location_id=model.location_id,
            status=RecordStatus(model.status),
            shop_id=row[1],
            external_product_id=row[2],
        )

    async def _load_entity_by_pk(self, marketplace_variant_id) -> MarketplaceVariant:
        result = await self._session.execute(
            select(
                MarketplaceVariantModel,
                MarketplaceProductModel.shop_id,
                MarketplaceProductModel.external_product_id,
            )
            .join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
            .where(
                MarketplaceVariantModel.marketplace_variant_id
                == marketplace_variant_id
            )
        )
        row = result.first()
        if not row:
            raise ValueError(
                f"MarketplaceVariant with id={marketplace_variant_id} not found"
            )
        return self._row_to_entity(row)
