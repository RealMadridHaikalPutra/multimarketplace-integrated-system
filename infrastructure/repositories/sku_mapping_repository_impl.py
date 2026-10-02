import uuid
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.sku_mapping import SKUMapping
from domain.enums import SkuType
from domain.repositories.sku_mapping_repository import SKUMappingRepository
from infrastructure.db.models import (
    MarketplaceProductModel,
    MarketplaceVariantModel,
    SKUMappingModel,
)

_JOIN_COLUMNS = (
    MarketplaceVariantModel.marketplace_product_id,
    MarketplaceVariantModel.external_variant_id,
    MarketplaceProductModel.shop_id,
    MarketplaceProductModel.external_product_id,
    MarketplaceVariantModel.seller_sku,
)


class SKUMappingRepositoryImpl(SKUMappingRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, mapping: SKUMapping) -> SKUMapping:
        model = SKUMappingModel(
            marketplace_variant_id=mapping.marketplace_variant_id,
            inventree_part_id=mapping.inventree_part_id,
            sku_type=mapping.sku_type.value
            if hasattr(mapping.sku_type, "value")
            else mapping.sku_type,
            conversion_qty=mapping.conversion_qty,
            status=mapping.status,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def get_by_id(self, mapping_id: uuid.UUID) -> Optional[SKUMapping]:
        result = await self._session.execute(
            select(SKUMappingModel).where(SKUMappingModel.sku_mapping_id == mapping_id)
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    def _select_joined(self):
        return (
            select(SKUMappingModel, *_JOIN_COLUMNS)
            .join(
                MarketplaceVariantModel,
                SKUMappingModel.marketplace_variant_id
                == MarketplaceVariantModel.marketplace_variant_id,
            )
            .join(
                MarketplaceProductModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
        )

    async def get_by_variant_id(
        self, marketplace_variant_id
    ) -> List[SKUMapping]:
        result = await self._session.execute(
            self._select_joined().where(
                SKUMappingModel.marketplace_variant_id == marketplace_variant_id
            )
        )
        return [self._row_to_entity(row) for row in result.all()]

    async def find_active_by_seller_sku(self, seller_sku: str) -> Optional[SKUMapping]:
        result = await self._session.execute(
            self._select_joined().where(
                MarketplaceVariantModel.seller_sku == seller_sku,
                SKUMappingModel.status == "ACTIVE",
            )
        )
        row = result.first()
        return self._row_to_entity(row) if row else None

    async def find_duplicate_by_seller_sku(
        self, seller_sku: str, inventree_part_id: int, sku_type_value: str
    ) -> Optional[SKUMapping]:
        result = await self._session.execute(
            self._select_joined().where(
                MarketplaceVariantModel.seller_sku == seller_sku,
                SKUMappingModel.inventree_part_id == inventree_part_id,
                SKUMappingModel.sku_type == sku_type_value,
                SKUMappingModel.status == "ACTIVE",
            )
        )
        row = result.first()
        return self._row_to_entity(row) if row else None

    async def find_active_by_part_id(
        self, inventree_part_id: int
    ) -> List[SKUMapping]:
        result = await self._session.execute(
            self._select_joined().where(
                SKUMappingModel.inventree_part_id == inventree_part_id,
                SKUMappingModel.status == "ACTIVE",
            )
        )
        return [self._row_to_entity(row) for row in result.all()]

    async def find_active_part_ids_by_external_product_ids(
        self, external_product_ids: List[str]
    ) -> List[int]:
        """inventree_part_id (ACTIVE) untuk daftar external_product_id marketplace.

        Jalur: ``marketplace_product.external_product_id`` -> ``marketplace_variant``
        -> ``sku_mapping`` (ACTIVE) -> ``inventree_part_id``. Hasil mengikuti urutan
        masukan dan duplikat dibuang — dipakai jalur webhook untuk mengisi
        variabel ``partId`` (satu product bisa memetakan ke BANYAK part).
        """
        wanted = [
            str(pid).strip()
            for pid in (external_product_ids or [])
            if str(pid or "").strip()
        ]
        if not wanted:
            return []

        result = await self._session.execute(
            select(
                MarketplaceProductModel.external_product_id,
                SKUMappingModel.inventree_part_id,
            )
            .join(
                MarketplaceVariantModel,
                MarketplaceVariantModel.marketplace_product_id
                == MarketplaceProductModel.marketplace_product_id,
            )
            .join(
                SKUMappingModel,
                SKUMappingModel.marketplace_variant_id
                == MarketplaceVariantModel.marketplace_variant_id,
            )
            .where(
                MarketplaceProductModel.external_product_id.in_(wanted),
                SKUMappingModel.status == "ACTIVE",
            )
        )

        by_product: dict = {}
        for external_product_id, part_id in (await result).all():
            if part_id is None:
                continue
            try:
                by_product.setdefault(str(external_product_id), []).append(int(part_id))
            except (TypeError, ValueError):
                continue

        ordered: List[int] = []
        for external_product_id in wanted:
            for part_id in by_product.get(external_product_id, []):
                if part_id not in ordered:
                    ordered.append(part_id)
        return ordered

    async def update(self, mapping: SKUMapping) -> SKUMapping:
        result = await self._session.execute(
            select(SKUMappingModel).where(
                SKUMappingModel.sku_mapping_id == mapping.sku_mapping_id
            )
        )
        model = result.scalar_one_or_none()
        if not model:
            raise ValueError(f"SKUMapping with id {mapping.sku_mapping_id} not found")

        model.marketplace_variant_id = mapping.marketplace_variant_id
        model.inventree_part_id = mapping.inventree_part_id
        model.sku_type = (
            mapping.sku_type.value if hasattr(mapping.sku_type, "value") else mapping.sku_type
        )
        model.conversion_qty = mapping.conversion_qty
        model.status = mapping.status
        model.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def list_all(self) -> List[SKUMapping]:
        result = await self._session.execute(
            self._select_joined().order_by(SKUMappingModel.created_at.desc())
        )
        return [self._row_to_entity(row) for row in result.all()]

    async def soft_delete(self, mapping_id: uuid.UUID) -> None:
        await self._session.execute(
            update(SKUMappingModel)
            .where(SKUMappingModel.sku_mapping_id == mapping_id)
            .values(status="DELETED", updated_at=datetime.now(timezone.utc))
        )
        await self._session.flush()

    @staticmethod
    def _to_entity(model: SKUMappingModel) -> SKUMapping:
        return SKUMapping(
            sku_mapping_id=model.sku_mapping_id,
            marketplace_variant_id=model.marketplace_variant_id,
            inventree_part_id=model.inventree_part_id,
            sku_type=SkuType(model.sku_type)
            if isinstance(model.sku_type, str)
            else model.sku_type,
            conversion_qty=model.conversion_qty,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    @staticmethod
    def _row_to_entity(row) -> SKUMapping:
        model = row[0]
        return SKUMapping(
            sku_mapping_id=model.sku_mapping_id,
            marketplace_variant_id=model.marketplace_variant_id,
            inventree_part_id=model.inventree_part_id,
            sku_type=SkuType(model.sku_type)
            if isinstance(model.sku_type, str)
            else model.sku_type,
            conversion_qty=model.conversion_qty,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
            shop_id=row[3],
            external_product_id=row[4],
            external_variant_id=row[2],
            seller_sku=row[5],
        )
