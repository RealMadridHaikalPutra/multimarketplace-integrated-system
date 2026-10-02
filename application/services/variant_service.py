"""MarketplaceVariant operations: create, update, delete, lookup."""

import logging
from typing import List, Optional

from domain.entities.marketplace_variant import MarketplaceVariant
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)

logger = logging.getLogger(__name__)


class VariantService:
    """Handles MarketplaceVariant operations.

    Duplicate-prevention contract (enforced here, layered with the DB-level
    UniqueConstraint & PK):

    * ``create_variant`` ALWAYS runs an existence check before insert, even
      when ``external_variant_id`` is None.
    * The check uses the ``_find_existing_variant`` helper: PK-first, then a
      triple-key fallback ``(shop_id, external_product_id, external_variant_id)``.
    * If a duplicate is detected the existing row is updated (idempotent).
    """

    def __init__(self, variant_repo: MarketplaceVariantRepository):
        self._variant_repo = variant_repo

    async def upsert_variants(self, variants: List[MarketplaceVariant]) -> int:
        """Batch upsert all variants in ONE PostgreSQL statement."""
        return await self._variant_repo.upsert_many(variants)

    async def create_variant(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        """Idempotent create: always run an existence check before insert."""
        existing = await self._find_existing_variant(variant)
        if existing is not None:
            logger.warning(
                "Variant already exists (shop=%s, product=%s, variant=%s, pk=%s). "
                "Updating instead (idempotent create).",
                variant.shop_id,
                variant.external_product_id,
                variant.external_variant_id,
                existing.marketplace_variant_id,
            )
            return await self.update_variant(variant)

        created = await self._variant_repo.create(variant)
        logger.info(
            "Created variant '%s' (shop=%s, product=%s, variant=%s, pk=%s)",
            created.seller_sku,
            created.shop_id,
            created.external_product_id,
            created.external_variant_id,
            created.marketplace_variant_id,
        )
        return created

    async def _find_existing_variant(
        self, variant: MarketplaceVariant
    ) -> Optional[MarketplaceVariant]:
        pk = variant.marketplace_variant_id
        if pk:
            existing_pk = await self._variant_repo.get_by_pk(pk)
            if existing_pk is not None:
                return existing_pk

        if variant.shop_id is not None and variant.external_variant_id:
            existing_triple = await self._variant_repo.get_by_external_variant_id(
                str(variant.shop_id),
                variant.external_product_id or "",
                variant.external_variant_id,
            )
            if existing_triple is not None:
                return existing_triple

        return None

    async def update_variant(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        existing = await self._find_existing_variant(variant)
        if existing is None:
            if variant.external_variant_id:
                raise ValueError(
                    f"Variant not found (shop={variant.shop_id}, "
                    f"product={variant.external_product_id}, "
                    f"variant={variant.external_variant_id})"
                )
            raise ValueError(
                f"Variant not found (shop={variant.shop_id}, "
                f"product={variant.external_product_id}, "
                f"pk={variant.marketplace_variant_id})"
            )

        existing.variant_name = (
            variant.variant_name if variant.variant_name is not None else existing.variant_name
        )
        existing.seller_sku = variant.seller_sku or existing.seller_sku
        existing.total_reserved_stock = (
            variant.total_reserved_stock
            if variant.total_reserved_stock is not None
            else existing.total_reserved_stock
        )
        existing.total_available_stock = (
            variant.total_available_stock
            if variant.total_available_stock is not None
            else existing.total_available_stock
        )
        # Keep existing warehouse when caller passes None; allow explicit "" to clear.
        existing.location_id = (
            variant.location_id if variant.location_id is not None else existing.location_id
        )
        existing.status = variant.status

        updated = await self._variant_repo.update(existing)
        logger.info(
            "Updated variant '%s' (shop=%s, product=%s, variant=%s)",
            updated.seller_sku,
            updated.shop_id,
            updated.external_product_id,
            updated.external_variant_id,
        )
        return updated

    async def soft_delete_variant(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> None:
        await self._variant_repo.soft_delete(shop_id, product_id, variant_id)
        logger.info(
            "Soft-deleted variant (shop=%s, product=%s, variant=%s)",
            shop_id,
            product_id,
            variant_id,
        )

    async def get_by_external_variant_id(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> Optional[MarketplaceVariant]:
        return await self._variant_repo.get_by_external_variant_id(
            shop_id, product_id, variant_id
        )
