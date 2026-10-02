import logging
from typing import Optional

from application.ports import InventoryProvider
from core.config import settings
from domain.entities.sku_mapping import SKUMapping
from domain.errors import InvenTreePartNotFoundError
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)
from domain.repositories.sku_mapping_repository import SKUMappingRepository
from domain.rules.sku import SkuParser, sku_qty_of
from domain.rules.sku_conflict import FAMILY_PACK, FAMILY_SX, category_family

logger = logging.getLogger(__name__)

# Mapping status CONFLICT — mapping is BLOCKED (not used by the pipeline for
# stock push) until the part is separated / reviewed.
MAPPING_STATUS_CONFLICT = "CONFLICT"


class MappingService:
    """Maps Marketplace Variants to InvenTree Parts."""

    def __init__(
        self,
        mapping_repo: SKUMappingRepository,
        variant_repo: MarketplaceVariantRepository,
        inventory_provider: InventoryProvider,
    ):
        self._mapping_repo = mapping_repo
        self._variant_repo = variant_repo
        self._inventory = inventory_provider

    async def _resolve_mapping_status(
        self,
        parsed_sku_type,
        inventree_part_id: int,
        parsed_sku_qty: Optional[int] = None,
        exclude_variant_id=None,
    ) -> str:
        """Decide the new mapping status per the BLOCK Case 1 (TC-SKU-006) rule.

        See the module documentation: NORMAL may share a part with anyone;
        PACK conflicts with S/X only when the qty NUMBER is equal; PACK is
        always the side that gets blocked.
        """
        if not settings.mapping_block_pack_shared_part:
            return "ACTIVE"

        try:
            existing = await self._mapping_repo.find_active_by_part_id(inventree_part_id)
        except Exception as exc:
            logger.warning(
                "Conflict-check find_active_by_part_id failed (part=%s): %s — "
                "proceeding with ACTIVE",
                inventree_part_id,
                exc,
            )
            return "ACTIVE"

        my_family = category_family(parsed_sku_type)
        others = [
            m
            for m in existing
            if str(m.marketplace_variant_id) != str(exclude_variant_id)
        ]

        if my_family == FAMILY_PACK:
            my_qty = parsed_sku_qty
            sx_same_qty = [
                m
                for m in others
                if category_family(m.sku_type) == FAMILY_SX
                and my_qty is not None
                and sku_qty_of(m) == my_qty
            ]
            if sx_same_qty:
                logger.warning(
                    "BLOCK SKU_PACK mapping: part=%s sudah dipakai SX "
                    "(SKU_S/SKU_X) dengan ANGKA qty sama (%s) — set status CONFLICT",
                    inventree_part_id,
                    my_qty,
                )
                return MAPPING_STATUS_CONFLICT
            return "ACTIVE"

        if my_family == FAMILY_SX:
            my_qty = parsed_sku_qty
            pack_conflicts = [
                m
                for m in others
                if category_family(m.sku_type) == FAMILY_PACK
                and m.status == "ACTIVE"
                and my_qty is not None
                and sku_qty_of(m) == my_qty
            ]
            for m in pack_conflicts:
                m.status = MAPPING_STATUS_CONFLICT
                try:
                    await self._mapping_repo.update(m)
                    logger.warning(
                        "Demote SKU_PACK mapping id=%s ke CONFLICT karena part=%s "
                        "kini dipakai SKU_S/SKU_X dengan angka qty sama (%s)",
                        m.sku_mapping_id,
                        inventree_part_id,
                        my_qty,
                    )
                except Exception as exc:
                    logger.warning(
                        "Demote PACK mapping id=%s gagal: %s", m.sku_mapping_id, exc
                    )
            return "ACTIVE"

        # NORMAL — compatible with every family, never blocked.
        return "ACTIVE"

    async def create_mapping(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> SKUMapping:
        variant = await self._variant_repo.get_by_id(shop_id, product_id, variant_id)
        if not variant:
            raise ValueError(
                f"MarketplaceVariant with shop={shop_id}, product={product_id}, "
                f"variant={variant_id} not found."
            )

        existing_mappings = await self._mapping_repo.get_by_variant_id(
            variant.marketplace_variant_id
        )
        if existing_mappings:
            existing = existing_mappings[0]
            parsed = SkuParser.parse(variant.seller_sku)
            existing.sku_type = parsed.sku_type
            existing.conversion_qty = SkuParser.compute_faktor_pengali(
                parsed.sku_type, parsed.conversion_qty
            )

            inventree_part_id = await self._inventory.find_part_by_sku(variant.seller_sku)
            if inventree_part_id:
                existing.inventree_part_id = int(inventree_part_id)
                existing.status = await self._resolve_mapping_status(
                    parsed.sku_type,
                    int(inventree_part_id),
                    parsed_sku_qty=parsed.conversion_qty,
                    exclude_variant_id=existing.marketplace_variant_id,
                )

            updated = await self._mapping_repo.update(existing)
            logger.info(
                "Updated existing mapping id=%s for variant=%s (type=%s, faktor=%s)",
                updated.sku_mapping_id,
                variant.marketplace_variant_id,
                parsed.sku_type.value,
                existing.conversion_qty,
            )
            return updated

        parsed = SkuParser.parse(variant.seller_sku)
        inventree_part_id = await self._inventory.find_part_by_sku(variant.seller_sku)
        if not inventree_part_id:
            raise InvenTreePartNotFoundError(variant.seller_sku)
        inventree_part_id_int: int = int(inventree_part_id)

        faktor_pengali = SkuParser.compute_faktor_pengali(
            parsed.sku_type, parsed.conversion_qty
        )
        mapping_status = await self._resolve_mapping_status(
            parsed.sku_type,
            inventree_part_id_int,
            parsed_sku_qty=parsed.conversion_qty,
            exclude_variant_id=variant.marketplace_variant_id,
        )
        mapping = SKUMapping(
            marketplace_variant_id=variant.marketplace_variant_id,
            shop_id=variant.shop_id,
            external_product_id=variant.external_product_id,
            external_variant_id=variant.external_variant_id,
            inventree_part_id=inventree_part_id_int,
            sku_type=parsed.sku_type,
            conversion_qty=faktor_pengali,
            status=mapping_status,
        )

        created = await self._mapping_repo.create(mapping)
        logger.info(
            "Created mapping: variant (shop=%s, product=%s, variant=%s) -> "
            "InvenTree part=%s (type=%s, faktor=%s)",
            shop_id,
            product_id,
            variant_id,
            inventree_part_id_int,
            parsed.sku_type.value,
            faktor_pengali,
        )
        return created

    async def update_mapping_for_variant(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> SKUMapping:
        variant = await self._variant_repo.get_by_id(shop_id, product_id, variant_id)
        if not variant:
            raise ValueError(
                f"MarketplaceVariant with shop={shop_id}, product={product_id}, "
                f"variant={variant_id} not found."
            )

        parsed = SkuParser.parse(variant.seller_sku)
        mappings = await self._mapping_repo.get_by_variant_id(
            variant.marketplace_variant_id
        )

        if not mappings:
            return await self.create_mapping(shop_id, product_id, variant_id)

        mapping = mappings[0]
        mapping.sku_type = parsed.sku_type
        mapping.conversion_qty = SkuParser.compute_faktor_pengali(
            parsed.sku_type, parsed.conversion_qty
        )

        inventree_part_id = await self._inventory.find_part_by_sku(variant.seller_sku)
        if inventree_part_id:
            mapping.inventree_part_id = int(inventree_part_id)
            mapping.status = await self._resolve_mapping_status(
                parsed.sku_type,
                int(inventree_part_id),
                parsed_sku_qty=parsed.conversion_qty,
                exclude_variant_id=mapping.marketplace_variant_id,
            )

        updated = await self._mapping_repo.update(mapping)
        logger.info(
            "Updated mapping id=%s for variant (shop=%s, product=%s, variant=%s) "
            "(type=%s, faktor=%s)",
            updated.sku_mapping_id,
            shop_id,
            product_id,
            variant_id,
            parsed.sku_type.value,
            mapping.conversion_qty,
        )
        return updated

    async def soft_delete_mapping(
        self, shop_id: str, product_id: str, variant_id: str
    ) -> None:
        variant = await self._variant_repo.get_by_id(shop_id, product_id, variant_id)
        if not variant:
            logger.warning(
                "No variant found for soft-delete (shop=%s, product=%s, variant=%s)",
                shop_id,
                product_id,
                variant_id,
            )
            return

        mappings = await self._mapping_repo.get_by_variant_id(
            variant.marketplace_variant_id
        )
        if not mappings:
            logger.warning(
                "No mapping found for variant (shop=%s, product=%s, variant=%s)",
                shop_id,
                product_id,
                variant_id,
            )
            return

        for mapping in mappings:
            await self._mapping_repo.soft_delete(mapping.sku_mapping_id)
            logger.info(
                "Soft-deleted mapping id=%s for variant (shop=%s, product=%s, variant=%s)",
                mapping.sku_mapping_id,
                shop_id,
                product_id,
                variant_id,
            )
