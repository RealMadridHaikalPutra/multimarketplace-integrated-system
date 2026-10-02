"""MappingEngine — orchestrates the mapping lifecycle.

Responsibilities:
- Process variants for a shop (find/create mapping)
- Reprocess a variant after update (recalculate mapping)
- Soft-deletion of variants / products (cascade to mappings)
- Enforce business rules (no duplicate active mappings per seller SKU)
"""

import logging
from typing import Optional

from application.services.mapping_service import MappingService
from application.services.mapping_validation_service import MappingValidationService
from application.services.variant_service import VariantService
from core.config import settings
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from domain.errors import DuplicateMappingError, InvenTreePartNotFoundError
from domain.repositories.marketplace_product_repository import (
    MarketplaceProductRepository,
)
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)
from domain.rules.sku import SkuParser

logger = logging.getLogger(__name__)


class MappingEngine:
    """Orchestrates the mapping lifecycle."""

    def __init__(
        self,
        mapping_service: MappingService,
        variant_service: VariantService,
        variant_repo: MarketplaceVariantRepository,
        product_repo: Optional[MarketplaceProductRepository] = None,
        shop_repo: Optional[MarketplaceShopRepository] = None,
        validation_service: Optional[MappingValidationService] = None,
    ):
        self._mapping_service = mapping_service
        self._variant_service = variant_service
        self._variant_repo = variant_repo
        self._product_repo = product_repo
        self._shop_repo = shop_repo
        self._validation_service = validation_service

    async def run_mapping_for_shop(self, shop_id: str) -> dict:
        """Run mapping for ALL marketplace variants in a given shop."""
        if not self._product_repo or not self._shop_repo:
            raise RuntimeError("run_mapping_for_shop requires product_repo and shop_repo")

        shop = await self._shop_repo.get_by_id(shop_id)
        if not shop:
            return {"status": "error", "message": f"Shop with id {shop_id} not found"}

        logger.info(
            "=== MAPPING RUN START: shop=%s (%s) ===",
            shop.shop_name,
            shop.external_shop_id,
        )

        products = await self._product_repo.list_by_shop(shop.external_shop_id)
        all_variants = await self._variant_repo.list_by_shop(shop.external_shop_id)

        deactivated_product_ids = {
            str(p.external_product_id or "").strip()
            for p in products
            if p.status == RecordStatus.DEACTIVATED
        }
        active_variants = [
            v
            for v in all_variants
            if v.status != RecordStatus.DELETED
            and str(v.external_product_id or "").strip() not in deactivated_product_ids
        ]
        skipped_deleted = len(all_variants) - len(active_variants)
        if skipped_deleted:
            logger.info(
                "Skipped %d DELETED/DEACTIVATED variant(s) dari mapping (shop='%s')",
                skipped_deleted,
                shop.shop_name,
            )

        logger.info(
            "Found %d variants across %d products for shop '%s' (%d DELETED di-skip)",
            len(active_variants),
            len(products),
            shop.shop_name,
            skipped_deleted,
        )

        total_mapped = 0
        total_not_found = 0
        total_duplicates = 0
        total_errors = 0
        total_skipped = 0
        total_blocked = 0
        results = []

        for variant in active_variants:
            result = await self._process_variant_for_mapping(variant)
            results.append(result)

            if result["status"] == "mapped":
                total_mapped += 1
            elif result["status"] == "conflict_blocked":
                total_blocked += 1
            elif result["status"] == "part_not_found":
                total_not_found += 1
            elif result["status"] == "duplicate_skipped":
                total_duplicates += 1
            elif result["status"] == "skipped":
                total_skipped += 1
            else:
                total_errors += 1

        validation_summary = {"total_conflicts": 0, "total_warnings": 0, "issues": []}
        if self._validation_service is not None and settings.mapping_validation_enabled:
            try:
                validation_summary = await self._validation_service.run_checks_for_shop(
                    shop.external_shop_id
                )
            except Exception as exc:
                logger.exception(
                    "Mapping validation failed (shop=%s): %s", shop.external_shop_id, exc
                )
                validation_summary = {
                    "total_conflicts": 0,
                    "total_warnings": 0,
                    "issues": [],
                    "error": str(exc),
                }

        summary = {
            "status": "success",
            "shop_id": str(shop_id),
            "total_variants": len(all_variants),
            "total_skipped_deleted": skipped_deleted,
            "total_mapped": total_mapped,
            "total_not_found": total_not_found,
            "total_duplicates": total_duplicates,
            "total_errors": total_errors,
            "total_skipped": total_skipped,
            "total_blocked": total_blocked,
            "validation": validation_summary,
            "results": results,
        }

        logger.info(
            "=== MAPPING RUN COMPLETE: %d variants (%d DELETED di-skip), "
            "%d mapped, %d blocked(conflict), %d not found, %d duplicates; "
            "validation: %d conflict(s), %d warning(s) ===",
            len(all_variants),
            skipped_deleted,
            total_mapped,
            total_blocked,
            total_not_found,
            total_duplicates,
            validation_summary.get("total_conflicts", 0),
            validation_summary.get("total_warnings", 0),
        )
        return summary

    async def _process_variant_for_mapping(self, variant: MarketplaceVariant) -> dict:
        """Process one variant — create if new, remap if already mapped."""
        if not variant.seller_sku or not variant.seller_sku.strip():
            return {
                "shop_id": variant.shop_id,
                "external_product_id": variant.external_product_id,
                "external_variant_id": variant.external_variant_id,
                "seller_sku": "",
                "status": "skipped",
                "message": "Empty seller_sku — cannot map",
            }

        try:
            parsed = SkuParser.parse(variant.seller_sku)

            mapping = await self._mapping_service.update_mapping_for_variant(
                variant.shop_id,
                variant.external_product_id or "",
                variant.external_variant_id or "",
            )

            faktor_pengali = SkuParser.compute_faktor_pengali(
                parsed.sku_type, parsed.conversion_qty
            )
            mapping_status = getattr(mapping, "status", "ACTIVE") or "ACTIVE"
            result_status = (
                "conflict_blocked" if mapping_status == "CONFLICT" else "mapped"
            )
            return {
                "shop_id": variant.shop_id,
                "external_product_id": variant.external_product_id,
                "external_variant_id": variant.external_variant_id,
                "seller_sku": variant.seller_sku,
                "sku_type": parsed.sku_type.value,
                "conversion_qty": faktor_pengali,
                "status": result_status,
                "inventree_part_id": str(mapping.inventree_part_id),
                "mapping_id": str(mapping.sku_mapping_id),
                "message": (
                    "BLOCKED: SKU_PACK berbagi part_id dengan SKU_S/SKU_X "
                    "ber-ANGKA qty sama — mapping status CONFLICT, tidak dipakai "
                    "pipeline utk push stok sampai part dipisah / di-review."
                    if result_status == "conflict_blocked"
                    else None
                ),
            }
        except InvenTreePartNotFoundError as e:
            parsed = SkuParser.parse(variant.seller_sku)
            faktor_pengali = SkuParser.compute_faktor_pengali(
                parsed.sku_type, parsed.conversion_qty
            )
            return {
                "shop_id": variant.shop_id,
                "external_product_id": variant.external_product_id,
                "external_variant_id": variant.external_variant_id,
                "seller_sku": variant.seller_sku,
                "sku_type": parsed.sku_type.value,
                "conversion_qty": faktor_pengali,
                "status": "part_not_found",
                "message": str(e),
            }
        except Exception as e:
            logger.error(
                "Error mapping variant (shop=%s, product=%s, variant=%s): %s",
                variant.shop_id,
                variant.external_product_id,
                variant.external_variant_id,
                e,
            )
            return {
                "shop_id": variant.shop_id,
                "external_product_id": variant.external_product_id,
                "external_variant_id": variant.external_variant_id,
                "seller_sku": variant.seller_sku,
                "status": "error",
                "message": str(e),
            }

    async def process_variant(
        self, shop_id: str, external_product_id: str, external_variant_id: str
    ) -> dict:
        """Process a marketplace variant — attempt to create a mapping."""
        try:
            mapping = await self._mapping_service.create_mapping(
                shop_id, external_product_id, external_variant_id
            )
            return {
                "status": "mapped",
                "mapping_id": str(mapping.sku_mapping_id),
                "sku_type": mapping.sku_type.value
                if hasattr(mapping.sku_type, "value")
                else mapping.sku_type,
                "conversion_qty": mapping.conversion_qty,
                "inventree_part_id": str(mapping.inventree_part_id),
            }
        except DuplicateMappingError as e:
            logger.warning("Duplicate mapping: %s", e)
            return {"status": "duplicate_skipped", "message": str(e)}
        except InvenTreePartNotFoundError as e:
            logger.warning("InvenTree part not found: %s", e)
            return {"status": "part_not_found", "seller_sku": e.seller_sku}
        except ValueError as e:
            logger.error(
                "Error processing variant (shop=%s, product=%s, variant=%s): %s",
                shop_id,
                external_product_id,
                external_variant_id,
                e,
            )
            return {"status": "error", "message": str(e)}

    async def reprocess_variant(
        self, shop_id: str, external_product_id: str, external_variant_id: str
    ) -> dict:
        """Reprocess an existing variant after an update."""
        try:
            mapping = await self._mapping_service.update_mapping_for_variant(
                shop_id, external_product_id, external_variant_id
            )
            return {
                "status": "remapped",
                "mapping_id": str(mapping.sku_mapping_id),
                "sku_type": mapping.sku_type.value
                if hasattr(mapping.sku_type, "value")
                else mapping.sku_type,
                "conversion_qty": mapping.conversion_qty,
                "inventree_part_id": str(mapping.inventree_part_id),
            }
        except InvenTreePartNotFoundError as e:
            logger.warning("Reprocess — InvenTree part not found: %s", e)
            return {"status": "part_not_found", "seller_sku": e.seller_sku}
        except Exception as e:
            logger.error(
                "Error reprocessing variant (shop=%s, product=%s, variant=%s): %s",
                shop_id,
                external_product_id,
                external_variant_id,
                e,
            )
            return {"status": "error", "message": str(e)}

    async def handle_variant_deletion(
        self, shop_id: str, external_product_id: str, external_variant_id: str
    ) -> None:
        """Soft-delete mapping when a variant is deleted."""
        await self._mapping_service.soft_delete_mapping(
            shop_id, external_product_id, external_variant_id
        )

    async def handle_product_deletion(
        self, shop_id: str, external_product_id: str
    ) -> None:
        """Soft-delete all variants + mappings when a product is deleted."""
        variants = await self._variant_repo.list_by_product(
            shop_id, external_product_id
        )
        for variant in variants:
            await self._mapping_service.soft_delete_mapping(
                variant.shop_id,
                variant.external_product_id or "",
                variant.external_variant_id or "",
            )
            await self._variant_service.soft_delete_variant(
                variant.shop_id,
                variant.external_product_id or "",
                variant.external_variant_id or "",
            )
