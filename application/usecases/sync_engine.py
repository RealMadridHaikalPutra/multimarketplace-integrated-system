"""SyncEngine — orchestrates the product sync pipeline.

1. Fetch products from the marketplace gateway (APISIX).
2. Parse the marketplace-specific response (parser injected via factory).
3. Persist products & variants (batch upsert).
4. Apply reserved-stock overrides, conflict gate, freeze/stale cleanup.

Behaviour is ported unchanged. The concrete gateway and parser factory are
injected so this use case does not import the infrastructure layer directly.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from application.ports import InventoryProvider, MarketplaceGateway
from application.services.mapping_service import MappingService
from application.services.mapping_validation_service import (
    MappingValidationService,
    product_entity_id,
)
from application.services.product_service import ProductService
from application.services.variant_service import VariantService
from core.config import settings
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_shop import MarketplaceShop
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from domain.repositories.marketplace_product_repository import (
    MarketplaceProductRepository,
)
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)

logger = logging.getLogger(__name__)


def build_reserved_stock_map(raw_data: Any) -> Dict[str, int]:
    """Build ``{variant_id: total_reserved_stock}`` from the /order payload."""
    reserved_map: Dict[str, int] = {}
    if not isinstance(raw_data, dict):
        return reserved_map

    entries = raw_data.get("reserved_stock")
    if not isinstance(entries, list):
        return reserved_map

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        variants = entry.get("variants")
        if not isinstance(variants, list):
            continue
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            variant_id = str(variant.get("variant_id") or "").strip()
            if not variant_id:
                continue
            total = variant.get("total_reserved_stock")
            if total is None:
                continue
            try:
                reserved_map[variant_id] = int(total)
            except (TypeError, ValueError):
                continue
    return reserved_map


def build_reserved_order_logs(
    raw_data: Any,
    marketplace: str,
    shop_external_id: str,
) -> List[Dict[str, Any]]:
    """Flatten visible /order rows into audit records for a pipeline log."""
    order_logs: List[Dict[str, Any]] = []
    if not isinstance(raw_data, dict):
        return order_logs

    for product in raw_data.get("reserved_stock") or []:
        if not isinstance(product, dict):
            continue
        for variant in product.get("variants") or []:
            if not isinstance(variant, dict):
                continue
            common = {
                "marketplace": marketplace,
                "shop_id": shop_external_id,
                "product_id": product.get("product_id"),
                "variant_id": variant.get("variant_id"),
                "seller_sku": variant.get("seller_sku"),
                "total_reserved_stock": variant.get("total_reserved_stock", 0),
            }
            orders = variant.get("orders")
            if not isinstance(orders, list):
                continue
            for order in orders:
                if not isinstance(order, dict):
                    continue
                order_logs.append(
                    {
                        **common,
                        "order_id": order.get("order_id"),
                        "status": order.get("status"),
                        "reserved_stock": order.get("reserved_stock", 0),
                    }
                )
    return order_logs


class SyncEngine:
    """Orchestrates product sync for one shop."""

    def __init__(
        self,
        gateway: MarketplaceGateway,
        product_service: ProductService,
        variant_service: VariantService,
        parser_factory: Optional[Callable[[str], Any]] = None,
        mapping_service: Optional[MappingService] = None,
        shop_repo: Optional[MarketplaceShopRepository] = None,
        product_repo: Optional[MarketplaceProductRepository] = None,
        variant_repo: Optional[MarketplaceVariantRepository] = None,
        marketplace_repo: Optional[Any] = None,
        validation_service: Optional[MappingValidationService] = None,
    ):
        self._gateway = gateway
        self._product_service = product_service
        self._variant_service = variant_service
        self._parser_factory = parser_factory
        self._mapping_service = mapping_service
        self._shop_repo = shop_repo
        self._product_repo = product_repo
        self._variant_repo = variant_repo
        self._marketplace_repo = marketplace_repo
        self._validation_service = validation_service
        self._last_reserved_order_logs: List[Dict[str, Any]] = []

    async def sync_by_shop_id(
        self, shop_id: str, status_filter: str = "ALL"
    ) -> Dict[str, Any]:
        """Sync products from the gateway for a given shop (no mapping here)."""
        if not self._shop_repo or not self._product_repo:
            raise RuntimeError("sync_by_shop_id requires shop_repo and product_repo")
        self._last_reserved_order_logs = []

        shop = await self._shop_repo.get_by_id(shop_id)
        if not shop:
            return {"status": "error", "message": f"Shop with id {shop_id} not found"}

        marketplace_code = "UNKNOWN"
        if self._marketplace_repo:
            marketplace = await self._marketplace_repo.get_by_id(shop.marketplace_id)
            if marketplace:
                marketplace_code = marketplace.code.upper()

        logger.info(
            "=== SYNC BY SHOP ID: shop=%s (%s), marketplace=%s, status_filter=%s ===",
            shop.shop_name,
            shop.external_shop_id,
            marketplace_code,
            status_filter,
        )

        raw_data = await self._gateway.fetch_products(
            marketplace_code.lower() if marketplace_code != "UNKNOWN" else "",
            shop.external_shop_id,
        )

        sweep_complete = raw_data.get("sweep_complete", True) is not False
        pages_fetched = int(raw_data.get("pages_fetched", 1) or 1)
        if not sweep_complete:
            logger.warning(
                "=== SYNC PRODUCT SWEEP TIDAK LENGKAP: shop=%s (%s), marketplace=%s, "
                "hanya %d halaman diambil — kemungkinan ada product terlewat ===",
                shop.shop_name,
                shop.external_shop_id,
                marketplace_code,
                pages_fetched,
            )

        if self._parser_factory is None:
            return {
                "status": "error",
                "message": "parser_factory not configured",
            }
        parser = self._parser_factory(marketplace_code.lower())
        if not parser:
            return {
                "status": "error",
                "message": f"No parser for marketplace '{marketplace_code}'",
            }

        parsed_products = parser.parse_products(raw_data)

        # TikTok listing-platform freeze (product absent from the TIKTOK_SHOP sweep).
        product_listing_platforms: Dict[str, List[str]] = (
            raw_data.get("product_listing_platforms") or {}
        )
        tiktok_missing_shop_platform = (
            marketplace_code.upper() == "TIKTOK"
            and sweep_complete
            and bool(product_listing_platforms)
        )
        if (
            marketplace_code.upper() == "TIKTOK"
            and not tiktok_missing_shop_platform
            and not sweep_complete
        ):
            logger.warning(
                "TikTok listing platform: sweep TIDAK lengkap (shop=%s, "
                "pages_fetched=%d) — paksaan FREEZE product yang hanya ada di "
                "sweep lain DILEWATI pada sync ini.",
                shop.external_shop_id,
                pages_fetched,
            )
        if tiktok_missing_shop_platform:
            forced = 0
            for p in parsed_products:
                platforms = product_listing_platforms.get(str(p.external_product_id))
                if platforms and "TIKTOK_SHOP" in platforms:
                    continue
                p.status = RecordStatus.FREEZE
                for v in p.variants or []:
                    v.status = RecordStatus.DEACTIVATED
                forced += 1
            if forced:
                logger.info(
                    "TikTok listing platform: %d product(s) tidak ada di sweep "
                    "TIKTOK_SHOP → status dipaksa FREEZE (shop=%s)",
                    forced,
                    shop.external_shop_id,
                )

        if status_filter and status_filter.upper() != "ALL":
            target_status = status_filter.upper()
            before = len(parsed_products)
            parsed_products = [
                p for p in parsed_products if p.status.value == target_status
            ]
            filtered_count = before - len(parsed_products)
            if filtered_count > 0:
                logger.info(
                    "Filtered out %d products with status != %s (kept %d)",
                    filtered_count,
                    target_status,
                    len(parsed_products),
                )

        reserved_map: Dict[str, int] = {}
        order_sweep_complete = False
        if marketplace_code.upper() in ("TIKTOK", "TIKTOKSANDBOX", "SHOPEE"):
            reserved_map, order_sweep_complete = await self._fetch_reserved_stock_map(
                marketplace_code.lower(), shop
            )

        stale_cleanup_enabled = sweep_complete and (
            not status_filter or status_filter.upper() == "ALL"
        )

        if not parsed_products:
            total_products_stale_frozen = 0
            if stale_cleanup_enabled:
                try:
                    total_products_stale_frozen = await self._freeze_stale_products(
                        shop, []
                    )
                except Exception as exc:
                    logger.warning(
                        "Stale product cleanup gagal (shop=%s): %s",
                        shop.external_shop_id,
                        exc,
                    )
            return {
                "status": "success",
                "shop_id": str(shop_id),
                "shop_name": shop.shop_name,
                "marketplace_code": marketplace_code,
                "total_products": 0,
                "total_variants": 0,
                "total_variants_stale_deleted": 0,
                "total_products_stale_frozen": total_products_stale_frozen,
                "pages_fetched": pages_fetched,
                "sweep_complete": sweep_complete,
                "message": "No products found in marketplace response",
                "details": [],
                "reserved_order_logs": self._last_reserved_order_logs,
            }

        if reserved_map:
            matched = self._apply_reserved_stock_overrides(parsed_products, reserved_map)
            if matched:
                logger.info(
                    "Reserved stock /order: %d variant(s) overridden by "
                    "variant_id (marketplace=%s, shop=%s)",
                    matched,
                    marketplace_code,
                    shop.external_shop_id,
                )

        if order_sweep_complete:
            zeroed = self._zero_absent_reserved_stock(parsed_products, reserved_map)
            if zeroed:
                logger.info(
                    "Reserved stock /order: %d variant(s) absent → "
                    "total_reserved_stock=0 (marketplace=%s, shop=%s)",
                    zeroed,
                    marketplace_code,
                    shop.external_shop_id,
                )

        total_variants_skipped_conflict = 0
        conflict_products: Dict[str, dict] = {}
        gate_enabled = (
            self._validation_service is not None and settings.mapping_validation_enabled
        )
        if gate_enabled:
            try:
                conflict_products = (
                    await self._validation_service.check_parsed_products_for_conflicts(
                        parsed_products,
                        shop_external_id=shop.external_shop_id,
                        marketplace_code=marketplace_code,
                        block=settings.sync_block_pack_sx_conflict,
                        resolve_stale=(
                            sweep_complete
                            and (not status_filter or status_filter.upper() == "ALL")
                        ),
                    )
                )
            except Exception as exc:
                logger.exception(
                    "GET PRODUCT conflict gate gagal (shop=%s): %s — lanjut sync "
                    "tanpa gate",
                    shop.external_shop_id,
                    exc,
                )

        should_skip_conflict = gate_enabled and settings.sync_block_pack_sx_conflict

        total_variants = 0
        total_variants_stale_deleted = 0
        total_products_stale_frozen = 0
        details = []
        all_variants: List[MarketplaceVariant] = []

        for parsed_product in parsed_products:
            product_entity_key = product_entity_id(
                shop.external_shop_id, parsed_product.external_product_id
            )
            info = conflict_products.get(product_entity_key)
            conflicting_skus = (
                set(info.get("conflict_skus", []))
                if (should_skip_conflict and info)
                else set()
            )

            product_detail, product_variants = await self._save_product_only(
                shop=shop,
                parsed_product=parsed_product,
                exclude_skus=conflicting_skus,
            )

            if tiktok_missing_shop_platform and product_listing_platforms.get(
                str(parsed_product.external_product_id)
            ) is not None:
                platforms = product_listing_platforms[
                    str(parsed_product.external_product_id)
                ]
                if "TIKTOK_SHOP" not in platforms:
                    await self._force_freeze_product(
                        shop, parsed_product, product_variants
                    )

            if stale_cleanup_enabled:
                try:
                    stale_deleted = await self._soft_delete_stale_variants(
                        shop, parsed_product
                    )
                    total_variants_stale_deleted += stale_deleted
                except Exception as exc:
                    logger.warning(
                        "Stale variant cleanup gagal (shop=%s, product=%s): %s",
                        shop.external_shop_id,
                        parsed_product.external_product_id,
                        exc,
                    )

            if info:
                product_detail["conflict_pairs"] = info.get("conflict_pairs", [])
                product_detail["conflict_variants"] = sorted(
                    info.get("conflict_skus", [])
                )
                product_detail["conflict_action"] = (
                    "variant_skipped" if should_skip_conflict else "reported_only"
                )
                if should_skip_conflict:
                    total_variants_skipped_conflict += len(
                        info.get("conflict_skus", [])
                    )
                    await self._soft_delete_existing_variants(
                        shop.external_shop_id,
                        parsed_product.external_product_id,
                        conflicting_skus,
                    )

            total_variants += product_detail.get("variants_count", 0)
            all_variants.extend(product_variants)
            details.append(product_detail)

        if stale_cleanup_enabled:
            try:
                total_products_stale_frozen = await self._freeze_stale_products(
                    shop, parsed_products
                )
            except Exception as exc:
                logger.warning(
                    "Stale product cleanup gagal (shop=%s): %s",
                    shop.external_shop_id,
                    exc,
                )

        if all_variants:
            try:
                affected = await self._variant_service.upsert_variants(all_variants)
                logger.info(
                    "Batch variant upsert: %d variant(s) dalam 1 statement "
                    "(shop=%s, affected=%d)",
                    len(all_variants),
                    shop.external_shop_id,
                    affected,
                )
            except Exception as exc:
                logger.exception(
                    "Batch variant upsert gagal — fallback create_variant per-variant "
                    "(shop=%s): %s",
                    shop.external_shop_id,
                    exc,
                )
                saved = 0
                for variant in all_variants:
                    try:
                        await self._variant_service.create_variant(variant)
                        saved += 1
                    except Exception as ve:
                        logger.warning(
                            "Fallback create_variant gagal (sku=%s): %s",
                            variant.seller_sku,
                            ve,
                        )
                logger.warning(
                    "Fallback create_variant selesai: %d/%d tersimpan (shop=%s)",
                    saved,
                    len(all_variants),
                    shop.external_shop_id,
                )

        result = {
            "status": "success",
            "shop_id": str(shop_id),
            "shop_name": shop.shop_name,
            "marketplace_code": marketplace_code,
            "total_products": len(parsed_products),
            "total_variants": total_variants,
            "total_variants_skipped_conflict": total_variants_skipped_conflict,
            "total_variants_stale_deleted": total_variants_stale_deleted,
            "total_products_stale_frozen": total_products_stale_frozen,
            "pages_fetched": pages_fetched,
            "sweep_complete": sweep_complete,
            "details": details,
            "reserved_order_logs": self._last_reserved_order_logs,
        }

        if total_variants_skipped_conflict:
            logger.warning(
                "=== SYNC BY SHOP COMPLETE: %d products, %d variants, "
                "%d variant SKIPPED (pasangan PACK+S/X konflik) ===",
                len(parsed_products),
                total_variants,
                total_variants_skipped_conflict,
            )
        else:
            logger.info(
                "=== SYNC BY SHOP COMPLETE: %d products, %d variants ===",
                len(parsed_products),
                total_variants,
            )
        return result

    async def _fetch_reserved_stock_map(
        self,
        marketplace_code: str,
        shop: MarketplaceShop,
    ) -> Tuple[Dict[str, int], bool]:
        raw = await self._gateway.fetch_reserved_stock(
            marketplace_code, shop.external_shop_id
        )
        if not isinstance(raw, dict):
            return {}, False
        order_sweep_complete = raw.get("sweep_complete", True) is not False
        if not order_sweep_complete:
            logger.error(
                "Reserved stock /order sweep TIDAK LENGKAP (marketplace=%s, shop=%s, "
                "pages_fetched=%s) — reserved stock parsial dipakai; sebagian variant "
                "fallback ke nilai payload product.",
                marketplace_code,
                shop.external_shop_id,
                raw.get("pages_fetched"),
            )
        self._last_reserved_order_logs = build_reserved_order_logs(
            raw, marketplace_code, shop.external_shop_id
        )
        return build_reserved_stock_map(raw), order_sweep_complete

    @staticmethod
    def _apply_reserved_stock_overrides(
        parsed_products: List[Any],
        reserved_map: Dict[str, int],
    ) -> int:
        matched = 0
        for parsed_product in parsed_products:
            for variant_data in parsed_product.variants or []:
                variant_id = str(variant_data.external_variant_id or "").strip()
                if variant_id and variant_id in reserved_map:
                    variant_data.reserved_stock = reserved_map[variant_id]
                    matched += 1
        return matched

    @staticmethod
    def _zero_absent_reserved_stock(
        parsed_products: List[Any],
        reserved_map: Dict[str, int],
    ) -> int:
        zeroed = 0
        for parsed_product in parsed_products:
            for variant_data in parsed_product.variants or []:
                variant_id = str(variant_data.external_variant_id or "").strip()
                if not variant_id:
                    continue
                if variant_id not in reserved_map:
                    variant_data.reserved_stock = 0
                    zeroed += 1
        return zeroed

    async def _soft_delete_stale_variants(
        self,
        shop: MarketplaceShop,
        parsed_product: Any,
    ) -> int:
        if not self._variant_repo:
            return 0
        payload_ids = {
            str(v.external_variant_id or "").strip()
            for v in (parsed_product.variants or [])
            if v.external_variant_id
        }
        try:
            existing = await self._variant_repo.list_by_product(
                shop.external_shop_id, parsed_product.external_product_id
            )
        except Exception as exc:
            logger.warning(
                "Stale variant list_by_product gagal (shop=%s, product=%s): %s",
                shop.external_shop_id,
                parsed_product.external_product_id,
                exc,
            )
            return 0

        deleted = 0
        for variant in existing:
            variant_id = str(variant.external_variant_id or "").strip()
            if not variant_id or variant_id in payload_ids:
                continue
            if variant.status == RecordStatus.DELETED:
                continue
            try:
                await self._variant_repo.soft_delete(
                    variant.shop_id,
                    variant.external_product_id or "",
                    variant.external_variant_id or "",
                )
                if self._mapping_service is not None:
                    try:
                        await self._mapping_service.soft_delete_mapping(
                            variant.shop_id,
                            variant.external_product_id or "",
                            variant.external_variant_id or "",
                        )
                    except Exception as me:
                        logger.warning(
                            "Soft-delete mapping variant %s gagal (product=%s): %s",
                            variant.marketplace_variant_id,
                            parsed_product.external_product_id,
                            me,
                        )
                deleted += 1
            except Exception as ve:
                logger.warning(
                    "Soft-delete stale variant %s gagal (product=%s): %s",
                    variant.marketplace_variant_id,
                    parsed_product.external_product_id,
                    ve,
                )

        if deleted:
            logger.info(
                "Stale variant cleanup: %d variant soft-deleted (shop=%s, product=%s)",
                deleted,
                shop.external_shop_id,
                parsed_product.external_product_id,
            )
        return deleted

    async def _freeze_stale_products(
        self,
        shop: MarketplaceShop,
        parsed_products: List[Any],
    ) -> int:
        if not self._product_repo:
            return 0
        payload_ids = {
            str(p.external_product_id or "").strip()
            for p in parsed_products
            if p.external_product_id
        }
        try:
            existing = await self._product_repo.list_by_shop(shop.external_shop_id)
        except Exception as exc:
            logger.warning(
                "Stale product list_by_shop gagal (shop=%s): %s",
                shop.external_shop_id,
                exc,
            )
            return 0

        frozen = 0
        for product in existing:
            product_id = str(product.external_product_id or "").strip()
            if not product_id or product_id in payload_ids:
                continue
            if product.status == RecordStatus.FREEZE:
                continue
            try:
                product.status = RecordStatus.FREEZE
                await self._product_repo.update(product)
                await self._deactivate_product_variants(
                    shop.external_shop_id, product_id
                )
                frozen += 1
            except Exception as exc:
                logger.warning(
                    "Freeze stale product %s gagal (shop=%s): %s",
                    product_id,
                    shop.external_shop_id,
                    exc,
                )

        if frozen:
            logger.info(
                "Stale product cleanup: %d product FREEZE + variant "
                "DEACTIVATED (shop=%s, marketplace=%s)",
                frozen,
                shop.external_shop_id,
                shop.marketplace_id,
            )
        return frozen

    async def _force_freeze_product(
        self,
        shop: MarketplaceShop,
        parsed_product: Any,
        variants: List[MarketplaceVariant],
    ) -> None:
        entity = MarketplaceProduct(
            marketplace_product_id=(
                shop.external_shop_id + "_" + parsed_product.external_product_id
            ),
            shop_id=shop.external_shop_id,
            external_product_id=parsed_product.external_product_id,
            product_name=parsed_product.product_name,
            product_image=parsed_product.product_image,
            category=parsed_product.category,
            description=parsed_product.description,
            status=RecordStatus.FREEZE,
        )
        try:
            if self._product_service is not None:
                await self._product_service.update_product(entity)
        except Exception as exc:
            logger.warning(
                "Force freeze product %s gagal (shop=%s): %s",
                parsed_product.external_product_id,
                shop.external_shop_id,
                exc,
            )
        for variant in variants or []:
            variant.status = RecordStatus.DEACTIVATED
        logger.info(
            "Product %s (shop=%s) tidak ada di sweep TIKTOK_SHOP → FREEZE, "
            "%d variant DEACTIVATED",
            parsed_product.external_product_id,
            shop.external_shop_id,
            len(variants or []),
        )

    async def _deactivate_product_variants(
        self,
        shop_external_id: str,
        external_product_id: str,
    ) -> None:
        if not self._variant_repo:
            return
        try:
            existing = await self._variant_repo.list_by_product(
                shop_external_id, external_product_id
            )
        except Exception as exc:
            logger.warning(
                "Stale product variant list_by_product gagal (shop=%s, product=%s): %s",
                shop_external_id,
                external_product_id,
                exc,
            )
            return

        deactivated = 0
        for variant in existing:
            if variant.status in (RecordStatus.DELETED, RecordStatus.DEACTIVATED):
                continue
            try:
                variant.status = RecordStatus.DEACTIVATED
                await self._variant_repo.update(variant)
                deactivated += 1
            except Exception as ve:
                logger.warning(
                    "Deactivate variant %s gagal (product=%s): %s",
                    variant.marketplace_variant_id,
                    external_product_id,
                    ve,
                )
        if deactivated:
            logger.info(
                "Stale product cleanup: %d variant DEACTIVATED "
                "(shop=%s, product=%s) — mapping dipertahankan",
                deactivated,
                shop_external_id,
                external_product_id,
            )

    async def _soft_delete_existing_variants(
        self,
        shop_external_id: str,
        external_product_id: str,
        conflicting_skus: Optional[set] = None,
    ) -> None:
        conflicting_skus = conflicting_skus or set()
        if not self._variant_repo or not conflicting_skus:
            return
        try:
            existing = await self._variant_repo.list_by_product(
                shop_external_id, external_product_id
            )
            to_delete = [
                v
                for v in existing
                if (v.seller_sku or "").strip() in conflicting_skus
            ]
            for variant in to_delete:
                try:
                    await self._variant_repo.soft_delete(
                        variant.shop_id,
                        variant.external_product_id or "",
                        variant.external_variant_id or "",
                    )
                except Exception as ve:
                    logger.warning(
                        "Soft-delete variant %s gagal (product=%s): %s",
                        variant.marketplace_variant_id,
                        external_product_id,
                        ve,
                    )
            if to_delete:
                logger.warning(
                    "Soft-deleted %d variant pasangan konflik lama "
                    "(PACK+S/X): shop=%s product=%s",
                    len(to_delete),
                    shop_external_id,
                    external_product_id,
                )
        except Exception as exc:
            logger.warning(
                "Cleanup variant pasangan konflik gagal (shop=%s product=%s): %s",
                shop_external_id,
                external_product_id,
                exc,
            )

    async def _save_product_only(
        self,
        shop: MarketplaceShop,
        parsed_product: Any,
        exclude_skus: Optional[set] = None,
    ) -> Tuple[Dict[str, Any], List[MarketplaceVariant]]:
        exclude_skus = exclude_skus or set()
        try:
            product = MarketplaceProduct(
                marketplace_product_id=shop.external_shop_id
                + "_"
                + parsed_product.external_product_id,
                shop_id=shop.external_shop_id,
                external_product_id=parsed_product.external_product_id,
                product_name=parsed_product.product_name,
                product_image=parsed_product.product_image,
                category=parsed_product.category,
                description=parsed_product.description,
                status=parsed_product.status,
            )
            product = await self._product_service.create_product(product)

            variants: List[MarketplaceVariant] = []
            for variant_data in parsed_product.variants or []:
                seller_sku = (variant_data.seller_sku or "").strip()
                if not seller_sku:
                    continue
                if seller_sku in exclude_skus:
                    continue

                variant = MarketplaceVariant(
                    marketplace_variant_id=shop.external_shop_id
                    + "_"
                    + variant_data.external_variant_id,
                    marketplace_product_id=product.marketplace_product_id,
                    shop_id=shop.external_shop_id,
                    external_product_id=product.external_product_id,
                    external_variant_id=variant_data.external_variant_id,
                    variant_name=variant_data.variant_name,
                    seller_sku=variant_data.seller_sku,
                    total_reserved_stock=variant_data.reserved_stock,
                    total_available_stock=variant_data.available_stock,
                    location_id=variant_data.location_id,
                    status=variant_data.status or RecordStatus.ACTIVE,
                )
                variants.append(variant)

            return {
                "shop_id": str(product.shop_id) if product.shop_id else None,
                "external_product_id": product.external_product_id,
                "product_name": parsed_product.product_name,
                "variants_count": len(variants),
            }, variants

        except Exception as e:
            logger.exception(
                "Error saving product '%s': %s",
                parsed_product.external_product_id,
                e,
            )
            return {
                "external_product_id": parsed_product.external_product_id,
                "product_name": parsed_product.product_name,
                "variants_count": 0,
                "error": str(e),
            }, []
