import logging
from typing import List, Optional

from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.parsers.base import (
    MarketplaceParser,
    ParsedProduct,
    ParsedVariant,
)

logger = logging.getLogger(__name__)


class TikTokParser(MarketplaceParser):
    """Parses TikTok Shop product data returned via APISIX."""

    def parse_products(self, raw_data: dict) -> List[ParsedProduct]:
        products = []
        raw_products = raw_data.get("products") or raw_data.get("data", {}).get(
            "products", []
        )
        for prod in raw_products:
            product = self._parse_single_product(prod)
            if product:
                products.append(product)
        logger.info("Parsed %d products from TikTok response", len(products))
        return products

    def _parse_single_product(self, prod: dict) -> Optional[ParsedProduct]:
        try:
            product_id = str(prod.get("external_product_id") or prod.get("id") or "")
            if not product_id:
                return None

            title = prod.get("product_name") or prod.get("title") or ""
            status = self._parse_status(prod.get("status"))

            detail_data = prod.get("_detail", {}).get("data", {})

            image = prod.get("product_image")
            if not image:
                main_images = detail_data.get("main_images", [])
                if main_images:
                    urls = main_images[0].get("urls", [])
                    if urls:
                        image = urls[0]

            category = prod.get("category")
            if not category:
                category_chains = detail_data.get("category_chains", [])
                if category_chains:
                    leaf = category_chains[-1]
                    category = leaf.get("local_name")

            description = prod.get("description") or detail_data.get("description")

            raw_skus = prod.get("skus") or detail_data.get("skus", [])
            variants = []
            for sku in raw_skus:
                variant = self._parse_sku(sku)
                if variant:
                    variants.append(variant)

            return ParsedProduct(
                external_product_id=product_id,
                product_name=title,
                product_image=image,
                category=category,
                description=description,
                status=status,
                variants=variants,
            )
        except Exception as e:
            logger.exception("Error parsing TikTok product: %s", e)
            return None

    def _parse_sku(self, sku: dict) -> Optional[ParsedVariant]:
        """Parse a SKU, preferring ``inventory[0]`` for stock + warehouse."""
        try:
            sku_id = str(sku.get("external_variant_id") or sku.get("id") or "")
            seller_sku = sku.get("seller_sku", "") or ""
            status = self._parse_status(sku.get("status"))

            stock: Optional[int] = None
            available_stock: Optional[int] = None
            reserved_stock: Optional[int] = None
            location_id: Optional[str] = sku.get("location_id")

            inventory_list = sku.get("inventory") or []
            if inventory_list:
                first_inv = inventory_list[0]
                available_stock = first_inv.get(
                    "available_stock", first_inv.get("quantity")
                )
                reserved_stock = first_inv.get("reserved_stock", 0)
                location_id = first_inv.get("location_id") or location_id
                if available_stock is None and reserved_stock is None:
                    logger.warning(
                        "TikTok SKU %s: inventory[0] has no available_stock/"
                        "reserved_stock/quantity keys — emitting 0. payload=%s",
                        sku_id,
                        first_inv,
                    )
            else:
                logger.debug(
                    "TikTok SKU %s has no inventory[] — falling back to top-level totals",
                    sku_id,
                )
                available_stock = sku.get("total_available_stock")
                reserved_stock = sku.get("total_reserved_stock")
                stock = sku.get("total_stock")

            if stock is None:
                if available_stock is not None and reserved_stock is not None:
                    stock = available_stock + reserved_stock
                else:
                    stock = available_stock if available_stock is not None else reserved_stock
            if stock is None:
                stock = available_stock if available_stock is not None else 0
            if available_stock is None:
                available_stock = stock
            if reserved_stock is None:
                reserved_stock = 0

            variant_name = sku.get("variant_name") or (
                seller_sku if seller_sku else f"Variant {sku_id}"
            )

            return ParsedVariant(
                external_variant_id=sku_id,
                variant_name=variant_name,
                seller_sku=seller_sku,
                stock=stock,
                reserved_stock=reserved_stock,
                available_stock=available_stock,
                location_id=location_id,
                status=status,
            )
        except Exception as e:
            logger.exception("Error parsing TikTok SKU: %s", e)
            return None

    @staticmethod
    def _parse_status(value: Optional[str]) -> RecordStatus:
        normalized = (value or "").strip().upper()
        if normalized in {"ACTIVE", "ACTIVATE", "ENABLED"}:
            return RecordStatus.ACTIVE
        if normalized in {"INACTIVE", "DISABLED"}:
            return RecordStatus.INACTIVE
        if normalized in {"DELETED", "REMOVED"}:
            return RecordStatus.DELETED
        if normalized == "FREEZE":
            return RecordStatus.FREEZE
        return RecordStatus.ACTIVE
