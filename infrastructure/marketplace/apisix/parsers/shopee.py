import logging
from typing import List, Optional

from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.parsers.base import (
    MarketplaceParser,
    ParsedProduct,
    ParsedVariant,
)

logger = logging.getLogger(__name__)


class ShopeeParser(MarketplaceParser):
    """Parses Shopee product data returned via APISIX (normalized or raw)."""

    def parse_products(self, raw_data: dict) -> List[ParsedProduct]:
        products = []
        raw_items = raw_data.get("products") or raw_data.get("response", {}).get(
            "item", []
        )
        for item in raw_items:
            product = self._parse_single_item(
                item, is_normalized_format="products" in raw_data
            )
            if product:
                products.append(product)
        logger.info("Parsed %d products from Shopee response", len(products))
        return products

    def _parse_single_item(
        self, item: dict, is_normalized_format: bool = False
    ) -> Optional[ParsedProduct]:
        try:
            if is_normalized_format:
                return self._parse_normalized_item(item)
            return self._parse_raw_item(item)
        except Exception as e:
            logger.exception("Error parsing Shopee item: %s", e)
            return None

    def _parse_normalized_item(self, item: dict) -> Optional[ParsedProduct]:
        item_id = str(item.get("external_product_id", ""))
        if not item_id:
            return None

        item_name = item.get("product_name", "")

        image_raw = item.get("product_image")
        image = None
        if isinstance(image_raw, list) and image_raw:
            image = image_raw[0]
        elif isinstance(image_raw, str):
            image = image_raw

        category = item.get("category")
        if category is not None:
            category = str(category)

        description = item.get("description") or None

        raw_skus = item.get("skus") or []
        # APISIX serializes empty skus as dict {} for products without a model.
        if isinstance(raw_skus, dict):
            raw_skus = [raw_skus] if raw_skus else []
        variants = []
        for sku in raw_skus:
            variant = self._parse_sku_normalized(sku)
            if variant and variant.seller_sku and variant.seller_sku.strip():
                variants.append(variant)

        # Product without skus → synthetic variant from item level.
        if not variants:
            item_variant = self._parse_item_as_single_variant(item)
            if item_variant:
                variants.append(item_variant)

        product_status = RecordStatus.ACTIVE
        raw_status = item.get("status", "ACTIVE")
        if isinstance(raw_status, str):
            normalized = raw_status.strip().upper()
            if normalized == "ACTIVE":
                product_status = RecordStatus.ACTIVE
            elif normalized == "INACTIVE":
                product_status = RecordStatus.INACTIVE
            elif normalized == "DELETED":
                product_status = RecordStatus.DELETED
            elif normalized == "FREEZE":
                product_status = RecordStatus.FREEZE

        return ParsedProduct(
            external_product_id=item_id,
            product_name=item_name,
            product_image=image,
            category=category,
            description=description,
            variants=variants,
            status=product_status,
        )

    def _parse_raw_item(self, item: dict) -> Optional[ParsedProduct]:
        item_id = str(item.get("item_id", ""))
        if not item_id:
            return None

        detail = item.get("_detail", {})
        item_name = detail.get("item_name", "") or item.get("item_name", "")

        image = None
        if detail.get("image"):
            image_list = detail["image"].get("image_url_list", [])
            if image_list:
                image = image_list[0]

        category = None
        category_id = detail.get("category_id")
        if category_id is not None:
            category = str(category_id)

        description = None
        desc_info = detail.get("description_info", {})
        extended = desc_info.get("extended_description", {})
        field_list = extended.get("field_list", [])
        if field_list:
            texts = []
            for field in field_list:
                if field.get("field_type") == "text":
                    text = field.get("text", "")
                    if text:
                        texts.append(text)
            if texts:
                description = "\n".join(texts)

        model_raw = item.get("_model_raw", {})
        models = model_raw.get("model", [])

        variants = []
        for model in models:
            variant = self._parse_model_raw(model)
            if variant and variant.seller_sku and variant.seller_sku.strip():
                variants.append(variant)

        if not variants:
            item_variant = self._parse_item_as_single_variant(item, detail)
            if item_variant:
                variants.append(item_variant)

        return ParsedProduct(
            external_product_id=item_id,
            product_name=item_name,
            product_image=image,
            category=category,
            description=description,
            variants=variants,
        )

    def _parse_sku_normalized(self, sku: dict) -> Optional[ParsedVariant]:
        """Parse a single SKU from the normalized APISIX format."""
        try:
            variant_id = sku.get("external_variant_id") or None
            seller_sku = sku.get("seller_sku", "") or ""
            variant_name = sku.get("variant_name") or (seller_sku if seller_sku else None)

            return ParsedVariant(
                external_variant_id=variant_id,
                variant_name=variant_name,
                seller_sku=seller_sku,
                stock=sku.get("total_stock"),
                reserved_stock=sku.get("total_reserved_stock"),
                available_stock=sku.get("total_available_stock"),
                location_id=sku.get("location_id"),
                status=RecordStatus.ACTIVE
                if sku.get("status", "ACTIVE") == "ACTIVE"
                else RecordStatus.INACTIVE,
            )
        except Exception as e:
            logger.exception("Error parsing normalized Shopee SKU: %s", e)
            return None

    def _parse_item_as_single_variant(
        self, item: dict, detail: Optional[dict] = None
    ) -> Optional[ParsedVariant]:
        """Build a synthetic single variant for a Shopee product WITHOUT a model.

        Returns None when the item has no ``item_sku``/``seller_sku`` at all.
        """
        try:
            detail = detail or {}
            item_id = str(item.get("external_product_id") or item.get("item_id") or "")
            if not item_id:
                return None

            # seller_sku sources: skus dict (normalized), item-level, raw detail.
            skus_raw = item.get("skus") or {}
            skus_seller_sku = ""
            if isinstance(skus_raw, dict):
                if skus_raw.get("seller_sku") is not None:
                    skus_seller_sku = str(skus_raw.get("seller_sku") or "").strip()
                else:
                    for _k, _v in skus_raw.items():
                        if isinstance(_v, dict):
                            skus_seller_sku = str(_v.get("seller_sku") or "").strip()
                        break

            item_sku = str(
                skus_seller_sku
                or item.get("seller_sku")
                or item.get("item_sku")
                or detail.get("item_sku")
                or ""
            ).strip()
            if not item_sku:
                logger.warning(
                    "Shopee item %s tanpa model & tanpa item_sku/seller_sku "
                    "di payload — variant sintetis dilewati (produk 0 variant).",
                    item_id,
                )
                return None

            stock_info = item.get("stock_info_v2", {}) or {}
            summary = stock_info.get("summary_info", {}) or {}
            seller_stock = stock_info.get("seller_stock", []) or []

            inventory_list = item.get("inventory") or []
            first_inventory = inventory_list[0] if inventory_list else {}

            available_stock = summary.get("total_available_stock")
            if available_stock is None:
                available_stock = item.get("total_available_stock")
            if available_stock is None:
                available_stock = item.get("total_available")
            if available_stock is None:
                available_stock = first_inventory.get("available_stock")
            if available_stock is None:
                available_stock = item.get("stock_available")

            reserved_stock = summary.get("total_reserved_stock")
            if reserved_stock is None:
                reserved_stock = item.get("total_reserved_stock")
            if reserved_stock is None:
                reserved_stock = item.get("total_reserved")
            if reserved_stock is None:
                reserved_stock = first_inventory.get("reserved_stock")
            if reserved_stock is None:
                reserved_stock = item.get("stock_reserved")

            stock = item.get("total_stock")
            if stock is None:
                stock = item.get("stock")
            if stock is None and available_stock is not None and reserved_stock is not None:
                stock = available_stock + reserved_stock
            if stock is None:
                stock = first_inventory.get("available_stock")

            location_id = item.get("location_id")
            if seller_stock:
                location_id = location_id or seller_stock[0].get("location_id") or None
            if not location_id:
                location_id = first_inventory.get("location_id") or None

            return ParsedVariant(
                external_variant_id=item_id,
                variant_name=item_sku,
                seller_sku=item_sku,
                stock=stock,
                reserved_stock=reserved_stock,
                available_stock=available_stock,
                location_id=location_id,
            )
        except Exception as e:
            logger.exception("Error parsing Shopee item-as-variant: %s", e)
            return None

    def _parse_model_raw(self, model: dict) -> Optional[ParsedVariant]:
        """Parse a single model/variant from the raw Shopee format."""
        try:
            model_id = str(model.get("model_id", ""))
            model_sku = model.get("model_sku", "") or ""
            model_name = model.get("model_name", "")

            stock_info = model.get("stock_info_v2", {})
            summary = stock_info.get("summary_info", {})
            seller_stock = stock_info.get("seller_stock", [])

            available_stock = summary.get("total_available_stock", 0)
            reserved_stock = summary.get("total_reserved_stock", 0)

            stock = None
            location_id = None
            if seller_stock:
                stock = seller_stock[0].get("stock", available_stock)
                location_id = seller_stock[0].get("location_id") or None

            return ParsedVariant(
                external_variant_id=model_id,
                variant_name=model_name,
                seller_sku=model_sku,
                stock=stock,
                reserved_stock=reserved_stock,
                available_stock=available_stock,
                location_id=location_id,
            )
        except Exception as e:
            logger.exception("Error parsing Shopee model: %s", e)
            return None
