import logging

from application.dto.requests import (
    SkuCreatedRequest,
    SkuUpdatedRequest,
    SkuDeletedRequest,
)
from application.services.product_service import ProductService
from application.services.variant_service import VariantService
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from application.usecases.mapping_engine import MappingEngine

logger = logging.getLogger(__name__)


class SkuEventHandler:
    """
    Handles variant-level events from marketplace webhooks.
    """

    def __init__(
        self,
        product_service: ProductService,
        variant_service: VariantService,
        mapping_engine: MappingEngine,
    ):
        self._product_service = product_service
        self._variant_service = variant_service
        self._mapping_engine = mapping_engine

    async def handle_variant_created(self, payload: SkuCreatedRequest) -> dict:
        shop = await self._product_service.get_or_create_shop(
            payload.marketplace_code,
            payload.shop_external_id,
            "",
        )

        product = await self._product_service.get_product_by_external_id(
            shop.id, payload.external_product_id
        )
        if not product:
            product = MarketplaceProduct(
                shop_id=shop.id,
                external_product_id=payload.external_product_id,
                product_name=payload.variant_name or "",
                status=RecordStatus.ACTIVE,
            )
            product = await self._product_service.create_product(product)

        variant = MarketplaceVariant(
            marketplace_product_id=product.marketplace_product_id,
            shop_id=shop.id,
            external_product_id=product.external_product_id,
            external_variant_id=payload.external_variant_id,
            variant_name=payload.variant_name,
            seller_sku=payload.seller_sku,
            total_reserved_stock=payload.reserved_stock,
            total_available_stock=payload.available_stock,
            location_id=payload.location_id,
            status=RecordStatus.ACTIVE,
        )
        variant = await self._variant_service.create_variant(variant)

        mapping_result = await self._mapping_engine.process_variant(
            str(variant.shop_id) if variant.shop_id else "",
            variant.external_product_id or "",
            variant.external_variant_id or "",
        )

        return {
            "status": "success",
            "shop_id": variant.shop_id,
            "external_product_id": variant.external_product_id,
            "external_variant_id": variant.external_variant_id,
            "mapping": mapping_result,
        }

    async def handle_variant_updated(self, payload: SkuUpdatedRequest) -> dict:
        shop = await self._product_service.get_or_create_shop(
            payload.marketplace_code,
            payload.shop_external_id,
            "",
        )
        product = await self._product_service.get_product_by_external_id(
            shop.id, payload.external_product_id
        )
        if not product:
            return {"status": "error", "message": f"Product {payload.external_product_id} not found"}

        if payload.external_variant_id:
            variant = await self._variant_service.get_by_external_variant_id(
                str(shop.id), product.external_product_id, payload.external_variant_id
            )
        else:
            variant = None

        if not variant:
            return {"status": "error", "message": f"Variant {payload.external_variant_id} not found"}

        if payload.seller_sku is not None:
            variant.seller_sku = payload.seller_sku
        if payload.variant_name is not None:
            variant.variant_name = payload.variant_name
        if payload.reserved_stock is not None:
            variant.total_reserved_stock = payload.reserved_stock
        if payload.available_stock is not None:
            variant.total_available_stock = payload.available_stock
        if payload.location_id is not None:
            variant.location_id = payload.location_id

        variant = await self._variant_service.update_variant(variant)

        mapping_result = await self._mapping_engine.reprocess_variant(
            str(variant.shop_id) if variant.shop_id else "",
            variant.external_product_id or "",
            variant.external_variant_id or "",
        )

        return {
            "status": "success",
            "shop_id": variant.shop_id,
            "external_product_id": variant.external_product_id,
            "external_variant_id": variant.external_variant_id,
            "mapping": mapping_result,
        }

    async def handle_variant_deleted(self, payload: SkuDeletedRequest) -> dict:
        shop = await self._product_service.get_or_create_shop(
            payload.marketplace_code,
            payload.shop_external_id,
            "",
        )
        product = await self._product_service.get_product_by_external_id(
            shop.id, payload.external_product_id
        )
        if not product:
            return {"status": "not_found", "external_product_id": payload.external_product_id}

        if payload.external_variant_id:
            variant = await self._variant_service.get_by_external_variant_id(
                str(shop.id), product.external_product_id, payload.external_variant_id
            )
        else:
            variant = None

        if not variant:
            return {"status": "not_found", "external_variant_id": payload.external_variant_id}

        await self._mapping_engine.handle_variant_deletion(
            str(variant.shop_id) if variant.shop_id else "",
            variant.external_product_id or "",
            variant.external_variant_id or "",
        )
        await self._variant_service.soft_delete_variant(
            str(variant.shop_id) if variant.shop_id else "",
            variant.external_product_id or "",
            variant.external_variant_id or "",
        )

        return {
            "status": "success",
            "shop_id": variant.shop_id,
            "external_product_id": variant.external_product_id,
            "external_variant_id": variant.external_variant_id,
        }
