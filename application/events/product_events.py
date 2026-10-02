import logging

from application.dto.requests import (
    ProductCreatedRequest,
    ProductUpdatedRequest,
    ProductDeletedRequest,
    VariantRequest,
)
from application.services.product_service import ProductService
from application.services.variant_service import VariantService
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_shop import MarketplaceShop
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from application.usecases.mapping_engine import MappingEngine

logger = logging.getLogger(__name__)


class ProductEventHandler:
    """
    Handles product-level events from marketplace webhooks.
    Coordinates ProductService, VariantService, and MappingEngine.
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

    async def handle_product_created(self, payload: ProductCreatedRequest) -> dict:
        shop = await self._product_service.get_or_create_shop(
            payload.marketplace_code,
            payload.shop_external_id,
            payload.shop_name,
        )

        product = MarketplaceProduct(
            shop_id=shop.id,
            external_product_id=payload.external_product_id,
            product_name=payload.product_name,
            product_image=payload.product_image,
            category=payload.category,
            description=payload.description,
            status=RecordStatus.ACTIVE,
        )
        product = await self._product_service.create_product(product)

        mapping_results = []
        for variant_data in payload.variants:
            variant = await self._save_variant(
                shop,
                product,
                variant_data,
            )
            if variant:
                result = await self._mapping_engine.process_variant(
                    variant.shop_id,
                    variant.external_product_id or "",
                    variant.external_variant_id or "",
                )
                mapping_results.append(result)

        return {
            "status": "success",
            "shop_id": product.shop_id,
            "external_product_id": product.external_product_id,
            "product_name": product.product_name,
            "mappings": mapping_results,
        }

    async def handle_product_updated(self, payload: ProductUpdatedRequest) -> dict:
        shop = await self._product_service.get_or_create_shop(
            payload.marketplace_code,
            payload.shop_external_id,
            payload.shop_name or "",
        )

        product = MarketplaceProduct(
            shop_id=shop.id,
            external_product_id=payload.external_product_id,
            product_name=payload.product_name or "",
            product_image=payload.product_image,
            category=payload.category,
            description=payload.description,
            status=RecordStatus.ACTIVE,
        )
        product = await self._product_service.update_product(product)

        mapping_results = []
        for variant_data in payload.variants:
            variant = await self._save_variant(
                shop,
                product,
                variant_data,
            )
            if variant:
                result = await self._mapping_engine.process_variant(
                    variant.shop_id,
                    variant.external_product_id or "",
                    variant.external_variant_id or "",
                )
                mapping_results.append(result)

        return {
            "status": "success",
            "shop_id": product.shop_id,
            "external_product_id": product.external_product_id,
            "mappings": mapping_results,
        }

    async def handle_product_deleted(self, payload: ProductDeletedRequest) -> dict:
        marketplace = await self._product_service._marketplace_repo.get_by_code(
            payload.marketplace_code
        )
        if not marketplace:
            return {
                "status": "not_found",
                "message": f"Marketplace '{payload.marketplace_code}' not found",
            }

        shop = await self._product_service._shop_repo.get_by_external_id(
            marketplace.id, payload.shop_external_id
        )
        if not shop:
            return {
                "status": "not_found",
                "message": f"Shop '{payload.shop_external_id}' not found for marketplace",
            }

        product = await self._product_service.get_product_by_external_id(
            shop.id, payload.external_product_id
        )
        if not product:
            return {"status": "not_found", "external_product_id": payload.external_product_id}

        await self._mapping_engine.handle_product_deletion(product.shop_id, product.external_product_id)

        return {
            "status": "success",
            "external_product_id": payload.external_product_id,
        }

    async def _save_variant(
        self, shop: MarketplaceShop, product: MarketplaceProduct, variant_data: VariantRequest
    ):
        variant = MarketplaceVariant(
            shop_id=shop.id,
            external_product_id=product.external_product_id,
            marketplace_product_id=product.marketplace_product_id,
            external_variant_id=variant_data.external_variant_id,
            variant_name=variant_data.variant_name,
            seller_sku=variant_data.seller_sku,
            total_reserved_stock=variant_data.reserved_stock,
            total_available_stock=variant_data.available_stock,
            location_id=variant_data.location_id,
            status=RecordStatus.ACTIVE,
        )

        try:
            created = await self._variant_service.create_variant(variant)
            return created
        except Exception as e:
            logger.error("Failed to save variant %s: %s", variant_data.external_variant_id, e)
            return None
