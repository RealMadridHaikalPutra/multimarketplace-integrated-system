import logging
from typing import Optional

from domain.entities.marketplace import Marketplace as MarketplaceEntity
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_shop import MarketplaceShop
from domain.enums import MarketplaceStatus, RecordStatus
from domain.repositories.marketplace_product_repository import (
    MarketplaceProductRepository,
)
from domain.repositories.marketplace_repository import MarketplaceRepository
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository

logger = logging.getLogger(__name__)


class ProductService:
    """Product-level operations: create, update, delete, get-or-create shop."""

    def __init__(
        self,
        marketplace_repo: MarketplaceRepository,
        shop_repo: MarketplaceShopRepository,
        product_repo: MarketplaceProductRepository,
    ):
        self._marketplace_repo = marketplace_repo
        self._shop_repo = shop_repo
        self._product_repo = product_repo

    async def get_or_create_marketplace(
        self, code: str, name: str = ""
    ) -> MarketplaceEntity:
        marketplace = await self._marketplace_repo.get_by_code(code)
        if not marketplace:
            marketplace = MarketplaceEntity(
                code=code,
                name=name or code,
                status=MarketplaceStatus.ACTIVE,
            )
            marketplace = await self._marketplace_repo.create(marketplace)
            logger.info("Created marketplace '%s' (code=%s)", marketplace.name, code)
        return marketplace

    async def get_or_create_shop(
        self, marketplace_code: str, marketplace_shop_id: str, shop_name: str
    ) -> MarketplaceShop:
        marketplace = await self.get_or_create_marketplace(marketplace_code)

        shop = await self._shop_repo.get_by_external_id(
            marketplace.id, marketplace_shop_id
        )
        if not shop:
            shop = MarketplaceShop(
                marketplace_id=marketplace.id,
                external_shop_id=marketplace_shop_id,
                shop_name=shop_name,
                status=RecordStatus.ACTIVE,
            )
            shop = await self._shop_repo.create(shop)
            logger.info("Created shop '%s' (id=%s)", shop_name, marketplace_shop_id)
        elif shop_name and shop.shop_name != shop_name:
            old_name = shop.shop_name
            shop.shop_name = shop_name
            shop = await self._shop_repo.update(shop)
            logger.info("Updated shop name: '%s' -> '%s'", old_name, shop_name)

        return shop

    async def create_product(self, product: MarketplaceProduct) -> MarketplaceProduct:
        existing = await self._product_repo.get_by_external_id(
            product.shop_id, product.external_product_id
        )
        if existing:
            logger.warning(
                "Product already exists (shop=%s, product=%s). Updating instead.",
                product.shop_id,
                product.external_product_id,
            )
            return await self.update_product(product)

        created = await self._product_repo.create(product)
        logger.info(
            "Created product '%s' (shop=%s, product=%s)",
            created.product_name,
            created.shop_id,
            created.external_product_id,
        )
        return created

    async def update_product(self, product: MarketplaceProduct) -> MarketplaceProduct:
        existing = await self._product_repo.get_by_external_id(
            product.shop_id, product.external_product_id
        )
        if not existing:
            raise ValueError(
                f"Product not found (shop={product.shop_id}, "
                f"product={product.external_product_id})"
            )

        existing.product_name = product.product_name
        existing.product_image = product.product_image
        existing.category = product.category
        existing.description = product.description
        existing.status = product.status
        updated = await self._product_repo.update(existing)
        logger.info(
            "Updated product '%s' (shop=%s, product=%s)",
            updated.product_name,
            updated.shop_id,
            updated.external_product_id,
        )
        return updated

    async def get_product_by_external_id(
        self, shop_id: str, external_product_id: str
    ) -> Optional[MarketplaceProduct]:
        return await self._product_repo.get_by_external_id(
            shop_id, external_product_id
        )
