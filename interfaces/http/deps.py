import logging
from typing import AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from application.events.product_events import ProductEventHandler
from application.events.sku_events import SkuEventHandler
from application.services.product_service import ProductService
from application.services.variant_service import VariantService
from domain.repositories.marketplace_product_repository import MarketplaceProductRepository
from domain.repositories.marketplace_repository import MarketplaceRepository
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from domain.repositories.marketplace_variant_repository import MarketplaceVariantRepository
from infrastructure.db.database import get_db_session
from infrastructure.repositories.marketplace_product_repository_impl import (
    MarketplaceProductRepositoryImpl,
)
from infrastructure.repositories.marketplace_repository_impl import (
    MarketplaceRepositoryImpl,
)
from infrastructure.repositories.marketplace_shop_repository_impl import (
    MarketplaceShopRepositoryImpl,
)
from infrastructure.repositories.marketplace_variant_repository_impl import (
    MarketplaceVariantRepositoryImpl,
)
from app.container import create_mapping_engine
from application.handlers.product_handler import InventoryHandler

logger = logging.getLogger(__name__)


def _get_mp_repo(session: AsyncSession) -> MarketplaceRepository:
    return MarketplaceRepositoryImpl(session)


def _get_shop_repo(session: AsyncSession) -> MarketplaceShopRepository:
    return MarketplaceShopRepositoryImpl(session)


def _get_marketplace_product_repo(session: AsyncSession) -> MarketplaceProductRepository:
    return MarketplaceProductRepositoryImpl(session)


def _get_variant_repo(session: AsyncSession) -> MarketplaceVariantRepository:
    return MarketplaceVariantRepositoryImpl(session)


async def get_product_event_handler(
    session: AsyncSession = Depends(get_db_session),
) -> AsyncGenerator[ProductEventHandler, None]:
    try:
        product_service = ProductService(
            marketplace_repo=_get_mp_repo(session),
            shop_repo=_get_shop_repo(session),
            product_repo=_get_marketplace_product_repo(session),
        )
        variant_service = VariantService(variant_repo=_get_variant_repo(session))

        yield ProductEventHandler(
            product_service=product_service,
            variant_service=variant_service,
            mapping_engine=create_mapping_engine(session),
        )
    finally:
        await session.close()


async def get_sku_event_handler(
    session: AsyncSession = Depends(get_db_session),
) -> AsyncGenerator[SkuEventHandler, None]:
    try:
        product_service = ProductService(
            marketplace_repo=_get_mp_repo(session),
            shop_repo=_get_shop_repo(session),
            product_repo=_get_marketplace_product_repo(session),
        )
        variant_service = VariantService(variant_repo=_get_variant_repo(session))

        yield SkuEventHandler(
            product_service=product_service,
            variant_service=variant_service,
            mapping_engine=create_mapping_engine(session),
        )
    finally:
        await session.close()


async def get_inventory_handler() -> AsyncGenerator[InventoryHandler, None]:
    yield InventoryHandler()
