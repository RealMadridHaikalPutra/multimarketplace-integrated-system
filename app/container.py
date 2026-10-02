"""
Composition root — single place that wires repositories, services and use-case
engines for one ``AsyncSession``.

Previously the construction of engines (repos + services + client) was
duplicated across:

  - ``application/usecases/stock_sync_pipeline.py`` (per-shop sync + mapping)
  - ``interfaces/scheduling/polling_engine.py``
  - ``interfaces/http/routers/sync_v2.py``
  - ``interfaces/http/deps.py``

Centralising the wiring here means a constructor change is made in one place
and every caller stays consistent. This module replaces the old
``synchronization/engine_factory.py``.

The concrete market/inventory adapters (``ApisixClient`` / ``InvenTreeClient``)
are instantiated here — this is the *only* layer allowed to know both the
application ports and their infrastructure implementations.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from application.services.mapping_service import MappingService
from application.services.mapping_validation_service import MappingValidationService
from application.services.product_service import ProductService
from application.services.variant_service import VariantService
from application.usecases.mapping_engine import MappingEngine
from application.usecases.sync_engine import SyncEngine
from infrastructure.inventory.inventree.client import InvenTreeClient
from infrastructure.marketplace.apisix.client import ApisixClient
from infrastructure.marketplace.apisix.parsers.factory import get_parser
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
from infrastructure.repositories.sku_mapping_repository_impl import (
    SKUMappingRepositoryImpl,
)
from infrastructure.repositories.validation_issue_repository_impl import (
    ValidationIssueRepositoryImpl,
)


def create_sync_engine(session: AsyncSession, apisix_client=None) -> SyncEngine:
    """Build a ``SyncEngine`` for one session.

    Args:
        session: active ``AsyncSession`` (never shared across parallel tasks).
        apisix_client: optional gateway (``ApisixClient``); a new one is created
            when ``None``. Name kept for call-site compatibility with the old
            factory; the new ``SyncEngine`` receives it as ``gateway``.
    """
    mp_repo = MarketplaceRepositoryImpl(session)
    shop_repo = MarketplaceShopRepositoryImpl(session)
    product_repo = MarketplaceProductRepositoryImpl(session)
    variant_repo = MarketplaceVariantRepositoryImpl(session)
    mapping_repo = SKUMappingRepositoryImpl(session)

    return SyncEngine(
        gateway=(
            apisix_client if apisix_client is not None else ApisixClient()
        ),
        product_service=ProductService(
            marketplace_repo=mp_repo,
            shop_repo=shop_repo,
            product_repo=product_repo,
        ),
        variant_service=VariantService(variant_repo=variant_repo),
        parser_factory=get_parser,
        # Dipakai SyncEngine utk soft-delete mapping variant yang di-set
        # DELETED (stale variant cleanup saat GET product).
        mapping_service=MappingService(
            mapping_repo=mapping_repo,
            variant_repo=variant_repo,
            inventory_provider=InvenTreeClient(),
        ),
        shop_repo=shop_repo,
        product_repo=product_repo,
        variant_repo=variant_repo,
        marketplace_repo=mp_repo,
        # Gate TC-SKU-006 saat GET PRODUCT (skip product PACK+S/X ber-qty sama).
        validation_service=create_validation_service(session),
    )


def create_validation_service(session: AsyncSession) -> MappingValidationService:
    """Build a ``MappingValidationService`` for one session (used by engines & pipeline)."""
    return MappingValidationService(
        validation_repo=ValidationIssueRepositoryImpl(session),
        mapping_repo=SKUMappingRepositoryImpl(session),
        variant_repo=MarketplaceVariantRepositoryImpl(session),
        inventory_provider=InvenTreeClient(),
    )


def create_mapping_engine(session: AsyncSession) -> MappingEngine:
    """Build a ``MappingEngine`` for one session.

    A single ``InvenTreeClient`` is shared between ``MappingService`` and
    ``MappingValidationService`` (the stock view cache is shared).
    """
    shop_repo = MarketplaceShopRepositoryImpl(session)
    product_repo = MarketplaceProductRepositoryImpl(session)
    variant_repo = MarketplaceVariantRepositoryImpl(session)
    mapping_repo = SKUMappingRepositoryImpl(session)
    inventree_client = InvenTreeClient()

    validation_service = MappingValidationService(
        validation_repo=ValidationIssueRepositoryImpl(session),
        mapping_repo=mapping_repo,
        variant_repo=variant_repo,
        inventory_provider=inventree_client,
    )

    return MappingEngine(
        mapping_service=MappingService(
            mapping_repo=mapping_repo,
            variant_repo=variant_repo,
            inventory_provider=inventree_client,
        ),
        variant_service=VariantService(variant_repo=variant_repo),
        variant_repo=variant_repo,
        product_repo=product_repo,
        shop_repo=shop_repo,
        validation_service=validation_service,
    )
