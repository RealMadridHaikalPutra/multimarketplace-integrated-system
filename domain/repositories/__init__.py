"""Repository interfaces (contracts) implemented by the infrastructure layer."""

from domain.repositories.inventory_report_repository import InventoryReportRepository
from domain.repositories.marketplace_product_repository import (
    MarketplaceProductRepository,
)
from domain.repositories.marketplace_repository import MarketplaceRepository
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)
from domain.repositories.sku_mapping_repository import SKUMappingRepository
from domain.repositories.validation_issue_repository import ValidationIssueRepository

__all__ = [
    "InventoryReportRepository",
    "MarketplaceProductRepository",
    "MarketplaceRepository",
    "MarketplaceShopRepository",
    "MarketplaceVariantRepository",
    "SKUMappingRepository",
    "ValidationIssueRepository",
]
