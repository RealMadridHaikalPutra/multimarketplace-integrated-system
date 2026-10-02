"""Domain entities (dataclasses) — pure business objects."""

from domain.entities.inventory_report import InventoryReport
from domain.entities.marketplace import Marketplace
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_shop import MarketplaceShop
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.entities.sku_mapping import SKUMapping
from domain.entities.stock_sync_log import StockSyncLog
from domain.entities.stock_sync_queue import StockSyncQueue
from domain.entities.validation_issue import ValidationIssue

__all__ = [
    "InventoryReport",
    "Marketplace",
    "MarketplaceProduct",
    "MarketplaceShop",
    "MarketplaceVariant",
    "SKUMapping",
    "StockSyncLog",
    "StockSyncQueue",
    "ValidationIssue",
]
