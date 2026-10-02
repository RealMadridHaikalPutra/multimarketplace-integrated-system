"""Pure domain rules (no I/O).

Each rule module is independently unit-testable.
"""

from domain.rules.sku import SkuParsedResult, SkuParser, sku_qty_of
from domain.rules.sku_conflict import (
    FAMILY_NORMAL,
    FAMILY_PACK,
    FAMILY_SX,
    category_family,
    detect_product_pack_sx_conflict,
)
from domain.rules.stock import StockCalculation, compute_stock_update

__all__ = [
    "SkuParsedResult",
    "SkuParser",
    "sku_qty_of",
    "FAMILY_NORMAL",
    "FAMILY_PACK",
    "FAMILY_SX",
    "category_family",
    "detect_product_pack_sx_conflict",
    "StockCalculation",
    "compute_stock_update",
]
