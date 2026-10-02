"""SKU parsing & conversion rules — pure domain logic.

Moved from ``infrastructure/mapping/sku_parser.py`` so the business rule no
longer depends on the infrastructure layer. Behaviour is unchanged.

Patterns:
    NORMAL     "1A1"            → qty=1
    SKU_X      "1A1-50x"        → qty=50
    SKU_S      "1A1-50s"        → qty=50
    SKU_PACK   "1A1-Pack-50"    → qty=50
    SKU_PACK   "1A1-Pack-100"   → qty=100
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from domain.enums import SkuType


@dataclass
class SkuParsedResult:
    """Result of parsing a seller SKU string."""

    base_sku: str
    sku_type: SkuType
    conversion_qty: int


class SkuParser:
    """Parses seller SKU strings into base SKU + type + raw qty."""

    # Regex order matters: more specific patterns first.
    PACK_PATTERN = re.compile(r"^(.*)-Pack-(\d+)$", re.IGNORECASE)
    X_PATTERN = re.compile(r"^(.*)-(\d+)x$", re.IGNORECASE)
    S_PATTERN = re.compile(r"^(.*)-(\d+)s$", re.IGNORECASE)

    @classmethod
    def parse(cls, seller_sku: str) -> SkuParsedResult:
        if not seller_sku or not seller_sku.strip():
            raise ValueError("seller_sku cannot be empty")

        seller_sku = seller_sku.strip()

        match = cls.PACK_PATTERN.match(seller_sku)
        if match:
            return SkuParsedResult(
                base_sku=match.group(1),
                sku_type=SkuType.SKU_PACK,
                conversion_qty=int(match.group(2)),
            )

        match = cls.X_PATTERN.match(seller_sku)
        if match:
            return SkuParsedResult(
                base_sku=match.group(1),
                sku_type=SkuType.SKU_X,
                conversion_qty=int(match.group(2)),
            )

        match = cls.S_PATTERN.match(seller_sku)
        if match:
            return SkuParsedResult(
                base_sku=match.group(1),
                sku_type=SkuType.SKU_S,
                conversion_qty=int(match.group(2)),
            )

        return SkuParsedResult(
            base_sku=seller_sku,
            sku_type=SkuType.NORMAL,
            conversion_qty=1,
        )

    @classmethod
    def parse_qty(cls, seller_sku: Optional[str]) -> Optional[int]:
        """Raw qty from an SKU string without raising.

        Examples: ``1A1-Pack-50`` → 50, ``1A1-50s`` → 50, ``1A1`` → 1.
        Returns ``None`` for empty / unparsable input.
        """
        if not seller_sku or not str(seller_sku).strip():
            return None
        try:
            return cls.parse(str(seller_sku)).conversion_qty
        except Exception:
            return None

    @classmethod
    def compute_faktor_pengali(cls, sku_type, conversion_qty: int) -> float:
        """Conversion factor from SKU type + raw qty to base InvenTree units.

        - SKU_X    ("50x"):      1/qty  (1 InvenTree unit = 50 SKU units)
        - SKU_S    ("50s"):      1.0    (1:1 with physical stock)
        - SKU_PACK ("Pack-50"):  50.0   (1 pack = 50 physical units)
        - NORMAL:                1.0
        """
        qty = conversion_qty if conversion_qty and conversion_qty > 0 else 1
        type_str = sku_type.value if hasattr(sku_type, "value") else str(sku_type or "")
        type_str = type_str.upper()

        if type_str == SkuType.SKU_X.value:
            return 1.0 / qty
        if type_str == SkuType.SKU_PACK.value:
            return float(qty)
        return 1.0


def sku_qty_of(mapping) -> Optional[int]:
    """Raw qty number of a mapping's ``seller_sku`` (``Pack-50`` → 50).

    Basis of the TC-SKU-006 rule: PACK vs SKU_S/SKU_X on the same part only
    conflicts when the qty NUMBER is equal (Pack-50 vs 50s → conflict;
    Pack-50 vs 2s → allowed). ``None`` means "unknown" → no conflict.
    """
    return SkuParser.parse_qty(getattr(mapping, "seller_sku", None))
