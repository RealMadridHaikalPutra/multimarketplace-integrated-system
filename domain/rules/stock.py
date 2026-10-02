"""Stock calculation formula — pure domain logic.

Reference formula (unchanged):

    qty_converted   = reserved_mp × faktor_pengali
    stok_di_update  = floor(max(0, (stok_inventree
                                     − part_reserved_converted
                                     − reserved_inventree
                                     − buffer_effective) / faktor_pengali))

- ``part_reserved_converted`` is the SUM of converted reservations for ALL
  SKUs mapped to the same part (inventory-wide), so it is subtracted ONCE per
  part to avoid double counting across SKUs of the same part.
- Result is floored and clamped to a minimum of 0 to prevent overselling.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class StockCalculation:
    qty_converted: float
    stok_di_update: float
    target_stock: int


def compute_stock_update(
    *,
    stok_inventree: float,
    part_reserved_converted: float,
    reserved_inventree: float,
    buffer_effective: int,
    reserved_mp: int,
    faktor_pengali: float,
) -> StockCalculation:
    """Compute the stock quantity to push for one SKU."""
    qty_converted = float(reserved_mp) * faktor_pengali
    stok_di_update_raw = (
        stok_inventree
        - part_reserved_converted
        - reserved_inventree
        - buffer_effective
    ) / faktor_pengali
    stok_di_update = math.floor(max(0.0, stok_di_update_raw))
    return StockCalculation(
        qty_converted=qty_converted,
        stok_di_update=stok_di_update,
        target_stock=int(stok_di_update),
    )
