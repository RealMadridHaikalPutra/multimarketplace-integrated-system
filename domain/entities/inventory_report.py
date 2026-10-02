import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class InventoryReport:
    """Domain entity for InventoryReport — per-SKU stock calculation output."""

    id: Optional[int] = None
    part_id: int = 0  # InvenTree part_id (integer)
    seller_sku: str = ""
    sku_type: str = "NORMAL"
    stok_inventree: float = 0.0
    buffer: int = 0
    faktor_pengali: float = 1.0
    reserved_mp: int = 0
    reserved_inventree: float = 0.0
    qty_converted: float = 0.0
    stok_di_update: float = 0.0
    shop_id: Optional[str] = None
    marketplace_code: Optional[str] = None
    marketplace_variant_id: Optional[str] = None
    external_product_id: Optional[str] = None
    external_variant_id: Optional[str] = None
    inventree_part_uuid: Optional[uuid.UUID] = None
    generated_at: Optional[datetime] = None
