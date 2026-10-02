import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from domain.enums import SkuType


@dataclass
class SKUMapping:
    sku_mapping_id: Optional[uuid.UUID] = None
    marketplace_variant_id: Optional[uuid.UUID] = None
    inventree_part_id: Optional[int] = None
    sku_type: SkuType = SkuType.NORMAL
    # Faktor pengali (SKU_X=1/qty, SKU_S=1, SKU_PACK=qty, NORMAL=1) — sama
    # dengan aturan faktor_pengali di inventory_report, BUKAN qty mentah SKU.
    conversion_qty: float = 1.0
    status: str = "ACTIVE"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    # Denormalized (hasil JOIN) untuk kemudahan query di use case.
    shop_id: Optional[uuid.UUID] = None
    external_product_id: Optional[str] = None
    external_variant_id: Optional[str] = None
    seller_sku: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.status == "ACTIVE"

    @property
    def is_deleted(self) -> bool:
        return self.status in ("DELETED", "INACTIVE")
