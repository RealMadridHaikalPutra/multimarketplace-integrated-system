from dataclasses import dataclass
from typing import Optional
import uuid

from domain.enums import RecordStatus


@dataclass
class MarketplaceVariant:
    marketplace_variant_id: Optional[uuid.UUID] = None
    marketplace_product_id: Optional[uuid.UUID] = None
    external_variant_id: Optional[str] = None
    variant_name: Optional[str] = None
    seller_sku: str = ""
    total_reserved_stock: Optional[int] = None
    total_available_stock: Optional[int] = None
    location_id: Optional[str] = None
    status: RecordStatus = RecordStatus.ACTIVE
    shop_id: Optional[uuid.UUID] = None
    external_product_id: Optional[str] = None

    @property
    def composite_id(self) -> tuple:
        """Return the composite primary key as a tuple."""
        return (self.marketplace_product_id, self.external_variant_id)

    @property
    def marketplace_shop_id(self) -> Optional[uuid.UUID]:
        """Backward-compatible alias for the shop FK."""
        return self.shop_id
