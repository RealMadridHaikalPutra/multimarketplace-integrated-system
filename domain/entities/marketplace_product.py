from dataclasses import dataclass
from typing import Optional
import uuid

from domain.enums import RecordStatus


@dataclass
class MarketplaceProduct:
    marketplace_product_id: Optional[uuid.UUID] = None
    shop_id: Optional[uuid.UUID] = None  # FK to marketplace_shop.id
    external_product_id: str = ""
    product_name: str = ""
    product_image: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    status: RecordStatus = RecordStatus.ACTIVE

    @property
    def composite_id(self) -> tuple:
        """Return the composite primary key as a tuple."""
        return (self.shop_id, self.external_product_id)

    @property
    def marketplace_shop_id(self) -> Optional[uuid.UUID]:
        """Backward-compatible alias for the shop FK."""
        return self.shop_id
