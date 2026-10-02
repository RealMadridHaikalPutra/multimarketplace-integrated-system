import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from domain.enums import RecordStatus


@dataclass
class MarketplaceShop:
    id: Optional[uuid.UUID] = None
    marketplace_id: Optional[uuid.UUID] = None  # FK to marketplace
    external_shop_id: str = ""
    shop_name: str = ""
    default_warehouse_id: Optional[str] = None  # per-shop warehouse for stock push
    status: RecordStatus = RecordStatus.ACTIVE
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def marketplace_shop_id(self) -> Optional[uuid.UUID]:
        """Backward-compatible alias for the primary key."""
        return self.id
