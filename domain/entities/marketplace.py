import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from domain.enums import MarketplaceStatus


@dataclass
class Marketplace:
    id: Optional[uuid.UUID] = None
    code: str = ""
    name: str = ""
    status: MarketplaceStatus = MarketplaceStatus.ACTIVE
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
