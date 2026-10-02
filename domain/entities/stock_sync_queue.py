import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StockSyncQueue:
    id: Optional[uuid.UUID] = None
    marketplace_variant_id: Optional[uuid.UUID] = None
    target_stock: int = 0
    trigger_source: str = ""
    status: str = "PENDING"
    retry_count: int = 0
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
