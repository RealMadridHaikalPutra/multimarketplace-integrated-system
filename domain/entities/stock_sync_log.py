import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StockSyncLog:
    id: Optional[uuid.UUID] = None
    marketplace_variant_id: Optional[uuid.UUID] = None
    inventree_part_id: Optional[uuid.UUID] = None
    reference_no: Optional[str] = None
    source: str = ""
    previous_stock: int = 0
    target_stock: int = 0
    final_stock: int = 0
    status: str = "SUCCESS"
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
