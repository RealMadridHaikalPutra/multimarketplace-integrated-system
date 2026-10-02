from abc import ABC, abstractmethod
from typing import List, Optional

from domain.entities.inventory_report import InventoryReport


class InventoryReportRepository(ABC):
    """Repository interface for InventoryReport (mapping + stock output)."""

    @abstractmethod
    async def create(self, report: InventoryReport) -> InventoryReport:
        raise NotImplementedError

    @abstractmethod
    async def list_for_shop(
        self, shop_id: str, marketplace_code: Optional[str] = None, limit: int = 1000
    ) -> List[InventoryReport]:
        raise NotImplementedError

    @abstractmethod
    async def list_latest_for_seller_sku(
        self, seller_sku: str, limit: int = 1
    ) -> List[InventoryReport]:
        raise NotImplementedError
