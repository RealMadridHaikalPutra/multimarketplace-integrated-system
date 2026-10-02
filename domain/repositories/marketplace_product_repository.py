import uuid
from abc import ABC, abstractmethod
from typing import List, Optional

from domain.entities.marketplace_product import MarketplaceProduct


class MarketplaceProductRepository(ABC):
    """Repository interface for the MarketplaceProduct aggregate."""

    @abstractmethod
    async def create(self, product: MarketplaceProduct) -> MarketplaceProduct:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(
        self, shop_id: uuid.UUID, marketplace_product_id: uuid.UUID
    ) -> Optional[MarketplaceProduct]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_external_id(
        self, shop_id: uuid.UUID, external_product_id: str
    ) -> Optional[MarketplaceProduct]:
        raise NotImplementedError

    @abstractmethod
    async def list_by_shop(self, shop_id: uuid.UUID) -> List[MarketplaceProduct]:
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> List[MarketplaceProduct]:
        """List ALL products across ALL shops (used by the global inventory cycle)."""
        raise NotImplementedError

    @abstractmethod
    async def update(self, product: MarketplaceProduct) -> MarketplaceProduct:
        raise NotImplementedError
