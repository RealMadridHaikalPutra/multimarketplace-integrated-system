from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import UUID

from domain.entities.marketplace_shop import MarketplaceShop


class MarketplaceShopRepository(ABC):
    """Repository interface for the MarketplaceShop aggregate."""

    @abstractmethod
    async def create(self, shop: MarketplaceShop) -> MarketplaceShop:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, shop_id: str) -> Optional[MarketplaceShop]:
        """Lookup by external_shop_id."""
        raise NotImplementedError

    @abstractmethod
    async def get_by_external_id(
        self, marketplace_id: UUID, external_shop_id: str
    ) -> Optional[MarketplaceShop]:
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> List[MarketplaceShop]:
        raise NotImplementedError

    @abstractmethod
    async def list_by_marketplace(
        self, marketplace_id: UUID
    ) -> List[MarketplaceShop]:
        raise NotImplementedError

    @abstractmethod
    async def update(self, shop: MarketplaceShop) -> MarketplaceShop:
        raise NotImplementedError
