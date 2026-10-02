import uuid
from abc import ABC, abstractmethod
from typing import List, Optional

from domain.entities.marketplace import Marketplace
from domain.enums import MarketplaceStatus


class MarketplaceRepository(ABC):
    """Repository interface for the Marketplace aggregate."""

    @abstractmethod
    async def create(self, marketplace: Marketplace) -> Marketplace:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, marketplace_id: uuid.UUID) -> Optional[Marketplace]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_code(self, code: str) -> Optional[Marketplace]:
        raise NotImplementedError

    @abstractmethod
    async def list_all(
        self, status: Optional[MarketplaceStatus] = None
    ) -> List[Marketplace]:
        raise NotImplementedError

    @abstractmethod
    async def update(self, marketplace: Marketplace) -> Marketplace:
        raise NotImplementedError
