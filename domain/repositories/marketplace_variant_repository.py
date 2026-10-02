from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import UUID

from domain.entities.marketplace_variant import MarketplaceVariant


class MarketplaceVariantRepository(ABC):
    """Repository interface for the MarketplaceVariant aggregate."""

    @abstractmethod
    async def create(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(
        self, shop_id: UUID, external_product_id: str, external_variant_id: str
    ) -> Optional[MarketplaceVariant]:
        """Lookup by composite key."""
        raise NotImplementedError

    @abstractmethod
    async def get_by_external_variant_id(
        self, shop_id: UUID, external_product_id: str, external_variant_id: str
    ) -> Optional[MarketplaceVariant]:
        """Lookup by composite key."""
        raise NotImplementedError

    @abstractmethod
    async def get_by_pk(
        self, marketplace_variant_id: str
    ) -> Optional[MarketplaceVariant]:
        """Direct lookup by the composite PK column ``marketplace_variant_id``."""
        raise NotImplementedError

    @abstractmethod
    async def list_by_product(
        self, shop_id: UUID, external_product_id: str
    ) -> List[MarketplaceVariant]:
        raise NotImplementedError

    @abstractmethod
    async def list_by_shop(self, shop_external_id: str) -> List[MarketplaceVariant]:
        """List ALL variants of one shop in a SINGLE query (no N+1)."""
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> List[MarketplaceVariant]:
        """List ALL variants across ALL shops (global inventory cycle)."""
        raise NotImplementedError

    @abstractmethod
    async def list_by_seller_sku(self, seller_sku: str) -> List[MarketplaceVariant]:
        raise NotImplementedError

    @abstractmethod
    async def list_all_seller_skus(self) -> List[str]:
        """List ALL distinct seller_skus across all shops (SKU parity check 2.2)."""
        raise NotImplementedError

    @abstractmethod
    async def update(self, variant: MarketplaceVariant) -> MarketplaceVariant:
        raise NotImplementedError

    @abstractmethod
    async def upsert_many(self, variants: List[MarketplaceVariant]) -> int:
        """Batch upsert all variants in a single PostgreSQL statement.

        Returns the number of affected rows (0 when the list is empty).
        """
        raise NotImplementedError

    @abstractmethod
    async def soft_delete(
        self, shop_id: UUID, external_product_id: str, external_variant_id: str
    ) -> None:
        raise NotImplementedError
