import uuid
from abc import ABC, abstractmethod
from typing import List, Optional

from domain.entities.sku_mapping import SKUMapping


class SKUMappingRepository(ABC):
    """Repository interface for the SKUMapping aggregate."""

    @abstractmethod
    async def create(self, mapping: SKUMapping) -> SKUMapping:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, mapping_id: uuid.UUID) -> Optional[SKUMapping]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_variant_id(
        self, marketplace_variant_id: uuid.UUID
    ) -> List[SKUMapping]:
        raise NotImplementedError

    @abstractmethod
    async def find_active_by_seller_sku(self, seller_sku: str) -> Optional[SKUMapping]:
        raise NotImplementedError

    @abstractmethod
    async def find_duplicate_by_seller_sku(
        self, seller_sku: str, inventree_part_id: int, sku_type_value: str
    ) -> Optional[SKUMapping]:
        raise NotImplementedError

    @abstractmethod
    async def find_active_by_part_id(
        self, inventree_part_id: int
    ) -> List[SKUMapping]:
        """List ALL ACTIVE mappings for one InvenTree part_id (conflict rules)."""
        raise NotImplementedError

    @abstractmethod
    async def find_active_part_ids_by_external_product_ids(
        self, external_product_ids: List[str]
    ) -> List[int]:
        """InvenTree part_id (ACTIVE) untuk daftar external_product_id marketplace.

        Jalur: marketplace_product.external_product_id -> marketplace_variant ->
        sku_mapping (ACTIVE) -> inventree_part_id. Hasil mengikuti urutan
        ``external_product_ids`` dan duplikat dibuang (dipakai jalur webhook
        untuk mengisi variabel ``partId``).
        """
        raise NotImplementedError

    @abstractmethod
    async def update(self, mapping: SKUMapping) -> SKUMapping:
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> List[SKUMapping]:
        raise NotImplementedError

    @abstractmethod
    async def soft_delete(self, mapping_id: uuid.UUID) -> None:
        raise NotImplementedError
