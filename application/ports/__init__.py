"""Application ports (interfaces to external systems).

These ``Protocol`` definitions let application use cases depend on *contracts*
instead of concrete infrastructure classes. The infrastructure adapters
(``ApisixClient``, ``InvenTreeClient``) structurally satisfy these protocols.

This is the key dependency-rule fix: ``application`` must not import
``infrastructure``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class MarketplaceGateway(Protocol):
    """APISIX unified gateway contract."""

    async def fetch_products(self, marketplace: str, shop_uuid: str) -> Dict[str, Any]:
        ...

    async def fetch_reserved_stock(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: Optional[str] = None,
        variant_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        ...

    async def update_status(
        self, marketplace: str, shop_uuid: str, product_id: str, status: str
    ) -> Dict[str, Any]:
        ...

    async def update_stock(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        variant_id: str,
        stock: int,
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...

    async def update_stock_batch(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        skus: List[Dict[str, Any]],
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...


@runtime_checkable
class InventoryProvider(Protocol):
    """InvenTree inventory source contract."""

    async def find_part_by_sku(self, seller_sku: str) -> Optional[int]:
        ...

    async def get_all_stock(self) -> List[dict]:
        ...

    async def get_stock_snapshot_by_part_id(self, part_id: int) -> Optional[dict]:
        ...


__all__ = ["MarketplaceGateway", "InventoryProvider"]
