import logging
from typing import Dict, List, Optional

import httpx

from core.config import settings

logger = logging.getLogger(__name__)


class InvenTreeStockItem:
    """Represents a single item from the InvenTree stock view."""

    name: str
    part_id: int
    qty_stock: int
    sku_toko: str

    def __init__(self, data: dict):
        self.name = data.get("name", "")
        self.part_id = data.get("part_id", 0)
        self.qty_stock = data.get("qty_stock", 0)
        self.sku_toko = data.get("sku_toko", "")


class InvenTreeStockSnapshot:
    """Represents one part entry from the stockAvailableSnapshot endpoint."""

    def __init__(self, data: dict):
        self.part_id = int(data.get("part_id", 0) or 0)
        self.stock_available = float(data.get("stock_available", 0) or 0)
        self.stock_reserved_others = float(data.get("stock_reserved_others", 0) or 0)
        self.stock_reserved_shopee = float(data.get("stock_reserved_shopee", 0) or 0)
        self.stock_reserved_tokopedia = float(
            data.get("stock_reserved_tokopedia", 0) or 0
        )
        self.stock_reserved_total = float(data.get("stock_reserved_total", 0) or 0)
        self.stok_buffer = float(data.get("stok_buffer", 0) or 0)
        self.updated_at = str(data.get("updated_at", ""))

    @property
    def reserved_inventree(self) -> float:
        """Reserved InvenTree stock = stock_reserved_total."""
        return self.stock_reserved_total

    def to_dict(self) -> dict:
        return {
            "part_id": self.part_id,
            "stock_available": self.stock_available,
            "stock_reserved_shopee": self.stock_reserved_shopee,
            "stock_reserved_others": self.stock_reserved_others,
            "stock_reserved_tokopedia": self.stock_reserved_tokopedia,
            "stock_reserved_total": self.stock_reserved_total,
            "stok_buffer": self.stok_buffer,
            "reserved_inventree": self.reserved_inventree,
            "updated_at": self.updated_at,
        }


class InvenTreeClient:
    """HTTP client for the InvenTree stock view & stock snapshot APIs.

    - vwStockAvailableInventree : lookup part_id by seller SKU (mapping).
    - stockAvailableSnapshot    : stock per part for the pipeline formula.

    Results are cached in-memory for the lifetime of the instance with O(1)
    lookup indexes, to avoid linear scans in the per-variant hot loop.
    """

    def __init__(self):
        self.stock_view_url = settings.inventree_stock_view_url.rstrip("/")
        self.stock_snapshot_url = settings.inventree_stock_snapshot_url.rstrip("/")
        self._stock_items: Optional[List[InvenTreeStockItem]] = None
        self._stock_snapshot: Optional[List[InvenTreeStockSnapshot]] = None
        # Indexes built once after the first fetch.
        self._sku_index: Optional[Dict[str, InvenTreeStockItem]] = None
        self._part_index: Optional[Dict[int, InvenTreeStockItem]] = None
        self._snapshot_index: Optional[Dict[int, dict]] = None
        # Shared HTTP client — reuse connections across calls.
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        return self._client

    async def aclose(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    async def find_part_by_sku(self, seller_sku: str) -> Optional[int]:
        """Look up an InvenTree Part ID by seller SKU (case-insensitive)."""
        if not seller_sku or not seller_sku.strip():
            logger.warning("Empty seller_sku — skipping InvenTree search")
            return None

        sku = seller_sku.strip().lower()
        items = await self._fetch_stock_view()
        if not items:
            return None

        item = self._sku_index.get(sku) if self._sku_index else None
        if item is not None:
            logger.info(
                "InvenTree stock view: found part_id=%s for SKU '%s'",
                item.part_id,
                seller_sku,
            )
            return item.part_id

        logger.warning(
            "InvenTree stock view: SKU '%s' not found in %d items", seller_sku, len(items)
        )
        return None

    async def get_stock_by_sku(self, seller_sku: str) -> Optional[dict]:
        """Get detailed stock info for a seller SKU from the stock view."""
        if not seller_sku or not seller_sku.strip():
            return None

        sku = seller_sku.strip().lower()
        items = await self._fetch_stock_view()
        if not items:
            return None

        item = self._sku_index.get(sku) if self._sku_index else None
        if item is not None:
            return {
                "name": item.name,
                "part_id": item.part_id,
                "qty_stock": item.qty_stock,
                "sku_toko": item.sku_toko,
            }
        return None

    async def get_stock_by_part_id(self, part_id: int) -> Optional[dict]:
        """Get stock info for a part_id from the stock view (first match wins)."""
        items = await self._fetch_stock_view()
        if not items:
            return None

        item = self._part_index.get(part_id) if self._part_index else None
        if item is not None:
            return {
                "name": item.name,
                "part_id": item.part_id,
                "qty_stock": item.qty_stock,
                "sku_toko": item.sku_toko,
            }
        return None

    async def get_all_stock(self) -> List[dict]:
        """Get the full stock view data as a list of dicts."""
        items = await self._fetch_stock_view()
        return [
            {
                "name": item.name,
                "part_id": item.part_id,
                "qty_stock": item.qty_stock,
                "sku_toko": item.sku_toko,
            }
            for item in items
        ]

    async def find_part_by_keywords(self, keywords: str) -> Optional[int]:
        """Look up a part by searching the stock view name/sku, with legacy fallback."""
        if not keywords:
            return None

        kw = keywords.strip().lower()
        items = await self._fetch_stock_view()
        if items:
            for item in items:
                if kw in item.name.lower() or kw in item.sku_toko.lower():
                    logger.info(
                        "Found part_id=%s for keyword '%s' via stock view",
                        item.part_id,
                        keywords,
                    )
                    return item.part_id

        return await self._legacy_search_parts({"keywords": keywords, "limit": 5})

    async def get_stock_snapshot_by_part_id(self, part_id: int) -> Optional[dict]:
        """Get the stock snapshot for a part_id, or None when absent."""
        if not part_id or part_id <= 0:
            return None

        snapshots = await self._fetch_stock_snapshot()
        if not snapshots:
            return None

        result = self._snapshot_index.get(part_id) if self._snapshot_index else None
        if result is None:
            logger.warning(
                "InvenTree stock snapshot: part_id=%s not found in %d snapshots",
                part_id,
                len(snapshots),
            )
            return None
        return result

    async def get_stock_at_location(self, part_id: int, location: str = "TOKO") -> dict:
        """Get stock for a part at a location (stock view first, legacy fallback)."""
        stock_info = await self.get_stock_by_part_id(part_id)
        if stock_info:
            return {
                "part_id": stock_info["part_id"],
                "quantity": stock_info["qty_stock"],
                "location_description": location,
                "source": "stock_view",
            }
        return await self._legacy_get_stock_at_location(part_id, location)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_stock_snapshot(self) -> Optional[List[InvenTreeStockSnapshot]]:
        if self._stock_snapshot is not None:
            logger.debug(
                "Returning %d cached items from stock snapshot",
                len(self._stock_snapshot),
            )
            return self._stock_snapshot

        try:
            client = self._get_client()
            response = await client.get(self.stock_snapshot_url, timeout=60.0)
            response.raise_for_status()
            data = response.json()

            raw_items = data.get("vw_marketplace_api_stock_inventree", [])
            items = [InvenTreeStockSnapshot(item) for item in raw_items]

            self._stock_snapshot = items
            self._snapshot_index = {}
            for snapshot in items:
                self._snapshot_index.setdefault(snapshot.part_id, snapshot.to_dict())

            logger.info(
                "Fetched and cached %d items from InvenTree stock snapshot", len(items)
            )
            return items
        except httpx.HTTPStatusError as e:
            logger.error(
                "InvenTree stock snapshot HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            return None
        except httpx.RequestError as e:
            logger.error("InvenTree stock snapshot request error: %s", e)
            return None
        except Exception as e:
            logger.exception("Unexpected error fetching InvenTree stock snapshot: %s", e)
            return None

    async def _fetch_stock_view(self) -> Optional[List[InvenTreeStockItem]]:
        if self._stock_items is not None:
            logger.debug(
                "Returning %d cached items from stock view", len(self._stock_items)
            )
            return self._stock_items

        try:
            client = self._get_client()
            response = await client.get(self.stock_view_url, timeout=60.0)
            response.raise_for_status()
            data = response.json()

            raw_items = data.get("vw_stock_available_inventree", [])
            items = [InvenTreeStockItem(item) for item in raw_items]

            self._stock_items = items
            self._sku_index = {}
            self._part_index = {}
            for item in items:
                self._sku_index.setdefault((item.sku_toko or "").lower(), item)
                self._part_index.setdefault(item.part_id, item)

            logger.info("Fetched and cached %d items from InvenTree stock view", len(items))
            return items
        except httpx.HTTPStatusError as e:
            logger.error(
                "InvenTree stock view HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            return None
        except httpx.RequestError as e:
            logger.error("InvenTree stock view request error: %s", e)
            return None
        except Exception as e:
            logger.exception("Unexpected error fetching InvenTree stock view: %s", e)
            return None

    async def _legacy_search_parts(self, params: dict) -> Optional[int]:
        """Fallback: search parts using the old InvenTree REST API."""
        url = f"{settings.inventree_api_url.rstrip('/')}/api/part/"
        headers = {}
        if settings.inventree_api_token:
            headers["Authorization"] = f"Token {settings.inventree_api_token}"

        try:
            client = self._get_client()
            response = await client.get(
                url, params=params, headers=headers, timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if results:
                return results[0].get("pk")
            return None
        except Exception as e:
            logger.warning("Legacy part search failed (params=%s): %s", params, e)
            return None

    async def _legacy_get_stock_at_location(self, part_id: int, location: str) -> dict:
        """Fallback: get stock using the old InvenTree REST API."""
        url = f"{settings.inventree_api_url.rstrip('/')}/api/stock/"
        params = {"part": part_id, "location_description": location}
        headers = {}
        if settings.inventree_api_token:
            headers["Authorization"] = f"Token {settings.inventree_api_token}"

        try:
            client = self._get_client()
            response = await client.get(
                url, params=params, headers=headers, timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if results:
                return {
                    "stock_id": results[0].get("pk"),
                    "part_id": results[0].get("part"),
                    "quantity": results[0].get("quantity", 0),
                    "location_name": results[0].get("location_name", ""),
                    "location_description": results[0].get("location_description", ""),
                    "source": "legacy",
                }
            return {"source": "legacy", "part_id": part_id, "quantity": 0}
        except Exception as e:
            logger.warning("Legacy stock lookup failed for part %s: %s", part_id, e)
            return {"source": "legacy", "error": str(e)}

    async def create_part(self, seller_sku: str, name: str) -> Optional[int]:
        """Create a new Part in InvenTree (NOT YET IMPLEMENTED)."""
        logger.warning("create_part is not yet fully implemented for SKU '%s'", seller_sku)
        return None
