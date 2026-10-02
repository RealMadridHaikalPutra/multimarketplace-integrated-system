"""APISIX gateway HTTP client.

Sends *unified* marketplace requests only — APISIX handles token lifecycle,
marketplace routing (Shopee / TikTok), request transformation, response
normalization and error mapping on the Lua side.

Unified endpoints used:
    GET  /products?marketplace=…&shop_uuid=…
    GET  /order?marketplace=…&shop_uuid=…
    POST /update-stock/{product_id}?marketplace=…&shop_uuid=…
    POST /update-status/{product_id}?marketplace=…&shop_uuid=…

The pure pagination rules live in ``pagination.py``; constants and the
HTTP-error helper live in ``constants.py``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

from core.config import settings
from infrastructure.marketplace.apisix.constants import (
    CONFIG_ERROR_PREFIX,
    HTTP_ERROR_BODY_MAX_CHARS,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STOCK_MAX,
    STOCK_MIN,
    TIKTOK_LISTING_PLATFORMS,
    ApisixConfigError,
    raise_http_error_with_body,
)
from infrastructure.marketplace.apisix.pagination import (
    is_cursor_marketplace,
    next_page_plan,
    normalize_path,
    pagination_has_more,
    parse_pagination,
    product_id_key,
)

logger = logging.getLogger(__name__)


class ApisixClient:
    """HTTP client for the APISIX unified gateway."""

    def __init__(self) -> None:
        self.base_url = settings.apisix_base_url.rstrip("/")
        self.products_path = normalize_path(
            settings.apisix_products_path, "/products", "APISIX_PRODUCTS_PATH"
        )
        self.reserved_stock_path = normalize_path(
            settings.apisix_reserved_stock_path, "/order", "APISIX_RESERVED_STOCK_PATH"
        )
        self.update_stock_path = normalize_path(
            settings.apisix_update_stock_path, "/update-stock", "APISIX_UPDATE_STOCK_PATH"
        )
        self.update_status_path = normalize_path(
            settings.apisix_update_status_path, "/update-status", "APISIX_UPDATE_STATUS_PATH"
        )
        self.update_status_timeout = settings.apisix_update_status_timeout
        self.update_stock_timeout = settings.apisix_update_stock_timeout
        # Shared HTTP client (lazy) — connection reuse across calls.
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        """Lazy shared AsyncClient (default timeout 120s; per-call overrides)."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0))
        return self._client

    async def aclose(self) -> None:
        """Close the shared AsyncClient (release pooled connections)."""
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    # ------------------------------------------------------------------
    # Pagination sweep primitives
    # ------------------------------------------------------------------

    async def _fetch_page_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """GET one page from APISIX. Raises on HTTP / network error."""
        try:
            client = self._get_client()
            response = await client.get(url, params=params, timeout=120.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                "APISIX HTTP error: %s %s", e.response.status_code, e.response.text[:500]
            )
            raise_http_error_with_body(e)
        except httpx.RequestError as e:
            logger.error("APISIX request error: %s", e)
            raise

    @staticmethod
    def _merge_reserved_entries(
        all_entries: List[Dict[str, Any]], new_entries: List[Any]
    ) -> None:
        """Merge ``reserved_stock`` entries across /order pages.

        Dedup per ``(product_id, variant_id)`` — when the same product appears
        again on a later page, only new variant_ids are appended.
        """
        for entry in new_entries:
            if not isinstance(entry, dict):
                continue
            product_id = str(entry.get("product_id") or "")
            existing = None
            if product_id:
                existing = next(
                    (e for e in all_entries if str(e.get("product_id") or "") == product_id),
                    None,
                )
            if existing is None:
                all_entries.append(entry)
                continue

            existing_variants = existing.get("variants")
            if not isinstance(existing_variants, list):
                existing_variants = []
                existing["variants"] = existing_variants
            existing_ids = {str(v.get("variant_id") or "") for v in existing_variants}
            for variant in entry.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                vid = str(variant.get("variant_id") or "")
                if vid and vid in existing_ids:
                    continue
                if vid:
                    existing_ids.add(vid)
                existing_variants.append(variant)

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------

    async def fetch_products(
        self, marketplace: str, shop_uuid: str
    ) -> Dict[str, Any]:
        """Fetch ALL products, sweeping every page.

        For TikTok the sweep runs once per ``listing_platforms`` value
        (sequential, so cursors do not mix) and merges the results. Other
        marketplaces keep the single-sweep behaviour.

        Returns a combined dict with keys ``products``, ``pagination``,
        ``pages_fetched``, ``sweep_complete`` and (TikTok only)
        ``listing_platforms_swept`` / ``product_listing_platforms``.
        """
        if not is_cursor_marketplace(marketplace):
            return await self._fetch_products_sweep(marketplace, shop_uuid)

        merged_items: List[Dict[str, Any]] = []
        product_platforms: Dict[str, List[str]] = {}
        seen_product_ids: set = set()
        total_pages = 0
        sweep_complete = True
        last_pagination: Dict[str, Any] = {}

        logger.info(
            "Fetching products from APISIX: marketplace=tiktok, shop_uuid=%s, "
            "listing_platforms sweep=%s",
            shop_uuid,
            TIKTOK_LISTING_PLATFORMS,
        )

        for platforms_value in TIKTOK_LISTING_PLATFORMS:
            sweep = await self._fetch_products_sweep(
                marketplace, shop_uuid, listing_platforms=platforms_value
            )
            total_pages += int(sweep.get("pages_fetched", 0) or 0)
            if sweep.get("sweep_complete", True) is False:
                sweep_complete = False
            last_pagination = sweep.get("pagination") or last_pagination

            for item in sweep.get("products") or []:
                key = product_id_key(item)
                if key is not None:
                    product_platforms.setdefault(key, []).append(platforms_value)
                    if key in seen_product_ids:
                        continue
                    seen_product_ids.add(key)
                merged_items.append(item)

        logger.info(
            "APISIX fetch_products (tiktok) complete: %d listing_platforms sweep, "
            "%d page(s) total, %d unique product(s), sweep_complete=%s (shop_uuid=%s)",
            len(TIKTOK_LISTING_PLATFORMS),
            total_pages,
            len(merged_items),
            sweep_complete,
            shop_uuid,
        )
        return {
            "marketplace": marketplace,
            "products": merged_items,
            "pagination": last_pagination,
            "pages_fetched": total_pages,
            "sweep_complete": sweep_complete,
            "listing_platforms_swept": list(TIKTOK_LISTING_PLATFORMS),
            "product_listing_platforms": product_platforms,
        }

    async def _fetch_products_sweep(
        self,
        marketplace: str,
        shop_uuid: str,
        listing_platforms: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Pagination sweep for products (single filter combination).

        See the reference documentation: page-based (Shopee) vs cursor-based
        (TikTok); safety cap ``apisix_max_pages``; ``sweep_complete`` gate for
        the SyncEngine FREEZE/cleanup logic.
        """
        url = f"{self.base_url}/{self.products_path}"
        base_params = {"marketplace": marketplace, "shop_uuid": shop_uuid}
        if listing_platforms:
            base_params["listing_platforms"] = listing_platforms

        logger.info(
            "Fetching products from APISIX (paginated): marketplace=%s, shop_uuid=%s%s",
            marketplace,
            shop_uuid,
            f", listing_platforms={listing_platforms}" if listing_platforms else "",
        )

        all_items: List[Dict[str, Any]] = []
        seen_product_ids: set = set()
        seen_tokens: set = set()
        pagination_meta: Dict[str, Any] = {}
        page_no = 0
        page_token: Optional[str] = None
        max_pages = settings.apisix_max_pages
        sweep_complete = True
        items_before = 0
        stalled_pages = 0
        expected_total: Optional[int] = None

        def _collected_everything() -> bool:
            return expected_total is not None and len(all_items) >= expected_total

        while page_no < max_pages:
            page_no += 1
            params = dict(base_params)
            if page_token is not None:
                params["next_page_token"] = page_token
            elif not is_cursor_marketplace(marketplace):
                params["page"] = page_no

            logger.info(
                "APISIX fetch_products: page=%d%s (marketplace=%s, shop_uuid=%s%s)",
                page_no,
                f" next_page_token={page_token}" if page_token else "",
                marketplace,
                shop_uuid,
                f", listing_platforms={listing_platforms}" if listing_platforms else "",
            )

            data = await self._fetch_page_json(url, params)
            pagination_meta = parse_pagination(data)
            if expected_total is None:
                total_raw = pagination_meta.get("total")
                if isinstance(total_raw, int) and not isinstance(total_raw, bool):
                    expected_total = total_raw

            raw_items = data.get("products")
            if not isinstance(raw_items, list):
                response = data.get("response")
                raw_items = response.get("item", []) if isinstance(response, dict) else []
                if not isinstance(raw_items, list):
                    raw_items = []

            new_items = 0
            for item in raw_items:
                key = product_id_key(item)
                if key is not None:
                    if key in seen_product_ids:
                        continue
                    seen_product_ids.add(key)
                all_items.append(item)
                new_items += 1

            has_more, next_token, repeated_token, unsupported = next_page_plan(
                pagination_meta, marketplace, seen_tokens
            )
            if repeated_token:
                sweep_complete = False
                logger.warning(
                    "APISIX fetch_products: next_page_token berulang di page %d — "
                    "sweep dihentikan (marketplace=%s, shop_uuid=%s).",
                    page_no,
                    marketplace,
                    shop_uuid,
                )
                break
            if unsupported:
                if new_items == 0 or _collected_everything():
                    logger.info(
                        "APISIX fetch_products: halaman ekor tanpa next_page_token "
                        "di page %d — %d/%s product terambil, sweep TUNTAS "
                        "(marketplace=%s, shop_uuid=%s).",
                        page_no,
                        len(all_items),
                        expected_total if expected_total is not None else "?",
                        marketplace,
                        shop_uuid,
                    )
                    break
                sweep_complete = False
                logger.warning(
                    "APISIX fetch_products: has_next tanpa next_page_token di page %d "
                    "(+%d product baru) — sweep TIDAK lengkap (marketplace=%s, shop_uuid=%s).",
                    page_no,
                    new_items,
                    marketplace,
                    shop_uuid,
                )
                break
            if not has_more:
                break

            if next_token is not None:
                page_token = next_token
                stalled_pages = 0
            else:
                page_token = None
                if new_items == 0 and len(all_items) == items_before:
                    stalled_pages += 1
                    if stalled_pages >= 2:
                        if not _collected_everything():
                            sweep_complete = False
                            logger.warning(
                                "APISIX fetch_products: %d halaman berturut-turut tidak "
                                "menambah produk baru padahal masih has_next — sweep "
                                "dihentikan TIDAK lengkap (marketplace=%s, shop_uuid=%s).",
                                stalled_pages + 1,
                                marketplace,
                                shop_uuid,
                            )
                        break
                else:
                    stalled_pages = 0
            items_before = len(all_items)

        if page_no >= max_pages and not _collected_everything():
            sweep_complete = False
            logger.warning(
                "APISIX fetch_products: mencapai safety cap %d halaman — "
                "kemungkinan masih ada product tersisa (marketplace=%s, shop_uuid=%s).",
                max_pages,
                marketplace,
                shop_uuid,
            )

        logger.info(
            "APISIX fetch_products complete: %d page(s) swept, %d unique product(s) "
            "(marketplace=%s, shop_uuid=%s, sweep_complete=%s)",
            page_no,
            len(all_items),
            marketplace,
            shop_uuid,
            sweep_complete,
        )
        return {
            "marketplace": marketplace,
            "products": all_items,
            "pagination": pagination_meta,
            "pages_fetched": page_no,
            "sweep_complete": sweep_complete,
        }

    # ------------------------------------------------------------------
    # Orders (reserved stock)
    # ------------------------------------------------------------------

    async def fetch_reserved_stock(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: Optional[str] = None,
        variant_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch reserved stock per variant from the APISIX ``/order`` endpoint.

        ``product_id``/``variant_id`` bersifat OPSIONAL: bila diisi, keduanya
        diteruskan sebagai filter gateway sehingga hanya produk/varian itu yang
        diagregasi (dipakai jalur webhook — satu panggilan per produk). Kosong =
        sweep seluruh toko (perilaku pipeline lama).

        Error handling is GRACEFUL: a first-page failure returns ``None``
        (caller falls back to the product payload); a later-page failure
        returns partial data with ``sweep_complete=False``.
        """
        url = f"{self.base_url}/{self.reserved_stock_path}"
        base_params = {"marketplace": marketplace, "shop_uuid": shop_uuid}
        if str(product_id or "").strip():
            base_params["product_id"] = str(product_id).strip()
        if str(variant_id or "").strip():
            base_params["variant_id"] = str(variant_id).strip()

        logger.info(
            "Fetching reserved stock from APISIX (paginated): marketplace=%s, shop_uuid=%s",
            marketplace,
            shop_uuid,
        )

        all_entries: List[Dict[str, Any]] = []
        seen_tokens: set = set()
        pagination_meta: Dict[str, Any] = {}
        page_no = 0
        page_token: Optional[str] = None
        first_page_ok = False
        max_pages = settings.apisix_max_pages
        sweep_complete = True
        entries_before = 0
        stalled_pages = 0
        filtered_by: Optional[Any] = None

        while page_no < max_pages:
            page_no += 1
            params = dict(base_params)
            if page_token is not None:
                params["next_page_token"] = page_token
            elif not is_cursor_marketplace(marketplace):
                params["page"] = page_no

            logger.info(
                "APISIX /order: page=%d%s (marketplace=%s, shop_uuid=%s)",
                page_no,
                f" next_page_token={page_token}" if page_token else "",
                marketplace,
                shop_uuid,
            )

            try:
                data = await self._fetch_page_json(url, params)
            except httpx.HTTPStatusError as e:
                logger.warning(
                    "APISIX /order page %d HTTP error: %s", page_no, e.response.status_code
                )
                if not first_page_ok:
                    return None
                logger.error(
                    "APISIX /order: SWEEP TIDAK LENGKAP — mengembalikan reserved "
                    "stock parsial dari %d halaman pertama (page %d gagal).",
                    page_no - 1,
                    page_no,
                )
                sweep_complete = False
                break
            except httpx.RequestError as e:
                logger.warning("APISIX /order page %d request error: %s", page_no, e)
                if not first_page_ok:
                    return None
                logger.error(
                    "APISIX /order: SWEEP TIDAK LENGKAP — mengembalikan reserved "
                    "stock parsial dari %d halaman pertama (page %d gagal).",
                    page_no - 1,
                    page_no,
                )
                sweep_complete = False
                break
            except Exception as e:
                logger.exception(
                    "Unexpected error fetching APISIX /order page %d: %s", page_no, e
                )
                if not first_page_ok:
                    return None
                logger.error(
                    "APISIX /order: SWEEP TIDAK LENGKAP — mengembalikan reserved "
                    "stock parsial dari %d halaman pertama (page %d gagal).",
                    page_no - 1,
                    page_no,
                )
                sweep_complete = False
                break

            first_page_ok = True
            pagination_meta = parse_pagination(data)
            # Penanda filter gateway (mis. "product_id") — diteruskan ke pemanggil
            # agar ia tahu hasilnya memang sudah terfilter (atau belum).
            if isinstance(data, dict) and data.get("filtered_by"):
                filtered_by = data.get("filtered_by")
            before_merge = len(all_entries)
            entries = data.get("reserved_stock")
            if isinstance(entries, list):
                self._merge_reserved_entries(all_entries, entries)
            new_entries = len(all_entries) - before_merge

            has_more, next_token, repeated_token, unsupported = next_page_plan(
                pagination_meta, marketplace, seen_tokens
            )
            if repeated_token:
                sweep_complete = False
                logger.warning(
                    "APISIX /order: next_page_token berulang di page %d — "
                    "sweep dihentikan (marketplace=%s, shop_uuid=%s).",
                    page_no,
                    marketplace,
                    shop_uuid,
                )
                break
            if unsupported:
                sweep_complete = False
                logger.warning(
                    "APISIX /order: has_next tanpa next_page_token di page %d — "
                    "sweep TIDAK lengkap (marketplace=%s, shop_uuid=%s).",
                    page_no,
                    marketplace,
                    shop_uuid,
                )
                break
            if not has_more:
                break

            if next_token is not None:
                page_token = next_token
                stalled_pages = 0
            else:
                page_token = None
                if new_entries == 0 and len(all_entries) == entries_before:
                    stalled_pages += 1
                    if stalled_pages >= 2:
                        sweep_complete = False
                        logger.warning(
                            "APISIX /order: %d halaman berturut-turut tidak menambah "
                            "entri baru padahal masih has_next — reserved stock "
                            "PARSIAL (marketplace=%s, shop_uuid=%s).",
                            stalled_pages + 1,
                            marketplace,
                            shop_uuid,
                        )
                        break
                else:
                    stalled_pages = 0
            entries_before = len(all_entries)

        if page_no >= max_pages:
            sweep_complete = False
            logger.warning(
                "APISIX /order: mencapai safety cap %d halaman — kemungkinan "
                "masih ada reserved stock tersisa (marketplace=%s, shop_uuid=%s).",
                max_pages,
                marketplace,
                shop_uuid,
            )

        logger.info(
            "APISIX /order complete: %d page(s) swept, %d reserved-stock entri "
            "(marketplace=%s, shop_uuid=%s, sweep_complete=%s)",
            page_no,
            len(all_entries),
            marketplace,
            shop_uuid,
            sweep_complete,
        )
        return {
            "marketplace": marketplace,
            "reserved_stock": all_entries,
            "pagination": pagination_meta,
            "pages_fetched": page_no,
            "sweep_complete": sweep_complete,
            "filtered_by": filtered_by,
        }

    # ------------------------------------------------------------------
    # Order detail (GET /order?…&ids=<order id>)
    # ------------------------------------------------------------------

    async def fetch_order_detail(
        self,
        marketplace: str,
        shop_uuid: str,
        order_id: str,
        translate: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single order's DETAIL from APISIX ``GET /order?...&ids=…``.

        Fail-open: returns ``None`` on incomplete input, HTTP error, network
        error or unexpected error — the caller (Camunda webhook trigger) must
        keep working even when the detail lookup fails.
        """
        marketplace_lc = (marketplace or "").lower()
        if not marketplace_lc or not str(shop_uuid or "").strip():
            return None
        if not str(order_id or "").strip():
            return None

        base_url = (settings.apisix_order_base_url or "").strip().rstrip("/")
        if not base_url:
            base_url = settings.apisix_base_url.rstrip("/")
        order_path = normalize_path(
            settings.apisix_order_path, "/order", "APISIX_ORDER_PATH"
        )
        url = f"{base_url}/{order_path}"
        params = {
            "marketplace": marketplace_lc,
            "shop_uuid": shop_uuid,
            "ids": order_id,
            "translate": "true" if translate else "false",
        }

        logger.info(
            "Fetching order detail from APISIX: marketplace=%s, shop_uuid=%s, "
            "order_id=%s, translate=%s",
            marketplace_lc,
            shop_uuid,
            order_id,
            params["translate"],
        )

        try:
            client = self._get_client()
            response = await client.get(
                url, params=params, timeout=timeout if timeout is not None else 120.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.warning(
                "APISIX order detail HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            return None
        except httpx.RequestError as e:
            logger.warning("APISIX order detail request error: %s", e)
            return None
        except Exception as e:  # defensive: invalid JSON / unexpected shape
            logger.warning("APISIX order detail unexpected error: %s", e)
            return None

    # ------------------------------------------------------------------
    # Update status
    # ------------------------------------------------------------------

    async def update_status(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        status: str = STATUS_ACTIVE,
    ) -> Dict[str, Any]:
        """Update product status (ACTIVE / INACTIVE) via APISIX /update-status."""
        marketplace_lc = (marketplace or "").lower()

        if marketplace_lc not in ("shopee", "tiktok"):
            mp_display = marketplace if marketplace else "(none/empty)"
            raise ApisixConfigError(
                f"Unsupported marketplace for APISIX update-status: '{mp_display}'. "
                "Expected 'shopee' or 'tiktok'."
            )

        if not str(product_id or "").strip():
            raise ApisixConfigError(
                "product_id is empty — cannot route update-status via APISIX. "
                "Check that MarketplaceProduct.external_product_id is populated."
            )

        status_upper = (status or "").upper()
        if status_upper not in (STATUS_ACTIVE, STATUS_INACTIVE):
            raise ApisixConfigError(
                f"Invalid status value: '{status}'. Must be 'ACTIVE' or 'INACTIVE'."
            )

        encoded_product_id = quote(product_id, safe="")
        url = f"{self.base_url}/{self.update_status_path}/{encoded_product_id}"
        params = {"marketplace": marketplace_lc, "shop_uuid": shop_uuid}
        payload = {"status": status_upper}

        logger.info(
            "APISIX update-status: marketplace=%s shop_uuid=%s product_id=%s status=%s",
            marketplace_lc,
            shop_uuid,
            product_id,
            status_upper,
        )

        try:
            client = self._get_client()
            response = await client.post(
                url, params=params, json=payload, timeout=self.update_status_timeout
            )
            response.raise_for_status()
            if not response.content:
                return {"status": "SUCCESS"}
            try:
                data = response.json()
            except ValueError as json_err:
                logger.warning(
                    "APISIX update-status returned non-JSON 2xx body — treating as "
                    "neutral: %s",
                    json_err,
                )
                data = {"status": "SUCCESS", "raw_body": response.text[:200]}
            logger.info(
                "APISIX update-status OK: marketplace=%s shop_uuid=%s product_id=%s status=%s",
                marketplace_lc,
                shop_uuid,
                product_id,
                status_upper,
            )
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                "APISIX update-status HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            raise_http_error_with_body(e)
        except httpx.RequestError as e:
            logger.error("APISIX update-status request error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error calling APISIX update-status: %s", e)
            raise

    # ------------------------------------------------------------------
    # Update stock (Format B flat)
    # ------------------------------------------------------------------

    async def update_stock(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        variant_id: str,
        stock: int,
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Push a stock update using Format B (flat single-warehouse) body.

        Body: ``{"skus": [{"id": variant_id, "stock": N, "warehouse_id": w}]}``.
        """
        marketplace_lc = (marketplace or "").lower()

        if marketplace_lc not in ("shopee", "tiktok"):
            mp_display = marketplace if marketplace else "(none/empty)"
            raise ApisixConfigError(
                f"Unsupported marketplace for APISIX update-stock: '{mp_display}'. "
                "Expected 'shopee' or 'tiktok'."
            )

        if not str(variant_id or "").strip():
            raise ApisixConfigError(
                "variant_id is empty — cannot route update-stock via APISIX. "
                "Check that MarketplaceVariant.external_variant_id is populated."
            )

        if not str(product_id or "").strip():
            raise ApisixConfigError(
                "product_id is empty — cannot route update-stock via APISIX. "
                "Check that MarketplaceProduct.external_product_id is populated."
            )

        resolved_warehouse_id = (str(warehouse_id or "").strip()) or (
            settings.apisix_shopee_default_warehouse_id
            if marketplace_lc == "shopee"
            else settings.apisix_tiktok_default_warehouse_id
        )

        if not resolved_warehouse_id:
            raise ApisixConfigError(
                f"No warehouse_id resolved for marketplace '{marketplace}'. "
                "Provide it via update_stock(warehouse_id=…) (preferred — "
                "MarketplaceShop.default_warehouse_id) OR set "
                "apisix_shopee_default_warehouse_id / apisix_tiktok_default_warehouse_id "
                "in config / env."
            )

        if stock < STOCK_MIN or stock > STOCK_MAX:
            clamped_to = max(STOCK_MIN, min(stock, STOCK_MAX))
            logger.warning(
                "APISIX update-stock: stock=%d outside valid range [%d, %d] — "
                "APISIX will clamp silently to %d. Verify intent.",
                stock,
                STOCK_MIN,
                STOCK_MAX,
                clamped_to,
            )

        encoded_product_id = quote(product_id, safe="")
        url = f"{self.base_url}/{self.update_stock_path}/{encoded_product_id}"
        params = {"marketplace": marketplace_lc, "shop_uuid": shop_uuid}
        payload = {
            "skus": [
                {
                    "id": variant_id,
                    "stock": stock,
                    "warehouse_id": resolved_warehouse_id,
                }
            ]
        }

        logger.info(
            "APISIX update-stock (Format B): marketplace=%s shop_uuid=%s product_id=%s "
            "variant_id=%s stock=%d warehouse_id=%s",
            marketplace_lc,
            shop_uuid,
            product_id,
            variant_id,
            stock,
            resolved_warehouse_id,
        )

        try:
            client = self._get_client()
            response = await client.post(
                url, params=params, json=payload, timeout=self.update_stock_timeout
            )
            response.raise_for_status()
            if not response.content:
                return {"status": "SUCCESS"}
            try:
                data = response.json()
            except ValueError as json_err:
                logger.warning(
                    "APISIX update-stock returned non-JSON 2xx body — treating as "
                    "neutral: %s",
                    json_err,
                )
                data = {"status": "SUCCESS", "raw_body": response.text[:200]}
            logger.info(
                "APISIX update-stock OK (Format B): marketplace=%s shop_uuid=%s stock=%d",
                marketplace_lc,
                shop_uuid,
                stock,
            )
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                "APISIX update-stock HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            raise_http_error_with_body(e)
        except httpx.RequestError as e:
            logger.error("APISIX update-stock request error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error calling APISIX update-stock: %s", e)
            raise

    # ------------------------------------------------------------------
    # Update stock (Format A grouped batch)
    # ------------------------------------------------------------------

    async def update_stock_batch(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        skus: List[Dict[str, Any]],
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Push a MULTI-SKU stock update using Format A (grouped) body.

        Body: ``{"skus": [{"id": v, "inventory": [{"warehouse_id": w,
        "quantity": n}]}]}``.
        """
        marketplace_lc = (marketplace or "").lower()
        if marketplace_lc not in ("shopee", "tiktok"):
            raise ApisixConfigError(
                f"Unsupported marketplace for APISIX update-stock-batch: "
                f"'{marketplace or '(none/empty)'}'. Expected 'shopee' or 'tiktok'."
            )
        if not str(product_id or "").strip():
            raise ApisixConfigError(
                "product_id is empty — cannot route update-stock via APISIX. "
                "Check that MarketplaceProduct.external_product_id is populated."
            )
        if not skus:
            raise ApisixConfigError(
                "skus is empty — update_stock_batch requires at least one SKU."
            )

        fallback_warehouse = (str(warehouse_id or "").strip()) or (
            settings.apisix_shopee_default_warehouse_id
            if marketplace_lc == "shopee"
            else settings.apisix_tiktok_default_warehouse_id
        )
        if not fallback_warehouse:
            raise ApisixConfigError(
                f"No warehouse_id resolved for marketplace '{marketplace}'. "
                "Provide it via skus[].warehouse_id, update_stock_batch(warehouse_id=…) "
                "(MarketplaceShop.default_warehouse_id), OR set "
                "apisix_shopee_default_warehouse_id / apisix_tiktok_default_warehouse_id."
            )

        body_skus: List[Dict[str, Any]] = []
        for sku in skus:
            variant_id = str((sku or {}).get("id") or "").strip()
            if not variant_id:
                raise ApisixConfigError(
                    "skus[].id is empty — every SKU entry needs the "
                    "marketplace-native variant/model ID."
                )
            quantity = int((sku or {}).get("quantity") or 0)
            sku_warehouse = (
                str((sku or {}).get("warehouse_id") or "").strip() or fallback_warehouse
            )
            body_skus.append(
                {
                    "id": variant_id,
                    "inventory": [
                        {"warehouse_id": sku_warehouse, "quantity": quantity}
                    ],
                }
            )

        encoded_product_id = quote(product_id, safe="")
        url = f"{self.base_url}/{self.update_stock_path}/{encoded_product_id}"
        params = {"marketplace": marketplace_lc, "shop_uuid": shop_uuid}
        payload = {"skus": body_skus}

        logger.info(
            "APISIX update-stock (Format A batch): marketplace=%s shop_uuid=%s "
            "product_id=%s sku_count=%d warehouse_id=%s",
            marketplace_lc,
            shop_uuid,
            product_id,
            len(body_skus),
            fallback_warehouse,
        )

        try:
            client = self._get_client()
            response = await client.post(
                url, params=params, json=payload, timeout=self.update_stock_timeout
            )
            response.raise_for_status()
            if not response.content:
                return {"status": "SUCCESS"}
            try:
                data = response.json()
            except ValueError as json_err:
                logger.warning(
                    "APISIX update-stock-batch returned non-JSON 2xx body — treating "
                    "as neutral: %s",
                    json_err,
                )
                data = {"status": "SUCCESS", "raw_body": response.text[:200]}
            logger.info(
                "APISIX update-stock OK (Format A batch): marketplace=%s shop_uuid=%s "
                "sku_count=%d",
                marketplace_lc,
                shop_uuid,
                len(body_skus),
            )
            return data
        except httpx.HTTPStatusError as e:
            logger.error(
                "APISIX update-stock-batch HTTP error: %s %s",
                e.response.status_code,
                e.response.text[:500],
            )
            raise_http_error_with_body(e)
        except httpx.RequestError as e:
            logger.error("APISIX update-stock-batch request error: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error calling APISIX update-stock-batch: %s", e)
            raise
