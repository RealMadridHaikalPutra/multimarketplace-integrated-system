"""
DETAIL ORDER dari APISIX → variabel Camunda (produk apa saja + berapa banyak).

DOKUMENTASI APISIX (apisix-conf/DOCUMENTATION.md §6.4 — "GET /order — Order
List & Detail"):

    GET {APISIX_BASE_URL}{APISIX_ORDER_PATH}?marketplace=…&shop_uuid=…&ids=<order id>

      - `ids` DIISI   → DETAIL mode
            Shopee: GET  /api/v2/order/get_order_detail
            TikTok: GET  /order/202507/orders?ids=...
      - `ids` KOSONG  → LIST mode (dipakai sync reserved stock)

  `translate` (default true) mengubah response menjadi agregasi
  `reserved_stock` yang TIDAK memuat nama produk. Karena webhook butuh
  "produk apa saja yang di-order dan berapa banyak", modul ini memakai
  `translate=false` → response MENTAH marketplace yang masih lengkap:

    Shopee: { "response": { "order_list": [ { "item_list": [ {…} ] } ] } }
    TikTok: { "data": { "orders": [ { "line_items": [ {…} ] } ] } }

  Response hasil standardisasi (`reserved_stock`) tetap didukung sebagai
  fallback seandainya `WEBHOOK_ORDER_DETAIL_TRANSLATE=true`.

Modul ini MURNI PARSING (tanpa HTTP) supaya mudah diuji; pemanggilan HTTP-nya
ada di `infrastructure/apisix/client.py::fetch_order_detail`. Hasilnya
dikonsumsi `camunda/service.py` dan diekspos ke aturan variabel lewat
namespace `order.*` (lihat camunda/rules.py), sehingga `camunda/config.json`
boleh memetakan:

    "orderItems":         {"path": "order.items"}
    "orderProducts":      {"path": "order.products_summary"}
    "orderTotalQuantity": {"path": "order.total_quantity"}
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from core.config import settings

logger = logging.getLogger(__name__)

# Field order id per marketplace (urutan = prioritas).
ORDER_ID_FIELDS_BY_MARKETPLACE: Dict[str, Tuple[str, ...]] = {
    "shopee": ("ordersn", "order_sn", "order_sn_list", "order_id"),
    "tiktok": ("order_id", "id", "order_sn"),
}
DEFAULT_ORDER_ID_FIELDS: Tuple[str, ...] = (
    "order_id",
    "ordersn",
    "order_sn",
    "order_no",
    "orderNo",
)

# Lokasi daftar order di response APISIX (translate=false) — dicoba berurutan.
_ORDER_LIST_PATHS: Tuple[Tuple[str, ...], ...] = (
    ("data", "orders"),       # TikTok
    ("response", "order_list"),  # Shopee
    ("data", "order_list"),
    ("orders",),
    ("order_list",),
)

# Kunci daftar line item di dalam satu order (Shopee: item_list, TikTok: line_items).
_LINE_ITEM_KEYS: Tuple[str, ...] = ("line_items", "item_list", "items", "order_items")

_PRODUCT_ID_KEYS: Tuple[str, ...] = ("product_id", "item_id")
_PRODUCT_NAME_KEYS: Tuple[str, ...] = ("product_name", "item_name", "title")
_VARIANT_NAME_KEYS: Tuple[str, ...] = ("sku_name", "model_name", "variation_name")
_SELLER_SKU_KEYS: Tuple[str, ...] = ("seller_sku", "model_sku", "item_sku", "sku")
_QUANTITY_KEYS: Tuple[str, ...] = ("quantity", "model_quantity_purchased", "qty")
_PRICE_KEYS: Tuple[str, ...] = (
    "sale_price",
    "model_discounted_price",
    "price",
    "model_original_price",
)
_STATUS_KEYS: Tuple[str, ...] = ("status", "order_status", "orderStatus")
# Field status order di payload WEBHOOK mentah (urutan = prioritas). Sama dengan
# `_STATUS_KEYS` + `new_status` (dipakai sebagian push status marketplace).
_WEBHOOK_STATUS_KEYS: Tuple[str, ...] = (
    "status",
    "order_status",
    "orderStatus",
    "new_status",
)
_ORDER_ID_IN_ORDER_KEYS: Tuple[str, ...] = ("id", "order_sn", "order_id", "ordersn")


# ---------------------------------------------------------------------------
# Helper murni
# ---------------------------------------------------------------------------


def _pick(container: Any, keys: Tuple[str, ...]) -> Any:
    """Nilai pertama yang ada (bukan None/\"\") dari *container* untuk *keys*."""
    if not isinstance(container, dict):
        return None
    for key in keys:
        value = container.get(key)
        if value not in (None, ""):
            return value
    return None


def _to_int(value: Any) -> Optional[int]:
    """Konversi longgar ke int; None bila bukan angka."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def _dig(root: Any, path: Tuple[str, ...]) -> Any:
    """Ambil nilai nested berdasarkan tuple path; None bila tidak ada."""
    current = root
    for segment in path:
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def extract_order_id(marketplace: str, body: Any) -> Optional[str]:
    """Ambil order id dari payload webhook (Shopee `ordersn`, TikTok `order_id`).

    Order id inilah yang dikirim sebagai parameter `ids` ke APISIX.
    """
    if not isinstance(body, dict):
        return None

    fields = ORDER_ID_FIELDS_BY_MARKETPLACE.get(
        (marketplace or "").strip().lower(), DEFAULT_ORDER_ID_FIELDS
    )
    data = body.get("data") if isinstance(body.get("data"), dict) else None

    # Utamakan blok `data` (isi event), lalu level atas payload.
    for container in (data, body):
        value = _pick(container, fields)
        if value is not None:
            return str(value)

    # Marketplace dengan nama field tak terduga → cari field generik.
    for container in (data, body):
        value = _pick(container, DEFAULT_ORDER_ID_FIELDS)
        if value is not None:
            return str(value)
    return None


def extract_webhook_order_status(body: Any) -> Optional[str]:
    """Ambil STATUS ORDER dari payload WEBHOOK mentah (bukan dari detail order).

    Dipakai gate status saat start event Camunda: `WEBHOOK_ORDER_STATUS_ALLOWLIST`
    (global, .env) dan `order_status` per aturan di camunda/config.json. Webhook
    dengan status di luar daftar TIDAK men-start proses (lihat camunda/service.py).

    Urutan field menyamai ringkasan log di api/routers/webhook.py: Shopee
    `data.status`, TikTok `data.order_status`, lalu `orderStatus` / `new_status`.
    Blok `data` diutamakan, baru level atas payload.
    """
    if not isinstance(body, dict):
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else None
    for container in (data, body):
        value = _pick(container, _WEBHOOK_STATUS_KEYS)
        if value is not None:
            return str(value)
    return None


def resolve_shop_uuid(marketplace: str, body: Any) -> str:
    """Tentukan `shop_uuid` untuk query APISIX.

    Urutan: setting per marketplace (APISIX_<MP>_SHOP_UUID) → payload webhook
    (`shop_id` / `shop_uuid`). Di credentials.json APISIX, `shop_uuid` untuk
    kedua shop sama dengan `shop_id` — jadi payload biasanya sudah cukup.
    """
    marketplace_lc = (marketplace or "").strip().lower()
    if marketplace_lc == "shopee":
        configured = settings.apisix_shopee_shop_uuid
    elif marketplace_lc == "tiktok":
        configured = settings.apisix_tiktok_shop_uuid
    else:
        configured = ""
    if configured and str(configured).strip():
        return str(configured).strip()

    if isinstance(body, dict):
        data = body.get("data") if isinstance(body.get("data"), dict) else None
        for container in (body, data):
            value = _pick(container, ("shop_id", "shop_uuid", "shopId", "shop_uuid"))
            if value is not None:
                return str(value)
    return ""


def _select_order(orders: List[Any], order_id: Optional[str]) -> Dict[str, Any]:
    """Pilih order yang cocok dengan *order_id* (fallback: order pertama)."""
    dicts = [order for order in orders if isinstance(order, dict)]
    if not dicts:
        return {}
    if order_id:
        wanted = str(order_id)
        for order in dicts:
            candidate = _pick(order, _ORDER_ID_IN_ORDER_KEYS)
            if candidate is not None and str(candidate) == wanted:
                return order
    return dicts[0]


def _pick_orders(raw: Dict[str, Any]) -> Optional[List[Any]]:
    """Cari daftar order di response APISIX; None bila bentuk tidak dikenali."""
    for path in _ORDER_LIST_PATHS:
        value = _dig(raw, path)
        if isinstance(value, list):
            return value
    return None


def _pick_line_items(order: Dict[str, Any]) -> List[Any]:
    """Line item satu order (Shopee `item_list`, TikTok `line_items`)."""
    for key in _LINE_ITEM_KEYS:
        value = order.get(key)
        if isinstance(value, list):
            return value
    return []


def _build_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalisasi SATU line item → struktur yang mudah dibaca."""
    quantity = _to_int(_pick(raw_item, _QUANTITY_KEYS))

    return {
        "product_id": str(_pick(raw_item, _PRODUCT_ID_KEYS) or ""),
        "product_name": str(_pick(raw_item, _PRODUCT_NAME_KEYS) or ""),
        "variant_name": str(_pick(raw_item, _VARIANT_NAME_KEYS) or ""),
        "seller_sku": str(_pick(raw_item, _SELLER_SKU_KEYS) or ""),
        # TikTok tidak mengirim `quantity` (1 line item = 1 unit) → fallback 1.
        "quantity": quantity if quantity and quantity > 0 else 1,
        "unit_price": _pick(raw_item, _PRICE_KEYS),
    }


def _merge_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Gabung line item identik (produk + SKU + variant) → jumlah diakumulasi."""
    merged: List[Dict[str, Any]] = []
    index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for item in items:
        key = (item["product_id"], item["seller_sku"], item["variant_name"])
        existing = index.get(key)
        if existing is None:
            index[key] = item
            merged.append(item)
            continue
        existing["quantity"] = int(existing.get("quantity") or 0) + int(item.get("quantity") or 0)
        if not existing.get("product_name") and item.get("product_name"):
            existing["product_name"] = item["product_name"]
    return merged


def _items_from_reserved_stock(entries: List[Any]) -> List[Dict[str, Any]]:
    """Fallback: bangun item dari agregasi `reserved_stock` (tanpa nama produk).

    Bentuk terstandardisasi APISIX hanya memuat product_id + variant + jumlah,
    jadi nama produk tidak tersedia — label memakai seller_sku/variant_id.
    """
    items: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        product_id = str(entry.get("product_id") or "")
        for variant in entry.get("variants") or []:
            if not isinstance(variant, dict):
                continue
            quantity = _to_int(variant.get("total_reserved_stock"))
            seller_sku = str(variant.get("seller_sku") or "")
            variant_id = str(variant.get("variant_id") or "")
            items.append(
                {
                    "product_id": product_id,
                    "product_name": seller_sku or product_id,
                    "variant_name": "" if seller_sku else variant_id,
                    "seller_sku": seller_sku,
                    "quantity": quantity if quantity and quantity > 0 else 1,
                    "unit_price": None,
                }
            )
    return items


def _item_label(item: Dict[str, Any], include_variant: bool = True) -> str:
    """Label singkat untuk ringkasan: \"Kaos Polos (M)\" / \"SKU-1A1\"."""
    name = item.get("product_name") or item.get("seller_sku") or item.get("product_id") or "?"
    variant = item.get("variant_name") or ""
    if include_variant and variant and str(variant) not in str(name):
        return f"{name} ({variant})"
    return str(name)


def _finalize(detail: Dict[str, Any]) -> Dict[str, Any]:
    """Hitung ringkasan (jumlah item, total unit, teks mudah dibaca)."""
    items = detail.get("items") or []
    total_quantity = 0
    for item in items:
        total_quantity += int(item.get("quantity") or 0)

    names: List[str] = []
    parts: List[str] = []
    for item in items:
        label = _item_label(item)
        if label not in names:
            names.append(label)
        parts.append(f"{label} x{item.get('quantity')}")

    detail["item_count"] = len(items)
    detail["total_quantity"] = total_quantity
    detail["product_names"] = names
    detail["products_summary"] = ", ".join(parts)
    return detail


def normalize_order_detail(
    marketplace: str,
    raw: Any,
    order_id: Optional[str] = None,
    shop_uuid: Optional[str] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """Ubah response APISIX menjadi detail order yang mudah dibaca.

    Struktur hasil (yang di-mapping ke variabel Camunda):

        {
          "ok": true,
          "source": "order_detail" | "reserved_stock" | "none",
          "marketplace": "shopee",
          "order_id": "2501080NKAMXA8",
          "shop_uuid": "341431138",
          "order_status": "UNPAID",
          "items": [
            {"product_id": "…", "product_name": "Kaos Polos",
             "variant_name": "M", "seller_sku": "1A1-M",
             "quantity": 2, "unit_price": "29900"}
          ],
          "item_count": 1,              # jumlah baris (produk berbeda)
          "total_quantity": 2,          # total unit yang di-order
          "product_names": ["Kaos Polos (M)"],
          "products_summary": "Kaos Polos (M) x2",
          "error": null
        }

    Saat gagal (``ok=false``), hanya kunci ok/source/marketplace/order_id/
    shop_uuid/order_status/error yang ada — keys item TIDAK diisi supaya
    variabel `order.*` benar-benar dilewati (bukan terkirim sebagai 0/[]).
    """
    # Catatan: kunci items/item_count/total_quantity/... SENGAJA tidak diisi saat
    # gagal — supaya variabel `order.*` di Camunda benar-benar DILEWATI (bukan
    # terkirim sebagai 0/[]), dan hanya `order.ok=false` + `order.error` yang ada.
    detail: Dict[str, Any] = {
        "ok": False,
        "source": "none",
        "marketplace": marketplace,
        "order_id": order_id,
        "shop_uuid": shop_uuid,
        "order_status": "",
        "error": error,
    }
    if not isinstance(raw, dict):
        return detail

    orders = _pick_orders(raw)
    if orders is None:
        # Fallback: hasil standardisasi (translate=true) → reserved_stock.
        entries = raw.get("reserved_stock")
        if isinstance(entries, list):
            detail["source"] = "reserved_stock"
            detail["items"] = _merge_items(_items_from_reserved_stock(entries))
            detail["ok"] = True
            return _finalize(detail)
        return detail

    detail["source"] = "order_detail"
    order = _select_order(orders, order_id)
    if not order:
        return detail

    status = _pick(order, _STATUS_KEYS)
    if status is not None:
        detail["order_status"] = str(status)
    if not detail["order_id"]:
        picked_id = _pick(order, _ORDER_ID_IN_ORDER_KEYS)
        if picked_id is not None:
            detail["order_id"] = str(picked_id)

    raw_items = [item for item in _pick_line_items(order) if isinstance(item, dict)]
    detail["items"] = _merge_items([_build_item(item) for item in raw_items])
    detail["ok"] = True
    return _finalize(detail)


def build_order_variables(detail: Dict[str, Any]) -> Dict[str, Any]:
    """Variabel Camunda standar dari detail order (opsional dipakai config.json).

    Berguna bila ingin mengaktifkan variabel order tanpa menulis mapping
    satu-satu di camunda/config.json. Nilai tetap boleh diubah namanya di
    config.json lewat namespace `order.*`.
    """
    if not isinstance(detail, dict) or not detail:
        return {}
    return {
        "orderItems": detail.get("items") or [],
        "orderProducts": detail.get("products_summary") or "",
        "orderProductNames": detail.get("product_names") or [],
        "orderItemCount": detail.get("item_count") or 0,
        "orderTotalQuantity": detail.get("total_quantity") or 0,
        "orderDetailStatus": detail.get("order_status") or "",
        "orderDetailOk": bool(detail.get("ok")),
    }
