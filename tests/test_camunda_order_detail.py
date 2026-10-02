"""
Test: WEBHOOK → ambil DETAIL ORDER dari APISIX → variabel start event Camunda.

Kontrak (apisix-conf/DOCUMENTATION.md §6.4 — "GET /order"):
  - `GET /order?marketplace=…&shop_uuid=…&ids=<order id>` → DETAIL mode.
  - Parameter `translate=false` → response MENTAH marketplace (memuat nama
    produk per line item) — dipakai untuk variabel "produk apa saja yang
    di-order & berapa banyak".
  - Hasilnya dinormalisasi (camunda/order_detail.py) dan diekspos ke aturan
    variabel lewat namespace `order.*` (camunda/rules.py).
  - FAIL-OPEN: kegagalan fetch tidak menghalangi start event Camunda.

Cakupan:
  1. order_detail.py — ekstraksi order id, resolusi shop_uuid, normalisasi
     Shopee & TikTok (termasuk merge line item identik + fallback reserved_stock).
  2. rules.py — path & template namespace `order.*`.
  3. client.py — URL & params fetch_order_detail, graceful saat error.
  4. service.py — enrich sebelum start event + fail-open + log.
"""

import json
import logging

import httpx
import pytest
from unittest.mock import patch

from infrastructure.camunda.order_detail import (
    build_order_variables,
    extract_order_id,
    extract_webhook_order_status,
    normalize_order_detail,
    resolve_shop_uuid,
)
from infrastructure.camunda.client import CamundaClient
from infrastructure.camunda.rules import resolve_path, resolve_variables
from infrastructure.camunda.service import CamundaWebhookTrigger
from core.config import settings
from infrastructure.marketplace.apisix.client import ApisixClient

PROCESS_KEY = "serviceWebhook"

SHOPEE_BODY = {
    "code": 3,
    "data": {"ordersn": "2501080NKAMXA8", "status": "UNPAID", "update_time": 1736323997},
    "shop_id": 341431138,
    "timestamp": 1736323998,
}

TIKTOK_BODY = {
    "type": 1,
    "data": {"order_id": "586196597913388837", "order_status": "UNPAID", "update_time": 1790059970},
    "shop_id": "7494709429666874412",
    "timestamp": 1790059971,
}

# Response MENTAH (translate=false) seperti yang dikembalikan APISIX.
TIKTOK_ORDER_RAW = {
    "code": 0,
    "message": "success",
    "data": {
        "orders": [
            {
                "id": "586196597913388837",
                "status": "UNPAID",
                "line_items": [
                    {
                        "id": "li-1",
                        "product_id": "1736033390941733932",
                        "product_name": "Kaos Polos",
                        "sku_id": "1736032926678615084",
                        "sku_name": "M",
                        "seller_sku": "1A1-M",
                        "sale_price": "29900",
                        "quantity": 2,
                    },
                    {
                        # TikTok lama tidak mengirim quantity → 1 line item = 1 unit
                        "id": "li-2",
                        "product_id": "1736268650108650540",
                        "product_name": "Celana Chino",
                        "sku_id": "1736268732805121068",
                        "sku_name": "L",
                        "seller_sku": "1B1-L",
                        "sale_price": "50000",
                    },
                ],
            }
        ]
    },
}

SHOPEE_ORDER_RAW = {
    "response": {
        "order_list": [
            {
                "order_sn": "2501080NKAMXA8",
                "order_status": "UNPAID",
                "item_list": [
                    {
                        "item_id": 111,
                        "item_name": "Sandal Kulit",
                        "model_id": 1,
                        "model_name": "40",
                        "model_sku": "SD-40",
                        "model_quantity_purchased": 3,
                        "model_discounted_price": 15000,
                    },
                    {
                        # line item identik → di-merge (3 + 2 = 5)
                        "item_id": 111,
                        "item_name": "Sandal Kulit",
                        "model_id": 1,
                        "model_name": "40",
                        "model_sku": "SD-40",
                        "model_quantity_purchased": 2,
                        "model_discounted_price": 15000,
                    },
                ],
            }
        ]
    }
}


class FakeApisixClient:
    """APISIX client palsu — merekam panggilan, tanpa network."""

    def __init__(
        self,
        raw=None,
        error=None,
        reserved=None,
        reserved_error=None,
        reserved_callable=None,
    ):
        self.raw = raw                 # response fetch_order_detail
        self.error = error             # exception utk fetch_order_detail
        self.reserved = reserved       # dict {product_id: payload} ATAU payload tunggal/None
        self.reserved_error = reserved_error
        # callable(call_index_1based, product_id) → payload; dipakai test yang
        # butuh snapshot BERUBAH antar-panggilan (mis. retry order pemicu).
        self.reserved_callable = reserved_callable
        self.calls = []                # kwargs fetch_order_detail
        self.reserved_calls = []       # kwargs fetch_reserved_stock
        self.closed = False

    async def fetch_order_detail(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.raw

    async def fetch_reserved_stock(self, marketplace, shop_uuid, product_id=None, variant_id=None):
        self.reserved_calls.append(
            {
                "marketplace": marketplace,
                "shop_uuid": shop_uuid,
                "product_id": product_id,
                "variant_id": variant_id,
            }
        )
        if self.reserved_error:
            raise self.reserved_error
        if self.reserved_callable is not None:
            return self.reserved_callable(len(self.reserved_calls), product_id)
        if isinstance(self.reserved, dict) and "reserved_stock" not in self.reserved:
            # map product_id → payload (bukan response /order)
            return self.reserved.get(product_id)
        return self.reserved

    async def aclose(self):
        self.closed = True


def _reserved_payload(product_id, total, variants=None, order_id="X"):
    """Response /order?product_id=… (sudah difilter gateway → ada filtered_by).

    `order_id` = id order yang muncul di breakdown `orders[]` variant default;
    dipakai test retry order pemicu (order_id pemicu harus BISA muncul).
    """
    return {
        "marketplace": "tiktok",
        "reserved_stock": [
            {
                "product_id": product_id,
                "variants": variants
                or [
                    {
                        "variant_id": "v1",
                        "seller_sku": "1A1-M",
                        "total_reserved_stock": total,
                        "orders": [
                            {"order_id": order_id, "status": "UNPAID", "reserved_stock": total}
                        ],
                    }
                ],
            }
        ],
        "filtered_by": {"product_id": product_id},
        "pagination": {
            "page": 1,
            "page_size": 20,
            "total": 1,
            "has_next": False,
            "next_page_token": "",
        },
    }


@pytest.fixture
def order_env(tmp_path, monkeypatch):
    """Setting: Camunda dummy + enrichment detail order AKTIF (APISIX palsu)."""
    monkeypatch.setattr(settings, "camunda_enabled", True)
    monkeypatch.setattr(settings, "camunda_base_url", "http://camunda.test:8080")
    monkeypatch.setattr(settings, "camunda_version", "7")
    monkeypatch.setattr(settings, "camunda_rest_path", "")
    monkeypatch.setattr(settings, "camunda_auth_type", "none")
    monkeypatch.setattr(settings, "camunda_max_attempts", 1)
    monkeypatch.setattr(settings, "camunda_retry_delay_seconds", 0)
    monkeypatch.setattr(settings, "camunda_typed_variables", True)
    monkeypatch.setattr(settings, "camunda_tenant_id", "")
    monkeypatch.setattr(settings, "camunda_process_definition_key", "")
    monkeypatch.setattr(settings, "camunda_process_definition_id", "")
    monkeypatch.setattr(settings, "camunda_message_name", "")
    monkeypatch.setattr(settings, "camunda_business_key_template", "")
    monkeypatch.setattr(settings, "camunda_rules_file", "infrastructure/camunda/config.json")
    monkeypatch.setattr(settings, "camunda_log_enabled", True)
    monkeypatch.setattr(settings, "camunda_log_dir", str(tmp_path))
    monkeypatch.setattr(settings, "camunda_log_filename", "camunda_trigger.log")

    monkeypatch.setattr(settings, "webhook_order_detail_enabled", True)
    monkeypatch.setattr(settings, "webhook_order_detail_translate", False)
    monkeypatch.setattr(settings, "webhook_order_detail_timeout", 5.0)
    monkeypatch.setattr(settings, "apisix_order_path", "/order")
    monkeypatch.setattr(settings, "apisix_order_base_url", "")
    monkeypatch.setattr(settings, "apisix_shopee_shop_uuid", "")
    monkeypatch.setattr(settings, "apisix_tiktok_shop_uuid", "")
    # Retry order pemicu OFF di unit test (deterministik, tanpa sleep); test
    # yang menguji retry men-set delay kecil sendiri.
    monkeypatch.setattr(settings, "webhook_reserved_retry_delay", 0)
    # Resolve part_id (DB sku_mapping) OFF di default — test khusus bagian ini
    # menyalakannya lagi + memalsukan resolve_part_ids_for_products.
    monkeypatch.setattr(settings, "webhook_part_id_enabled", False)

    # Nama variant (DB) dipalsukan di default supaya unit test tidak menyentuh
    # DB; test yang menguji variant_name menggantinya sendiri.
    async def _fake_variant_names(product_ids, shop_uuid=""):
        return {}

    monkeypatch.setattr(
        "infrastructure.camunda.service.resolve_variant_names_for_products", _fake_variant_names
    )
    return tmp_path


def _make_transport(responses):
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        index = min(len(calls) - 1, len(responses) - 1)
        status, payload = responses[index]
        return httpx.Response(status, json=payload)

    return httpx.MockTransport(handler), calls


# ===========================================================================
# 1. order_detail.py
# ===========================================================================


def test_extract_order_id_per_marketplace():
    assert extract_order_id("shopee", SHOPEE_BODY) == "2501080NKAMXA8"
    assert extract_order_id("tiktok", TIKTOK_BODY) == "586196597913388837"
    # marketplace tak dikenal → field generik
    assert extract_order_id("lazada", {"data": {"order_sn": "LZ-1"}}) == "LZ-1"
    assert extract_order_id("shopee", {"code": 3, "data": {}}) is None
    assert extract_order_id("shopee", None) is None


def test_extract_webhook_order_status_per_marketplace():
    """Status order dibaca dari payload WEBHOOK mentah — dipakai gate status."""
    # Shopee (data.status) & TikTok (data.order_status)
    assert extract_webhook_order_status({"data": {"status": "READY_TO_SHIP"}}) == "READY_TO_SHIP"
    assert extract_webhook_order_status({"data": {"order_status": "UNPAID"}}) == "UNPAID"
    # variasi nama field lain
    assert extract_webhook_order_status({"data": {"orderStatus": "ON_HOLD"}}) == "ON_HOLD"
    assert extract_webhook_order_status({"data": {"new_status": "CANCELLED"}}) == "CANCELLED"
    # blok `data` diutamakan; level atas hanya fallback
    assert extract_webhook_order_status({"status": "UNPAID", "data": {}}) == "UNPAID"
    assert extract_webhook_order_status(
        {"status": "DIABAIKAN", "data": {"status": "UNPAID"}}
    ) == "UNPAID"
    # nilai non-string tetap dibaca sebagai string
    assert extract_webhook_order_status({"data": {"status": 3}}) == "3"
    # tanpa status / bentuk tidak dikenal
    assert extract_webhook_order_status({"data": {"order_id": "X"}}) is None
    assert extract_webhook_order_status({"data": None}) is None
    assert extract_webhook_order_status(None) is None


def test_resolve_shop_uuid_from_payload_and_settings(order_env, monkeypatch):
    # payload shop_id == shop_uuid di credentials.json APISIX
    assert resolve_shop_uuid("shopee", SHOPEE_BODY) == "341431138"
    assert resolve_shop_uuid("tiktok", TIKTOK_BODY) == "7494709429666874412"
    assert resolve_shop_uuid("shopee", {}) == ""

    # override lewat setting (bila shop_uuid beda dari shop_id)
    monkeypatch.setattr(settings, "apisix_shopee_shop_uuid", "SHOP-UUID-X")
    assert resolve_shop_uuid("shopee", SHOPEE_BODY) == "SHOP-UUID-X"


def test_normalize_tiktok_raw_order_detail():
    detail = normalize_order_detail("tiktok", TIKTOK_ORDER_RAW, "586196597913388837", "7494709429666874412")

    assert detail["ok"] is True
    assert detail["source"] == "order_detail"
    assert detail["order_status"] == "UNPAID"
    assert detail["item_count"] == 2
    # 2 (quantity eksplisit) + 1 (fallback TikTok) = 3
    assert detail["total_quantity"] == 3
    assert detail["products_summary"] == "Kaos Polos (M) x2, Celana Chino (L) x1"
    assert detail["product_names"] == ["Kaos Polos (M)", "Celana Chino (L)"]
    assert detail["items"][0]["product_id"] == "1736033390941733932"
    assert detail["items"][0]["quantity"] == 2
    assert detail["items"][1]["quantity"] == 1


def test_normalize_shopee_raw_merges_identical_line_items():
    detail = normalize_order_detail("shopee", SHOPEE_ORDER_RAW, "2501080NKAMXA8", "341431138")

    assert detail["ok"] is True
    assert detail["item_count"] == 1          # dua line item identik → satu baris
    assert detail["total_quantity"] == 5      # 3 + 2
    assert detail["products_summary"] == "Sandal Kulit (40) x5"
    assert detail["items"][0]["seller_sku"] == "SD-40"
    assert detail["items"][0]["unit_price"] == 15000


def test_normalize_fallback_standardized_reserved_stock():
    """translate=true (hasil standardisasi) tetap didukung (tanpa nama produk)."""
    raw = {
        "marketplace": "tiktok",
        "reserved_stock": [
            {
                "product_id": "1736",
                "variants": [
                    {"variant_id": "v1", "seller_sku": "1A1", "total_reserved_stock": 4}
                ],
            }
        ],
    }
    detail = normalize_order_detail("tiktok", raw, "586196597913388837", "7494709429666874412")

    assert detail["ok"] is True
    assert detail["source"] == "reserved_stock"
    assert detail["total_quantity"] == 4
    assert detail["products_summary"] == "1A1 x4"


def test_normalize_invalid_or_empty_response_is_not_ok():
    assert normalize_order_detail("shopee", None, "X", "1")["ok"] is False
    assert normalize_order_detail("shopee", {"foo": "bar"}, "X", "1")["ok"] is False
    # error diteruskan apa adanya
    failed = normalize_order_detail("shopee", None, "X", "1", error="fetch_failed")
    assert failed["error"] == "fetch_failed"


def test_build_order_variables_shape():
    detail = normalize_order_detail("tiktok", TIKTOK_ORDER_RAW, "586196597913388837", "749")
    variables = build_order_variables(detail)

    assert variables["orderTotalQuantity"] == 3
    assert variables["orderItemCount"] == 2
    assert variables["orderProducts"] == "Kaos Polos (M) x2, Celana Chino (L) x1"
    assert variables["orderDetailOk"] is True
    assert isinstance(variables["orderItems"], list)
    assert build_order_variables({}) == {}


# ===========================================================================
# 2. rules.py — namespace order.*
# ===========================================================================


def test_resolve_path_order_namespace(order_env):
    ctx = {
        "body": {"data": {"ordersn": "A1"}},
        "order": {"items": [{"x": 1}], "total_quantity": 3, "products_summary": "Kaos x3"},
    }
    assert resolve_path("order.total_quantity", ctx) == 3
    assert resolve_path("order.products_summary", ctx) == "Kaos x3"
    assert resolve_path("order.items.0.x", ctx) == 1
    assert resolve_path("order_detail.total_quantity", ctx) == 3
    # tanpa prefix → payload webhook
    assert resolve_path("data.ordersn", ctx) == "A1"


def test_variables_can_map_order_namespace_and_template(order_env):
    ctx = {
        "body": {"data": {"ordersn": "A1"}},
        "order": {"total_quantity": 3, "products_summary": "Kaos x3"},
        "meta": {"marketplace": "shopee", "signal_code": 3},
        "headers": {},
        "variables": {},
    }
    variables = resolve_variables(
        {
            "orderProducts": {"path": "order.products_summary"},
            "orderTotalQuantity": {"path": "order.total_quantity"},
            "orderSummaryText": {"template": "{{order.products_summary}} ({{marketplace}})"},
            "missingOrder": {"path": "order.tidak_ada"},
        },
        {},
        ctx,
    )

    assert variables["orderProducts"] == "Kaos x3"
    assert variables["orderTotalQuantity"] == 3
    assert variables["orderSummaryText"] == "Kaos x3 (shopee)"
    assert "missingOrder" not in variables  # detail order tidak ada → dilewati


# ===========================================================================
# 3. client.py — fetch_order_detail
# ===========================================================================


@pytest.mark.asyncio
async def test_fetch_order_detail_url_and_params(order_env):
    raw = {"data": {"orders": []}}
    transport, calls = _make_transport([(200, raw)])
    client = ApisixClient()
    client._client = httpx.AsyncClient(transport=transport)

    result = await client.fetch_order_detail(
        marketplace="shopee",
        shop_uuid="341431138",
        order_id="2501080NKAMXA8",
        translate=False,
    )
    await client.aclose()

    assert result == raw
    assert len(calls) == 1
    request = calls[0]
    assert request.url.path.endswith("/order")
    assert request.url.params["marketplace"] == "shopee"
    assert request.url.params["shop_uuid"] == "341431138"
    assert request.url.params["ids"] == "2501080NKAMXA8"
    assert request.url.params["translate"] == "false"


@pytest.mark.asyncio
async def test_fetch_order_detail_uses_order_base_url_override(order_env, monkeypatch):
    """APISIX_ORDER_BASE_URL memisahkan gateway order dari APISIX_BASE_URL."""
    monkeypatch.setattr(settings, "apisix_base_url", "https://tidak-terjangkau.example/apisix")
    monkeypatch.setattr(settings, "apisix_order_base_url", "http://192.168.1.185:9080")
    transport, calls = _make_transport([(200, {"data": {"orders": []}})])
    client = ApisixClient()
    client._client = httpx.AsyncClient(transport=transport)

    await client.fetch_order_detail("tiktok", "749", "586")
    await client.aclose()

    assert str(calls[0].url).startswith("http://192.168.1.185:9080/order?")


@pytest.mark.asyncio
async def test_fetch_order_detail_graceful_on_http_error(order_env):
    transport, calls = _make_transport([(500, {"error": "boom"})])
    client = ApisixClient()
    client._client = httpx.AsyncClient(transport=transport)

    result = await client.fetch_order_detail("tiktok", "749", "586")
    await client.aclose()

    assert result is None  # fail-open, bukan exception


@pytest.mark.asyncio
async def test_fetch_order_detail_graceful_on_connection_error(order_env):
    """Koneksi/TLS gagal (str exception bisa kosong) tetap tidak crash."""
    async def handler(request):
        raise httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF")

    client = ApisixClient()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    result = await client.fetch_order_detail("tiktok", "749", "586")
    await client.aclose()

    assert result is None


@pytest.mark.asyncio
async def test_fetch_order_detail_skips_incomplete_input(order_env):
    client = ApisixClient()
    assert await client.fetch_order_detail("shopee", "", "A1") is None
    assert await client.fetch_order_detail("shopee", "341", "") is None


# ===========================================================================
# 4. service.py — enrich lalu start event
# ===========================================================================


@pytest.mark.asyncio
async def test_service_enriches_camunda_variables_with_order_products(order_env):
    """Start event menerima produk yang di-order & jumlahnya (payload referensi)."""
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW)
    transport, calls = _make_transport([(200, {"id": "proc-1"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(
        marketplace="tiktok",
        signal_code=1,
        body=TIKTOK_BODY,
        raw_body=json.dumps(TIKTOK_BODY),
    )

    assert result.ok is True
    # APISIX dipanggil dengan order id webhook + shop id payload
    assert fake_apisix.calls == [
        {
            "marketplace": "tiktok",
            "shop_uuid": "7494709429666874412",
            "order_id": "586196597913388837",
            "translate": False,
            "timeout": 5.0,
        }
    ]
    assert fake_apisix.closed is True

    body = json.loads(calls[0].content)
    variables = body["variables"]
    assert variables["orderTotalQuantity"] == {"value": 3, "type": "Long"}
    assert variables["orderItemCount"] == {"value": 2, "type": "Long"}
    assert variables["orderProducts"] == {
        "value": "Kaos Polos (M) x2, Celana Chino (L) x1",
        "type": "String",
    }
    assert variables["orderDetailOk"] == {"value": True, "type": "Boolean"}
    # Tipe Json dikirim sebagai TEKS JSON (wajib di Camunda 7) dan tetap
    # identik isinya — cukup parse ulang di sisi proses.
    assert variables["orderItems"]["type"] == "Json"
    items = json.loads(variables["orderItems"]["value"])
    assert items[0]["product_name"] == "Kaos Polos"
    assert items[1]["quantity"] == 1

    # Ringkasan mudah dibaca juga tercatat di log trigger
    log_path = order_env / "camunda_trigger.log"
    entry = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
    assert entry["order_detail"]["total_quantity"] == 3
    assert entry["order_detail"]["products_summary"] == "Kaos Polos (M) x2, Celana Chino (L) x1"


@pytest.mark.asyncio
async def test_service_shopee_order_detail_variables(order_env):
    fake_apisix = FakeApisixClient(raw=SHOPEE_ORDER_RAW)
    transport, calls = _make_transport([(200, {"id": "proc-s"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    variables = json.loads(calls[0].content)["variables"]
    assert variables["orderProducts"] == {"value": "Sandal Kulit (40) x5", "type": "String"}
    assert variables["orderTotalQuantity"] == {"value": 5, "type": "Long"}
    assert fake_apisix.calls[0]["order_id"] == "2501080NKAMXA8"
    assert fake_apisix.calls[0]["shop_uuid"] == "341431138"


@pytest.mark.asyncio
async def test_service_fail_open_when_apisix_fetch_fails(order_env, caplog):
    """APISIX gagal (client mengembalikan None) → start event TETAP dijalankan."""
    caplog.set_level(logging.INFO)
    fake_apisix = FakeApisixClient(raw=None)
    transport, calls = _make_transport([(200, {"id": "proc-2"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert len(calls) == 1  # Camunda tetap dipanggil
    variables = json.loads(calls[0].content)["variables"]
    # item/kount TIDAK dikirim (path-nya kosong → dilewati) …
    assert "orderItems" not in variables
    assert "orderTotalQuantity" not in variables
    assert "orderItemCount" not in variables
    # …orderProducts punya fallback {"literal": ""} di config.json, dan
    # orderDetailOk tetap terkirim sebagai False (alasan kegagalan ada di log).
    assert variables["orderProducts"] == {"value": "", "type": "String"}
    assert variables["orderDetailOk"] == {"value": False, "type": "Boolean"}
    assert variables["orderId"] == {"value": "586196597913388837", "type": "String"}

    # Anti-regresi: pemanggilan log detail order TIDAK boleh memakai %d saat
    # nilai bisa None (pernah membuat logging error + traceback panjang).
    detail_records = [r for r in caplog.records if "detail order" in str(r.msg)]
    assert detail_records, "log detail order tidak tercatat"
    detail_record = detail_records[-1]
    assert "%d" not in detail_record.msg
    assert "TIDAK terbaca" in str(detail_record.msg)
    # Pastikan record benar-benar bisa diformat (tidak ada argumen yang meleset).
    assert detail_record.getMessage()

    log_path = order_env / "camunda_trigger.log"
    entry = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
    assert entry["order_detail"]["ok"] is False
    assert entry["order_detail"]["error"] == "fetch_failed"


@pytest.mark.asyncio
async def test_service_fail_open_when_apisix_client_raises(order_env):
    """Exception tak terduga dari client APISIX juga tidak boleh menggagalkan webhook."""
    fake_apisix = FakeApisixClient(error=httpx.ConnectError("gateway down"))
    transport, calls = _make_transport([(200, {"id": "proc-raise"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert len(calls) == 1
    variables = json.loads(calls[0].content)["variables"]
    assert "orderItems" not in variables
    assert variables["orderDetailOk"] == {"value": False, "type": "Boolean"}
    assert fake_apisix.closed is True  # client tetap ditutup


@pytest.mark.asyncio
async def test_service_does_not_fetch_when_disabled(order_env, monkeypatch):
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW)
    transport, calls = _make_transport([(200, {"id": "proc-3"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert fake_apisix.calls == []
    variables = json.loads(calls[0].content)["variables"]
    assert "orderItems" not in variables


@pytest.mark.asyncio
async def test_service_skips_fetch_when_no_order_id(order_env):
    """Payload tanpa order id → tidak ada request APISIX, start event tetap jalan.

    Status ordernya tetap harus lolos gate `order_status` config.json
    (lihat tests/test_camunda_trigger.py — gate status order).
    """
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW)
    transport, calls = _make_transport([(200, {"id": "proc-4"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(
        marketplace="shopee",
        signal_code=9,
        body={"code": 9, "data": {"status": "UNPAID"}},
    )

    assert fake_apisix.calls == []
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_service_accepts_prefetched_order_detail(order_env):
    """order_detail boleh disuplai pemanggil (tanpa fetch ulang)."""
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW)
    transport, calls = _make_transport([(200, {"id": "proc-5"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )
    prefetched = normalize_order_detail("tiktok", TIKTOK_ORDER_RAW, "586196597913388837", "749")

    await service.trigger(
        marketplace="tiktok", signal_code=1, body=TIKTOK_BODY, order_detail=prefetched
    )

    assert fake_apisix.calls == []
    variables = json.loads(calls[0].content)["variables"]
    assert variables["orderTotalQuantity"] == {"value": 3, "type": "Long"}


# ===========================================================================
# 5. variables.py — tipe Json wajib teks JSON (anti HTTP 400 Camunda 7)
# ===========================================================================


def test_json_typed_variable_is_serialized_to_string():
    """Camunda 7 menolak nilai array/objek untuk tipe Json.

    Error aslinya (dari Camunda nyata):
        Must provide 'null' or String value for value of SerializableValue type 'Json'.
    """
    from infrastructure.camunda.variables import build_typed_variable, serialize_json_value

    # dict/list → teks JSON
    value, vtype = build_typed_variable([{"a": 1}], "json")
    assert vtype == "Json"
    assert isinstance(value, str)
    assert json.loads(value) == [{"a": 1}]

    # tipe Json TANPA deklarasi eksplisit (hasil inferensi) tetap diserialize
    value, vtype = build_typed_variable({"a": "Kaos"})
    assert (vtype, value) == ("Json", '{"a": "Kaos"}')

    # teks JSON yang sudah string → diteruskan apa adanya (tidak dobel encode)
    assert serialize_json_value('[{"a": 1}]', "Json") == '[{"a": 1}]'

    # non-Json tidak tersentuh
    assert serialize_json_value([1, 2], "String") == [1, 2]
    assert build_typed_variable(3, "long") == (3, "Long")

    # karakter non-ASCII (nama produk Indonesia) tidak di-escape
    value, _ = build_typed_variable({"name": "Kaos x2 (cm)"}, "Json")
    assert "Kaos x2 (cm)" in value


def test_describe_config_includes_order_detail(order_env):
    config = CamundaWebhookTrigger().describe_config()
    assert config["order_detail"]["enabled"] is True
    assert config["order_detail"]["path"] == "/order"
    assert config["order_detail"]["translate"] is False
    assert "orderItems" in config["variables"]
    assert "orderTotalQuantity" in config["variables"]
    # reserved stock per produk (baru)
    assert config["order_reserved"]["enabled"] is True
    assert config["order_reserved"]["max_products"] >= 1
    assert "total_reserved" in config["variables"]
    assert "reserved_products" in config["variables"]


# ===========================================================================
# 6. TOTAL RESERVED STOCK per produk (APISIX /order?product_id=…)
# ===========================================================================


@pytest.mark.asyncio
async def test_service_fetches_total_reserved_for_order_products(order_env):
    """Setiap produk di order di-query ke /order?product_id=… → variabel
    `total_reserved` (jumlah) + `reserved_products` (map per produk)."""
    reserved_map = {
        "1736033390941733932": _reserved_payload("1736033390941733932", 3),
        "1736268650108650540": _reserved_payload("1736268650108650540", 5),
    }
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved=reserved_map)
    transport, calls = _make_transport([(200, {"id": "proc-reserved"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    # satu panggilan /order PER PRODUK, dengan filter product_id gateway
    assert [c["product_id"] for c in fake_apisix.reserved_calls] == [
        "1736033390941733932",
        "1736268650108650540",
    ]
    assert all(c["shop_uuid"] == "7494709429666874412" for c in fake_apisix.reserved_calls)
    assert all(c["variant_id"] is None for c in fake_apisix.reserved_calls)

    variables = json.loads(calls[0].content)["variables"]
    assert variables["total_reserved"] == {"value": 8, "type": "Long"}
    assert variables["reserved_products"]["type"] == "Json"
    # Bentuk NESTED: total per produk + breakdown per VARIANT (bukan hanya
    # satu angka per produk seperti dulu).
    assert json.loads(variables["reserved_products"]["value"]) == {
        "1736033390941733932": {
            "total_reserved": 3,
            "variants": {
                "v1": {
                    "variant_id": "v1",
                    "seller_sku": "1A1-M",
                    "total_reserved_stock": 3,
                    "variant_name": None,
                }
            },
        },
        "1736268650108650540": {
            "total_reserved": 5,
            "variants": {
                "v1": {
                    "variant_id": "v1",
                    "seller_sku": "1A1-M",
                    "total_reserved_stock": 5,
                    "variant_name": None,
                }
            },
        },
    }

    log_path = order_env / "camunda_trigger.log"
    entry = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
    assert entry["order_detail"]["total_reserved"] == 8
    assert (
        entry["order_detail"]["reserved_by_product"]["1736033390941733932"][
            "total_reserved"
        ]
        == 3
    )


@pytest.mark.asyncio
async def test_service_reserved_products_lists_each_variant_of_multivariant_product(
    order_env, monkeypatch
):
    """Order dengan SATU produk berisi DUA variant → keduanya muncul di
    ``reserved_products`` bersama variant_id, seller_sku, jumlah, DAN nama variant.

    Regresi: dulu ``reserved_products`` hanya ``{product_id: total}`` sehingga
    produk multi-variant tampil sebagai SATU angka (isi variant hilang).
    """
    variants = [
        {
            "variant_id": "1736672772166353964",
            "seller_sku": "8F14",
            "total_reserved_stock": 3,
            "orders": [
                {"order_id": "X", "status": "UNPAID", "reserved_stock": 3}
            ],
        },
        {
            "variant_id": "1736672772166288428",
            "seller_sku": "8F14-Pack-100",
            "total_reserved_stock": 6,
            "orders": [
                {"order_id": "X", "status": "UNPAID", "reserved_stock": 6}
            ],
        },
    ]
    product_id = "1736033390941733932"
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW,
        reserved={product_id: _reserved_payload(product_id, 9, variants=variants)},
    )
    transport, calls = _make_transport([(200, {"id": "proc-multivariant"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    async def _fake_variant_names(product_ids, shop_uuid=""):
        return {
            (product_id, "1736672772166353964"): "Single Pack",
            (product_id, "1736672772166288428"): "Pack of 100",
        }

    monkeypatch.setattr(
        "infrastructure.camunda.service.resolve_variant_names_for_products", _fake_variant_names
    )

    result = await service.trigger(
        marketplace="tiktok", signal_code=1, body=TIKTOK_BODY
    )

    assert result.ok is True
    variables = json.loads(calls[0].content)["variables"]
    reserved = json.loads(variables["reserved_products"]["value"])

    product_entry = reserved[product_id]
    assert product_entry["total_reserved"] == 9
    assert product_entry["variants"] == {
        "1736672772166353964": {
            "variant_id": "1736672772166353964",
            "seller_sku": "8F14",
            "total_reserved_stock": 3,
            "variant_name": "Single Pack",
        },
        "1736672772166288428": {
            "variant_id": "1736672772166288428",
            "seller_sku": "8F14-Pack-100",
            "total_reserved_stock": 6,
            "variant_name": "Pack of 100",
        },
    }


@pytest.mark.asyncio
async def test_service_reserved_fetch_failure_is_fail_open(order_env):
    """Gagal ambil reserved → variabel reserved dilewati, start event tetap jalan."""
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW, reserved_error=httpx.ConnectError("gateway down")
    )
    transport, calls = _make_transport([(200, {"id": "proc-fail"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert len(calls) == 1
    variables = json.loads(calls[0].content)["variables"]
    assert "total_reserved" not in variables
    assert "reserved_products" not in variables
    # detail order TETAP terkirim
    assert "orderItems" in variables
    assert fake_apisix.closed is True


@pytest.mark.asyncio
async def test_service_reserved_non_object_body_is_fail_open(order_env, caplog):
    """REGRESI: gateway membalas body literal `true` (bug Lua pcall terbalik).

    Sebelumnya meledak `AttributeError: 'bool' object has no attribute 'get'`
    dan variabel reserved hilang tanpa keterangan. Sekarang: fail-open rapi —
    variabel reserved dilewati, start event tetap jalan, warning jelas.
    """
    caplog.set_level(logging.WARNING)
    fake_apisix = FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved=True)  # body `true`
    transport, calls = _make_transport([(200, {"id": "proc-bool"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert len(calls) == 1
    variables = json.loads(calls[0].content)["variables"]
    assert "total_reserved" not in variables
    assert "reserved_products" not in variables
    assert "orderItems" in variables
    assert "tidak terbaca" in caplog.text
    assert "AttributeError" not in caplog.text


@pytest.mark.asyncio
async def test_service_reserved_disabled_skips_order_calls(order_env, monkeypatch):
    """WEBHOOK_RESERVED_ENABLED=false → tidak ada panggilan /order?product_id=."""
    monkeypatch.setattr(settings, "webhook_reserved_enabled", False)
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW,
        reserved={"1736033390941733932": _reserved_payload("1736033390941733932", 3)},
    )
    transport, calls = _make_transport([(200, {"id": "proc-off"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert fake_apisix.reserved_calls == []
    variables = json.loads(calls[0].content)["variables"]
    assert "total_reserved" not in variables


@pytest.mark.asyncio
async def test_service_reserved_max_products_cap(order_env, monkeypatch):
    """WEBHOOK_RESERVED_MAX_PRODUCTS membatasi jumlah panggilan per order."""
    monkeypatch.setattr(settings, "webhook_reserved_max_products", 1)
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW,
        reserved={
            "1736033390941733932": _reserved_payload("1736033390941733932", 3),
            "1736268650108650540": _reserved_payload("1736268650108650540", 5),
        },
    )
    transport, calls = _make_transport([(200, {"id": "proc-cap"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert len(fake_apisix.reserved_calls) == 1
    variables = json.loads(calls[0].content)["variables"]
    assert variables["total_reserved"] == {"value": 3, "type": "Long"}


def test_sum_product_reserved_filters_client_side(caplog):
    """Bila gateway BELUM di-reload (filter diabaikan), total tetap per produk."""
    caplog.set_level(logging.WARNING)
    raw = {
        "marketplace": "shopee",
        "reserved_stock": [
            {"product_id": "A", "variants": [{"variant_id": "a1", "total_reserved_stock": 2}]},
            {
                "product_id": "B",
                "variants": [
                    {"variant_id": "b1", "total_reserved_stock": 9},
                    {"variant_id": "b2", "total_reserved_stock": 1},
                ],
            },
        ],
        "pagination": {"page": 1, "page_size": 20, "total": 2, "has_next": False},
        # TANPA filtered_by → penanda filter gateway belum aktif
    }

    assert CamundaWebhookTrigger._sum_product_reserved(raw, "A") == 2
    assert CamundaWebhookTrigger._sum_product_reserved(raw, "B") == 10
    assert CamundaWebhookTrigger._sum_product_reserved(raw, "C") == 0
    assert "tidak menerapkan filter product_id" in caplog.text

    # bentuk tidak dikenal → None (variabel reserved dilewati)
    assert CamundaWebhookTrigger._sum_product_reserved(None, "A") is None
    assert CamundaWebhookTrigger._sum_product_reserved({"foo": 1}, "A") is None


def test_sum_product_reserved_accepts_gateway_filtered_response():
    """Response gateway yang sudah difilter (ada filtered_by) dijumlah apa adanya."""
    payload = _reserved_payload("802023254", 7)
    assert CamundaWebhookTrigger._sum_product_reserved(payload, "802023254") == 7

    # reserved_stock kosong ([]) = produk tanpa order ber-status reserved → 0
    empty = {"reserved_stock": [], "filtered_by": {"product_id": "X"}}
    assert CamundaWebhookTrigger._sum_product_reserved(empty, "X") == 0


def test_collect_product_ids_unique_and_ordered():
    detail = {
        "items": [
            {"product_id": "A"},
            {"product_id": "B"},
            {"product_id": "A"},   # duplikat → di-skip
            {"product_id": ""},    # kosong → di-skip
            {},                     # tanpa product_id → di-skip
        ]
    }
    assert CamundaWebhookTrigger._collect_product_ids(detail) == ["A", "B"]
    assert CamundaWebhookTrigger._collect_product_ids({}) == []


# ===========================================================================
# 7. Retry order pemicu (indeks daftar order marketplace tertinggal)
# ===========================================================================

TRIGGER_ORDER_ID = "586196597913388837"  # order_id di TIKTOK_BODY
# Total product di TIKTOK_ORDER_RAW (urutan = _collect_product_ids).
_STALE_TOTALS = {"1736033390941733932": 3, "1736268650108650540": 5}


def _variant(orders):
    """Satu variant reserved-stock dengan breakdown order (untuk uji retry)."""
    return {
        "variant_id": "v1",
        "seller_sku": "1A1-M",
        "total_reserved_stock": sum(int(o["reserved_stock"]) for o in orders),
        "orders": orders,
    }


def _order(order_id, reserved, status="ON_HOLD"):
    return {"order_id": order_id, "status": status, "reserved_stock": reserved}


def _lagging_gateway(call_index, product_id):
    """Snapshot round-1 TANPA order pemicu, round-2 sudah memuatnya.

    Meniru TikTok: webhook tiba beberapa detik setelah order dibuat, sementara
    daftar order (`orders/search`) yang diagregasi gateway baru memuat order
    itu beberapa detik kemudian. 2 produk per round.
    """
    stale_total = _STALE_TOTALS[product_id]
    if call_index <= len(_STALE_TOTALS):  # round pertama (per produk)
        return _reserved_payload(product_id, stale_total)
    return _reserved_payload(
        product_id,
        stale_total + 1,
        variants=[
            _variant([
                _order("586190000000000000", stale_total),
                _order(TRIGGER_ORDER_ID, 1),
            ])
        ],
    )


@pytest.mark.asyncio
async def test_service_reserved_retries_when_trigger_order_not_indexed(
    order_env, monkeypatch, caplog
):
    """REGRESI: total_reserved kurang 1 karena order pemicu belum terindeks.

    Kasus nyata: order dibuat 16:49:40, webhook 16:49:41.9, snapshot /order
    16:49:43 → total_reserved=4 padahal beberapa saat kemudian gateway bilang
    5. Backend sekarang menunggu WEBHOOK_RESERVED_RETRY_DELAY lalu ambil ulang.
    """
    caplog.set_level(logging.INFO, logger="infrastructure.camunda.service")
    monkeypatch.setattr(settings, "webhook_reserved_retry_delay", 0.01)
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW, reserved_callable=_lagging_gateway
    )
    transport, calls = _make_transport([(200, {"id": "proc-retry"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    # 2 produk × 2 round (snapshot awal + retry)
    assert [c["product_id"] for c in fake_apisix.reserved_calls] == [
        "1736033390941733932",
        "1736268650108650540",
        "1736033390941733932",
        "1736268650108650540",
    ]
    variables = json.loads(calls[0].content)["variables"]
    # (3+1) + (5+1) — bukan 8 seperti snapshot pertama
    assert variables["total_reserved"] == {"value": 10, "type": "Long"}
    reserved = json.loads(variables["reserved_products"]["value"])
    assert reserved["1736033390941733932"]["total_reserved"] == 4
    assert reserved["1736268650108650540"]["total_reserved"] == 6
    assert "belum ada di snapshot reserved stock" in caplog.text
    assert "sudah terhitung setelah retry" in caplog.text


@pytest.mark.asyncio
async def test_service_reserved_no_retry_when_trigger_order_present(
    order_env, monkeypatch
):
    """Order pemicu sudah ada di snapshot pertama → TIDAK ada panggilan ulang."""
    monkeypatch.setattr(settings, "webhook_reserved_retry_delay", 0.01)
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW,
        reserved={
            "1736033390941733932": _reserved_payload(
                "1736033390941733932", 4, order_id=TRIGGER_ORDER_ID
            ),
            "1736268650108650540": _reserved_payload(
                "1736268650108650540", 6, order_id=TRIGGER_ORDER_ID
            ),
        },
    )
    transport, calls = _make_transport([(200, {"id": "proc-fresh"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert len(fake_apisix.reserved_calls) == 2  # tanpa retry
    variables = json.loads(calls[0].content)["variables"]
    assert variables["total_reserved"] == {"value": 10, "type": "Long"}


@pytest.mark.asyncio
async def test_service_reserved_retry_disabled_keeps_first_snapshot(order_env):
    """WEBHOOK_RESERVED_RETRY_DELAY=0 → perilaku lama: satu snapshot apa adanya."""
    fake_apisix = FakeApisixClient(
        raw=TIKTOK_ORDER_RAW, reserved_callable=_lagging_gateway
    )
    transport, calls = _make_transport([(200, {"id": "proc-noretry"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert len(fake_apisix.reserved_calls) == 2
    variables = json.loads(calls[0].content)["variables"]
    assert variables["total_reserved"] == {"value": 8, "type": "Long"}  # 3 + 5


def test_reserved_order_ids_collects_breakdown():
    """`_reserved_order_ids` membaca order_id dari breakdown orders[] variant."""
    payload = _reserved_payload("P", 2, order_id="ORDER-1")
    assert CamundaWebhookTrigger._reserved_order_ids(payload) == {"ORDER-1"}
    assert CamundaWebhookTrigger._collect_reserved_order_ids(
        {"P": payload, "Q": _reserved_payload("Q", 1, order_id="ORDER-2")}
    ) == {"ORDER-1", "ORDER-2"}

    # bentuk tidak dikenal → set kosong (tidak crash, tidak retry palsu)
    assert CamundaWebhookTrigger._reserved_order_ids(None) == set()


# ===========================================================================
# 7. PART ID dari sku_mapping (product_id order → inventree_part_id)
# ===========================================================================


def _part_service(fake_apisix):
    transport, calls = _make_transport([(200, {"id": "proc-part"})])
    service = CamundaWebhookTrigger(
        client_factory=lambda: CamundaClient(transport=transport),
        apisix_client_factory=lambda: fake_apisix,
    )
    return service, calls


@pytest.mark.asyncio
async def test_service_attaches_part_ids_from_sku_mapping(order_env, monkeypatch):
    """product_id line item → sku_mapping → order.part_id (ARRAY) → variabel
    `partId` (teks JSON array) ikut dikirim saat START EVENT (serviceWebhookTest).

    Satu product marketplace bisa memetakan ke BANYAK part_id, jadi `partId`
    berbentuk array — bukan skalar, dan TIDAK ada variabel `partIds` terpisah.
    """
    captured: dict = {}

    async def fake_resolve(product_ids):
        captured["product_ids"] = list(product_ids)
        return [7, 9]

    monkeypatch.setattr(settings, "webhook_part_id_enabled", True)
    monkeypatch.setattr("infrastructure.camunda.service.resolve_part_ids_for_products", fake_resolve)

    service, calls = _part_service(FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved={}))

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    # product_id unik + BERURUTAN sesuai line item order (part pertama = utama)
    assert captured["product_ids"] == [
        "1736033390941733932",
        "1736268650108650540",
    ]
    variables = json.loads(calls[0].content)["variables"]
    # partId = TIPE STRING berisi teks JSON ARRAY (tipe Json Camunda Spin tidak
    # bisa di-round-trip lewat fetchAndLock external task → isi array hilang).
    assert variables["partId"]["type"] == "String"
    assert json.loads(variables["partId"]["value"]) == [7, 9]
    assert "partIds" not in variables


@pytest.mark.asyncio
async def test_service_part_id_resolver_failure_is_fail_open(order_env, monkeypatch):
    """DB sku_mapping gagal → partId dilewati, start event + variabel order tetap jalan."""

    async def boom(product_ids):
        raise RuntimeError("db down")

    monkeypatch.setattr(settings, "webhook_part_id_enabled", True)
    monkeypatch.setattr("infrastructure.camunda.service.resolve_part_ids_for_products", boom)

    service, calls = _part_service(FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved={}))

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert len(calls) == 1
    variables = json.loads(calls[0].content)["variables"]
    assert "partId" not in variables
    assert "partIds" not in variables
    assert "orderItems" in variables  # detail order TETAP terkirim


@pytest.mark.asyncio
async def test_service_part_id_no_mapping_leaves_variables_out(order_env, monkeypatch):
    """Product tidak punya sku_mapping ACTIVE → variabel partId tidak ditulis."""

    async def empty(product_ids):
        return []

    monkeypatch.setattr(settings, "webhook_part_id_enabled", True)
    monkeypatch.setattr("infrastructure.camunda.service.resolve_part_ids_for_products", empty)

    service, calls = _part_service(FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved={}))

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    variables = json.loads(calls[0].content)["variables"]
    assert "partId" not in variables
    assert "partIds" not in variables


@pytest.mark.asyncio
async def test_service_part_id_disabled_skips_lookup(order_env, monkeypatch):
    """WEBHOOK_PART_ID_ENABLED=false → resolver tidak dipanggil sama sekali."""
    called: list = []

    async def fake_resolve(product_ids):
        called.append(list(product_ids))
        return [7]

    monkeypatch.setattr(settings, "webhook_part_id_enabled", False)
    monkeypatch.setattr("infrastructure.camunda.service.resolve_part_ids_for_products", fake_resolve)

    service, calls = _part_service(FakeApisixClient(raw=TIKTOK_ORDER_RAW, reserved={}))

    await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert called == []
    variables = json.loads(calls[0].content)["variables"]
    assert "partId" not in variables
    assert CamundaWebhookTrigger._reserved_order_ids({"reserved_stock": "x"}) == set()
    assert CamundaWebhookTrigger._reserved_order_ids({"reserved_stock": [{"a": 1}]}) == set()
    assert CamundaWebhookTrigger._collect_reserved_order_ids(None) == set()
