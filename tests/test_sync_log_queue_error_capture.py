"""
test_sync_log_queue_error_capture.py

Test apakah ERROR MESSAGE dari APISIX "diterima" (tersimpan penuh) oleh
stock_sync_queue.error_message & stock_sync_log.error_message.

Skenario yang diuji (semuanya level pipeline — mock APISIX client, mock DB
session, TIDAK butuh DB / jaringan asli):

  Format A (batch multi-SKU, update_stock_batch):
    1. HTTP 500 + body JSON error APISIX  → error_message memuat FULL body
    2. HTTP 200 + envelope {"error": …}   → error_message memuat FULL envelope
    3. Body error > 2000 karakter         → di-truncate + suffix "...[truncated]"
    4. Sukses ( kontrol )                 → error_message None / tidak FAILED

  Format B (flat per-SKU, update_stock):
    5. HTTP 500 + body JSON error APISIX  → error_message memuat FULL body
    6. HTTP 200 + envelope {"error": …}   → error_message memuat FULL envelope
    7. HTTP 429 (rate limit)              → pesan tetap diterima utk queue+log

Rantai yang diverifikasi:
    APISIX response body
      → httpx.HTTPStatusError (message di-embed body oleh _raise_http_error_with_body)
      → pipeline _http_error_detail() → item["push_error"]
      → StockSyncQueueModel.error_message + StockSyncLogModel.error_message

Untuk HTTP 200 + envelope {"error": …} rantainya:
    → pipeline _format_apisix_error_envelope() → item["push_error"]
      → StockSyncQueueModel.error_message + StockSyncLogModel.error_message

Run:
    pytest tests/test_sync_log_queue_error_capture.py -v
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from core.config import settings
from domain.enums import RecordStatus
from infrastructure.db.models import (
    SKUMappingModel,
    StockSyncLogModel,
    StockSyncQueueModel,
)
from application.usecases.stock_sync_pipeline import (
    PUSH_ERROR_MAX_CHARS,
    PipelineCycleResult,
    StockSyncPipelineEngine,
)

# --------------------------------------------------------------------------
# Payload error APISIX yang realistis (mirip e2e-reports/LOGS_*.JSON)
# --------------------------------------------------------------------------

APISIX_HTTP_500_BODY = json.dumps({
    "error": {
        "code": "UPSTREAM_ERROR",
        "message": "model not found in shop",
        "request_id": "req-xyz-999",
        "api_error": "shopee.model.not_found",
    }
})

APISIX_HTTP_429_BODY = json.dumps({
    "error": {
        "code": "RATE_LIMIT",
        "message": "too many requests, retry after 30s",
        "request_id": "req-429-001",
        "api_error": "shopee.rate_limit.exceeded",
    }
})

APISIX_200_ERROR_ENVELOPE = {
    "marketplace": "shopee",
    "action": "update_stock",
    "error": {
        "code": "MP_ERROR",
        "message": "invalid stock quantity",
        "request_id": "req-200-err-001",
        "api_error": "shopee.stock.invalid",
    },
    "raw_response": {"error": 10012, "msg": "invalid stock quantity"},
}


# --------------------------------------------------------------------------
# Mock helpers (mengikuti konvensi test_pipeline_activation.py)
# --------------------------------------------------------------------------


class _MockSnapshotInvenTree:
    """InvenTree mock: snapshot stok tetap (stock_available=100)."""

    async def get_stock_snapshot_by_part_id(self, part_id: int) -> dict:
        return {
            "part_id": part_id,
            "stock_available": 100.0,
            "stock_reserved_shopee": 0.0,
            "stock_reserved_others": 0.0,
            "stock_reserved_tokopedia": 0.0,
            "stock_reserved_total": 0.0,
            "stok_buffer": 0,
            "reserved_inventree": 0.0,
        }


def _make_http_status_error(status_code: int, body: str) -> httpx.HTTPStatusError:
    """Bangun httpx.HTTPStatusError asli dengan response body APISIX."""
    resp = httpx.Response(
        status_code=status_code,
        content=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        request=httpx.Request("POST", "http://mock.test/update-stock/prod456"),
    )
    return httpx.HTTPStatusError(
        f"Server error '{status_code}' for url 'http://mock.test/update-stock/prod456'",
        request=resp.request,
        response=resp,
    )


def _make_session() -> AsyncMock:
    """Mock AsyncSession sesuai konvensi conftest/test_pipeline_activation.

    Semua query (mapping lookup, pending-check, resolve shop/marketplace)
    dibuat mengembalikan hasil 'kosong/aman':
      - scalar()           → 0      (query agregasi reserved_mp)
      - scalars().first()  → None   (pending-check → tidak ada yang di-skip)
      - first()            → None   (resolve warehouse/marketplace → fallback)
    """
    session = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()

    def _execute(*args, **kwargs):
        res = MagicMock()
        res.scalar.return_value = 0
        res.scalars.return_value.first.return_value = None
        res.first.return_value = None
        return res

    session.execute = AsyncMock(side_effect=_execute)
    return session


@pytest.fixture
def variant_factory():
    """Factory variant entity sederhana (atribut apa adanya, tanpa ORM)."""

    def _factory(
        marketplace_variant_id: str,
        external_variant_id: str,
        seller_sku: str,
        total_available_stock: int = 25,
        external_product_id: str = "prod456",
        shop_id: str = "shop123",
        location_id: str = "WH-001",
    ):
        v = MagicMock()
        v.marketplace_variant_id = marketplace_variant_id
        v.shop_id = shop_id
        v.external_product_id = external_product_id
        v.external_variant_id = external_variant_id
        v.variant_name = "Variant X"
        v.seller_sku = seller_sku
        v.total_reserved_stock = 0
        v.total_available_stock = total_available_stock
        v.location_id = location_id
        v.status = RecordStatus.ACTIVE
        return v

    return _factory


def _make_mapping(variant_id: str):
    m = MagicMock(spec=SKUMappingModel)
    m.inventree_part_id = 12345
    m.sku_type = "NORMAL"
    m.conversion_qty = 1
    m.marketplace_variant_id = variant_id
    m.status = "ACTIVE"
    return m


class _BatchRaiseClient:
    """APISIX mock: update_stock_batch raise HTTPStatusError, update_stock OK."""

    def __init__(self, exc: Exception):
        self.exc = exc
        self.update_status_calls = 0
        self.update_stock_calls = 0

    async def update_status(self, *args, **kwargs):
        self.update_status_calls += 1
        return {"status": "ACTIVE"}

    async def update_stock(self, *args, **kwargs):
        self.update_stock_calls += 1
        return {"marketplace": "shopee", "raw_response": {}}

    async def update_stock_batch(self, *args, **kwargs):
        raise self.exc

    async def fetch_products(self, *args, **kwargs):
        return {"products": []}


class _SingleRaiseClient:
    """APISIX mock: update_stock (Format B) raise HTTPStatusError, batch OK."""

    def __init__(self, exc: Exception):
        self.exc = exc
        self.update_status_calls = 0
        self.update_stock_calls = 0

    async def update_status(self, *args, **kwargs):
        self.update_status_calls += 1
        return {"status": "ACTIVE"}

    async def update_stock(self, *args, **kwargs):
        raise self.exc

    async def update_stock_batch(self, *args, **kwargs):
        return {"marketplace": "shopee", "raw_response": {}}

    async def fetch_products(self, *args, **kwargs):
        return {"products": []}


class _EnvelopeErrorClient:
    """APISIX mock: HTTP 200 tapi body memuat envelope error {"error": …}."""

    def __init__(self, envelope: dict, target: str = "single"):
        self.envelope = envelope
        self.target = target  # "single" (Format B) | "batch" (Format A)

    async def update_status(self, *args, **kwargs):
        return {"status": "ACTIVE"}

    async def update_stock(self, *args, **kwargs):
        if self.target == "single":
            return dict(self.envelope)
        return {"marketplace": "shopee", "raw_response": {}}

    async def update_stock_batch(self, *args, **kwargs):
        if self.target == "batch":
            return dict(self.envelope)
        return {"marketplace": "shopee", "raw_response": {}}

    async def fetch_products(self, *args, **kwargs):
        return {"products": []}


class _OkClient:
    """APISIX mock: semua push sukses (kontrol)."""

    async def update_status(self, *args, **kwargs):
        return {"status": "ACTIVE"}

    async def update_stock(self, *args, **kwargs):
        return {"marketplace": "shopee", "raw_response": {"success_list": []}}

    async def update_stock_batch(self, *args, **kwargs):
        return {"marketplace": "shopee", "raw_response": {"success_list": []}}

    async def fetch_products(self, *args, **kwargs):
        return {"products": []}


# --------------------------------------------------------------------------
# Helper eksekusi: jalankan 1 part dengan 2 variant (Format A) atau 1 variant
# (Format B) milik product yang SAMA, lalu kumpulkan model yang di-add.
# --------------------------------------------------------------------------


async def _run_part(
    pipeline: StockSyncPipelineEngine,
    variants: list,
    cycle_index: int = 1,
):
    """Jalankan pipeline._push_part dengan variant yang diberikan.

    Mengembalikan (session, added_models) — added_models = semua object yang
    di-add ke session (report model, queue entries, log entries).
    """
    session = _make_session()

    # Mapping lookup di _process_part_item: query pertama per variant harus
    # mengembalikan mapping aktif. Gunakan side_effect berurutan.
    mapping_results = []
    for v in variants:
        res = MagicMock()
        res.scalars.return_value.first.return_value = _make_mapping(
            v.marketplace_variant_id
        )
        mapping_results.append(res)

    async def _execute_side_effect(*args, **kwargs):
        # Query mapping selalu paling depan per variant (order di _push_part:
        # resolve warehouse/marketplace per shop → mapping per variant).
        res = MagicMock()
        res.scalar.return_value = 0
        res.scalars.return_value.first.return_value = None
        res.first.return_value = None
        return res

    session.execute = AsyncMock(side_effect=_execute_side_effect)

    result = PipelineCycleResult(
        cycle_index=cycle_index, started_at=datetime.now(timezone.utc)
    )
    inventree = _MockSnapshotInvenTree()

    with patch.object(settings, "stock_sync_push_enabled", True):
        await pipeline._push_part(
            session=session,
            part_id=12345,
            part_variants=variants,
            cycle_index=cycle_index,
            result=result,
            inventree=inventree,
            buffer=0,
            reserved_mp_map={},  # map kosong → tanpa query agregasi per-SKU
            part_reserved_converted_map={12345: 0.0},
            product_by_key={
                (str(v.shop_id), str(v.external_product_id)): None for v in variants
            },
        )

    added_models = [
        call_args.args[0] for call_args in session.add.call_args_list if call_args.args
    ]
    return session, added_models, result


def _split_added(added_models):
    queue_entries = [m for m in added_models if isinstance(m, StockSyncQueueModel)]
    log_entries = [m for m in added_models if isinstance(m, StockSyncLogModel)]
    return queue_entries, log_entries


# ==========================================================================
# FORMAT A — batch multi-SKU (update_stock_batch)
# ==========================================================================


@pytest.mark.asyncio
async def test_format_a_http_500_body_captured_in_queue_and_log(variant_factory):
    """
    APISIX balas HTTP 500 dengan body JSON error.

    EXPECTED: error_message di stock_sync_queue & stock_sync_log "diterima"
    — memuat FULL body (code, message, request_id, api_error), bukan hanya
    summary line 'Server error ...'.
    """
    exc = _make_http_status_error(500, APISIX_HTTP_500_BODY)
    client = _BatchRaiseClient(exc)
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    v2 = variant_factory("shop123_var002", "variant002", "SKU-002")
    _, added, result = await _run_part(pipeline, [v1, v2])

    queue_entries, log_entries = _split_added(added)
    assert len(queue_entries) == 2, "2 variant → 2 queue entry"
    assert len(log_entries) == 2, "2 variant → 2 log entry"

    for q in queue_entries:
        assert q.status == "FAILED"
        assert q.error_message, "queue.error_message TIDAK BOLEH kosong"
        # Summary line tetap ada...
        assert "Server error '500" in q.error_message
        # ...dan FULL response body APISIX tersimpan:
        assert "UPSTREAM_ERROR" in q.error_message
        assert "model not found in shop" in q.error_message
        assert "req-xyz-999" in q.error_message
        assert "shopee.model.not_found" in q.error_message
        assert "HTTP 500 response body:" in q.error_message

    for lg in log_entries:
        assert lg.status == "FAILED"
        assert lg.error_message == queue_entries[0].error_message, (
            "log.error_message harus identik dengan queue.error_message"
        )
        assert "UPSTREAM_ERROR" in lg.error_message
        assert "req-xyz-999" in lg.error_message

    assert result.total_queued == 2
    assert result.total_logged == 2
    assert len(result.failures) == 2, "failure summary terisi utk tiap variant"


@pytest.mark.asyncio
async def test_format_a_http_200_error_envelope_captured(variant_factory):
    """
    APISIX balas HTTP 200 tapi body memuat envelope error {"error": {...}}.

    EXPECTED: pipeline menandai FAILED dan menyimpan SELURUH envelope ke
    error_message (bukan hanya field message).
    """
    client = _EnvelopeErrorClient(APISIX_200_ERROR_ENVELOPE, target="batch")
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    v2 = variant_factory("shop123_var002", "variant002", "SKU-002")
    _, added, _ = await _run_part(pipeline, [v1, v2])

    queue_entries, log_entries = _split_added(added)

    for q in queue_entries:
        assert q.status == "FAILED"
        assert "invalid stock quantity" in q.error_message
        assert "MP_ERROR" in q.error_message
        assert "req-200-err-001" in q.error_message
        assert "shopee.stock.invalid" in q.error_message
        assert "APISIX response:" in q.error_message, (
            "error_message harus memuat FULL envelope APISIX"
        )
        # Seluruh envelope (raw_response) juga harus ada di dalam pesan.
        assert '"error": 10012' in q.error_message

    for lg in log_entries:
        assert lg.status == "FAILED"
        assert "req-200-err-001" in lg.error_message


@pytest.mark.asyncio
async def test_format_a_long_body_truncated_to_push_error_max_chars(variant_factory):
    """
    Body error > PUSH_ERROR_MAX_CHARS (2000) → error_message di-truncate
    dengan suffix '...[truncated]' supaya aman utk kolom TEXT.
    """
    long_message = "X" * 5000
    body = json.dumps({
        "error": {"code": "BIG", "message": long_message, "request_id": "req-big-001"}
    })
    assert len(body) > PUSH_ERROR_MAX_CHARS

    exc = _make_http_status_error(500, body)
    client = _BatchRaiseClient(exc)
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    v2 = variant_factory("shop123_var002", "variant002", "SKU-002")
    _, added, _ = await _run_part(pipeline, [v1, v2])

    queue_entries, log_entries = _split_added(added)

    for q in queue_entries:
        assert q.status == "FAILED"
        assert q.error_message.endswith("...[truncated]")
        assert len(q.error_message) < 5000, "pesan jauh lebih pendek dari body asli"
        assert "req-big-001" in q.error_message or "HTTP 500" in q.error_message

    for lg in log_entries:
        assert lg.error_message.endswith("...[truncated]")


@pytest.mark.asyncio
async def test_format_a_success_leaves_error_message_empty(variant_factory):
    """
    Kontrol: push sukses → queue PROCESSED tanpa error_message, log SUCCESS.
    """
    client = _OkClient()
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    v2 = variant_factory("shop123_var002", "variant002", "SKU-002")
    _, added, result = await _run_part(pipeline, [v1, v2])

    queue_entries, log_entries = _split_added(added)

    for q in queue_entries:
        assert q.status == "PROCESSED"
        assert q.error_message is None
        assert q.processed_at is not None

    for lg in log_entries:
        assert lg.status == "SUCCESS"
        assert lg.error_message is None

    assert result.total_pushed == 2
    assert result.failures == []


# ==========================================================================
# FORMAT B — flat per-SKU (update_stock)
# ==========================================================================


@pytest.mark.asyncio
async def test_format_b_http_500_body_captured_in_queue_and_log(variant_factory):
    """
    Sama seperti Format A tapi via jalur _process_variant_push (update_stock).
    """
    exc = _make_http_status_error(500, APISIX_HTTP_500_BODY)
    client = _SingleRaiseClient(exc)
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    _, added, _ = await _run_part(pipeline, [v1])

    queue_entries, log_entries = _split_added(added)
    assert len(queue_entries) == 1
    assert len(log_entries) == 1

    q, lg = queue_entries[0], log_entries[0]
    assert q.status == "FAILED"
    assert "UPSTREAM_ERROR" in q.error_message
    assert "model not found in shop" in q.error_message
    assert "req-xyz-999" in q.error_message
    assert "HTTP 500 response body:" in q.error_message

    assert lg.status == "FAILED"
    assert lg.error_message == q.error_message


@pytest.mark.asyncio
async def test_format_b_http_200_error_envelope_captured(variant_factory):
    client = _EnvelopeErrorClient(APISIX_200_ERROR_ENVELOPE, target="single")
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    _, added, _ = await _run_part(pipeline, [v1])

    queue_entries, log_entries = _split_added(added)

    q, lg = queue_entries[0], log_entries[0]
    assert q.status == "FAILED"
    assert "invalid stock quantity" in q.error_message
    assert "req-200-err-001" in q.error_message
    assert "APISIX response:" in q.error_message

    assert lg.status == "FAILED"
    assert "req-200-err-001" in lg.error_message


@pytest.mark.asyncio
async def test_format_b_http_429_rate_limit_captured(variant_factory):
    """
    HTTP 429 (rate limit marketplace) → pesan error tetap diterima penuh
    oleh queue + log.
    """
    exc = _make_http_status_error(429, APISIX_HTTP_429_BODY)
    client = _SingleRaiseClient(exc)
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    v1 = variant_factory("shop123_var001", "variant001", "SKU-001")
    _, added, _ = await _run_part(pipeline, [v1])

    queue_entries, log_entries = _split_added(added)

    q, lg = queue_entries[0], log_entries[0]
    assert q.status == "FAILED"
    assert "RATE_LIMIT" in q.error_message
    assert "too many requests, retry after 30s" in q.error_message
    assert "req-429-001" in q.error_message
    assert "HTTP 429 response body:" in q.error_message

    assert lg.status == "FAILED"
    assert lg.error_message == q.error_message


# ==========================================================================
# Unit: helper _http_error_detail (pembentuk isi push_error)
# ==========================================================================


def test_http_error_detail_contains_full_body():
    """_http_error_detail() harus menghasilkan detail yang 'self-sufficient'."""
    from application.usecases.stock_sync_pipeline import _http_error_detail

    exc = _make_http_status_error(500, APISIX_HTTP_500_BODY)
    detail = _http_error_detail(exc)

    assert "HTTPStatusError" in detail
    assert "Server error '500" in detail
    assert "HTTP 500 response body:" in detail
    assert "UPSTREAM_ERROR" in detail
    assert "req-xyz-999" in detail


def test_http_error_detail_no_body():
    """Response tanpa body → tetap ada summary, tanpa bagian 'response body'."""
    from application.usecases.stock_sync_pipeline import _http_error_detail

    exc = _make_http_status_error(500, "")
    detail = _http_error_detail(exc)

    assert "HTTPStatusError" in detail
    assert "response body:" not in detail
