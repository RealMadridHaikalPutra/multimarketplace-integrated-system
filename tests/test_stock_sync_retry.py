"""
test_stock_sync_retry.py

Test RETRY PUSH FAILED di AKHIR cycle (bukan saat kegagalan terjadi):

  - Entry.queue FAILED pada cycle ini dengan error retryable (default memuat
    "400") di-retry SEKALIGUS di akhir, maksimal stock_sync_max_retry_count.
  - Tiap attempt menaikkan retry_count + menulis baris stock_sync_log BARU
    (source POLLING_1H_RETRY).
  - Entry yang sukses berhenti di-retry; yang habis retry_count tetap FAILED.
  - Error non-retryable (mis. HTTP 500) TIDAK di-retry.
  - Gate stock_sync_retry_enabled / stock_sync_push_enabled.

Test level pipeline — mock APISIX client + mock session, TIDAK butuh DB/network.

Run:
    pytest tests/test_stock_sync_retry.py -v
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from core.config import settings
from infrastructure.db.models import StockSyncLogModel
from application.usecases.stock_sync_pipeline import (
    PipelineCycleResult,
    StockSyncPipelineEngine,
    _is_retryable_push_error,
)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _make_http_status_error(status_code: int, message: str) -> httpx.HTTPStatusError:
    body = (
        '{"error":{"code":"PARTIAL_FAILURE","original_status":%d,'
        '"marketplace":"tiktok","original_code":"12052901","message":"%s"}}'
        % (status_code, message)
    )
    resp = httpx.Response(
        status_code=status_code,
        content=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        request=httpx.Request("POST", "http://mock.test/update-stock/prod456"),
    )
    return httpx.HTTPStatusError(
        f"Client error '{status_code} Bad Request' for url "
        f"'http://mock.test/update-stock/prod456'",
        request=resp.request,
        response=resp,
    )


class _FlakyClient:
    """APISIX mock: N pemanggilan pertama gagal (raise exc), lalu sukses.

    ``single_failures`` mengatur berapa kali ``update_stock`` (Format B) gagal.
    ``batch_failures`` mengatur berapa kali ``update_stock_batch`` (Format A) gagal.
    """

    def __init__(self, exc, single_failures=0, batch_failures=0):
        self.exc = exc
        self.single_remaining = single_failures
        self.batch_remaining = batch_failures
        self.update_stock_calls = 0
        self.update_stock_batch_calls = 0

    async def update_status(self, *args, **kwargs):
        return {"status": "ACTIVE"}

    async def update_stock(self, *args, **kwargs):
        self.update_stock_calls += 1
        if self.single_remaining > 0:
            self.single_remaining -= 1
            raise self.exc
        return {"marketplace": "shopee", "raw_response": {}}

    async def update_stock_batch(self, *args, **kwargs):
        self.update_stock_batch_calls += 1
        if self.batch_remaining > 0:
            self.batch_remaining -= 1
            raise self.exc
        return {"marketplace": "shopee", "raw_response": {}}

    async def fetch_products(self, *args, **kwargs):
        return {"products": []}


def _make_queue_entry(queue_id="queue-001", retry_count=0):
    q = MagicMock()
    q.id = queue_id
    q.status = "FAILED"
    q.retry_count = retry_count
    q.error_message = "HTTP 400 response body: {}"
    q.processed_at = None
    return q


def _make_item(
    queue_id="queue-001",
    product_id="prod456",
    variant_id="variant001",
    seller_sku="SKU-001",
    marketplace_variant_id="shop123_var001",
    target_stock=10,
    previous_stock=5,
    is_orphan=False,
    push_error=None,
    retry_count=0,
):
    variant = MagicMock()
    variant.marketplace_variant_id = marketplace_variant_id
    variant.external_product_id = product_id
    variant.external_variant_id = variant_id

    return {
        "variant": variant,
        "seller_sku": seller_sku,
        "product_status": "ACTIVE",
        "product_id": product_id,
        "variant_id": variant_id,
        "is_orphan": is_orphan,
        "marketplace_code": "shopee",
        "shop_db_id": "shop123",
        "warehouse_id": "WH-001",
        "target_stock": target_stock,
        "previous_stock": previous_stock,
        "final_stock": previous_stock,
        "push_status": "FAILED",
        "push_error": push_error
        or "HTTPStatusError: Client error '400 Bad Request' | HTTP 400 response body: {}",
        "queue_entry": _make_queue_entry(queue_id, retry_count),
        "part_id": 12345,
    }


def _make_session_factory(added_models):
    """Factory AsyncSession mock (context manager) — kumpulkan model yang di-add."""

    @asynccontextmanager
    async def _factory():
        session = AsyncMock()
        session.execute = AsyncMock(return_value=MagicMock())
        session.commit = AsyncMock()
        session.flush = AsyncMock()
        session.refresh = AsyncMock()
        session.add = MagicMock(side_effect=lambda m: added_models.append(m))
        yield session

    return _factory


async def _run_retry(pipeline, items, added_models, max_retry=3, backoff=0.0):
    result = PipelineCycleResult(
        cycle_index=1, started_at=datetime.now(timezone.utc)
    )
    with patch.object(settings, "stock_sync_push_enabled", True), patch.object(
        settings, "stock_sync_retry_enabled", True
    ), patch.object(settings, "stock_sync_max_retry_count", max_retry), patch.object(
        settings, "stock_sync_retry_backoff_seconds", backoff
    ):
        await pipeline._retry_failed_pushes(
            items=items,
            cycle_index=1,
            result=result,
            session_factory=_make_session_factory(added_models),
        )
    return result


# --------------------------------------------------------------------------
# _is_retryable_push_error
# --------------------------------------------------------------------------


def test_retryable_marker_matches_400():
    with patch.object(settings, "stock_sync_retry_error_markers", "400"):
        assert _is_retryable_push_error("HTTP 400 response body: {}") is True
        assert _is_retryable_push_error("HTTP 500 UPSTREAM_ERROR") is False
        assert _is_retryable_push_error(None) is False
        assert _is_retryable_push_error("") is False


# ==========================================================================
# Format B (flat per-SKU)
# ==========================================================================


@pytest.mark.asyncio
async def test_retry_400_stops_after_success():
    """Retry pertama langsung sukses: retry_count=1, queue PROCESSED, log baru 1."""
    client = _FlakyClient(_make_http_status_error(400, "Operation Not Allowed"), single_failures=0)
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item()
    added = []

    result = await _run_retry(pipeline, [item], added)

    assert client.update_stock_calls == 1, "cukup 1 attempt retry (lalu sukses)"
    assert item["queue_entry"].retry_count == 1
    assert item["queue_entry"].status == "PROCESSED"
    assert item["queue_entry"].error_message is None
    assert result.total_retry_attempted == 1
    assert result.total_retry_succeeded == 1
    assert result.total_retry_exhausted == 0

    logs = [m for m in added if isinstance(m, StockSyncLogModel)]
    assert len(logs) == 1, "tiap attempt menulis baris log BARU"
    assert logs[0].source == "POLLING_1H_RETRY"
    assert logs[0].status == "SUCCESS"


@pytest.mark.asyncio
async def test_retry_400_fails_once_then_succeeds():
    """Retry attempt 1 gagal (400), attempt 2 sukses → retry_count=2."""
    client = _FlakyClient(
        _make_http_status_error(400, "Operation Not Allowed"), single_failures=1
    )
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item()
    added = []

    result = await _run_retry(pipeline, [item], added)

    assert client.update_stock_calls == 2
    assert item["queue_entry"].retry_count == 2
    assert item["queue_entry"].status == "PROCESSED"
    assert result.total_retry_attempted == 2
    assert result.total_retry_succeeded == 1
    assert result.total_retry_exhausted == 0

    logs = [m for m in added if isinstance(m, StockSyncLogModel)]
    assert [lg.status for lg in logs] == ["FAILED", "SUCCESS"]


@pytest.mark.asyncio
async def test_retry_400_exhausts_after_max_retries():
    """Selalu gagal 400: 3 attempt, retry_count=3, queue FAILED, 3 baris log."""
    client = _FlakyClient(_make_http_status_error(400, "Operation Not Allowed"), single_failures=99)
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item()
    added = []

    result = await _run_retry(pipeline, [item], added, max_retry=3)

    assert client.update_stock_calls == 3, "maksimal 3 attempt"
    assert item["queue_entry"].retry_count == 3
    assert item["queue_entry"].status == "FAILED"
    assert "400" in item["queue_entry"].error_message
    assert result.total_retry_attempted == 3
    assert result.total_retry_succeeded == 0
    assert result.total_retry_exhausted == 1

    logs = [m for m in added if isinstance(m, StockSyncLogModel)]
    assert len(logs) == 3, "3 attempt → 3 baris log retry"
    assert all(lg.source == "POLLING_1H_RETRY" for lg in logs)
    assert all(lg.status == "FAILED" for lg in logs)


@pytest.mark.asyncio
async def test_retry_skips_non_retryable_error():
    """Error HTTP 500 (bukan 400) TIDAK di-retry."""
    pipeline = StockSyncPipelineEngine(apisix_client=_FlakyClient(Exception()))
    item = _make_item(push_error="HTTPStatusError: Server error '500' | HTTP 500 response body: UPSTREAM_ERROR")
    added = []

    result = await _run_retry(pipeline, [item], added)

    assert result.total_retry_attempted == 0
    assert item["queue_entry"].retry_count == 0
    assert added == []


@pytest.mark.asyncio
async def test_retry_partially_exhausted_entry_not_retried_beyond_max():
    """Entry dengan retry_count yang sudah mencapai max tidak di-retry lagi."""
    client = _FlakyClient(_make_http_status_error(400, "Operation Not Allowed"), single_failures=99)
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item(retry_count=3)
    added = []

    result = await _run_retry(pipeline, [item], added, max_retry=3)

    assert client.update_stock_calls == 0
    assert result.total_retry_attempted == 0


# ==========================================================================
# Format A (batch multi-SKU)
# ==========================================================================


@pytest.mark.asyncio
async def test_retry_format_a_batch_then_success():
    """2 variant se-product gagal 400 sekali → retry via update_stock_batch."""
    client = _FlakyClient(
        _make_http_status_error(400, "Operation Not Allowed"), batch_failures=0
    )
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    v1 = _make_item(queue_id="q1", variant_id="variant001", marketplace_variant_id="shop_var1")
    v2 = _make_item(queue_id="q2", variant_id="variant002", marketplace_variant_id="shop_var2")
    added = []

    result = await _run_retry(pipeline, [v1, v2], added)

    assert client.update_stock_batch_calls == 1, "Format A dipakai untuk multi-variant"
    assert client.update_stock_calls == 0
    assert v1["queue_entry"].retry_count == 1
    assert v2["queue_entry"].retry_count == 1
    assert v1["queue_entry"].status == "PROCESSED"
    assert v2["queue_entry"].status == "PROCESSED"
    assert result.total_retry_attempted == 2
    assert result.total_retry_succeeded == 2


# ==========================================================================
# Gate config
# ==========================================================================


@pytest.mark.asyncio
async def test_retry_disabled_by_config():
    client = _FlakyClient(_make_http_status_error(400, "Operation Not Allowed"), single_failures=99)
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item()
    added = []

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    with patch.object(settings, "stock_sync_push_enabled", True), patch.object(
        settings, "stock_sync_retry_enabled", False
    ):
        await pipeline._retry_failed_pushes(
            items=[item],
            cycle_index=1,
            result=result,
            session_factory=_make_session_factory(added),
        )

    assert client.update_stock_calls == 0
    assert result.total_retry_attempted == 0


@pytest.mark.asyncio
async def test_retry_skipped_when_push_disabled():
    client = _FlakyClient(_make_http_status_error(400, "Operation Not Allowed"), single_failures=99)
    pipeline = StockSyncPipelineEngine(apisix_client=client)
    item = _make_item()
    added = []

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    with patch.object(settings, "stock_sync_push_enabled", False), patch.object(
        settings, "stock_sync_retry_enabled", True
    ):
        await pipeline._retry_failed_pushes(
            items=[item],
            cycle_index=1,
            result=result,
            session_factory=_make_session_factory(added),
        )

    assert client.update_stock_calls == 0
    assert result.total_retry_attempted == 0
