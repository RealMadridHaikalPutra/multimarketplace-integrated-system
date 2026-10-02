"""
Comprehensive E2E tests for the stock sync pipeline auto-activation logic.

Tests cover:
1. INACTIVE product + target_stock > 0  → activation before stock update
2. ACTIVE product + target_stock > 0     → NO activation (normal flow)
3. INACTIVE product + target_stock == 0  → NO activation (skip)
4. Activation API failure → still proceeds with stock update
5. Full pipeline cycle with activation tracking
6. Response logging capture for audit
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.client import ApisixClient, ApisixConfigError, STATUS_ACTIVE
from infrastructure.db.models import (
    SKUMappingModel,
    StockSyncQueueModel,
    StockSyncLogModel,
    InventoryReportModel,
    MarketplaceProductModel,
    MarketplaceVariantModel,
)
from application.usecases.stock_sync_pipeline import (
    PipelineCycleResult,
    StockSyncPipelineEngine,
)

logger = logging.getLogger(__name__)


# ======================================================================
# Mock helpers
# ======================================================================


class MockApisixClient(ApisixClient):
    """
    A mock ApisixClient that records all calls made to it
    and returns configurable responses for testing.
    """

    def __init__(
        self,
        update_status_response: Optional[dict] = None,
        update_stock_response: Optional[dict] = None,
        update_status_raise: Optional[Exception] = None,
        update_stock_raise: Optional[Exception] = None,
    ):
        super().__init__()
        self.call_log: Dict[str, list] = {
            "update_status": [],
            "update_stock": [],
            "fetch_products": [],
        }
        self._update_status_response = update_status_response or {
            "marketplace": "shopee",
            "action": "update_status",
            "status": "ACTIVE",
            "raw_response": {
                "error": "",
                "message": "",
                "request_id": "mock-req-id-001",
                "response": {
                    "item_id": 802023254,
                    "item_status": "NORMAL",
                },
            },
        }
        self._update_stock_response = update_stock_response or {
            "marketplace": "shopee",
            "raw_response": {
                "error": "",
                "message": "",
                "request_id": "mock-req-id-002",
                "response": {
                    "failure_list": [],
                    "success_list": [{"model_id": 0, "location_id": "", "stock": 150}],
                },
            },
        }
        self._update_status_raise = update_status_raise
        self._update_stock_raise = update_stock_raise

    async def update_status(
        self, marketplace: str, shop_uuid: str, product_id: str, status: str = STATUS_ACTIVE
    ) -> Dict[str, Any]:
        self.call_log["update_status"].append({
            "marketplace": marketplace,
            "shop_uuid": shop_uuid,
            "product_id": product_id,
            "status": status,
        })
        if self._update_status_raise:
            raise self._update_status_raise
        return dict(self._update_status_response)  # shallow copy

    async def update_stock(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        variant_id: str,
        stock: int,
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.call_log["update_stock"].append({
            "marketplace": marketplace,
            "shop_uuid": shop_uuid,
            "product_id": product_id,
            "variant_id": variant_id,
            "stock": stock,
            "warehouse_id": warehouse_id,
        })
        if self._update_stock_raise:
            raise self._update_stock_raise
        return dict(self._update_stock_response)

    async def update_stock_batch(
        self,
        marketplace: str,
        shop_uuid: str,
        product_id: str,
        skus: List[Dict[str, Any]],
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Replikasi transformasi body Format A dari ApisixClient asli:
        # per-SKU: id + inventory[{warehouse_id, quantity}] dengan precedence
        # per-SKU warehouse_id > argumen warehouse_id.
        fallback_warehouse = str(warehouse_id or "").strip() or "WH-DEFAULT"
        body_skus = [
            {
                "id": str((sku or {}).get("id") or "").strip(),
                "inventory": [
                    {
                        "warehouse_id": str((sku or {}).get("warehouse_id") or "").strip()
                        or fallback_warehouse,
                        "quantity": int((sku or {}).get("quantity") or 0),
                    }
                ],
            }
            for sku in skus
        ]
        self.call_log["update_stock"].append({
            "marketplace": marketplace,
            "shop_uuid": shop_uuid,
            "product_id": product_id,
            "format": "A",
            "skus": body_skus,
            "warehouse_id": warehouse_id,
        })
        if self._update_stock_raise:
            raise self._update_stock_raise
        return dict(self._update_stock_response)

    async def fetch_products(
        self, marketplace: str, shop_uuid: str
    ) -> Dict[str, Any]:
        # Mock level PUBLIC API (sudah termasuk loop listing_platforms TikTok).
        self.call_log["fetch_products"].append({
            "marketplace": marketplace,
            "shop_uuid": shop_uuid,
        })
        return {
            "marketplace": marketplace,
            "products": [],
            "pagination": {"page": 1, "page_size": 50, "total": 0, "has_next": False, "next_page_token": None},
            "pages_fetched": 1,
            "sweep_complete": True,
            "listing_platforms_swept": [],
        }

    def reset_call_log(self):
        for key in self.call_log:
            self.call_log[key] = []


class MockInvenTreeClient:
    """Mock InvenTree client for testing."""

    async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
        return {
            "part_id": part_id,
            "stock_available": 100.0,
            "stock_reserved_shopee": 0.0,
            "stock_reserved_others": 0.0,
            "stock_reserved_tokopedia": 0.0,
            "stock_reserved_total": 0.0,
            "stok_buffer": 0,
            "reserved_inventree": 0.0,
            "updated_at": "2026-08-03T14:00:01",
        }


@pytest_asyncio.fixture
async def mock_session():
    """Create a mock async session.

    IMPORTANT: ``session.execute()`` is ``await``-ed by the pipeline code,
    but the returned ``Result`` object's ``.scalars()`` and ``.first()``
    are sync calls (not coroutines).  So we use a **MagicMock** for the
    result and set ``session.execute = AsyncMock(return_value=…)``.
    """
    session = AsyncMock(spec=AsyncSession)

    # ``await session.execute()`` → MagicMock (Result-like)
    # ``result.scalars().first()`` → sync chaining via MagicMock
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None

    # Override the ``execute`` attribute on the AsyncMock so that when
    # the pipeline awaits ``session.execute(...)`` it gets back the
    # MagicMock result (not a Coroutine that resolves to yet another Mock).
    session.execute = AsyncMock(return_value=mock_result)

    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.add = MagicMock()

    return session


# ======================================================================
# Fixture: aktifkan push secara default agar test activation berjalan.
# Test yang mengetes monitoring mode (push_enabled=False) meng-override
# secara eksplisit di dalam test masing-masing.
# ======================================================================

import pytest
from core.config import settings


@pytest.fixture(autouse=True)
def _enable_push_by_default_for_activation_tests(monkeypatch):
    """Kirim test activation dalam mode push-enabled."""
    monkeypatch.setattr(settings, "stock_sync_push_enabled", True)


# ======================================================================
# Tests: _process_single_variant auto-activation logic
# ======================================================================


@pytest.mark.asyncio
async def test_activation_inactive_product_with_stock(
    mock_session,
    sample_product_inactive,
    sample_variant,
    sample_shop,
):
    """
    Scenario: Product is INACTIVE and target_stock > 0
    Expected: update_status(ACTIVE) is called BEFORE update_stock
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    # Mock the SKU mapping to return an active mapping
    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    # ``await session.execute()`` → MagicMock whose ``.scalars().first()`` chain
    # returns ``mock_mapping``.
    # Use side_effect so first ``await session.execute()`` returns the
    # mapping (for SKU mapping lookup), and subsequent calls return
    # None (for pending-queue check, report repo, etc).
    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        # Always return a real integer for .scalar() (needed for reserved_mp SUM query)
        res.scalar.return_value = 0
        if _call_idx == 1:
            # SKU mapping query → return our mock_mapping
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            # Pending queue check / other queries → return None (no duplicate)
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify: update_status was called BEFORE update_stock
    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 1, (
        f"Expected 1 update_status call for INACTIVE product, got {len(status_calls)}"
    )
    assert len(stock_calls) == 1, (
        f"Expected 1 update_stock call, got {len(stock_calls)}"
    )

    # Check the activation call details
    activation_call = status_calls[0]
    assert activation_call["product_id"] == "prod456"
    assert activation_call["marketplace"] == "shopee"
    assert activation_call["status"] == STATUS_ACTIVE

    # Check the stock update call details
    stock_call = stock_calls[0]
    assert stock_call["product_id"] == "prod456"
    assert stock_call["variant_id"] == "variant001"
    assert stock_call["stock"] > 0  # target_stock should be > 0

    # Verify the activation was tracked in result
    assert result.total_activated == 1, (
        f"Expected total_activated=1, got {result.total_activated}"
    )
    assert len(result.activation_logs) == 1
    log_entry = result.activation_logs[0]
    assert log_entry["product_id"] == "prod456"
    assert log_entry["seller_sku"] == "TEST-SKU-001"
    assert "apisix_response" in log_entry

    # Verify: product status was updated in DB via product_repo.update
    assert sample_product_inactive.status == RecordStatus.ACTIVE, (
        "In-memory product status should be updated to ACTIVE"
    )


@pytest.mark.asyncio
async def test_activation_active_product_no_activation(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Scenario: Product is ACTIVE and target_stock > 0
    Expected: NO update_status call, only update_stock
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    # Mock mapping
    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify: NO update_status call for ACTIVE product
    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 0, (
        f"Expected 0 update_status calls for ACTIVE product, got {len(status_calls)}"
    )
    assert len(stock_calls) == 1, (
        f"Expected 1 update_stock call, got {len(stock_calls)}"
    )

    # Verify no activation was tracked
    assert result.total_activated == 0
    assert len(result.activation_logs) == 0


@pytest.mark.asyncio
async def test_activation_inactive_product_zero_stock(
    mock_session,
    sample_product_inactive,
    sample_variant,
):
    """
    Scenario: Product is INACTIVE but target_stock == 0
    Expected: NO activation call, only update_stock (with zero stock)
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    class ZeroStockInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": 0.0,
                "stock_reserved_shopee": 0.0,
                "stock_reserved_others": 0.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": 0.0,
                "stok_buffer": 0,
                "reserved_inventree": 0.0,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = ZeroStockInvenTree()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    sample_variant.total_reserved_stock = 100
    sample_variant.total_available_stock = 0

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify: NO activation call because target_stock == 0
    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 0, (
        f"Expected 0 update_status calls for zero stock, got {len(status_calls)}"
    )
    assert len(stock_calls) == 1

    assert result.total_activated == 0
    assert len(result.activation_logs) == 0


@pytest.mark.asyncio
async def test_activation_api_failure_proceeds_with_stock_update(
    mock_session,
    sample_product_inactive,
    sample_variant,
):
    """
    Scenario: Product is INACTIVE, target_stock > 0, but activation API fails
    Expected: update_stock is still called despite activation failure
    """
    mock_client = MockApisixClient(
        update_status_response={
            "error": {
                "code": "UPSTREAM_ERROR",
                "message": "product.error_unknown: activation failed",
                "marketplace": "shopee",
            }
        }
    )
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify: update_status WAS called (but returned error)
    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 1, "update_status should be called even if it fails"
    assert len(stock_calls) == 1, (
        "update_stock should still be called even when activation fails"
    )

    assert len(result.activation_logs) == 1
    error_response = result.activation_logs[0]["apisix_response"]
    assert "error" in error_response
    assert error_response["error"]["code"] == "UPSTREAM_ERROR"

    assert result.total_activated == 0


@pytest.mark.asyncio
async def test_activation_api_exception_proceeds_with_stock_update(
    mock_session,
    sample_product_inactive,
    sample_variant,
):
    """
    Scenario: Product is INACTIVE, target_stock > 0, but activation raises exception
    Expected: update_stock is still called (resilient fallback)
    """
    mock_client = MockApisixClient(
        update_status_raise=ApisixConfigError("credentials not found for shop")
    )
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify: update_status was attempted, update_stock still called
    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 1, "update_status should be attempted"
    assert len(stock_calls) == 1, (
        "update_stock should still be called despite config error"
    )

    assert result.total_activated == 0


# ======================================================================
# Tests: Formula stok_di_update dengan reserved_inventree
# ======================================================================


@pytest.mark.asyncio
async def test_formula_subtracts_reserved_inventree_and_buffer(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Snapshot: stock_available=100, stock_reserved_total=15, stok_buffer=2,
    conversion_qty=1, faktor_pengali=1, reserved_mp=0.

    Expected stok_di_update = (100 * 1 / 1) - 0 - 15 - 2 = 83
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    class ReservedInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": 100.0,
                "stock_reserved_shopee": 10.0,
                "stock_reserved_others": 5.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": 15.0,
                "stok_buffer": 2,
                "reserved_inventree": 15.0,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = ReservedInvenTree()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0  # reserved_mp SUM = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1, "Expected 1 update_stock call"
    assert stock_calls[0]["stock"] == 83, (
        f"Expected stock=83 (100 - 15 reserved_inventree - 2 buffer), got {stock_calls[0]['stock']}"
    )
    assert result.total_report_rows == 1


@pytest.mark.asyncio
async def test_formula_uses_precomputed_reserved_mp_map(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Optimasi: reserved_mp_map dihitung SEKALI per cycle oleh
    _run_inventory_cycle (1 query) dan di-pass ke _process_single_variant —
    hot loop TIDAK menjalankan query SUM per variant.

    Snapshot: stock_available=100, reserved_inventree=0, buffer=0.
    Map: {\"TEST-SKU-001\": 10}.

    Expected stok_di_update = (100 * 1 / 1) - 10 - 0 - 0 = 90.
    (Bila map diabaikan, fallback query SUM mengembalikan 0 → stock 100.)
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    class SnapshotInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": 100.0,
                "stock_reserved_shopee": 0.0,
                "stock_reserved_others": 0.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": 0.0,
                "stok_buffer": 0,
                "reserved_inventree": 0.0,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = SnapshotInvenTree()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0  # fallback SUM (tidak boleh dipakai)
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
        reserved_mp_map={"TEST-SKU-001": 10},
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1
    assert stock_calls[0]["stock"] == 90, (
        f"Expected stock=90 (100 - 10 dari reserved_mp_map), got {stock_calls[0]['stock']}"
    )
    assert result.total_report_rows == 1


@pytest.mark.asyncio
async def test_formula_snapshot_missing_uses_zero_stock(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Snapshot tidak ditemukan (None) → stok_inventree=0, reserved_inventree=0,
    buffer fallback ke settings (0). stok_di_update = 0.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    class MissingInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Optional[Dict[str, Any]]:
            return None

    mock_inventree = MissingInvenTree()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1, "Expected 1 update_stock call"
    assert stock_calls[0]["stock"] == 0, (
        f"Expected stock=0 when snapshot missing, got {stock_calls[0]['stock']}"
    )
    assert result.total_report_rows == 1


# ======================================================================
# Tests: Faktor pengali per SKU type & formula stok_di_update yang benar
# ======================================================================


def test_sku_parser_faktor_pengali_rules():
    """
    Faktor pengali ditentukan dari tipe SKU:
      SKU_X   50x → 1/50 = 0.02   (kebalikan jumlah SKU → 1 unit InvenTree)
      SKU_X   100x → 1/100 = 0.01
      SKU_S   50s  → 1.0          (1:1 dengan stok fisik)
      SKU_PACK     → sebesar isi paket (Pack-50 → 50)
      NORMAL       → 1.0
    """
    from domain.rules.sku import SkuParser

    assert SkuParser.compute_faktor_pengali("SKU_X", 50) == 0.02
    assert SkuParser.compute_faktor_pengali("SKU_X", 100) == 0.01
    assert SkuParser.compute_faktor_pengali("SKU_S", 50) == 1.0
    assert SkuParser.compute_faktor_pengali("SKU_PACK", 50) == 50.0
    assert SkuParser.compute_faktor_pengali("SKU_PACK", 20) == 20.0
    assert SkuParser.compute_faktor_pengali("NORMAL", 1) == 1.0
    assert SkuParser.compute_faktor_pengali("SKU_X", 0) == 1.0  # defensif


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seller_sku, part_id, stock_available, reserved_inventree, stok_buffer, "
    "reserved_mp, part_reserved_converted, expected_stock, "
    "expected_factor, expected_qty_converted",
    [
        # Part 1: stok=100, reserved_inventree=2, buffer=1, Σ qty_converted part=2
        # 1A1-50x → faktor 1/50=0.02 → (100 - 2 - 2 - 1) / 0.02 = 4750
        ("1A1-50x", 1, 100.0, 2.0, 1, 50, 2.0, 4750, 0.02, 1.0),
        # 1A1-50s → faktor 1 → (100 - 2 - 2 - 1) / 1 = 95
        ("1A1-50s", 1, 100.0, 2.0, 1, 1, 2.0, 95, 1.0, 1.0),
        # Part 2: stok=10000, reserved_inventree=50, buffer=100, Σ qty_converted part=150
        # 1A1-Pack-50 → faktor 50 → (10000 - 150 - 50 - 100) / 50 = 194
        ("1A1-Pack-50", 2, 10000.0, 50.0, 100, 1, 150.0, 194, 50.0, 50.0),
        # 1A1-Pack-20 → faktor 20 → (10000 - 150 - 50 - 100) / 20 = 485
        ("1A1-Pack-20", 2, 10000.0, 50.0, 100, 5, 150.0, 485, 20.0, 100.0),
    ],
)
async def test_formula_faktor_pengali_per_sku_type(
    mock_session,
    sample_product_active,
    seller_sku,
    part_id,
    stock_available,
    reserved_inventree,
    stok_buffer,
    reserved_mp,
    part_reserved_converted,
    expected_stock,
    expected_factor,
    expected_qty_converted,
):
    """
    Formula stok_di_update yang BENAR (verifikasi manual dari user):

      qty_converted        = reserved_mp × faktor_pengali
      stok_di_update       = (stok_inventree - Σ qty_converted part
                              - reserved_inventree - buffer) ÷ faktor_pengali

    Data verifikasi:
      part 1: 1A1-50x  → (100 - 2 - 2 - 1) / 0.02 = 4750
              1A1-50s  → (100 - 2 - 2 - 1) / 1    = 95
      part 2: Pack-50  → (10000 - 150 - 50 - 100) / 50 = 194
              Pack-20  → (10000 - 150 - 50 - 100) / 20 = 485
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    class PartSnapshotInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": stock_available,
                "stock_reserved_shopee": 0.0,
                "stock_reserved_others": 0.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": reserved_inventree,
                "stok_buffer": stok_buffer,
                "reserved_inventree": reserved_inventree,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = PartSnapshotInvenTree()

    variant = MarketplaceVariant(
        marketplace_variant_id="shop123_variant001",
        shop_id="shop123",
        external_product_id="prod456",
        external_variant_id="variant001",
        variant_name="Variant A",
        seller_sku=seller_sku,
        total_reserved_stock=reserved_mp,
        total_available_stock=0,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = part_id
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
        reserved_mp_map={seller_sku: reserved_mp},
        part_reserved_converted_map={part_id: part_reserved_converted},
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1, "Expected 1 update_stock call"
    assert stock_calls[0]["stock"] == expected_stock, (
        f"SKU {seller_sku}: expected stock={expected_stock}, got {stock_calls[0]['stock']}"
    )
    assert result.total_report_rows == 1

    # Verifikasi nilai yang ditulis ke inventory_report
    report_row = None
    for call_args in mock_session.add.call_args_list:
        model = call_args[0][0]
        if isinstance(model, InventoryReportModel):
            report_row = model
            break
    assert report_row is not None, "InventoryReportModel harus ditulis ke session"
    assert abs(report_row.faktor_pengali - expected_factor) < 1e-9, (
        f"SKU {seller_sku}: faktor_pengali expected={expected_factor}, got {report_row.faktor_pengali}"
    )
    assert abs(report_row.qty_converted - expected_qty_converted) < 1e-9, (
        f"SKU {seller_sku}: qty_converted expected={expected_qty_converted}, got {report_row.qty_converted}"
    )
    assert report_row.reserved_mp == reserved_mp
    assert report_row.stok_di_update == expected_stock, (
        f"SKU {seller_sku}: stok_di_update expected={expected_stock}, got {report_row.stok_di_update}"
    )


@pytest.mark.asyncio
async def test_formula_stok_di_update_uses_floor_not_round(
    mock_session,
    sample_product_active,
):
    """
    stok_di_update harus FLOOR (pembulatan ke bawah), BUKAN round/ceiling.

    Snapshot: stock_available=104.1, reserved_inventree=0, buffer=0,
    part_reserved_converted=0, faktor_pengali=3 (SKU_PACK Pack-3).

    stok_di_update_raw = (104.1 - 0 - 0 - 0) / 3 = 34.7
      round(34.7) = 35, floor(34.7) = 34 → harus 34 (floor).
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    variant = MarketplaceVariant(
        marketplace_variant_id="shop123_variant001",
        shop_id="shop123",
        external_product_id="prod456",
        external_variant_id="variant001",
        variant_name="Variant A",
        seller_sku="TEST-Pack-3",  # SKU_PACK → faktor_pengali = 3.0
        total_reserved_stock=0,
        total_available_stock=100,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "SKU_PACK"
    mock_mapping.conversion_qty = 3
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    # raw = (104.1 - 0 - 0 - 0) / 3 = 34.7 → floor = 34 (round = 35).
    class FloorInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": 104.1,
                "stock_reserved_shopee": 0.0,
                "stock_reserved_others": 0.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": 0.0,
                "stok_buffer": 0,
                "reserved_inventree": 0.0,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = FloorInvenTree()

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
        reserved_mp_map={"TEST-Pack-3": 0},
        part_reserved_converted_map={12345: 0.0},
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1
    # raw = (104.1 - 0 - 0 - 0) / 3 = 34.7 → FLOOR = 34 (bukan round=35)
    assert stock_calls[0]["stock"] == 34, (
        f"Expected floor(34.7)=34, got {stock_calls[0]['stock']} (round akan memberi 35)"
    )
    assert result.total_report_rows == 1

    # Verifikasi nilai yang ditulis ke inventory_report juga floor
    report_row = None
    for call_args in mock_session.add.call_args_list:
        model = call_args[0][0]
        if isinstance(model, InventoryReportModel):
            report_row = model
            break
    assert report_row is not None
    assert report_row.stok_di_update == 34, (
        f"inventory_report.stok_di_update expected=34 (floor), got {report_row.stok_di_update}"
    )


@pytest.mark.asyncio
async def test_formula_stok_di_update_floor_negative_clamped_to_zero(
    mock_session,
    sample_product_active,
):
    """
    stok_di_update_raw negatif → max(0.0, raw) = 0 → floor(0) = 0.
    Snapshot: stock_available=10, reserved_inventree=0, buffer=0,
    part_reserved_converted=50 (lebih besar dari stok), faktor=1.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    variant = MarketplaceVariant(
        marketplace_variant_id="shop123_variant001",
        shop_id="shop123",
        external_product_id="prod456",
        external_variant_id="variant001",
        variant_name="Variant A",
        seller_sku="TEST-SKU-001",
        total_reserved_stock=50,
        total_available_stock=10,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    class LowStockInvenTree:
        async def get_stock_snapshot_by_part_id(self, part_id: int) -> Dict[str, Any]:
            return {
                "part_id": part_id,
                "stock_available": 10.0,
                "stock_reserved_shopee": 0.0,
                "stock_reserved_others": 0.0,
                "stock_reserved_tokopedia": 0.0,
                "stock_reserved_total": 0.0,
                "stok_buffer": 0,
                "reserved_inventree": 0.0,
                "updated_at": "2026-08-03T14:00:01",
            }

    mock_inventree = LowStockInvenTree()

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
        reserved_mp_map={"TEST-SKU-001": 50},
        part_reserved_converted_map={12345: 50.0},
    )

    stock_calls = mock_client.call_log["update_stock"]
    assert len(stock_calls) == 1
    # raw = (10 - 50 - 0 - 0) / 1 = -40 → max(0) → 0 → floor = 0
    assert stock_calls[0]["stock"] == 0
    assert result.total_report_rows == 1


# ======================================================================
# Tests: Full pipeline cycle with activation logging
# ======================================================================


@pytest.mark.asyncio
async def test_pipeline_activation_response_logging(
    mock_session,
    sample_product_inactive,
    sample_variant,
):
    """
    Scenario: Full activation flow with response logging
    Expected: All APISIX responses are captured in activation_logs
              with full response detail for audit
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result,
    )

    # Verify activation log structure (complete audit trail)
    assert len(result.activation_logs) == 1
    log_entry = result.activation_logs[0]

    required_fields = [
        "marketplace_code",
        "shop_id",
        "product_id",
        "seller_sku",
        "variant_id",
        "target_stock",
        "apisix_response",
    ]
    for field in required_fields:
        assert field in log_entry, (
            f"Activation log missing required field '{field}'"
        )

    assert log_entry["marketplace_code"] == "shopee"
    assert log_entry["shop_id"] == "shop123"
    assert log_entry["product_id"] == "prod456"
    assert log_entry["seller_sku"] == "TEST-SKU-001"
    assert log_entry["variant_id"] == "variant001"
    assert log_entry["target_stock"] > 0

    apisix_response = log_entry["apisix_response"]
    assert isinstance(apisix_response, dict)
    assert "marketplace" in apisix_response
    assert "action" in apisix_response
    assert "raw_response" in apisix_response


# ======================================================================
# Tests: PipelineCycleResult serialization with activation data
# ======================================================================


def test_pipeline_cycle_result_with_activation():
    """
    Given a PipelineCycleResult with activation data
    When to_dict() is called
    Then activation_logs and total_activated are included in the dict
    """
    result = PipelineCycleResult(
        cycle_index=1,
        started_at=datetime(2026, 7, 30, 10, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 30, 10, 5, 0, tzinfo=timezone.utc),
        total_report_rows=50,
        total_queued=45,
        total_pushed=40,
        total_logged=40,
        total_skipped_pending=3,
        total_skipped_no_part=2,
        total_activated=5,
        activation_logs=[
            {
                "marketplace_code": "shopee",
                "shop_id": "shop123",
                "product_id": "prod456",
                "seller_sku": "TEST-SKU-001",
                "variant_id": "variant001",
                "target_stock": 150,
                "apisix_response": {
                    "marketplace": "shopee",
                    "action": "update_status",
                    "status": "ACTIVE",
                    "raw_response": {"error": "", "message": "", "response": {}},
                },
            }
        ],
    )

    d = result.to_dict()

    assert d["cycle_index"] == 1
    assert d["total_activated"] == 5
    assert len(d["activation_logs"]) == 1

    assert isinstance(d["started_at"], str)
    assert isinstance(d["finished_at"], str)

    log_entry = d["activation_logs"][0]
    assert log_entry["marketplace_code"] == "shopee"
    assert log_entry["apisix_response"]["action"] == "update_status"
    assert log_entry["target_stock"] == 150


# ======================================================================
# Tests: Multiple variants with mixed activation status
# ======================================================================


@pytest.mark.asyncio
async def test_activation_multiple_variants_mixed_status(
    mock_session,
    sample_product_inactive,
    sample_product_active,
    sample_variant,
):
    """
    Scenario: Two products — one ACTIVE, one INACTIVE, both with stock > 0
    Expected: Only the INACTIVE product triggers activation
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    mock_inventree = MockInvenTreeClient()
    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    # Process INACTIVE product (first)
    result_1 = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_inactive,
        variant=sample_variant,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result_1,
    )

    # Process ACTIVE product (second)
    sample_variant2 = MarketplaceVariant(
        marketplace_variant_id="shop123_variant002",
        shop_id="shop123",
        external_product_id="prod789",
        external_variant_id="variant002",
        variant_name="Variant B",
        seller_sku="TEST-SKU-002",
        total_reserved_stock=3,
        total_available_stock=200,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )

    mock_mapping2 = MagicMock(spec=SKUMappingModel)
    mock_mapping2.inventree_part_id = 67890
    mock_mapping2.sku_type = "NORMAL"
    mock_mapping2.conversion_qty = 1
    mock_mapping2.marketplace_variant_id = "shop123_variant002"
    mock_mapping2.status = "ACTIVE"

    _call_idx2 = 0

    async def _execute_side_effect2(*args, **kwargs):
        nonlocal _call_idx2
        _call_idx2 += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx2 == 1:
            res.scalars.return_value.first.return_value = mock_mapping2
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect2)

    result_2 = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    await pipeline._process_single_variant(
        session=mock_session,
        marketplace_code="shopee",
        marketplace_code_value="SHOPEE",
        shop_db_id_str="shop123",
        shop_warehouse_id="WH-001",
        product=sample_product_active,
        variant=sample_variant2,
        inventree=mock_inventree,
        buffer=0,
        cycle_index=1,
        result=result_2,
    )

    # Verify: only INACTIVE product triggered activation
    assert result_1.total_activated == 1, (
        "INACTIVE product should trigger activation"
    )
    assert result_2.total_activated == 0, (
        "ACTIVE product should NOT trigger activation"
    )

    status_calls = mock_client.call_log["update_status"]
    stock_calls = mock_client.call_log["update_stock"]

    assert len(status_calls) == 1
    assert len(stock_calls) == 2

    assert status_calls[0]["product_id"] == "prod456"


# ======================================================================
# Tests: MODE MONITORING — stock_sync_push_enabled=false
# ======================================================================


@pytest.mark.asyncio
async def test_monitoring_mode_inactive_product_no_push(
    mock_session,
    sample_product_inactive,
    sample_variant,
):
    """
    Scenario: stock_sync_push_enabled=false, product INACTIVE + stock > 0.

    Expected (mode monitoring):
      - update_status (auto-activation) TIDAK dipanggil (status tidak berubah)
      - update_stock TIDAK dipanggil
      - inventory_report tetap ditulis (total_report_rows == 1)
      - total_skipped_push == 1, total_pushed == 0
      - queue ditandai PROCESSED (bukan PENDING) agar retention tetap jalan
      - log berstatus SKIPPED dengan pesan monitoring
      - product.status TETAP INACTIVE (tidak di-update DB)
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()  # stock_available=100 → target_stock > 0

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    with patch.object(settings, "stock_sync_push_enabled", False):
        await pipeline._process_single_variant(
            session=mock_session,
            marketplace_code="shopee",
            marketplace_code_value="SHOPEE",
            shop_db_id_str="shop123",
            shop_warehouse_id="WH-001",
            product=sample_product_inactive,
            variant=sample_variant,
            inventree=mock_inventree,
            buffer=0,
            cycle_index=1,
            result=result,
        )

    # Tidak ada push APISIX sama sekali
    assert len(mock_client.call_log["update_status"]) == 0, (
        "Monitoring mode: update_status TIDAK boleh dipanggil"
    )
    assert len(mock_client.call_log["update_stock"]) == 0, (
        "Monitoring mode: update_stock TIDAK boleh dipanggil"
    )

    # Report tetap dibuat (tujuan monitoring)
    assert result.total_report_rows == 1
    assert result.total_skipped_push == 1
    assert result.total_pushed == 0
    assert result.total_activated == 0
    assert len(result.activation_logs) == 0

    # Product status di DB TIDAK berubah
    assert sample_product_inactive.status == RecordStatus.INACTIVE


@pytest.mark.asyncio
async def test_monitoring_mode_active_product_no_push(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Scenario: stock_sync_push_enabled=false, product ACTIVE + stock > 0.
    Expected: report dibuat, update_stock TIDAK dipanggil, total_skipped_push=1.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    with patch.object(settings, "stock_sync_push_enabled", False):
        await pipeline._process_single_variant(
            session=mock_session,
            marketplace_code="shopee",
            marketplace_code_value="SHOPEE",
            shop_db_id_str="shop123",
            shop_warehouse_id="WH-001",
            product=sample_product_active,
            variant=sample_variant,
            inventree=mock_inventree,
            buffer=0,
            cycle_index=1,
            result=result,
        )

    assert len(mock_client.call_log["update_stock"]) == 0
    assert result.total_report_rows == 1
    assert result.total_skipped_push == 1
    assert result.total_pushed == 0


@pytest.mark.asyncio
async def test_monitoring_mode_queue_marked_processed_and_log_skipped(
    mock_session,
    sample_product_active,
    sample_variant,
):
    """
    Scenario: stock_sync_push_enabled=false.

    Expected audit trail:
      - StockSyncQueueModel ditulis dengan status PROCESSED (bukan PENDING)
        supaya idempotency pending-check tidak memblokir cycle berikutnya.
      - StockSyncLogModel ditulis dengan status "SKIPPED" dan error_message
        yang menjelaskan mode monitoring.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))
    mock_inventree = MockInvenTreeClient()

    mock_mapping = MagicMock(spec=SKUMappingModel)
    mock_mapping.inventree_part_id = 12345
    mock_mapping.sku_type = "NORMAL"
    mock_mapping.conversion_qty = 1
    mock_mapping.marketplace_variant_id = "shop123_variant001"
    mock_mapping.status = "ACTIVE"

    # previous_stock diambil dari variant.total_available_stock (100); dengan
    # snapshot stock_available=100 & reserved=0, target_stock=100. Supaya
    # final_stock=previous_stock teruji dengan nilai yang BERBEDA dari target,
    # set stock lama variant ke nilai lain (25).
    sample_variant.total_available_stock = 25

    _call_idx = 0

    async def _execute_side_effect(*args, **kwargs):
        nonlocal _call_idx
        _call_idx += 1
        res = MagicMock()
        res.scalar.return_value = 0
        if _call_idx == 1:
            res.scalars.return_value.first.return_value = mock_mapping
        else:
            res.scalars.return_value.first.return_value = None
        return res

    mock_session.execute = AsyncMock(side_effect=_execute_side_effect)

    with patch.object(settings, "stock_sync_push_enabled", False):
        await pipeline._process_single_variant(
            session=mock_session,
            marketplace_code="shopee",
            marketplace_code_value="SHOPEE",
            shop_db_id_str="shop123",
            shop_warehouse_id="WH-001",
            product=sample_product_active,
            variant=sample_variant,
            inventree=mock_inventree,
            buffer=0,
            cycle_index=1,
            result=result,
        )

    # Kumpulkan object yang di-add ke session
    added_models = [
        call_args.args[0]
        for call_args in mock_session.add.call_args_list
        if call_args.args
    ]

    queue_entries = [m for m in added_models if isinstance(m, StockSyncQueueModel)]
    log_entries = [m for m in added_models if isinstance(m, StockSyncLogModel)]

    assert len(queue_entries) == 1
    assert queue_entries[0].status == "PROCESSED", (
        "Monitoring mode: queue harus PROCESSED agar tidak memblokir cycle berikutnya"
    )
    assert queue_entries[0].processed_at is not None

    assert len(log_entries) == 1
    assert log_entries[0].status == "SKIPPED"
    assert "stock_sync_push_enabled=false" in (log_entries[0].error_message or "")
    # Tidak ada push → final_stock = previous_stock (stock marketplace tidak
    # berubah), BUKAN target_stock.
    assert log_entries[0].target_stock == 100
    assert log_entries[0].final_stock == 25

    assert result.total_queued == 1
    assert result.total_logged == 1
    assert result.total_skipped_push == 1


def test_pipeline_cycle_result_monitoring_fields():
    """PipelineCycleResult.to_dict() menyertakan push_enabled & total_skipped_push."""
    result = PipelineCycleResult(
        cycle_index=1,
        started_at=datetime(2026, 7, 30, 10, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 7, 30, 10, 5, 0, tzinfo=timezone.utc),
        total_report_rows=10,
        total_queued=10,
        total_pushed=0,
        total_logged=10,
        total_skipped_push=10,
        push_enabled=False,
    )
    d = result.to_dict()
    assert d["push_enabled"] is False
    assert d["total_skipped_push"] == 10


# ======================================================================
# Tests: Step 3 MAPPING utk SEMUA shop ACTIVE (paralel)
# ======================================================================


@pytest.mark.asyncio
async def test_map_all_active_shops_maps_all_active_shops():
    """
    Step 3 MAPPING dijalankan untuk SEMUA shop ACTIVE (paralel), bukan hanya
    shop target job ini — supaya saat inventory cycle dieksekusi, SEMUA variant
    dari SEMUA marketplace sudah ter-mapping (agregasi part_reserved_converted
    lengkap → stok_di_update konsisten antar shop).

    Verifikasi: kedua shop ACTIVE di-map, dan total_mapped = SUM lintas shop.
    """
    from types import SimpleNamespace

    from application.usecases import stock_sync_pipeline as pipeline_module

    mp_shopee = SimpleNamespace(id="mp-1", code="shopee")
    mp_tiktok = SimpleNamespace(id="mp-2", code="tiktok")
    shop_shopee = SimpleNamespace(
        id="shop-uuid-shopee", marketplace_id="mp-1",
        external_shop_id="227674818", shop_name="Shopee",
        status=RecordStatus.ACTIVE,
    )
    shop_tiktok = SimpleNamespace(
        id="shop-uuid-tiktok", marketplace_id="mp-2",
        external_shop_id="7494709429666874412", shop_name="TikTok",
        status=RecordStatus.ACTIVE,
    )

    fake_mp_repo = MagicMock()
    fake_mp_repo.list_all = AsyncMock(return_value=[mp_shopee, mp_tiktok])
    fake_shop_repo = MagicMock()
    fake_shop_repo.list_all = AsyncMock(return_value=[shop_shopee, shop_tiktok])

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    fake_engine = MagicMock()
    fake_engine.run_mapping_for_shop = AsyncMock(
        side_effect=[
            {"status": "success", "total_mapped": 5, "total_not_found": 1,
             "total_duplicates": 0, "total_errors": 0, "total_skipped": 0},
            {"status": "success", "total_mapped": 3, "total_not_found": 2,
             "total_duplicates": 1, "total_errors": 0, "total_skipped": 0},
        ]
    )

    # Global validation (TC-SKU-006) — mock service supaya tidak butuh DB asli.
    fake_validation_service = MagicMock()
    fake_validation_service.run_global_checks = AsyncMock(
        return_value={"total_conflicts": 0, "total_warnings": 0, "issues": []}
    )

    pipeline = StockSyncPipelineEngine(apisix_client=MagicMock())

    with patch.object(pipeline_module, "async_session_factory", return_value=fake_session), \
         patch.object(pipeline_module, "MarketplaceRepositoryImpl", return_value=fake_mp_repo), \
         patch.object(pipeline_module, "MarketplaceShopRepositoryImpl", return_value=fake_shop_repo), \
         patch.object(pipeline_module, "create_mapping_engine", return_value=fake_engine), \
         patch.object(pipeline_module, "create_validation_service", return_value=fake_validation_service):
        result = await pipeline._map_all_active_shops(
            cycle_index=1,
        )

    assert result["status"] == "success"
    assert result["total_mapped"] == 8, "total_mapped harus SUM lintas shop"
    assert result["total_not_found"] == 3
    assert result["total_duplicates"] == 1
    assert result["total_errors"] == 0
    assert len(result["shops"]) == 2
    assert result["failures"] == []

    # Kedua shop ACTIVE di-map (bukan hanya shop target)
    assert fake_engine.run_mapping_for_shop.call_count == 2
    mapped_shop_ids = [c.args[0] for c in fake_engine.run_mapping_for_shop.call_args_list]
    assert "shop-uuid-shopee" in mapped_shop_ids
    assert "shop-uuid-tiktok" in mapped_shop_ids


@pytest.mark.asyncio
async def test_map_all_active_shops_skips_inactive_shop_and_aggregates_failures():
    """
    Hanya shop ACTIVE yang di-map (job global), dan kegagalan satu shop TIDAK
    menggagalkan shop lain — error diakumulasi ke ``failures`` dan status
    agregasi menjadi "partial".
    """
    from types import SimpleNamespace

    from application.usecases import stock_sync_pipeline as pipeline_module

    mp_shopee = SimpleNamespace(id="mp-1", code="shopee")
    mp_tiktok = SimpleNamespace(id="mp-2", code="tiktok")
    # Shop Shopee INACTIVE → tidak boleh di-map oleh job global
    shop_target = SimpleNamespace(
        id="shop-uuid-target", marketplace_id="mp-1",
        external_shop_id="227674818", shop_name="Shopee (INACTIVE)",
        status=RecordStatus.INACTIVE,
    )
    shop_tiktok = SimpleNamespace(
        id="shop-uuid-tiktok", marketplace_id="mp-2",
        external_shop_id="7494709429666874412", shop_name="TikTok",
        status=RecordStatus.ACTIVE,
    )

    fake_mp_repo = MagicMock()
    fake_mp_repo.list_all = AsyncMock(return_value=[mp_shopee, mp_tiktok])
    fake_shop_repo = MagicMock()
    fake_shop_repo.list_all = AsyncMock(return_value=[shop_target, shop_tiktok])

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    # Hanya 1 shop ACTIVE (tiktok) yang di-map; mapping-nya dibuat gagal.
    async def _run_mapping_for_shop(shop_id: str) -> dict:
        if shop_id == "shop-uuid-tiktok":
            raise RuntimeError("APISIX timeout mapping tiktok")
        return {
            "status": "success", "total_mapped": 7, "total_not_found": 0,
            "total_duplicates": 0, "total_errors": 0, "total_skipped": 0,
        }

    fake_engine = MagicMock()
    fake_engine.run_mapping_for_shop = AsyncMock(side_effect=_run_mapping_for_shop)

    # Global validation (TC-SKU-006) — mock service supaya tidak butuh DB asli.
    fake_validation_service = MagicMock()
    fake_validation_service.run_global_checks = AsyncMock(
        return_value={"total_conflicts": 0, "total_warnings": 0, "issues": []}
    )

    pipeline = StockSyncPipelineEngine(apisix_client=MagicMock())

    with patch.object(pipeline_module, "async_session_factory", return_value=fake_session), \
         patch.object(pipeline_module, "MarketplaceRepositoryImpl", return_value=fake_mp_repo), \
         patch.object(pipeline_module, "MarketplaceShopRepositoryImpl", return_value=fake_shop_repo), \
         patch.object(pipeline_module, "create_mapping_engine", return_value=fake_engine), \
         patch.object(pipeline_module, "create_validation_service", return_value=fake_validation_service):
        result = await pipeline._map_all_active_shops(
            cycle_index=1,
        )

    # Shop INACTIVE tidak di-map sama sekali
    assert fake_engine.run_mapping_for_shop.call_count == 1
    mapped_shop_ids = [c.args[0] for c in fake_engine.run_mapping_for_shop.call_args_list]
    assert mapped_shop_ids == ["shop-uuid-tiktok"]

    # Shop TikTok (satu-satunya ACTIVE) gagal → error terakumulasi, status partial
    assert result["status"] == "partial"
    assert result["total_mapped"] == 0
    assert len(result["failures"]) == 1
    assert "APISIX timeout mapping tiktok" in result["failures"][0]
    # Hasil error shop tetap tercatat di shops
    assert len(result["shops"]) == 1
