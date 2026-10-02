"""
test_stock_sync_manual_part.py

Test RUN MANUAL per part_id (``POST /api/v2/stock-sync/run-part``):

  1. Scope dibaca dari tabel ``sku_mapping`` → ``marketplace_variant_id`` +
     ``marketplace_product_id`` (TANPA key gabungan shop_id).
  2. HANYA product/variant di scope yang diproses — variant part lain tidak
     di-push ke APISIX.
  3. Baris ``stock_sync_queue.trigger_source`` / ``stock_sync_log.source``
     ditandai ``MANUAL_PART`` (retry: ``MANUAL_PART_RETRY``) sehingga bisa
     dibedakan dari pipeline GLOBAL (``POLLING_1H``) & jalur webhook.
  4. Step 1-3 (GET product/SYNC/MAPPING) tidak dijalankan (tanpa fetch_products).
  5. part_id tanpa mapping yang bisa diproses → ValueError (endpoint membalas 404).

Test level pipeline — mock DB session + mock APISIX/InvenTree, tanpa DB/network.

Run:
    pytest tests/test_stock_sync_manual_part.py -v
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from application.dto.requests import (
    StockSyncManualPartResponse,
    StockSyncPartScopeResponse,
)
from core.config import settings
from infrastructure.db.models import (
    InventoryReportModel,
    StockSyncLogModel,
    StockSyncQueueModel,
)
from application.usecases.stock_sync_pipeline import (
    TRIGGER_SOURCE_MANUAL_PART,
    TRIGGER_SOURCE_MANUAL_PART_RETRY,
    TRIGGER_SOURCE_PIPELINE,
    TRIGGER_SOURCE_PIPELINE_RETRY,
    PartScope,
    PipelineCycleResult,
    StockSyncPipelineEngine,
    _retry_source_for,
)
from tests.test_pipeline_activation import MockApisixClient, MockInvenTreeClient
from tests.test_pipeline_inventory_global import (
    _mapping_side_effect,
    _mk_mapping,
    _mk_product,
    _mk_variant,
    _session_factory,
)


@pytest.fixture(autouse=True)
def _enable_push(monkeypatch):
    """Run manual per part harus benar-benar push (bukan mode monitoring)."""
    monkeypatch.setattr(settings, "stock_sync_push_enabled", True)


# ======================================================================
# Helpers
# ======================================================================


def _scope_row(variant_id, mapping_status, product_id, variant_status):
    """Satu baris hasil query resolve_part_scope (sku_mapping ⋈ marketplace_variant)."""
    return SimpleNamespace(
        variant_id=variant_id,
        mapping_status=mapping_status,
        product_id=product_id,
        variant_status=variant_status,
    )


def _rows_session_factory(rows):
    """async_session_factory mock: execute(...).all() → rows."""
    result = MagicMock()
    result.all.return_value = rows
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    return _session_factory(session)


def _add_calls(session, model_type):
    return [c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], model_type)]


def _mock_write_session():
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    return session


# ======================================================================
# resolve_part_scope — baca sku_mapping tanpa gabungan shop_id
# ======================================================================


@pytest.mark.asyncio
async def test_resolve_part_scope_uses_variant_and_product_id_only():
    """
    Dua mapping dengan shop BERBEDA tapi product_id SAMA tetap menghasilkan
    satu product_id pada scope (tidak ada penggabungan dengan shop_id).
    """
    pipeline = StockSyncPipelineEngine(apisix_client=MockApisixClient())
    rows = [
        _scope_row(
            "7494709429666874412_1736672744536507436", "ACTIVE",
            "1736672745058173996", "ACTIVE",
        ),
        _scope_row(
            "227674818_11250494", "ACTIVE",
            "1736672745058173996", "ACTIVE",
        ),
        _scope_row(
            "7494709429666874412_1736672744536638508", "INACTIVE",
            "1736672745058173996", "ACTIVE",
        ),
        _scope_row(
            "7494709429666874412_999", "ACTIVE",
            "1736672745058173996", "DELETED",
        ),
        _scope_row("orphan_variant", "ACTIVE", None, None),
    ]

    with patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _rows_session_factory(rows),
    ):
        scope = await pipeline.resolve_part_scope(42)

    assert scope.part_id == 42
    assert scope.mappings == 5
    assert scope.is_empty is False
    assert scope.variant_ids == [
        "7494709429666874412_1736672744536507436",
        "227674818_11250494",
        "7494709429666874412_1736672744536638508",
    ]
    # Satu product_id untuk dua shop berbeda — TANPA key gabungan shop_id.
    assert scope.product_ids == ["1736672745058173996"]
    assert scope.inactive_mapping_ids == ["7494709429666874412_1736672744536638508"]
    assert scope.deleted_variant_ids == ["7494709429666874412_999"]
    assert scope.missing_variant_ids == ["orphan_variant"]


@pytest.mark.asyncio
async def test_resolve_part_scope_empty_when_no_mapping():
    pipeline = StockSyncPipelineEngine(apisix_client=MockApisixClient())

    with patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _rows_session_factory([]),
    ):
        scope = await pipeline.resolve_part_scope(777)

    assert scope.is_empty is True
    assert scope.variant_ids == []
    assert scope.mappings == 0


@pytest.mark.asyncio
async def test_run_once_for_part_rejects_empty_scope():
    """part_id tanpa variant yang bisa diproses → ValueError (endpoint → 404)."""
    pipeline = StockSyncPipelineEngine(apisix_client=MockApisixClient())

    with pytest.raises(ValueError) as exc:
        await pipeline.run_once_for_part(777, scope=PartScope(part_id=777))

    assert "777" in str(exc.value)


# ======================================================================
# Scope execution + penanda MANUAL_PART
# ======================================================================


@pytest.mark.asyncio
async def test_run_once_for_part_only_touches_scoped_variant():
    """
    Dua variant dari dua product berbeda (part 42 & part 99): run manual
    part_id=42 HANYA memproses variant part 42 — variant part 99 tidak
    di-push, tidak di-queue, tidak di-log.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    in_scope = _mk_variant("7494709429666874412", "prodIn", "varIn", "SKU-IN")
    other = _mk_variant("227674818", "prodOut", "varOut", "SKU-OUT")

    session = _mock_write_session()
    session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(42), _mk_mapping(99)])
    )

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[in_scope, other])
    # Jalur scope WAJIB memakai list_by_ids (hanya variant scope dibaca dari DB).
    repo_variants.list_by_ids = AsyncMock(return_value=[in_scope])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(
        return_value=[
            _mk_product("7494709429666874412", "prodIn"),
            _mk_product("227674818", "prodOut"),
        ]
    )
    repo_products.list_by_ids = AsyncMock(
        return_value=[_mk_product("7494709429666874412", "prodIn")]
    )

    scope = PartScope(
        part_id=42,
        mappings=1,
        variant_ids=[in_scope.marketplace_variant_id],
        product_ids=["prodIn"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ):
        result = await pipeline.run_once_for_part(42, scope=scope)

    # Hanya variant in-scope yang di-push (Format B flat, 1 call).
    assert len(mock_client.call_log["update_stock"]) == 1
    call = mock_client.call_log["update_stock"][0]
    assert call["variant_id"] == "varIn"
    assert call["product_id"] == "prodIn"

    # Tanpa scope→ produk lain ikut ter-push; di sini tidak boleh ada SKU-OUT.
    assert all(c.get("variant_id") != "varOut" for c in mock_client.call_log["update_stock"])

    assert result.total_parts_processed == 1
    assert result.total_queued == 1
    assert result.total_pushed == 1
    assert result.total_logged == 1
    assert result.total_report_rows == 1
    assert result.failures == []

    # Step 1-2 (GET product dari marketplace) TIDAK dijalankan.
    assert mock_client.call_log["fetch_products"] == []


@pytest.mark.asyncio
async def test_run_once_for_part_reads_only_scope_not_whole_catalog():
    """Regresi performa jalur run-part: cukup baca SCOPE, bukan seluruh katalog.

    ``list_all()`` (SEMUA product & variant dari SEMUA shop) tidak boleh
    dipanggil pada mode scope — bacaannya harus lewat ``list_by_ids()`` untuk
    variant scope dan product-nya saja, supaya biaya run satu part tidak lagi
    O(seluruh katalog).
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    variant = _mk_variant("227674818", "prodA", "varA", "SKU-A")
    session = _mock_write_session()
    session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(11)]))

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant])
    repo_variants.list_by_ids = AsyncMock(return_value=[variant])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(
        return_value=[_mk_product("227674818", "prodA")]
    )
    repo_products.list_by_ids = AsyncMock(
        return_value=[_mk_product("227674818", "prodA")]
    )

    scope = PartScope(
        part_id=11,
        mappings=1,
        variant_ids=[variant.marketplace_variant_id],
        product_ids=["prodA"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ), patch.object(
        pipeline,
        "_refresh_reserved_stock_for_scope",
        AsyncMock(return_value={}),
    ):
        result = await pipeline.run_once_for_part(11, scope=scope)

    # Katalog penuh TIDAK dibaca; hanya query ter-index per scope.
    repo_variants.list_all.assert_not_awaited()
    repo_products.list_all.assert_not_awaited()
    repo_variants.list_by_ids.assert_awaited_once()
    repo_products.list_by_ids.assert_awaited_once()

    # Hasil pipeline tetap sama seperti sebelumnya.
    assert result.total_pushed == 1
    assert result.total_report_rows == 1


@pytest.mark.asyncio
async def test_run_once_for_part_tags_queue_and_log_as_manual_part():
    """Semua baris queue/log run manual ditandai MANUAL_PART (beda dari POLLING_1H)."""
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    variant = _mk_variant("227674818", "prodA", "varA", "SKU-A")
    session = _mock_write_session()
    session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(11)]))

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant])
    repo_variants.list_by_ids = AsyncMock(return_value=[variant])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[_mk_product("227674818", "prodA")])
    repo_products.list_by_ids = AsyncMock(
        return_value=[_mk_product("227674818", "prodA")]
    )

    scope = PartScope(
        part_id=11,
        mappings=1,
        variant_ids=[variant.marketplace_variant_id],
        product_ids=["prodA"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ):
        await pipeline.run_once_for_part(11, scope=scope)

    queues = _add_calls(session, StockSyncQueueModel)
    logs = _add_calls(session, StockSyncLogModel)
    reports = _add_calls(session, InventoryReportModel)

    assert len(queues) == 1
    assert queues[0].trigger_source == TRIGGER_SOURCE_MANUAL_PART
    assert len(logs) == 1
    assert logs[0].source == TRIGGER_SOURCE_MANUAL_PART
    assert logs[0].inventree_part_id == 11
    assert len(reports) == 1
    assert reports[0].part_id == 11


@pytest.mark.asyncio
async def test_run_once_for_part_records_and_pushes_unchanged_for_all_marketplaces():
    """Run MANUAL per part: stok TIDAK berubah tetap dicatat ke report/queue/log
    dan tetap di-push ke SEMUA marketplace di scope (mis. Shopee + TikTok).

    Regresi: dulu variant dengan ``previous_stock == target_stock`` hanya
    menulis inventory_report lalu ``return None`` → marketplace-nya TIDAK pernah
    menerima update-stock dan tidak ada baris stock_sync_queue / stock_sync_log,
    sehingga operator mengira part hanya ter-update ke sebagian marketplace.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    shopee = _mk_variant("227674818", "prodShopee", "varSP", "SKU-SP")
    tiktok = _mk_variant("7494709429666874412", "prodTikTok", "varTT", "SKU-TT")
    # MockInvenTreeClient: stock_available=100, reserved/buffer 0 → target=100.
    # Jadikan previous=100 supaya kondisi "stok tidak berubah" (yang dulu
    # men-skip queue/log/push) benar-benar terpicu untuk kedua marketplace.
    for variant in (shopee, tiktok):
        variant.total_available_stock = 100

    session = _mock_write_session()
    session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(11), _mk_mapping(11)])
    )

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[shopee, tiktok])
    repo_variants.list_by_ids = AsyncMock(return_value=[shopee, tiktok])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(
        return_value=[
            _mk_product("227674818", "prodShopee"),
            _mk_product("7494709429666874412", "prodTikTok"),
        ]
    )
    repo_products.list_by_ids = AsyncMock(
        return_value=[
            _mk_product("227674818", "prodShopee"),
            _mk_product("7494709429666874412", "prodTikTok"),
        ]
    )

    scope = PartScope(
        part_id=11,
        mappings=2,
        variant_ids=[shopee.marketplace_variant_id, tiktok.marketplace_variant_id],
        product_ids=["prodShopee", "prodTikTok"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ):
        result = await pipeline.run_once_for_part(11, scope=scope)

    # Kedua variant tetap di-push walau stoknya tidak berubah.
    pushed_variants = {
        call["variant_id"] for call in mock_client.call_log["update_stock"]
    }
    assert pushed_variants == {"varSP", "varTT"}

    # Ketiga tabel terisi untuk SEMUA variant scope (tidak ada yang di-skip).
    assert result.total_report_rows == 2
    assert result.total_queued == 2
    assert result.total_logged == 2
    assert result.total_pushed == 2
    assert result.total_skipped_unchanged == 0

    queues = _add_calls(session, StockSyncQueueModel)
    logs = _add_calls(session, StockSyncLogModel)
    reports = _add_calls(session, InventoryReportModel)
    assert len(queues) == 2
    assert [q.trigger_source for q in queues] == [TRIGGER_SOURCE_MANUAL_PART] * 2
    assert len(logs) == 2
    assert [lg.source for lg in logs] == [TRIGGER_SOURCE_MANUAL_PART] * 2
    assert len(reports) == 2


@pytest.mark.asyncio
async def test_run_inventory_cycle_without_scope_still_uses_polling_1h():
    """Regresi: jalur pipeline GLOBAL tetap ditandai POLLING_1H."""
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    variant = _mk_variant("227674818", "prodA", "varA", "SKU-A")
    session = _mock_write_session()
    session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(11)]))

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[_mk_product("227674818", "prodA")])

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    queues = _add_calls(session, StockSyncQueueModel)
    logs = _add_calls(session, StockSyncLogModel)
    assert [q.trigger_source for q in queues] == [TRIGGER_SOURCE_PIPELINE]
    assert [lg.source for lg in logs] == [TRIGGER_SOURCE_PIPELINE]


# ======================================================================
# Retry pass
# ======================================================================


def test_retry_source_follows_run_mode():
    assert _retry_source_for(TRIGGER_SOURCE_MANUAL_PART) == TRIGGER_SOURCE_MANUAL_PART_RETRY
    assert _retry_source_for(TRIGGER_SOURCE_PIPELINE) == TRIGGER_SOURCE_PIPELINE_RETRY
    assert _retry_source_for(None) == TRIGGER_SOURCE_PIPELINE_RETRY


@pytest.mark.asyncio
async def test_retry_pass_on_manual_item_writes_manual_part_retry_log():
    """Item hasil run manual yang di-retry menulis log MANUAL_PART_RETRY."""
    client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=client)

    queue_entry = MagicMock()
    queue_entry.id = "queue-manual-1"
    queue_entry.status = "FAILED"
    queue_entry.retry_count = 0
    queue_entry.error_message = "HTTP 400 response body: {}"
    queue_entry.processed_at = None

    variant = MagicMock()
    variant.marketplace_variant_id = "7494709429666874412_1736672744536507436"

    item = {
        "variant": variant,
        "seller_sku": "SKU-IN",
        "product_status": "ACTIVE",
        "product_id": "prodIn",
        "variant_id": "varIn",
        "is_orphan": False,
        "marketplace_code": "shopee",
        "shop_db_id": "7494709429666874412",
        "warehouse_id": "WH-001",
        "target_stock": 100,
        "previous_stock": 50,
        "final_stock": 50,
        "push_status": "FAILED",
        "push_error": "HTTP 400 response body: {}",
        "queue_entry": queue_entry,
        "part_id": 42,
        "trigger_source": TRIGGER_SOURCE_MANUAL_PART,
    }

    added = []

    def _session_factory_retry():
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _cm():
            session = AsyncMock()
            session.execute = AsyncMock(return_value=MagicMock())
            session.commit = AsyncMock()
            session.add = MagicMock(side_effect=lambda m: added.append(m))
            yield session

        return _cm

    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    with patch.object(settings, "stock_sync_push_enabled", True), patch.object(
        settings, "stock_sync_retry_enabled", True
    ), patch.object(settings, "stock_sync_max_retry_count", 3), patch.object(
        settings, "stock_sync_retry_backoff_seconds", 0.0
    ):
        await pipeline._retry_failed_pushes(
            items=[item],
            cycle_index=1,
            result=result,
            session_factory=_session_factory_retry(),
        )

    assert result.total_retry_attempted == 1
    assert result.total_retry_succeeded == 1

    logs = [m for m in added if isinstance(m, StockSyncLogModel)]
    assert len(logs) == 1
    assert logs[0].source == TRIGGER_SOURCE_MANUAL_PART_RETRY


# ======================================================================
# DTO respons
# ======================================================================


# ======================================================================
# Refresh reserved stock ter-scope (pengganti step 1-3)
# ======================================================================


class _ReservedApisixClient(MockApisixClient):
    """MockApisixClient + ``fetch_reserved_stock`` (filter per product_id)."""

    def __init__(self, reserved_by_product):
        super().__init__()
        self.call_log["fetch_reserved_stock"] = []
        self._reserved_by_product = reserved_by_product

    async def fetch_reserved_stock(
        self, marketplace, shop_uuid, product_id=None, variant_id=None
    ):
        self.call_log["fetch_reserved_stock"].append(
            {"marketplace": marketplace, "shop_uuid": shop_uuid, "product_id": product_id}
        )
        return self._reserved_by_product.get(str(product_id))


def _reserved_row(internal, external, product, shop, code):
    return SimpleNamespace(
        marketplace_variant_id=internal,
        external_variant_id=external,
        external_product_id=product,
        shop_id=shop,
        code=code,
    )


@pytest.mark.asyncio
async def test_refresh_reserved_stock_for_scope_writes_per_variant():
    """Nilai reserved ditulis PER-VARIANT dari /order (bukan total produk).

    Satu product punya dua variant: yang muncul di /order memakai angkanya,
    yang tidak muncul (sweep tuntas) di-set 0.
    """
    mock_client = _ReservedApisixClient(
        {
            "EXT-PROD-1": {
                "marketplace": "shopee",
                "sweep_complete": True,
                "reserved_stock": [
                    {
                        "product_id": "EXT-PROD-1",
                        "variants": [
                            {"variant_id": "EXT-VAR-1", "total_reserved_stock": 3}
                        ],
                    }
                ],
            }
        }
    )
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    read_result = MagicMock()
    read_result.all.return_value = [
        _reserved_row("INT-VAR-1", "EXT-VAR-1", "EXT-PROD-1", "SHOP-A", "shopee"),
        _reserved_row("INT-VAR-2", "EXT-VAR-2", "EXT-PROD-1", "SHOP-A", "shopee"),
    ]

    session = _mock_write_session()

    def _execute(*args, **kwargs):
        stmt = str(args[0]) if args else ""
        if "marketplace_product" in stmt and "marketplace_variant" in stmt:
            return read_result
        return MagicMock()

    session.execute = AsyncMock(side_effect=_execute)

    scope = PartScope(
        part_id=42,
        mappings=2,
        variant_ids=["INT-VAR-1", "INT-VAR-2"],
        product_ids=["PROD-PK-1"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ):
        updates = await pipeline._refresh_reserved_stock_for_scope(scope)

    assert updates == {"INT-VAR-1": 3, "INT-VAR-2": 0}
    assert mock_client.call_log["fetch_reserved_stock"] == [
        {"marketplace": "shopee", "shop_uuid": "SHOP-A", "product_id": "EXT-PROD-1"}
    ]
    session.commit.assert_awaited()
    # 1 query baca scope + 2 update (satu per variant).
    assert session.execute.await_count == 3


@pytest.mark.asyncio
async def test_run_once_for_part_refreshes_reserved_stock_before_cycle():
    """run_once_for_part memanggil refresh reserved sebelum inventory cycle."""
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    variant = _mk_variant("227674818", "prodA", "varA", "SKU-A")
    session = _mock_write_session()
    session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(11)]))

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant])
    repo_variants.list_by_ids = AsyncMock(return_value=[variant])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[_mk_product("227674818", "prodA")])
    repo_products.list_by_ids = AsyncMock(
        return_value=[_mk_product("227674818", "prodA")]
    )

    scope = PartScope(
        part_id=11,
        mappings=1,
        variant_ids=[variant.marketplace_variant_id],
        product_ids=["prodA"],
    )

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ), patch.object(
        pipeline,
        "_refresh_reserved_stock_for_scope",
        AsyncMock(return_value={variant.marketplace_variant_id: 7}),
    ) as refresh:
        result = await pipeline.run_once_for_part(11, scope=scope)

    refresh.assert_awaited_once()
    assert result.total_reserved_refreshed == 1


@pytest.mark.asyncio
async def test_run_once_for_parts_full_pipeline_runs_steps_1_3_and_scopes_all_parts():
    """``full_pipeline=True`` → step 1-3 dijalankan TER-SCOPE, bukan batch.

    Pipeline-dari-awal HANYA mengambil product yang terhubung dengan part_id
    (``resolve_part_product_targets`` → ``sync_by_shop_id(product_ids=…)``),
    lalu mapping variant product tsb saja — BUKAN sweep seluruh toko
    (``_sync_all_active_shops`` / ``_map_all_active_shops`` tidak dipanggil).

    Regresi: satu product marketplace bisa memetakan ke BANYAK part_id
    (mis. product TikTok → [3000100, 3000101, 3000102]). Dulu hanya part
    PERTAMA yang dijalankan, dan saat diperbaiki malah menjalankan pipeline
    batch (seluruh toko).
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)

    shopee = _mk_variant("227674818", "prodShopee", "varSP", "SKU-SP")
    tiktok = _mk_variant("7494709429666874412", "prodTikTok", "varTT", "SKU-TT")

    session = _mock_write_session()
    session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(11), _mk_mapping(22)])
    )

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[shopee, tiktok])
    repo_variants.list_by_shop = AsyncMock(
        side_effect=lambda shop_ext: {
            "227674818": [shopee],
            "7494709429666874412": [tiktok],
        }.get(shop_ext, [])
    )
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(
        return_value=[
            _mk_product("227674818", "prodShopee"),
            _mk_product("7494709429666874412", "prodTikTok"),
        ]
    )
    repo_products.list_by_ids = AsyncMock(
        return_value=[
            _mk_product("227674818", "prodShopee"),
            _mk_product("7494709429666874412", "prodTikTok"),
        ]
    )
    repo_variants.list_by_ids = AsyncMock(return_value=[shopee, tiktok])
    repo_shops = MagicMock()
    repo_shops.list_all = AsyncMock(
        return_value=[
            SimpleNamespace(id="shop-1", external_shop_id="227674818", shop_name="Shopee"),
            SimpleNamespace(
                id="shop-2",
                external_shop_id="7494709429666874412",
                shop_name="Tiktok",
            ),
        ]
    )

    scopes_by_part = {
        11: PartScope(
            part_id=11,
            mappings=1,
            variant_ids=[shopee.marketplace_variant_id],
            product_ids=["prodShopee"],
        ),
        22: PartScope(
            part_id=22,
            mappings=1,
            variant_ids=[tiktok.marketplace_variant_id],
            product_ids=["prodTikTok"],
        ),
    }

    async def fake_resolve(pid):
        return scopes_by_part[int(pid)]

    targets_mock = AsyncMock(
        return_value=[
            {"shop_id": "227674818", "external_product_id": "prodShopee"},
            {
                "shop_id": "7494709429666874412",
                "external_product_id": "prodTikTok",
            },
        ]
    )
    sync_engine_obj = SimpleNamespace(
        sync_by_shop_id=AsyncMock(
            return_value={
                "status": "success",
                "total_products": 1,
                "total_variants": 1,
                "reserved_order_logs": [],
            }
        )
    )
    mapping_engine_obj = SimpleNamespace(
        run_mapping_for_variants=AsyncMock(
            return_value={
                "status": "success",
                "scoped": True,
                "total_variants": 1,
                "total_mapped": 1,
                "total_not_found": 0,
                "total_duplicates": 0,
                "total_errors": 0,
                "total_blocked": 0,
            }
        )
    )

    batch_sync = AsyncMock(return_value={"status": "success"})
    batch_mapping = AsyncMock(return_value={"status": "success"})

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceShopRepositoryImpl",
        return_value=repo_shops,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(session),
    ), patch(
        "application.usecases.stock_sync_pipeline.create_sync_engine",
        return_value=sync_engine_obj,
    ), patch(
        "application.usecases.stock_sync_pipeline.create_mapping_engine",
        return_value=mapping_engine_obj,
    ), patch.object(
        pipeline, "resolve_part_product_targets", targets_mock
    ), patch.object(
        pipeline, "resolve_part_scope", side_effect=fake_resolve
    ), patch.object(
        pipeline, "_write_reserved_order_log", MagicMock(return_value={})
    ), patch.object(
        pipeline, "_sync_all_active_shops", batch_sync
    ), patch.object(
        pipeline, "_map_all_active_shops", batch_mapping
    ):
        result = await pipeline.run_once_for_parts([11, 22], full_pipeline=True)

    # Product target diambil dari sku_mapping part tsb (bukan sweep toko).
    targets_mock.assert_awaited_once()
    batch_sync.assert_not_awaited()
    batch_mapping.assert_not_awaited()

    # Step 1-2: sync DIPANGGIL per shop dengan daftar product TER-SCOPE.
    assert sync_engine_obj.sync_by_shop_id.await_count == 2
    called_kwargs = {
        str(call.kwargs["product_ids"][0])
        for call in sync_engine_obj.sync_by_shop_id.await_args_list
    }
    assert called_kwargs == {"prodShopee", "prodTikTok"}
    assert result.sync_result["scoped"] is True

    # Step 3: mapping variant product tsb saja.
    assert mapping_engine_obj.run_mapping_for_variants.await_count == 2
    assert result.mapping_result["scoped"] is True

    # SEMUA varian dari SEMUA part di scope ikut di-push (Shopee + TikTok).
    pushed = {c["variant_id"] for c in mock_client.call_log["update_stock"]}
    assert pushed == {"varSP", "varTT"}
    assert result.total_parts_processed == 2
    assert result.total_pushed == 2
    assert {s["part_id"] for s in result.resolved_part_scopes} == {11, 22}


def test_manual_part_response_exposes_run_mode_and_scope():
    payload = StockSyncManualPartResponse(
        status="success",
        part_id=42,
        scope=StockSyncPartScopeResponse(
            part_id=42,
            mappings=1,
            variant_ids=["7494709429666874412_1736672744536507436"],
            product_ids=["1736672745058173996"],
        ),
        message="ok",
    )

    dumped = payload.model_dump()
    assert dumped["run_mode"] == TRIGGER_SOURCE_MANUAL_PART
    assert dumped["trigger_source"] == TRIGGER_SOURCE_MANUAL_PART
    assert dumped["log_source"] == TRIGGER_SOURCE_MANUAL_PART
    assert dumped["scope"]["product_ids"] == ["1736672745058173996"]
