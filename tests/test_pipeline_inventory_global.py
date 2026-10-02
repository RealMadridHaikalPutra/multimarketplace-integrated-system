"""
Unit tests untuk inventory cycle GLOBAL stock_sync_pipeline:

1. Ordering: variant dikelompokkan per part_id dan diproses urut ASC.
2. Grouping Format A: product dengan >=2 variant asli → SATU call
   update_stock_batch (body Format A: skus[].inventory[].warehouse_id/quantity).
3. Format B: product tanpa variant asli ATAU variant tunggal → call
   update_stock per-SKU (body flat). Product tanpa variant asli dipush
   dengan id "0" (sentinel no-variant), bukan product_id.
4. Part yang sama muncul di banyak shop/toko → semua variant se-part
   selesai dipush (batch per marketplace) sebelum lanjut part berikutnya.

Setiap test di module ini mengaktifkan stock_sync_push_enabled=True agar
push ke APISIX mock benar-benar terjalan (default config=False = mode
monitoring, di mana push di-skip).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from core.config import settings
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from infrastructure.db.models import (
    SKUMappingModel,
    StockSyncLogModel,
    StockSyncQueueModel,
)
from application.usecases.stock_sync_pipeline import (
    PipelineCycleResult,
    StockSyncPipelineEngine,
)
from tests.test_pipeline_activation import MockApisixClient, MockInvenTreeClient


@pytest.fixture(autouse=True)
def _enable_push_for_inventory_tests(monkeypatch):
    """Kirim semua test di module ini dalam mode push-enabled."""
    monkeypatch.setattr(settings, "stock_sync_push_enabled", True)


# ======================================================================
# Helpers
# ======================================================================


def _mk_product(shop: str, product_id: str, status=RecordStatus.ACTIVE) -> MarketplaceProduct:
    return MarketplaceProduct(
        marketplace_product_id=f"{shop}_{product_id}",
        shop_id=shop,
        external_product_id=product_id,
        product_name=f"Product {product_id}",
        status=status,
    )


def _mk_variant(
    shop: str,
    product_id: str,
    variant_id: Optional[str],
    sku: str,
    reserved: int = 0,
) -> MarketplaceVariant:
    """Variant tunggal tanpa external_variant_id otomatis memakai product_id."""
    return MarketplaceVariant(
        marketplace_variant_id=f"{shop}_{variant_id or product_id}",
        shop_id=shop,
        external_product_id=product_id,
        external_variant_id=variant_id,
        variant_name=f"Variant {sku}",
        seller_sku=sku,
        total_reserved_stock=reserved,
        total_available_stock=50,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )


def _mk_mapping(part_id: int) -> MagicMock:
    mapping = MagicMock(spec=SKUMappingModel)
    mapping.inventree_part_id = part_id
    mapping.sku_type = "NORMAL"
    mapping.conversion_qty = 1
    mapping.status = "ACTIVE"
    return mapping


def _mapping_side_effect(mappings: List[MagicMock]):
    """session.execute() side-effect utk inventory cycle global.

    Satu result mock pintar utk SEMUA query:
    - ``.all()`` (query aggregate reserved_mp / part_reserved) → selalu [].
    - ``.scalar()`` (SUM fallback) → selalu 0.
    - ``.scalars().first()`` → mapping berikutnya HANYA utk mapping-lookup
      (SQL memuat 'sku_mapping' tanpa GROUP BY); query lain (pending-queue
      check, warehouse lookup) → None. Pop dilakukan LAZY saat .first()
      benar-benar dipanggil, jadi query aggregate tidak mengkonsumsi queue."""
    queue = list(mappings)

    async def _execute(*args, **kwargs):
        stmt_str = str(args[0]) if args else ""
        is_mapping_lookup = (
            "sku_mapping" in stmt_str and "group by" not in stmt_str.lower()
        )
        res = MagicMock()
        res.scalar.return_value = 0  # reserved_mp fallback = 0
        res.all.return_value = []  # aggregate rows kosong
        # Warehouse/marketplace-shop lookup pakai .first() langsung (bukan
        # .scalars().first()) → default None (row tidak ada di mock DB).
        res.first.return_value = None

        def _first():  # LAZY: pop hanya saat .first() benar-benar dipanggil
            if not is_mapping_lookup:
                return None  # warehouse/marketplace-shop lookup → no row
            return queue.pop(0) if queue else None

        res.scalars.return_value.first.side_effect = _first
        return res

    return _execute


def _session_factory(session) -> MagicMock:
    """async_session_factory mock: `async with factory() as s` → session."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm)


# ======================================================================
# Ordering per part_id ASC
# ======================================================================


@pytest.mark.asyncio
async def test_resolve_push_order_groups_and_sorts_by_part_id_asc():
    """
    Variant dari part yang berbeda-beda dikelompokkan per part dan hasilnya
    urut ASC: part 100 → 200 → 300 (bukan urutan asli 300, 100, 200).
    """
    pipeline = StockSyncPipelineEngine(apisix_client=MockApisixClient())
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.commit = AsyncMock()
    variants = []
    mappings = []
    for vname, part in [("A-1", 300), ("B-1", 100), ("C-1", 200)]:
        v = _mk_variant(f"shop_{vname}", f"prod_{vname}", f"var_{vname}", f"SKU-{vname}")
        variants.append(v)
        mappings.append(_mk_mapping(part))

    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect(mappings))

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=variants)
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[])

    pushed_parts: List[int] = []

    async def _capture_push_part(session, part_id, part_variants, cycle_index, result, **kwargs):
        pushed_parts.append(part_id)

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline, "_push_part", new=AsyncMock(side_effect=_capture_push_part)
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    # _push_part dipanggil urut part_id ASC (urutan eksekusi stock sync queue)
    assert pushed_parts == [100, 200, 300]


@pytest.mark.asyncio
async def test_resolve_push_order_groups_same_part_across_shops():
    """
    Part yang SAMA dipakai variant dari beberapa shop → masuk SATU kelompok
    part (semua variant se-part diproses bersama sebelum part berikutnya).
    """
    pipeline = StockSyncPipelineEngine(apisix_client=MockApisixClient())
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    v1 = _mk_variant("shopMP_A", "prodM1", "varM1", "SKU-M1")
    v2 = _mk_variant("shopMP_B", "prodM2", "varM2", "SKU-M2")
    v3 = _mk_variant("shopMP_C", "prodM3", "varM3", "SKU-M3")

    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(42)] * 3)
    )

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v1, v2, v3])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[])

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ):
        order = await pipeline._resolve_push_order(
            session=mock_session,
            cycle_index=1,
            variants=[v1, v2, v3],
            shop_warehouse_map={},
        )

    assert len(order) == 1
    part_id, part_variants = order[0]
    assert part_id == 42
    assert {v.marketplace_variant_id for v in part_variants} == {
        v1.marketplace_variant_id,
        v2.marketplace_variant_id,
        v3.marketplace_variant_id,
    }


# ======================================================================
# Grouping Format A: product multi-variant → 1 batch call
# ======================================================================


@pytest.mark.asyncio
async def test_product_with_multiple_variants_uses_format_a_batch():
    """
    1 product dengan 2 variant asli (varA1, varA2) → SATU call
    update_stock_batch berisi kedua SKU (Format A):
      {"skus": [{"id": ..., "inventory": [{"warehouse_id": ..., "quantity": ...}]}]}
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(700), _mk_mapping(700)])
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    variant_a = _mk_variant("227674818", "prodA", "varA1", "SKU-A1")
    variant_b = _mk_variant("227674818", "prodA", "varA2", "SKU-A2")
    products = {"prodA": _mk_product("227674818", "prodA")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_a, variant_b])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(700, [variant_a, variant_b])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]
    flat_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"]

    assert len(batch_calls) == 1, f"expected 1 Format A call, got {len(batch_calls)}"
    assert len(flat_calls) == 0, "multi-variant product must NOT use flat Format B"

    call = batch_calls[0]
    assert call["marketplace"] == "shopee"
    assert call["shop_uuid"] == "227674818"
    assert call["product_id"] == "prodA"
    assert len(call["skus"]) == 2
    skus_by_id = {s["id"]: s for s in call["skus"]}
    # Format A: quantity + warehouse_id nested di inventory[]
    assert skus_by_id["varA1"]["inventory"] == [
        {"warehouse_id": "WH-001", "quantity": 100}
    ]
    assert skus_by_id["varA2"]["inventory"] == [
        {"warehouse_id": "WH-001", "quantity": 100}
    ]

    assert result.total_push_calls_format_a == 1
    assert result.total_push_calls_format_b == 0
    assert result.total_pushed == 2
    assert result.total_report_rows == 2
    assert result.total_queued == 2
    assert result.total_logged == 2


@pytest.mark.asyncio
async def test_product_with_variants_across_different_parts_uses_format_a_batch():
    """
    KASUS NYATA PRODUKSI:
    Variant-variant dari satu produk yang sama (misal Size S dan Size M) masing-masing
    ter-mapping ke InvenTree part_id yang BERBEDA (misal part 101 dan part 102).
    Sebelumnya, karena push dilakukan per part_id, len(real_variants) selalu 1 sehingga
    seluruh 4000 variant ter-push dengan Format B (1 call per SKU).
    Dengan perbaikan grouping lintas part, produk multi-variant ini kini berhasil di-push
    dalam SATU call update_stock_batch (Format A).
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(101), _mk_mapping(102)])
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    variant_s = _mk_variant("227674818", "prodMultiPart", "varS", "SKU-S")
    variant_m = _mk_variant("227674818", "prodMultiPart", "varM", "SKU-M")
    products = {"prodMultiPart": _mk_product("227674818", "prodMultiPart")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_s, variant_m])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    # Simulasi push_order di mana variant_s di part 101 dan variant_m di part 102
    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(101, [variant_s]), (102, [variant_m])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]
    flat_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"]

    # Harus menjadi 1 call batch Format A, BUKAN 2 call flat Format B!
    assert len(batch_calls) == 1, f"expected 1 Format A call, got {len(batch_calls)}"
    assert len(flat_calls) == 0, f"expected 0 Format B calls, got {len(flat_calls)}"

    call = batch_calls[0]
    assert call["marketplace"] == "shopee"
    assert call["shop_uuid"] == "227674818"
    assert call["product_id"] == "prodMultiPart"
    assert len(call["skus"]) == 2
    skus_by_id = {s["id"]: s for s in call["skus"]}
    assert "varS" in skus_by_id
    assert "varM" in skus_by_id

    assert result.total_push_calls_format_a == 1
    assert result.total_push_calls_format_b == 0
    assert result.total_pushed == 2
    assert result.total_report_rows == 2
    assert result.total_queued == 2
    assert result.total_logged == 2


# ======================================================================
# Format B: single variant & no real variant
# ======================================================================


@pytest.mark.asyncio
async def test_product_with_single_variant_uses_format_b():
    """
    Product dengan TEPAT 1 variant asli → Format B (flat update_stock
    per-SKU), sesuai keputusan desain.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(800)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    variant_single = _mk_variant("227674818", "prodS", "varS1", "SKU-S1")
    products = {"prodS": _mk_product("227674818", "prodS")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_single])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(800, [variant_single])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]
    flat_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"]

    assert len(batch_calls) == 0
    assert len(flat_calls) == 1
    assert flat_calls[0]["variant_id"] == "varS1"
    assert flat_calls[0]["product_id"] == "prodS"
    assert flat_calls[0]["stock"] == 100

    assert result.total_push_calls_format_a == 0
    assert result.total_push_calls_format_b == 1


@pytest.mark.asyncio
async def test_product_without_real_variant_uses_format_b_with_zero_id():
    """
    Product TANPA variant asli (external_variant_id NULL → orphan) → Format B
    dengan skus[].id = "0" (sentinel no-variant, BUKAN product_id);
    product_id tetap dibawa di path URL /update-stock/{product_id}.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(900)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    variant_pid = _mk_variant("227674818", "prodZ", None, "SKU-Z1")
    products = {"prodZ": _mk_product("227674818", "prodZ")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_pid])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(900, [variant_pid])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]
    flat_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"]

    assert len(batch_calls) == 0
    assert len(flat_calls) == 1
    # Id push = "0" (sentinel no-variant); product_id tetap utk path URL.
    assert flat_calls[0]["variant_id"] == "0"
    assert flat_calls[0]["product_id"] == "prodZ"
    assert result.total_push_calls_format_b == 1


@pytest.mark.asyncio
async def test_mixed_orphan_and_real_variants_split_format_a_and_b():
    """
    Product dengan 2 variant asli + 1 variant orphan (external_variant_id kosong):
      - 2 variant asli → 1 batch Format A
      - variant orphan → 1 flat Format B dengan id "0" (sentinel no-variant)
    Semuanya tetap dalam SATU kelompok part.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(950)] * 3)
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    v1 = _mk_variant("227674818", "prodMix", "varMix1", "SKU-MIX1")
    v2 = _mk_variant("227674818", "prodMix", "varMix2", "SKU-MIX2")
    vorphan = _mk_variant("227674818", "prodMix", None, "SKU-MIX-ORPHAN")
    products = {"prodMix": _mk_product("227674818", "prodMix")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v1, v2, vorphan])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(950, [v1, v2, vorphan])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]
    flat_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"]

    assert len(batch_calls) == 1
    assert {s["id"] for s in batch_calls[0]["skus"]} == {"varMix1", "varMix2"}
    assert len(flat_calls) == 1
    # Orphan (no-variant) di-push dengan id sentinel "0", bukan product_id.
    assert flat_calls[0]["variant_id"] == "0"
    assert flat_calls[0]["product_id"] == "prodMix"

    assert result.total_pushed == 3
    assert result.total_push_calls_format_a == 1
    assert result.total_push_calls_format_b == 1


# ======================================================================
# Multi-shop / multi-marketplace dalam satu kelompok part
# ======================================================================


@pytest.mark.asyncio
async def test_same_part_across_marketplaces_pushes_batch_per_marketplace():
    """
    Part 777 dipakai product Shopee (2 variant) + product TikTok (2 variant):
      - 1 batch call marketplace=shopee shop_uuid=227674818
      - 1 batch call marketplace=tiktok shop_uuid=TOKO_TT_B
    (batch TIDAK bisa lintas marketplace — 1 call per marketplace per part)
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(777)] * 4)
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    v_sp1 = _mk_variant("227674818", "prodM1", "varM1a", "SKU-M1a")
    v_sp2 = _mk_variant("227674818", "prodM1", "varM1b", "SKU-M1b")
    v_tt1 = _mk_variant("TOKO_TT_B", "prodM2", "varM2a", "SKU-M2a")
    v_tt2 = _mk_variant("TOKO_TT_B", "prodM2", "varM2b", "SKU-M2b")
    v_shopee = v_sp1
    v_tiktok = v_tt1
    products = {
        "prodM1": _mk_product("227674818", "prodM1"),
        "prodM2": _mk_product("TOKO_TT_B", "prodM2"),
    }

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v_sp1, v_sp2, v_tt1, v_tt2])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(777, [v_sp1, v_sp2, v_tt1, v_tt2])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    batch_calls = [c for c in mock_client.call_log["update_stock"] if c.get("format") == "A"]

    assert len(batch_calls) == 2
    by_marketplace = {c["marketplace"]: c for c in batch_calls}
    assert set(by_marketplace) == {"shopee", "tiktok"}
    assert by_marketplace["shopee"]["shop_uuid"] == "227674818"
    assert [s["id"] for s in by_marketplace["shopee"]["skus"]] == ["varM1a", "varM1b"]
    assert by_marketplace["tiktok"]["shop_uuid"] == "TOKO_TT_B"
    assert [s["id"] for s in by_marketplace["tiktok"]["skus"]] == ["varM2a", "varM2b"]

    assert result.total_pushed == 4
    assert result.total_push_calls_format_a == 2


# ======================================================================
# Failure isolation dalam satu part
# ======================================================================


@pytest.mark.asyncio
async def test_format_a_batch_error_records_failure_and_continues():
    """
    update_stock gagal → kegagalan dicatat di result.failures, queue ditandai
    FAILED, dan part BERIKUTNYA tetap di-proses (failure isolation per part).
    """
    mock_client = MockApisixClient(update_stock_raise=RuntimeError("gateway down"))
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect([_mk_mapping(10), _mk_mapping(20)])
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    v1a = _mk_variant("227674818", "prodP1", "varP1a", "SKU-P1a")
    v1b = _mk_variant("227674818", "prodP1", "varP1b", "SKU-P1b")
    v2 = _mk_variant("227674818", "prodP2", "varP2", "SKU-P2")
    products = {
        "prodP1": _mk_product("227674818", "prodP1"),
        "prodP2": _mk_product("227674818", "prodP2"),
    }

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v1a, v1b, v2])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(10, [v1a, v1b]), (20, [v2])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    # Mock client raise utk SEMUA call → part 10 gagal (2 variant),
    # part 20 (Format B) juga gagal, tapi TETAP diproses (isolation per part).
    assert len(result.failures) == 3
    assert result.total_pushed == 0
    # Part 20 tetap mendapat attempt push (2 batch call total: part 10 A, part 20 B)
    assert len(mock_client.call_log["update_stock"]) == 2
    assert result.total_report_rows == 3  # report tetap ditulis untuk semua


def _mk_http_status_error(body: str) -> httpx.HTTPStatusError:
    """httpx.HTTPStatusError realistis (HTTP 500 + JSON body) untuk mock."""
    response = httpx.Response(
        status_code=500,
        content=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        request=httpx.Request(
            "POST",
            "https://apisix.test/update-stock/prodP1"
            "?marketplace=shopee&shop_uuid=227674818",
        ),
    )
    return httpx.HTTPStatusError(
        f"Server error '500 Internal Server Error' for url '{response.request.url}'",
        request=response.request,
        response=response,
    )


@pytest.mark.asyncio
async def test_http_push_failure_stores_full_response_body_in_error_message():
    """
    Push gagal dengan HTTP 500 dari APISIX → error_message pada
    stock_sync_queue (FAILED) & stock_sync_log (FAILED) memuat FULL response
    body APISIX (bukan hanya summary line exception).
    """
    apisix_body = (
        '{"error":{"code":"UPSTREAM_ERROR",'
        '"message":"shopee.model.not_found","request_id":"req-abc-123"}}'
    )
    mock_client = MockApisixClient(
        update_stock_raise=_mk_http_status_error(apisix_body)
    )
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(
        side_effect=_mapping_side_effect(
            [_mk_mapping(10), _mk_mapping(10), _mk_mapping(20)]
        )
    )
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    # Part 10: product 2 variant asli → Format A batch.
    v1a = _mk_variant("227674818", "prodP1", "varP1a", "SKU-P1a")
    v1b = _mk_variant("227674818", "prodP1", "varP1b", "SKU-P1b")
    # Part 20: product tanpa variant asli (orphan) → Format B flat.
    v2 = _mk_variant("227674818", "prodP2", None, "SKU-P2")
    products = {
        "prodP1": _mk_product("227674818", "prodP1"),
        "prodP2": _mk_product("227674818", "prodP2"),
    }

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v1a, v1b, v2])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(10, [v1a, v1b]), (20, [v2])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    # Kedua part gagal dengan HTTP 500.
    assert result.total_pushed == 0
    assert len(result.failures) == 3
    for failure in result.failures:
        assert "HTTPStatusError" in failure
        assert "shopee.model.not_found" in failure
        assert "req-abc-123" in failure
        assert "UPSTREAM_ERROR" in failure

    added = [c.args[0] for c in mock_session.add.call_args_list]
    queue_entries = [
        m for m in added
        if isinstance(m, StockSyncQueueModel) and m.status == "FAILED"
    ]
    log_entries = [
        m for m in added
        if isinstance(m, StockSyncLogModel) and m.status == "FAILED"
    ]
    assert len(queue_entries) == 3, "semua queue entry harus FAILED"
    assert len(log_entries) == 3, "semua log entry harus FAILED"

    for entry in (*queue_entries, *log_entries):
        msg = entry.error_message or ""
        # Summary line exception tetap ada…
        assert "HTTPStatusError" in msg
        assert "Server error '500" in msg
        # …dan FULL response body APISIX tersimpan di error_message:
        assert "HTTP 500 response body:" in msg
        assert "UPSTREAM_ERROR" in msg
        assert "shopee.model.not_found" in msg
        assert "req-abc-123" in msg


# ======================================================================
# PRODUCT FREEZE (variant DEACTIVATED): report YES, queue/push/log NO
# ======================================================================


@pytest.mark.asyncio
async def test_frozen_product_variant_report_only_no_queue_push_log():
    """
    REGRESI TikTok FREEZE: product sudah pernah tersimpan di DB, lalu di GET
    products berikutnya TIDAK muncul lagi (status marketplace FREEZE) →
    product di-DB FREEZE + variant-nya DEACTIVATED (mapping dipertahankan).

    Di inventory cycle:
      - inventory_report TETAP ditulis (ikut perhitungan stok),
      - stock_sync_queue TIDAK dibuat,
      - push ke APISIX TIDAK dipanggil,
      - stock_sync_log TIDAK dibuat.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(555)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    # Product FREEZE + variant DEACTIVATED (hasil SyncEngine._freeze_stale_products).
    frozen_product = _mk_product("227674818", "prodFrozen", status=RecordStatus.FREEZE)
    variant_frozen = _mk_variant("227674818", "prodFrozen", "varFrozen", "SKU-FRZ")
    variant_frozen.status = RecordStatus.DEACTIVATED

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_frozen])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[frozen_product])

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    # Report tetap ditulis untuk variant FREEZE...
    assert result.total_report_rows == 1
    assert result.total_skipped_freeze == 1
    # ...tapi TIDAK masuk queue / push / log.
    assert result.total_queued == 0
    assert result.total_pushed == 0
    assert result.total_logged == 0
    assert len(mock_client.call_log["update_stock"]) == 0
    assert len(result.failures) == 0

    added = [c.args[0] for c in mock_session.add.call_args_list]
    assert not any(isinstance(m, StockSyncQueueModel) for m in added), (
        "variant FREEZE tidak boleh masuk stock_sync_queue"
    )
    assert not any(isinstance(m, StockSyncLogModel) for m in added), (
        "variant FREEZE tidak boleh masuk stock_sync_log"
    )


@pytest.mark.asyncio
async def test_frozen_product_status_alone_gates_queue_push_log():
    """
    Variant masih ACTIVE tapi product-nya FREEZE → tetap report-only
    (gate kedua berdasar status product, defensif untuk variant yang
    belum ikut ter-deactivate).
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(556)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    frozen_product = _mk_product("227674818", "prodFrozen2", status=RecordStatus.FREEZE)
    variant_active = _mk_variant("227674818", "prodFrozen2", "varFRZ2", "SKU-FRZ2")

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_active])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[frozen_product])

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    assert result.total_report_rows == 1
    assert result.total_skipped_freeze == 1
    assert result.total_queued == 0
    assert result.total_pushed == 0
    assert result.total_logged == 0
    assert len(mock_client.call_log["update_stock"]) == 0


@pytest.mark.asyncio
async def test_active_variants_still_pushed_alongside_frozen_product():
    """
    Product ACTIVE + variant ACTIVE tetap di-push normal — gate FREEZE
    hanya mengenai product FREEZE / variant DEACTIVATED.
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_mapping_side_effect([_mk_mapping(557)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    active_product = _mk_product("227674818", "prodActive", status=RecordStatus.ACTIVE)
    variant_active = _mk_variant("227674818", "prodActive", "varACT", "SKU-ACT")

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[variant_active])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=[active_product])

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    assert result.total_skipped_freeze == 0
    assert result.total_report_rows == 1
    assert result.total_queued == 1
    assert result.total_pushed == 1
    assert result.total_logged == 1
    assert len(mock_client.call_log["update_stock"]) == 1


# ======================================================================
# Routing marketplace dari DB (bug: shop TikTok ber-ID numerik murni
# terdeteksi sebagai 'shopee' oleh heuristic → HTTP 500 dari APISIX)
# ======================================================================


def _db_side_effect(mappings: List[MagicMock]):
    """session.execute() side-effect yang MENGHITUNG urutan query ala
    _push_part: mapping-lookup per variant (scalars().first()), lalu per shop
    (warehouse lookup → .first(), marketplace lookup → .first()).

    Marketplace lookup DIBEDAKAN lewat stmt: query JOIN marketplace mengandung
    'marketplace_shop' + 'marketplace.code'. Warehouse lookup hanya
    'marketplace_shop'. Mapping lookup mengandung 'sku_mapping'."""
    queue = list(mappings)

    async def _execute(*args, **kwargs):
        stmt_str = str(args[0]) if args else ""
        lowered = stmt_str.lower()
        res = MagicMock()
        res.scalar.return_value = 0
        res.all.return_value = []

        if "sku_mapping" in lowered and "group by" not in lowered:
            # Mapping lookup → berikan mapping berikutnya.
            res.scalars.return_value.first.side_effect = (
                lambda: queue.pop(0) if queue else None
            )
        elif "marketplace_shop" in lowered and "join" in lowered:
            # Marketplace-code lookup (JOIN marketplace_shop → marketplace)
            res.first.return_value = ("tiktok",)
        elif "marketplace_shop" in lowered:
            # Warehouse lookup (select default_warehouse_id)
            res.first.return_value = ("WH-TT",)
        else:
            res.scalars.return_value.first.side_effect = lambda: None
            res.first.return_value = None
        return res

    return _execute


@pytest.mark.asyncio
async def test_numeric_tiktok_shop_id_resolved_as_tiktok_from_db():
    """
    REGRESI: shop TikTok dengan external_shop_id numerik murni
    ('7494709429666874412') tidak boleh di-route sebagai 'shopee' oleh
    heuristic string — marketplace code WAJIB dibaca dari DB
    (marketplace_shop → marketplace.code = 'tiktok').
    """
    mock_client = MockApisixClient()
    pipeline = StockSyncPipelineEngine(apisix_client=mock_client)
    result = PipelineCycleResult(cycle_index=1, started_at=datetime.now(timezone.utc))

    mock_session = MagicMock()
    mock_session.execute = AsyncMock(side_effect=_db_side_effect([_mk_mapping(700)]))
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    mock_session.flush = AsyncMock()
    mock_session.refresh = AsyncMock()

    # Shop TikTok ber-ID numerik murni → heuristic akan salah bilang 'shopee'
    numeric_tiktok_shop = "7494709429666874412"
    v = _mk_variant(numeric_tiktok_shop, "prodTT", "varTT", "SKU-TT")
    products = {"prodTT": _mk_product(numeric_tiktok_shop, "prodTT")}

    repo_variants = MagicMock()
    repo_variants.list_all = AsyncMock(return_value=[v])
    repo_products = MagicMock()
    repo_products.list_all = AsyncMock(return_value=list(products.values()))

    with patch(
        "application.usecases.stock_sync_pipeline.MarketplaceVariantRepositoryImpl",
        return_value=repo_variants,
    ), patch(
        "application.usecases.stock_sync_pipeline.MarketplaceProductRepositoryImpl",
        return_value=repo_products,
    ), patch(
        "application.usecases.stock_sync_pipeline.async_session_factory",
        _session_factory(mock_session),
    ), patch.object(
        pipeline,
        "_resolve_push_order",
        new=AsyncMock(return_value=[(700, [v])]),
    ):
        await pipeline._run_inventory_cycle(
            cycle_index=1,
            result=result,
            status_filter="ALL",
            inventree=MockInvenTreeClient(),
        )

    flat_calls = [
        c for c in mock_client.call_log["update_stock"] if c.get("format") != "A"
    ]
    assert len(flat_calls) == 1
    # Ini inti regresinya: marketplace=tiktok (dari DB), BUKAN 'shopee'.
    assert flat_calls[0]["marketplace"] == "tiktok"
    assert flat_calls[0]["shop_uuid"] == numeric_tiktok_shop
    # warehouse per-variant (location_id 'WH-001') menang di atas per-shop —
    # yang diregresi di sini hanya ROUTING marketplace-nya.
    assert flat_calls[0]["warehouse_id"] == "WH-001"
    assert result.total_pushed == 1
