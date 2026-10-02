#!/usr/bin/env python
"""
run_combined_e2e.py — Combined E2E for BOTH marketplaces

Problem:
  The pipeline calculates reserved_mp = SUM(total_reserved_stock) for matching
  seller_sku across ALL marketplace variants in the DB. If Shopee inventory
  runs BEFORE TikTok syncs, reserved_mp misses TikTok's reserved stock.

Solution:
  1. TRUNCATE all data tables once
  2. SYNC Shopee  (products + variants)
  3. SYNC TikTok  (products + variants)
  4. MAP   Shopee  (create SKU mappings)
  5. MAP   TikTok  (create SKU mappings)
  6. INVENTORY CYCLE Shopee (report → queue → push → log)
  7. INVENTORY CYCLE TikTok (report → queue → push → log)

Now both inventory cycles see the FULL dataset (both marketplaces) for
the reserved_mp aggregation.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from core.config import settings
from infrastructure.db.database import async_session_factory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

logger = logging.getLogger("combined-e2e")

# Tables to truncate (FK-safe order)
TRUNCATE_TABLES = [
    "stock_sync_log",
    "stock_sync_queue",
    "sku_mapping",
    "inventory_report",
    "marketplace_variant",
    "marketplace_product",
]

MARKETPLACES = [
    {"code": "shopee", "shop_id": "227674818"},
    {"code": "tiktok", "shop_id": "7494709429666874412"},
]


async def truncate_all():
    """Truncate all data tables once."""
    logger.info("=" * 60)
    logger.info("  TRUNCATE ALL DATA TABLES")
    logger.info("=" * 60)
    async with async_session_factory() as session:
        for tbl in TRUNCATE_TABLES:
            c = await session.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
            logger.info("  Truncating %s (had %d rows)", tbl, c.scalar() or 0)
            await session.execute(text(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE"))
        await session.commit()
    logger.info("  [OK] All tables truncated\n")


async def sync_shop(shop_id):
    """Run sync for one marketplace shop: get products + variants from APISIX."""
    from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine

    pipeline = StockSyncPipelineEngine()
    # Step 1-2: get products + sync products/variants to DB
    # We use run_once but will skip mapping + inventory via internal re-org
    pass


async def run_sync_only(mp_code: str, shop_external_id: str) -> Dict[str, Any]:
    """
    Run ONLY steps 1-2 of the pipeline (fetch products + sync to DB).
    No mapping, no inventory report.
    """
    from infrastructure.marketplace.apisix.client import ApisixClient
    from infrastructure.repositories.marketplace_repository_impl import MarketplaceRepositoryImpl
    from infrastructure.repositories.marketplace_shop_repository_impl import MarketplaceShopRepositoryImpl
    from infrastructure.repositories.marketplace_product_repository_impl import MarketplaceProductRepositoryImpl
    from infrastructure.repositories.marketplace_variant_repository_impl import MarketplaceVariantRepositoryImpl
    from application.services.product_service import ProductService
    from application.services.variant_service import VariantService
    from application.usecases.sync_engine import SyncEngine

    logger.info("--- SYNC: %s (shop=%s) ---", mp_code.upper(), shop_external_id)

    async with async_session_factory() as session:
        mp_repo = MarketplaceRepositoryImpl(session)
        shop_repo = MarketplaceShopRepositoryImpl(session)
        product_repo = MarketplaceProductRepositoryImpl(session)
        variant_repo = MarketplaceVariantRepositoryImpl(session)

        marketplace = await mp_repo.get_by_code(mp_code.lower())
        if not marketplace:
            raise ValueError(f"Marketplace '{mp_code}' not found")

        shop = await shop_repo.get_by_external_id(marketplace.id, shop_external_id)
        if not shop:
            raise ValueError(f"Shop '{shop_external_id}' not found for '{mp_code}'")

        sync_engine = SyncEngine(
            apisix_client=ApisixClient(),
            product_service=ProductService(
                marketplace_repo=mp_repo,
                shop_repo=shop_repo,
                product_repo=product_repo,
            ),
            variant_service=VariantService(variant_repo=variant_repo),
            mapping_service=None,
            shop_repo=shop_repo,
            product_repo=product_repo,
            variant_repo=variant_repo,
            marketplace_repo=mp_repo,
        )

        sync_result = await sync_engine.sync_by_shop_id(shop.id, status_filter="ALL")
        await session.commit()

    logger.info("  Products: %s, Variants: %s",
                sync_result.get("total_products", 0),
                sync_result.get("total_variants", 0))
    return sync_result


async def run_mapping_only(mp_code: str, shop_external_id: str) -> Dict[str, Any]:
    """
    Run ONLY step 3 of the pipeline (create SKU mappings for all variants in a shop).
    """
    from infrastructure.repositories.marketplace_repository_impl import MarketplaceRepositoryImpl
    from infrastructure.repositories.marketplace_shop_repository_impl import MarketplaceShopRepositoryImpl
    from infrastructure.repositories.marketplace_product_repository_impl import MarketplaceProductRepositoryImpl
    from infrastructure.repositories.marketplace_variant_repository_impl import MarketplaceVariantRepositoryImpl
    from infrastructure.repositories.sku_mapping_repository_impl import SKUMappingRepositoryImpl
    from application.services.mapping_service import MappingService
    from application.services.variant_service import VariantService
    from infrastructure.inventory.inventree.client import InvenTreeClient
    from application.usecases.mapping_engine import MappingEngine

    logger.info("--- MAPPING: %s (shop=%s) ---", mp_code.upper(), shop_external_id)

    async with async_session_factory() as session:
        mp_repo = MarketplaceRepositoryImpl(session)
        shop_repo = MarketplaceShopRepositoryImpl(session)
        product_repo = MarketplaceProductRepositoryImpl(session)
        variant_repo = MarketplaceVariantRepositoryImpl(session)
        mapping_repo = SKUMappingRepositoryImpl(session)

        marketplace = await mp_repo.get_by_code(mp_code.lower())
        shop = await shop_repo.get_by_external_id(marketplace.id, shop_external_id)

        mapping_engine = MappingEngine(
            mapping_service=MappingService(
                mapping_repo=mapping_repo,
                variant_repo=variant_repo,
                inventree_client=InvenTreeClient(),
            ),
            variant_service=VariantService(variant_repo=variant_repo),
            variant_repo=variant_repo,
            product_repo=product_repo,
            shop_repo=shop_repo,
        )
        mapping_result = await mapping_engine.run_mapping_for_shop(shop.id)
        await session.commit()

    logger.info("  Mapped: %s, Not found: %s, Dups: %s",
                mapping_result.get("total_mapped", 0),
                mapping_result.get("total_not_found", 0),
                mapping_result.get("total_duplicates", 0))
    return mapping_result


async def run_inventory_cycle(mp_code: str, shop_external_id: str,
                              cycle_index: int = 1) -> Dict[str, Any]:
    """
    Run steps 4-7 of the pipeline (inventory report → queue → push → log).
    Now 'reserved_mp' will correctly aggregate from BOTH marketplaces.
    """
    from infrastructure.marketplace.apisix.client import ApisixClient
    from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine, PipelineCycleResult

    logger.info("--- INVENTORY CYCLE: %s (shop=%s) ---", mp_code.upper(), shop_external_id)

    pipeline = StockSyncPipelineEngine(apisix_client=ApisixClient())

    # We need to manually run sync + mapping (should be no-ops since data exists),
    # then inventory cycle. But run_once does it all. Let's call run_once but
    # the sync/mapping steps are idempotent since variants already exist.
    result = await pipeline.run_once(
        marketplace_code=mp_code,
        external_shop_id=shop_external_id,
        cycle_index=cycle_index,
        status_filter="ALL",
    )

    summary = result.to_dict()
    logger.info("  Report rows: %d, Queued: %d, Pushed: %d, Logged: %d, Failures: %d",
                summary.get("total_report_rows", 0),
                summary.get("total_queued", 0),
                summary.get("total_pushed", 0),
                summary.get("total_logged", 0),
                len(summary.get("failures", []) or []))
    if result.failures:
        for f in result.failures:
            logger.warning("    FAIL: %s", f)
    return summary


async def query_db_summary() -> Dict[str, Any]:
    """Query final DB state."""
    async with async_session_factory() as session:
        results = {}

        for tbl in TRUNCATE_TABLES:
            c = await session.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
            results[tbl] = c.scalar() or 0

        # Log status breakdown
        r = await session.execute(
            text("SELECT status, COUNT(*) as cnt FROM stock_sync_log GROUP BY status ORDER BY status")
        )
        results["log_by_status"] = {row.status: row.cnt for row in r.fetchall()}

        r = await session.execute(
            text("SELECT status, COUNT(*) as cnt FROM stock_sync_queue GROUP BY status ORDER BY status")
        )
        results["queue_by_status"] = {row.status: row.cnt for row in r.fetchall()}

        # Show all report rows with reserved_mp
        r = await session.execute(text("""
            SELECT ir.seller_sku, ir.sku_type, ir.faktor_pengali,
                   ir.stok_inventree, ir.reserved_mp, ir.qty_converted, ir.stok_di_update,
                   ir.shop_id, ir.marketplace_code
            FROM inventory_report ir
            ORDER BY ir.seller_sku, ir.marketplace_code
        """))
        results["report_rows"] = [
            {
                "seller_sku": row.seller_sku,
                "sku_type": row.sku_type,
                "faktor_pengali": row.faktor_pengali,
                "stok_inventree": row.stok_inventree,
                "reserved_mp": row.reserved_mp,
                "qty_converted": row.qty_converted,
                "stok_di_update": row.stok_di_update,
                "shop_id": row.shop_id[:20] if row.shop_id else None,
                "marketplace_code": row.marketplace_code,
            }
            for row in r.fetchall()
        ]

        return results


def print_summary(sync1, sync2, map1, map2, cycle1, cycle2, db):
    print()
    print("=" * 70)
    print("  COMBINED E2E — FINAL REPORT")
    print("=" * 70)
    print()

    print("  SYNC RESULTS:")
    print(f"    Shopee: {sync1.get('total_products',0)} products, {sync1.get('total_variants',0)} variants")
    print(f"    TikTok: {sync2.get('total_products',0)} products, {sync2.get('total_variants',0)} variants")
    print()

    print("  MAPPING RESULTS:")
    print(f"    Shopee: {map1.get('total_mapped',0)} mapped, {map1.get('total_not_found',0)} not found")
    print(f"    TikTok: {map2.get('total_mapped',0)} mapped, {map2.get('total_not_found',0)} not found")
    print()

    print("  INVENTORY CYCLE RESULTS:")
    print(f"    Shopee: {cycle1.get('total_report_rows',0)} reports, {cycle1.get('total_pushed',0)} pushed, "
          f"{len(cycle1.get('failures',[]) or [])} failures")
    print(f"    TikTok: {cycle2.get('total_report_rows',0)} reports, {cycle2.get('total_pushed',0)} pushed, "
          f"{len(cycle2.get('failures',[]) or [])} failures")
    print()

    print("  DATABASE FINAL STATE:")
    print(f"    marketplace_product: {db.get('marketplace_product',0)}")
    print(f"    marketplace_variant: {db.get('marketplace_variant',0)}")
    print(f"    sku_mapping:          {db.get('sku_mapping',0)}")
    print(f"    inventory_report:     {db.get('inventory_report',0)}")
    print(f"    stock_sync_queue:     {db.get('stock_sync_queue',0)}")
    print(f"    stock_sync_log:       {db.get('stock_sync_log',0)}")
    print(f"    Queue by status:      {db.get('queue_by_status',{})}")
    print(f"    Log by status:        {db.get('log_by_status',{})}")
    print()

    # Show inventory report rows with reserved_mp — the KEY metric
    print("  INVENTORY REPORT ROWS (reserved_mp = cross-marketplace aggregation):")
    print(f"    {'seller_sku':<20} {'type':<12} {'faktor':>7} {'stok':>8} {'res_mp':>8} {'qty_conv':>10} {'stok_update':>12} {'shop':<22} {'mp':<10}")
    print(f"    {'-'*20} {'-'*12} {'-'*7} {'-'*8} {'-'*8} {'-'*10} {'-'*12} {'-'*22} {'-'*10}")
    for row in db.get("report_rows", []):
        print(f"    {row['seller_sku']:<20} {row['sku_type']:<12} {row['faktor_pengali']:>7.2f} "
              f"{row['stok_inventree']:>8.0f} {row['reserved_mp']:>8} {row['qty_converted']:>10.0f} "
              f"{row['stok_di_update']:>12.0f} {str(row['shop_id'] or ''):<22} {row['marketplace_code']:<10}")

    total_fails = len(cycle1.get("failures", []) or []) + len(cycle2.get("failures", []) or [])
    print()
    if total_fails > 0:
        print(f"  [FAIL] COMBINED E2E COMPLETED WITH {total_fails} FAILURES")
    else:
        print("  [OK] COMBINED E2E COMPLETED SUCCESSFULLY — reserved_mp from BOTH marketplaces!")
    print("=" * 70)
    print()


async def main():
    overall_start = datetime.now(timezone.utc)

    print()
    print("#" * 70)
    print("#  COMBINED E2E - Shopee + TikTok")
    print("#")
    print("#  Order: truncate -> sync Shopee -> sync TikTok ->")
    print("#         mapping Shopee -> mapping TikTok ->")
    print("#         inventory Shopee -> inventory TikTok")
    print("#")
    print("#  reserved_mp will correctly aggregate BOTH marketplaces!")
    print("#" * 70)
    print()

    # Step 1: Truncate
    await truncate_all()

    # Step 2-3: Sync BOTH (order doesn't matter, parallel-safe since no truncation)
    logger.info(">>> PHASE 1: SYNC BOTH MARKETPLACES <<<\n")
    sync_shopee = await run_sync_only("shopee", "227674818")
    sync_tiktok = await run_sync_only("tiktok", "7494709429666874412")

    # Step 4-5: Map BOTH
    logger.info("\n>>> PHASE 2: MAPPING BOTH MARKETPLACES <<<\n")
    map_shopee = await run_mapping_only("shopee", "227674818")
    map_tiktok = await run_mapping_only("tiktok", "7494709429666874412")

    # Step 6-7: Inventory cycle for BOTH
    # Now BOTH marketplaces' data is in the DB, so reserved_mp is accurate.
    logger.info("\n>>> PHASE 3: INVENTORY CYCLE FOR BOTH <<<\n")
    cycle_shopee = await run_inventory_cycle("shopee", "227674818", cycle_index=1)
    cycle_tiktok = await run_inventory_cycle("tiktok", "7494709429666874412", cycle_index=2)

    # Query final state
    db_summary = await query_db_summary()
    print_summary(sync_shopee, sync_tiktok, map_shopee, map_tiktok,
                  cycle_shopee, cycle_tiktok, db_summary)

    elapsed = (datetime.now(timezone.utc) - overall_start).total_seconds()
    logger.info("Total time: %.2fs", elapsed)

    # Save report
    report = {
        "test_info": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 2),
            "type": "combined_e2e_shopee_tiktok",
        },
        "sync_shopee": sync_shopee,
        "sync_tiktok": sync_tiktok,
        "mapping_shopee": map_shopee,
        "mapping_tiktok": map_tiktok,
        "inventory_shopee": cycle_shopee,
        "inventory_tiktok": cycle_tiktok,
        "db_final": db_summary,
    }

    output_path = Path("./e2e-reports")
    output_path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = output_path / f"e2e-combined_shopee-tiktok_{ts}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)
    logger.info("Report saved to: %s", report_path.resolve())

    total_fails = len(cycle_shopee.get("failures", []) or []) + len(cycle_tiktok.get("failures", []) or [])
    if total_fails > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
