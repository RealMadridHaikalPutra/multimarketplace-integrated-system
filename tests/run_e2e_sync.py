#!/usr/bin/env python
"""
run_e2e_sync.py — E2E Sync Test Engine

Mesin test untuk menjalankan pipeline stock-sync secara end-to-end dari awal.
Semua APISIX response yang dikirim/diterima di-log ke file JSON untuk audit.

Flow:
  1. TRUNCATE semua table data (kecuali marketplace, marketplace_shop)
  2. RUN pipeline: SYNC -> MAPPING -> REPORT -> QUEUE -> UPDATE-STOCK (via APISIX) -> LOG
  3. QUERY hasil dari database (queue, log, report rows)
  4. OUTPUT comprehensive report + APISIX response logs

USAGE:
    python tests/run_e2e_sync.py shopee 227674818
    python tests/run_e2e_sync.py tiktok 7494709429666874412
    python tests/run_e2e_sync.py --help
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Pastikan project root ada di sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from infrastructure.db.database import async_session_factory
from application.usecases.stock_sync_pipeline import (
    PipelineCycleResult,
    StockSyncPipelineEngine,
)

# ---------------------------------------------------------------------------
# Unicode-safe markers (Windows cp1252 can't print emoji)
# ---------------------------------------------------------------------------

_ENC = getattr(sys.stdout, "encoding", "") or ""
if _ENC.upper() in ("UTF-8", "UTF8", "UTF-16"):
    _OK, _FAIL, _WARN = "[OK]", "[FAIL]", "[!]"
else:
    _OK, _FAIL, _WARN = "[OK]", "[FAIL]", "[!]"

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

logger = logging.getLogger("e2e-sync")

# ---------------------------------------------------------------------------
# Tables to truncate (in FK-safe order, all except marketplace/shop)
# ---------------------------------------------------------------------------

TRUNCATE_TABLES_ORDERED = [
    "stock_sync_log",
    "stock_sync_queue",
    "sku_mapping",
    "inventory_report",
    "marketplace_variant",
    "marketplace_product",
]
PRESERVE_TABLES = ["marketplace", "marketplace_shop"]

# ---------------------------------------------------------------------------
# Step 1: Truncate
# ---------------------------------------------------------------------------


async def truncate_data_tables(session: AsyncSession) -> Dict[str, int]:
    logger.info("=" * 60)
    logger.info("  STEP 1: TRUNCATE DATA TABLES")
    logger.info("  Preserved tables: %s", ", ".join(PRESERVE_TABLES))
    logger.info("  Truncating: %s", ", ".join(TRUNCATE_TABLES_ORDERED))
    logger.info("=" * 60)

    result: Dict[str, int] = {}
    try:
        for table_name in TRUNCATE_TABLES_ORDERED:
            count = await session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            result[table_name] = count.scalar() or 0

        for table_name in TRUNCATE_TABLES_ORDERED:
            logger.info("  Truncating: %s (had %d rows)", table_name, result[table_name])
            await session.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))

        await session.commit()
        logger.info("  %s All data tables truncated successfully", _OK)
    except Exception as exc:
        await session.rollback()
        logger.error("  %s Truncate failed: %s", _FAIL, exc)
        raise
    return result

# ---------------------------------------------------------------------------
# Step 2: Verify seed
# ---------------------------------------------------------------------------


async def verify_seed_data(session: AsyncSession) -> bool:
    mp_count = (await session.execute(text("SELECT COUNT(*) FROM marketplace"))).scalar() or 0
    shop_count = (await session.execute(text("SELECT COUNT(*) FROM marketplace_shop"))).scalar() or 0
    logger.info("  Marketplace records: %d", mp_count)
    logger.info("  Marketplace Shop records: %d", shop_count)
    if mp_count == 0:
        logger.warning("  %s Marketplace table is EMPTY - seed data required!", _WARN)
        return False
    if shop_count == 0:
        logger.warning("  %s Marketplace_shop table is EMPTY!", _WARN)
        return False
    return True

# ---------------------------------------------------------------------------
# Step 3: Run pipeline
# ---------------------------------------------------------------------------


async def run_pipeline(
    marketplace_code: str, shop_external_id: str,
    cycle_index: int = 1, status_filter: str = "ALL",
) -> PipelineCycleResult:
    logger.info("=" * 60)
    logger.info("  STEP 3: RUN PIPELINE (marketplace=%s, shop=%s, cycle=%d, filter=%s)",
                marketplace_code, shop_external_id, cycle_index, status_filter)
    logger.info("=" * 60)

    pipeline = StockSyncPipelineEngine()
    start_time = datetime.now(timezone.utc)
    result = await pipeline.run_once(
        marketplace_code=marketplace_code, external_shop_id=shop_external_id,
        cycle_index=cycle_index, status_filter=status_filter,
    )
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info("  Pipeline selesai dalam %.2f detik", elapsed)

    summary = result.to_dict()
    logger.info("  Cycle index         : %d", summary.get("cycle_index", 0))
    logger.info("  Sync result         : %s", _safe(summary.get("sync_result")))
    logger.info("  Mapping result      : %s", _safe(summary.get("mapping_result")))
    logger.info("  Report rows         : %d", summary.get("total_report_rows", 0))
    logger.info("  Queued              : %d", summary.get("total_queued", 0))
    logger.info("  Pushed (APISIX)     : %d", summary.get("total_pushed", 0))
    logger.info("  Logged              : %d", summary.get("total_logged", 0))
    logger.info("  Skipped pending     : %d", summary.get("total_skipped_pending", 0))
    logger.info("  Skipped no-part     : %d", summary.get("total_skipped_no_part", 0))
    logger.info("  Total activated     : %d", summary.get("total_activated", 0))
    if result.failures:
        logger.warning("  Failures (%d):", len(result.failures))
        for f in result.failures:
            logger.warning("    - %s", f)
    return result

# ---------------------------------------------------------------------------
# Step 4: Query results
# ---------------------------------------------------------------------------


async def query_results(session: AsyncSession) -> Dict[str, Any]:
    logger.info("=" * 60)
    logger.info("  STEP 4: QUERY RESULTS")
    logger.info("=" * 60)
    results: Dict[str, Any] = {}

    r = await session.execute(text("SELECT COUNT(*) FROM stock_sync_queue"))
    results["queue_total"] = r.scalar() or 0
    r = await session.execute(text("SELECT status, COUNT(*) as cnt FROM stock_sync_queue GROUP BY status ORDER BY status"))
    results["queue_by_status"] = {row.status: row.cnt for row in r.fetchall()}

    r = await session.execute(text("SELECT COUNT(*) FROM stock_sync_log"))
    results["log_total"] = r.scalar() or 0
    r = await session.execute(text("SELECT status, COUNT(*) as cnt FROM stock_sync_log GROUP BY status ORDER BY status"))
    results["log_by_status"] = {row.status: row.cnt for row in r.fetchall()}

    for tbl in ("inventory_report", "marketplace_product", "marketplace_variant", "sku_mapping"):
        r = await session.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
        results[f"{tbl.split('_')[-1]}_total" if '_' in tbl else f"{tbl}_total"] = r.scalar() or 0

    # Use cleaner key names
    results["report_total"] = results.pop("report_total", 0)
    results["product_total"] = results.pop("product_total", 0)
    results["variant_total"] = results.pop("variant_total", 0)
    results["mapping_total"] = results.pop("mapping_total", 0)

    r = await session.execute(text("""
        SELECT id, marketplace_variant_id, inventree_part_id, source,
               previous_stock, target_stock, final_stock, status, error_message, created_at
        FROM stock_sync_log ORDER BY created_at DESC LIMIT 50
    """))
    results["recent_logs"] = [
        {
            "id": str(row.id),
            "marketplace_variant_id": row.marketplace_variant_id,
            "inventree_part_id": row.inventree_part_id,
            "source": row.source, "previous_stock": row.previous_stock,
            "target_stock": row.target_stock, "final_stock": row.final_stock,
            "status": row.status, "error_message": row.error_message,
            "created_at": str(row.created_at) if row.created_at else None,
        }
        for row in r.fetchall()
    ]

    r = await session.execute(text("""
        SELECT id, marketplace_variant_id, target_stock, trigger_source,
               status, retry_count, error_message, created_at, processed_at
        FROM stock_sync_queue ORDER BY created_at DESC LIMIT 50
    """))
    results["recent_queues"] = [
        {
            "id": str(row.id), "marketplace_variant_id": row.marketplace_variant_id,
            "target_stock": row.target_stock, "trigger_source": row.trigger_source,
            "status": row.status, "retry_count": row.retry_count,
            "error_message": row.error_message,
            "created_at": str(row.created_at) if row.created_at else None,
            "processed_at": str(row.processed_at) if row.processed_at else None,
        }
        for row in r.fetchall()
    ]
    return results

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe(obj: Any) -> str:
    if obj is None:
        return "N/A"
    if isinstance(obj, dict):
        return json.dumps(obj, default=str)[:120]
    return str(obj)[:120]


def build_report(truncate_result: Dict[str, int], seed_ok: bool,
                 pipeline_result: PipelineCycleResult, db_results: Dict[str, Any],
                 elapsed_seconds: float) -> Dict[str, Any]:
    pd = pipeline_result.to_dict()
    return {
        "test_info": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed_seconds, 2),
            "apisix_base_url": settings.apisix_base_url,
        },
        "step_1_truncate": {
            "tables_truncated": list(truncate_result.keys()),
            "rows_before_truncate": truncate_result,
            "preserved_tables": PRESERVE_TABLES,
        },
        "step_2_seed_check": {"seed_data_ok": seed_ok},
        "step_3_pipeline": {
            "cycle_index": pd.get("cycle_index", 0),
            "started_at": pd.get("started_at"),
            "finished_at": pd.get("finished_at"),
            "sync_result": pd.get("sync_result"),
            "mapping_result": pd.get("mapping_result"),
            "total_report_rows": pd.get("total_report_rows", 0),
            "total_queued": pd.get("total_queued", 0),
            "total_pushed": pd.get("total_pushed", 0),
            "total_logged": pd.get("total_logged", 0),
            "total_skipped_pending": pd.get("total_skipped_pending", 0),
            "total_skipped_no_part": pd.get("total_skipped_no_part", 0),
            "total_activated": pd.get("total_activated", 0),
            "activation_logs": pd.get("activation_logs", []),
            "failures": pd.get("failures", []),
        },
        "step_4_db_results": db_results,
    }


def print_report(report: Dict[str, Any]) -> None:
    info = report.get("test_info", {})
    print()
    print("=" * 70)
    print("  E2E SYNC TEST REPORT")
    print("=" * 70)
    print(f"  Timestamp       : {info.get('timestamp', 'N/A')}")
    print(f"  Elapsed         : {info.get('elapsed_seconds', 0)}s")
    print(f"  APISIX Base URL : {info.get('apisix_base_url', 'N/A')}")
    print()

    print("-" * 70)
    print("  STEP 1: TRUNCATE DATA TABLES")
    print("-" * 70)
    for tbl, cnt in report.get("step_1_truncate", {}).get("rows_before_truncate", {}).items():
        print(f"    {_OK if cnt >= 0 else _FAIL} {tbl}: truncated (had {cnt} rows)")
    print()

    print("-" * 70)
    print("  STEP 2: SEED DATA CHECK")
    print("-" * 70)
    if report.get("step_2_seed_check", {}).get("seed_data_ok", False):
        print(f"    {_OK} Seed data verified (marketplace + marketplace_shop)")
    else:
        print(f"    {_WARN} Seed data MISSING - run seeding first!")
    print()

    print("-" * 70)
    print("  STEP 3: PIPELINE RESULTS")
    print("-" * 70)
    p = report.get("step_3_pipeline", {})
    sr = p.get("sync_result") or {}
    mr = p.get("mapping_result") or {}
    print(f"    Cycle index     : {p.get('cycle_index', 0)}")
    print(f"    Products synced : {sr.get('total_products', 'N/A')}")
    print(f"    Variants synced : {sr.get('total_variants', 'N/A')}")
    print(f"    Mapped          : {mr.get('total_mapped', 'N/A')}")
    print(f"    Report rows     : {p.get('total_report_rows', 0)}")
    print(f"    Queued          : {p.get('total_queued', 0)}")
    print(f"    Pushed (APISIX) : {p.get('total_pushed', 0)}")
    print(f"    Logged          : {p.get('total_logged', 0)}")
    print(f"    Skipped pending : {p.get('total_skipped_pending', 0)}")
    print(f"    Skipped no-part : {p.get('total_skipped_no_part', 0)}")
    print(f"    Auto-activated  : {p.get('total_activated', 0)}")

    if p.get("activation_logs"):
        print(f"\n    Activation Logs ({len(p['activation_logs'])} entries):")
        for i, e in enumerate(p["activation_logs"], 1):
            print(f"      [{i}] product={e.get('product_id')}, sku={e.get('seller_sku')}, "
                  f"status={e.get('apisix_response', {}).get('status', '?')}")

    if p.get("failures"):
        print(f"\n    Failures ({len(p['failures'])}):")
        for f in p["failures"]:
            print(f"      {_FAIL} {f}")
    else:
        print(f"    {_OK} No failures")

    print()
    print("-" * 70)
    print("  STEP 4: DATABASE RESULTS")
    print("-" * 70)
    db = report.get("step_4_db_results", {})
    print(f"    Products        : {db.get('product_total', 0)}")
    print(f"    Variants        : {db.get('variant_total', 0)}")
    print(f"    SKU Mappings    : {db.get('mapping_total', 0)}")
    print(f"    Report rows     : {db.get('report_total', 0)}")
    print(f"    Queue total     : {db.get('queue_total', 0)}")
    for st, cnt in db.get("queue_by_status", {}).items():
        print(f"      - {st}: {cnt}")
    print(f"    Log total       : {db.get('log_total', 0)}")
    for st, cnt in db.get("log_by_status", {}).items():
        print(f"      - {st}: {cnt}")

    for log_entry in db.get("recent_logs", []):
        m = _OK if log_entry.get("status") == "SUCCESS" else _FAIL
        print(f"      {m} variant={log_entry.get('marketplace_variant_id', '?')[:30]}... "
              f"prev={log_entry.get('previous_stock')}->target={log_entry.get('target_stock')}->"
              f"final={log_entry.get('final_stock')} status={log_entry.get('status')} "
              f"err={log_entry.get('error_message', '') or 'ok'}")

    print()
    print("=" * 70)
    if p.get("failures"):
        print(f"  {_FAIL} E2E TEST COMPLETED WITH FAILURES")
    else:
        print(f"  {_OK} E2E TEST COMPLETED SUCCESSFULLY")
    print(f"     Total time: {info.get('elapsed_seconds', 0)}s")
    print("=" * 70)
    print()


def save_report(report: Dict[str, Any], output_dir: str = "./e2e-reports",
                marketplace_code: str = "unknown", shop_id: str = "unknown") -> str:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_path / f"e2e-sync_{marketplace_code}_{shop_id}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)
    logger.info("  Report saved to: %s", path.resolve())
    return str(path.resolve())

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run_e2e_sync(marketplace_code: str, shop_external_id: str,
                        cycle_index: int = 1, status_filter: str = "ALL",
                        output_dir: str = "./e2e-reports", skip_truncate: bool = False) -> Dict[str, Any]:
    overall_start = datetime.now(timezone.utc)
    logger.info("")
    logger.info("#" * 70)
    logger.info("#  E2E SYNC TEST ENGINE")
    logger.info("#" * 70)
    logger.info("#  Marketplace : %s", marketplace_code)
    logger.info("#  Shop ID     : %s", shop_external_id)
    logger.info("#  Status      : %s", status_filter)
    logger.info("#  Cycle       : %d", cycle_index)
    logger.info("#  Skip trunc  : %s", skip_truncate)
    logger.info("#" * 70)
    logger.info("")

    # Step 1
    truncate_result = {}
    if not skip_truncate:
        async with async_session_factory() as session:
            truncate_result = await truncate_data_tables(session)
    else:
        async with async_session_factory() as session:
            for tn in TRUNCATE_TABLES_ORDERED:
                c = await session.execute(text(f"SELECT COUNT(*) FROM {tn}"))
                truncate_result[tn] = c.scalar() or 0

    # Step 2
    async with async_session_factory() as session:
        seed_ok = await verify_seed_data(session)
    if not seed_ok:
        logger.warning("  %s Seed data missing. Pipeline mungkin gagal.", _WARN)

    # Step 3
    pipeline_result = await run_pipeline(marketplace_code, shop_external_id, cycle_index, status_filter)

    # Step 4
    async with async_session_factory() as session:
        db_results = await query_results(session)

    # Build & save
    elapsed = (datetime.now(timezone.utc) - overall_start).total_seconds()
    report = build_report(truncate_result, seed_ok, pipeline_result, db_results, elapsed)
    print_report(report)
    saved = save_report(report, output_dir, marketplace_code, shop_external_id)
    report["_saved_to"] = saved
    return report

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="E2E Sync Test Engine - jalankan pipeline stock-sync end-to-end",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh:
  python tests/run_e2e_sync.py shopee 227674818
  python tests/run_e2e_sync.py tiktok 7494709429666874412 --skip-truncate
  python tests/run_e2e_sync.py shopee 227674818 --status-filter ACTIVE
  python tests/run_e2e_sync.py shopee 227674818 --verbose
        """)
    p.add_argument("marketplace_code", help="Marketplace code (e.g. 'shopee', 'tiktok')")
    p.add_argument("shop_external_id", help="External shop ID (e.g. '227674818')")
    p.add_argument("--cycle-index", type=int, default=1, help="Cycle index (default: 1)")
    p.add_argument("--status-filter", default="ALL", choices=["ALL", "ACTIVE", "INACTIVE", "DELETED"],
                   help="Product status filter (default: ALL)")
    p.add_argument("--output-dir", default="./e2e-reports", help="Report output dir")
    p.add_argument("--skip-truncate", action="store_true", help="Skip truncation")
    p.add_argument("--verbose", action="store_true", help="DEBUG logging")
    return p.parse_args(argv)


def setup_logging(verbose: bool = False) -> None:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)
    result = asyncio.run(run_e2e_sync(
        marketplace_code=args.marketplace_code,
        shop_external_id=args.shop_external_id,
        cycle_index=args.cycle_index,
        status_filter=args.status_filter,
        output_dir=args.output_dir,
        skip_truncate=args.skip_truncate,
    ))
    if result.get("step_3_pipeline", {}).get("failures"):
        sys.exit(1)


if __name__ == "__main__":
    main()
