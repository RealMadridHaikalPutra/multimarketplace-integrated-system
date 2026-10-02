#!/usr/bin/env python
"""
E2E webhook runner: fake webhook/order detail + real DB product/mapping data.

The runner never truncates product, variant, or mapping data. It selects ACTIVE
mappings from the database, creates synthetic order payloads, starts the normal
Camunda webhook process, and polls the configured external-task worker until
those webhook log rows reach SUCCESS or ON_HOLD.

Examples:
    python tests/run_e2e_webhook.py --dry-run --limit 5
    python tests/run_e2e_webhook.py --execute --part-id 123 --part-id 456
    python tests/run_e2e_webhook.py --execute --marketplace shopee --limit 10

Requirements for --execute:
  - .env points to the intended database and APISIX environment.
  - Camunda is reachable and the process rule in camunda/config.json is valid.
  - The backend is running at BACKEND_BASE_URL because the existing
    serviceWebhookTest external task calls /api/v2/stock-sync/run-part.
  - STOCK_SYNC_PUSH_ENABLED=true if real marketplace updates are intended.
"""

import argparse
import asyncio
from copy import deepcopy
import json
import logging
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select

from core.config import settings
from infrastructure.camunda.external_task import external_tasks_enabled, get_worker
from infrastructure.camunda.service import camunda_webhook_trigger
from infrastructure.db.database import async_session_factory, init_db
from infrastructure.db.models import (
    MarketplaceModel,
    MarketplaceProductModel,
    MarketplaceShopModel,
    MarketplaceVariantModel,
    InventoryReportModel,
    SKUMappingModel,
    StockSyncLogModel,
    WebhookLogModel,
)
from infrastructure.repositories.webhook_log_repository_impl import (
    WebhookLogRepositoryImpl,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logger = logging.getLogger("e2e-webhook")


class E2EError(RuntimeError):
    """Expected runner failure with an actionable message."""


async def select_real_scope(
    part_ids: Optional[Sequence[int]],
    marketplace: Optional[str],
    shop_id: Optional[str],
    limit: int,
) -> List[Any]:
    """Read real ACTIVE mapping/product/variant rows from the database."""
    stmt = (
        select(
            SKUMappingModel.inventree_part_id.label("part_id"),
            SKUMappingModel.marketplace_variant_id.label("mapping_variant_id"),
            MarketplaceVariantModel.external_variant_id,
            MarketplaceVariantModel.variant_name,
            MarketplaceVariantModel.seller_sku,
            MarketplaceVariantModel.marketplace_product_id,
            MarketplaceProductModel.external_product_id,
            MarketplaceProductModel.product_name,
            MarketplaceProductModel.shop_id,
            MarketplaceShopModel.shop_name,
            MarketplaceModel.code.label("marketplace_code"),
        )
        .join(
            MarketplaceVariantModel,
            MarketplaceVariantModel.marketplace_variant_id
            == SKUMappingModel.marketplace_variant_id,
        )
        .join(
            MarketplaceProductModel,
            MarketplaceProductModel.marketplace_product_id
            == MarketplaceVariantModel.marketplace_product_id,
        )
        .join(
            MarketplaceShopModel,
            MarketplaceShopModel.external_shop_id == MarketplaceProductModel.shop_id,
        )
        .join(
            MarketplaceModel,
            MarketplaceModel.id == MarketplaceShopModel.marketplace_id,
        )
        .where(
            SKUMappingModel.status == "ACTIVE",
            MarketplaceVariantModel.status == "ACTIVE",
            MarketplaceProductModel.status == "ACTIVE",
        )
        .order_by(
            MarketplaceModel.code,
            MarketplaceProductModel.shop_id,
            SKUMappingModel.inventree_part_id,
            MarketplaceProductModel.external_product_id,
        )
    )
    if part_ids:
        stmt = stmt.where(SKUMappingModel.inventree_part_id.in_(list(part_ids)))
    if marketplace:
        stmt = stmt.where(MarketplaceModel.code == marketplace.lower())
    if shop_id:
        stmt = stmt.where(MarketplaceProductModel.shop_id == str(shop_id))
    stmt = stmt.limit(max(1, int(limit)))

    async with async_session_factory() as session:
        return list((await session.execute(stmt)).all())


def _unique(values: Iterable[Any]) -> List[Any]:
    result: List[Any] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def build_fake_orders(rows: Sequence[Any], run_id: str) -> List[Dict[str, Any]]:
    """Build one synthetic webhook/order per real marketplace product."""
    grouped: Dict[Tuple[str, str, str], List[Any]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row.marketplace_code).lower(),
                str(row.shop_id),
                str(row.external_product_id),
            )
        ].append(row)

    orders: List[Dict[str, Any]] = []
    for index, ((marketplace, shop_id, product_id), group) in enumerate(
        sorted(grouped.items())
    ):
        order_id = f"E2E-{run_id}-{marketplace}-{index + 1}"
        part_ids = sorted({int(row.part_id) for row in group})
        items = []
        for row in group:
            items.append(
                {
                    "product_id": str(row.external_product_id),
                    "product_name": row.product_name or "E2E product",
                    "variant_id": str(row.external_variant_id or ""),
                    "variant_name": row.variant_name or "",
                    "seller_sku": row.seller_sku,
                    "quantity": 1,
                    "unit_price": 0,
                }
            )

        body = {
            "code": 3 if marketplace == "shopee" else 1,
            "shop_id": shop_id,
            "timestamp": int(datetime.now(timezone.utc).timestamp()),
            "data": {
                "ordersn": order_id,
                "order_id": order_id,
                "product_id": product_id,
                "status": "UNPAID",
                "order_status": "UNPAID",
                "e2e_test": True,
            },
        }
        detail = {
            "ok": True,
            "source": "e2e_fake_order_detail",
            "order_id": order_id,
            "order_status": "UNPAID",
            "items": items,
            "item_count": len(items),
            "total_quantity": len(items),
            "products_summary": ", ".join(
                f"{item['product_name']} x{item['quantity']}" for item in items
            ),
            "part_id": part_ids,
            "total_reserved": 0,
            "reserved_by_product": {},
        }
        orders.append(
            {
                "marketplace": marketplace,
                "shop_id": shop_id,
                "order_id": order_id,
                "product_id": product_id,
                "body": body,
                "detail": detail,
                "part_ids": part_ids,
                "product_id": product_id,
                "rows": group,
            }
        )
    return orders


def build_burst_orders(
    rows: Sequence[Any], run_id: str, count: int
) -> List[Dict[str, Any]]:
    """Repeat real products to create a burst with intentionally shared parts."""
    base_orders = build_fake_orders(rows, run_id)
    if not base_orders:
        return []

    orders: List[Dict[str, Any]] = []
    for index in range(max(1, int(count))):
        order = deepcopy(base_orders[index % len(base_orders)])
        order_id = f"E2E-{run_id}-burst-{index + 1}"
        order["order_id"] = order_id
        order["body"]["data"]["ordersn"] = order_id
        order["body"]["data"]["order_id"] = order_id
        order["body"]["data"]["burst_index"] = index + 1
        order["detail"]["order_id"] = order_id
        orders.append(order)
    return orders


def expected_business_key(order: Dict[str, Any]) -> str:
    """Match the configured ``invoice:courier:retur_date`` business key."""
    return f"{order['order_id']}::{order['body']['timestamp']}"


def restrict_worker_to_business_key(worker: Any, business_key: str) -> None:
    """Prevent this runner from consuming unrelated Camunda backlog tasks."""
    for topic in worker.topics:
        if topic.name == "serviceWebhookTest":
            topic.business_key = business_key


async def create_webhook_log(order: Dict[str, Any]) -> uuid.UUID:
    """Persist the synthetic webhook before sending it into Camunda."""
    log_id = uuid.uuid4()
    async with async_session_factory() as session:
        repo = WebhookLogRepositoryImpl(session)
        await repo.create(
            marketplace=order["marketplace"],
            signal_code=order["body"].get("code"),
            order_id=order["order_id"],
            order_status="UNPAID",
            raw_payload=order["body"],
            status="PENDING",
            log_id=log_id,
        )
        await session.commit()
    return log_id


async def start_fake_webhook(order: Dict[str, Any], log_id: uuid.UUID) -> List[Any]:
    raw_body = json.dumps(order["body"], separators=(",", ":"))
    return await camunda_webhook_trigger.trigger_all(
        marketplace=order["marketplace"],
        signal_code=order["body"].get("code"),
        body=order["body"],
        headers={"X-E2E-Test": "true"},
        raw_body=raw_body,
        order_detail=order["detail"],
        webhook_log_id=str(log_id),
    )


async def read_statuses(log_ids: Sequence[uuid.UUID]) -> Dict[str, Any]:
    async with async_session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(WebhookLogModel).where(WebhookLogModel.id.in_(list(log_ids)))
                )
            ).scalars().all()
        )
        logs = {
            str(row.id): {
                "status": row.status,
                "order_id": row.order_id,
                "marketplace": row.marketplace,
                "part_ids": row.part_ids,
                "service_task_id": row.service_task_id,
                "error_message": row.error_message,
                "processed_at": row.processed_at.isoformat() if row.processed_at else None,
            }
            for row in rows
        }
        part_ids = sorted(
            {
                int(part_id)
                for row in rows
                for part_id in (row.part_ids or [])
                if str(part_id).isdigit() and int(part_id) > 0
            }
        )
        stock_logs = []
        reports = []
        if part_ids:
            reports = [
                {
                    "part_id": row.part_id,
                    "seller_sku": row.seller_sku,
                    "previous_stock": row.stok_inventree,
                    "target_stock": row.stok_di_update,
                    "reserved_mp": row.reserved_mp,
                }
                for row in (
                    await session.execute(
                        select(InventoryReportModel)
                        .where(InventoryReportModel.part_id.in_(part_ids))
                        .order_by(InventoryReportModel.generated_at.desc())
                        .limit(200)
                    )
                ).scalars().all()
            ]
            stock_logs = [
                {
                    "part_id": row.inventree_part_id,
                    "variant_id": row.marketplace_variant_id,
                    "status": row.status,
                    "source": row.source,
                    "error_message": row.error_message,
                }
                for row in (
                    await session.execute(
                        select(StockSyncLogModel)
                        .where(StockSyncLogModel.inventree_part_id.in_(part_ids))
                        .order_by(StockSyncLogModel.created_at.desc())
                        .limit(200)
                    )
                ).scalars().all()
            ]
        return {"logs": logs, "reports": reports, "stock_logs": stock_logs}


async def poll_until_terminal(
    worker: Any,
    log_ids: Sequence[uuid.UUID],
    timeout_seconds: float,
    interval_seconds: float,
    poller: Optional[Any] = None,
) -> Dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    terminal = {"SUCCESS", "ON_HOLD"}
    last: Dict[str, Any] = {"logs": {}, "reports": [], "stock_logs": []}
    while asyncio.get_running_loop().time() < deadline:
        outcomes = await (poller() if poller is not None else worker.poll_once())
        if outcomes:
            logger.info(
                "Camunda poll: %d outcome(s): %s",
                len(outcomes),
                ", ".join(f"{item.topic}:{item.action}" for item in outcomes),
            )
        last = await read_statuses(log_ids)
        statuses = [item["status"] for item in last["logs"].values()]
        if statuses and all(status in terminal for status in statuses):
            return last
        await asyncio.sleep(max(0.1, interval_seconds))
    raise TimeoutError(
        f"Webhook E2E timeout setelah {timeout_seconds}s; status terakhir="
        f"{json.dumps(last.get('logs', {}), default=str)}"
    )


async def backend_worker_action(action: str) -> None:
    """Stop/start the backend's competing external-task worker."""
    import httpx

    url = (
        f"{str(settings.backend_base_url or 'http://127.0.0.1:8000').rstrip('/')}/"
        f"api/v1/webhook/camunda/external-tasks/{action}"
    )
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url)
    if response.status_code >= 400:
        raise E2EError(f"Gagal {action} backend worker: HTTP {response.status_code} {response.text}")


async def poll_targeted_batch(worker: Any, business_keys: set[str]) -> List[Any]:
    """Fetch a burst, unlock unrelated backlog, then execute one batch group."""
    worker.max_tasks = max(worker.max_tasks, 100)
    tasks, error = await worker.fetch_and_lock()
    if error:
        logger.warning("Batch fetch gagal: %s", error)
        return []

    wanted = [task for task in tasks if task.business_key in business_keys]
    for task in tasks:
        if task not in wanted:
            await worker.unlock(task)
    if not wanted:
        return []

    logger.info(
        "BATCH DEMO: %d task Camunda dikumpulkan, business_keys=%d",
        len(wanted),
        len({task.business_key for task in wanted}),
    )
    return await worker.execute_batch_group("serviceWebhookTest", wanted)


def print_plan(orders: Sequence[Dict[str, Any]]) -> None:
    print("E2E WEBHOOK PLAN")
    print(f"  database: {settings.database_url.split('@')[-1]}")
    print(f"  orders:   {len(orders)}")
    for order in orders:
        print(
            f"  - {order['marketplace']}/{order['shop_id']} "
            f"product={order['product_id']} order={order['order_id']} "
            f"parts={order['part_ids']} "
            f"variants={len(order['detail']['items'])}"
        )


async def main(args: argparse.Namespace) -> int:
    # The database can be stamped at the current Alembic head while a table
    # was removed manually. The application startup uses the same repair-safe
    # create_all path, so make the standalone runner behave consistently.
    await init_db()
    requested_parts = [int(value) for value in (args.part_id or [])]
    rows = await select_real_scope(
        requested_parts or None,
        args.marketplace,
        args.shop_id,
        args.limit,
    )
    if not rows:
        raise E2EError("Tidak ada mapping ACTIVE + product/variant ACTIVE yang cocok.")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    orders = (
        build_burst_orders(rows, run_id, args.batch_demo)
        if args.batch_demo
        else build_fake_orders(rows, run_id)
    )
    print_plan(orders)
    if args.dry_run:
        print("DRY RUN: tidak membuat webhook_log, tidak memanggil Camunda, tidak update stok.")
        return 0
    if not args.execute:
        raise E2EError("Gunakan --dry-run atau --execute secara eksplisit.")
    if not external_tasks_enabled():
        raise E2EError("external_tasks.enabled=false di infrastructure/camunda/config.json.")
    if not settings.stock_sync_push_enabled:
        raise E2EError("STOCK_SYNC_PUSH_ENABLED=false; gunakan environment push=true untuk update nyata.")

    worker = get_worker(reload_config=True)
    if not worker.enabled_topics:
        raise E2EError("Tidak ada external-task topic dengan handler yang aktif.")

    batch_demo = bool(args.batch_demo)
    if batch_demo:
        await backend_worker_action("stop")

    log_ids: List[uuid.UUID] = []
    try:
        business_keys: set[str] = set()
        for order in orders:
            log_id = await create_webhook_log(order)
            log_ids.append(log_id)
            results = await start_fake_webhook(order, log_id)
            business_key = next(
                (
                    result.business_key
                    for result in results
                    if result.request_type != "skip" and result.business_key
                ),
                expected_business_key(order),
            )
            if not args.batch_demo:
                restrict_worker_to_business_key(worker, business_key)
            business_keys.add(business_key)
            logger.info(
                "Camunda start %s business_key=%s: %s",
                order["order_id"],
                business_key,
                ", ".join(f"{result.request_type}:{result.ok}" for result in results),
            )
            if not batch_demo:
                await poll_until_terminal(
                    worker,
                    [log_id],
                    args.timeout,
                    args.poll_interval,
                )

        if batch_demo:
            await poll_until_terminal(
                worker,
                log_ids,
                args.timeout,
                args.poll_interval,
                poller=lambda: poll_targeted_batch(worker, business_keys),
            )

        result = await read_statuses(log_ids)
        print(json.dumps(result, indent=2, default=str))
        return 0 if all(item["status"] == "SUCCESS" for item in result["logs"].values()) else 2
    finally:
        await worker.aclose()
        if batch_demo:
            await backend_worker_action("start")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Pilih data saja; tidak ada side effect.")
    mode.add_argument("--execute", action="store_true", help="Jalankan Camunda dan update marketplace nyata.")
    parser.add_argument("--part-id", action="append", help="Part ID asli; boleh diulang.")
    parser.add_argument("--marketplace", choices=("shopee", "tiktok"))
    parser.add_argument("--shop-id")
    parser.add_argument("--limit", type=int, default=10, help="Maksimum mapping yang dipilih (default: 10).")
    parser.add_argument(
        "--batch-demo",
        type=int,
        metavar="N",
        help="Buat N order burst, kirim semua ke Camunda, lalu proses dalam satu batch.",
    )
    parser.add_argument("--timeout", type=float, default=300, help="Batas tunggu task Camunda (detik).")
    parser.add_argument("--poll-interval", type=float, default=2, help="Jeda polling Camunda (detik).")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main(parse_args())))
    except (E2EError, TimeoutError) as exc:
        logger.error("E2E webhook gagal: %s", exc)
        raise SystemExit(1)
    except Exception as exc:  # noqa: BLE001 - present a useful CLI error
        logger.error(
            "E2E webhook gagal (%s): %s. Pastikan PostgreSQL, Camunda, "
            "backend, dan APISIX dapat diakses.",
            type(exc).__name__,
            exc,
        )
        raise SystemExit(1)
