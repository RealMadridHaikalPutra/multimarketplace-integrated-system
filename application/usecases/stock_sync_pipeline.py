"""
StockSyncPipelineEngine — orchestrator for the 7-step hourly pipeline:

  1. GET product baru       (ApisixClient.fetch_products)
  2. SYNC product           (SyncEngine.sync_by_shop_id)
  3. MAPPING product        (MappingEngine.run_mapping_for_shop)
  4. GENERATE report        (write InventoryReportModel per mapping)
  5. INSERT stock_sync_queue per variant
  6. UPDATE-STOCK via APISIX — GLOBAL, diurutkan per part_id ASC:
       - product multi-variant → Format A (1 call batch multi-SKU)
       - product tanpa variant asli / variant tunggal → Format B (flat per SKU)
  7. INSERT stock_sync_log per result

Steps 1-3 dijalankan **secara PARALEL untuk SEMUA shop ACTIVE** (bukan hanya
shop job ini) via `asyncio.gather`: sync (1-2) mensinkronkan product & variant
dari SEMUA marketplace, lalu mapping (3) memastikan SEMUA variant (dari semua
marketplace) sudah ter-mapping sebelum inventory cycle dieksekusi.

Step 4-7 (inventory cycle) kini juga GLOBAL — TIDAK lagi per-shop. Semua
variant dari SEMUA shop digabung, dikelompokkan per `inventree_part_id` dari
sku_mapping ACTIVE, lalu diproses **urut part_id ASC** (dari stock sync queue):
part terkecil diselesaikan dulu untuk SEMUA toko/marketplace (semua variant
se-part di-push, di-batch per marketplace), baru pindah ke part_id berikutnya.
Urutan per part ini (bukan per channel) memastikan stok part yang sama
konsisten lintas marketplace dan meminimalkan roundtrip.

Format B untuk product TANPA variant asli memakai id sentinel "0"
  (konvensi no-variant marketplace; product_id tetap di path URL).

Format push step 6 (body APISIX):
  • Format A (grouped) — 1 product punya >=2 variant asli:
      {"skus": [{"id": v1, "inventory": [{"warehouse_id": w, "quantity": n}]},
                {"id": v2, "inventory": [...]}]}
    → SATU HTTP call untuk seluruh variant product (update_stock_batch).
  • Format B (flat) — product tanpa variant asli ATAU variant tunggal:
      {"skus": [{"id": variant_id, "stock": n, "warehouse_id": w}]}
    → 1 call per SKU (update_stock). Format B untuk kasus simple ini
      memotong overhead body & parsing di gateway.
    Product tanpa variant asli (external_variant_id kosong / == product_id)
    memakai id sentinel "0" (NO_VARIANT_ID) — BUKAN product_id.

Step 6 routes through the APISIX unified gateway — APISIX handles token
lifecycle (auto-refresh), marketplace routing (Shopee / TikTok), request
transformation, response normalization, and error mapping. Backend only sends
unified fields (marketplace, shop_uuid, product_id, variant_id, stock).
`warehouse_id` is resolved by `ApisixClient.update_stock` /
`update_stock_batch` from per-variant snapshot > per-shop default > settings.

GATE `settings.stock_sync_push_enabled` (env STOCK_SYNC_PUSH_ENABLED):
- TRUE (default)  → pipeline dapat update-status & update-stock ke APISIX.
- FALSE           → mode MONITORING: step 1-5 tetap jalan penuh (sync, mapping,
  inventory_report, queue) tapi step 6 (auto-activation update-status &
  update-stock) di-skip; queue ditandai PROCESSED & log berstatus SKIPPED.
  Berlaku untuk semua jalur pipeline (scheduler otomatis maupun manual).

Each part commits independently so cycles do not lose partial progress if
the APISIX or upstream marketplace call fails mid-cycle (queue rows per part
ditandai FAILED; part berikutnya tetap diproses). Saat push FAILED,
error_message pada stock_sync_queue & stock_sync_log memuat FULL response
APISIX (status code + body, atau error envelope JSON untuk HTTP 200) — bukan
hanya summary line exception — supaya audit trail cukup untuk debugging
broker/marketplace tanpa membuka log aplikasi.

PRODUCT FREEZE (variant DEACTIVATED):
  Product yang TIDAK muncul lagi di payload GET products (mis. listing
  TikTok berstatus FREEZE) di-set status FREEZE oleh SyncEngine dan SEMUA
  variant-nya di-set DEACTIVATED (mapping DIPERTAHANKAN). Di inventory
  cycle, variant DEACTIVATED / product FREEZE TETAP ditulis ke
  inventory_report (ikut perhitungan stok), tapi TIDAK masuk
  stock_sync_queue, TIDAK di-push ke APISIX, dan TIDAK masuk
  stock_sync_log (dihitung di total_skipped_freeze).

RETRY PUSH FAILED (di AKHIR cycle):
  Setelah SEMUA product group selesai di-push, queue entry yang berakhir
  FAILED pada cycle ini dengan error RETRYABLE (default: message memuat
  "400") di-retry SEKALIGUS di akhir — BUKAN saat kegagalan terjadi — dengan
  jeda ``stock_sync_retry_backoff_seconds`` sebelum tiap pass supaya rate
  limit marketplace tidak menumpuk. Maksimal ``stock_sync_max_retry_count``
  attempt (default 3); tiap attempt menaikkan ``stock_sync_queue.retry_count``
  dan menulis baris ``stock_sync_log`` BARU (source ``POLLING_1H_RETRY``).
  Entry yang sukses berhenti di-retry; yang habis retry_count tetap FAILED.
  Gate: ``stock_sync_retry_enabled`` && ``stock_sync_push_enabled``.
"""

import asyncio
import httpx
import json
import logging
import math
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from domain.entities.inventory_report import InventoryReport
from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.client import ApisixClient
from infrastructure.marketplace.apisix.constants import (
    CONFIG_ERROR_PREFIX,
    ApisixConfigError,
)
from infrastructure.db.database import async_session_factory
from infrastructure.db.models import (
    InventoryReportModel,
    MarketplaceProductModel,
    MarketplaceVariantModel,
    SKUMappingModel,
    StockSyncLogModel,
    StockSyncQueueModel,
    ValidationIssueModel,
)
from infrastructure.inventory.inventree.client import InvenTreeClient
from domain.rules.sku import SkuParser
from infrastructure.repositories.inventory_report_repository_impl import (
    InventoryReportRepositoryImpl,
)
from infrastructure.repositories.marketplace_product_repository_impl import (
    MarketplaceProductRepositoryImpl,
)
from infrastructure.repositories.marketplace_repository_impl import (
    MarketplaceRepositoryImpl,
)
from infrastructure.repositories.marketplace_shop_repository_impl import (
    MarketplaceShopRepositoryImpl,
)
from infrastructure.repositories.marketplace_variant_repository_impl import (
    MarketplaceVariantRepositoryImpl,
)
from app.container import (
    create_mapping_engine,
    create_sync_engine,
    create_validation_service,
)

logger = logging.getLogger(__name__)

# Sentinel id untuk product TANPA variant asli (external_variant_id kosong /
# == product_id). Marketplace mewakili no-variant dengan model_id "0"
# (lihat response APISIX: success_list[].model_id == 0), jadi push Format B
# untuk product ini memakai id "0" — BUKAN product_id.
NO_VARIANT_ID = "0"

# Batas karakter untuk response body/error envelope APISIX yang di-embed ke
# push_error (→ stock_sync_queue.error_message / stock_sync_log.error_message).
# Kolom DB bertipe TEXT (65,535 byte) — 2000 aman & cukup untuk payload error
# marketplace yang penuh (code, message, request_id, api_…).
PUSH_ERROR_MAX_CHARS = 2000


def _http_error_detail(exc: "httpx.HTTPStatusError") -> str:
    """Ringkasan error HTTP + FULL response body APISIX/marketplace.

    Dipakai sebagai isi push_error (stock_sync_queue.error_message &
    stock_sync_log.error_message) supaya audit trail memuat respons error
    lengkap, bukan hanya summary line ``Server error '500 ...' for url``.
    (Client APISIX juga meng-embed body ke message exception — fungsi ini
    membaca e.response secara langsung sehingga tetap akurat.)
    """
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", "?")
    try:
        body = (response.text or "").strip() if response is not None else ""
    except Exception:
        body = ""
    detail = f"HTTPStatusError: {exc}"
    if body:
        if len(body) > PUSH_ERROR_MAX_CHARS:
            body = body[:PUSH_ERROR_MAX_CHARS] + "...[truncated]"
        detail = f"{detail} | HTTP {status} response body: {body}"
    return detail


def _format_apisix_error_envelope(base_msg: str, response: Any) -> str:
    """Embed SELURUH error envelope APISIX (HTTP 200 dengan {"error": ...})
    ke pesan error — audit trail memuat respons lengkap, bukan hanya
    field message/code saja."""
    try:
        envelope_json = json.dumps(response, ensure_ascii=False, default=str)
    except Exception:
        envelope_json = str(response)
    if len(envelope_json) > PUSH_ERROR_MAX_CHARS:
        envelope_json = envelope_json[:PUSH_ERROR_MAX_CHARS] + "...[truncated]"
    return f"{base_msg} | APISIX response: {envelope_json}"


def _retry_error_markers() -> List[str]:
    """Penanda error retryable dari config (dipisah koma, di-strip, non-kosong)."""
    raw = str(getattr(settings, "stock_sync_retry_error_markers", "") or "")
    return [m.strip() for m in raw.split(",") if m.strip()]


def _is_retryable_push_error(error_message: Optional[str]) -> bool:
    """True bila error_message memuat salah satu penanda retryable (default 400).

    Dicocokkan sebagai substring karena ``error_message`` memuat status HTTP +
    FULL body APISIX (mis. "HTTP 400 response body: {\"error\": {...}}").
    """
    if not error_message:
        return False
    markers = _retry_error_markers()
    if not markers:
        return False
    return any(marker in error_message for marker in markers)


# ---------------------------------------------------------------------------
# Lock global untuk fase SYNC (Step 1-2 paralel).
#
# Karena setiap job scheduler (kini SATU job global) mensync SEMUA shop ACTIVE,
# dua job yang berjalan bersamaan bisa menulis baris yang sama ke
# marketplace_product / marketplace_variant (double-fetch). Lock ini
# men-serialkan fase sync antar-job supaya tidak ada race INSERT/UPDATE
# (IntegrityError) — sync antar-shop dalam SATU fase tetap paralel via gather.
# ---------------------------------------------------------------------------
_SYNC_ALL_LOCK = asyncio.Lock()

# ---------------------------------------------------------------------------
# Lock global untuk fase MAPPING (Step 3 paralel untuk SEMUA shop ACTIVE).
#
# Dua job scheduler bisa menulis sku_mapping untuk varian yang sama secara
# bersamaan (upsert/remap). Lock ini men-serialkan fase mapping antar-job
# supaya tidak ada race update/create di sku_mapping — mapping antar-shop
# dalam SATU fase tetap paralel via gather.
# ---------------------------------------------------------------------------
_MAPPING_ALL_LOCK = asyncio.Lock()

TRIGGER_SOURCE_PIPELINE = "POLLING_1H"
TRIGGER_SOURCE_PIPELINE_RETRY = "POLLING_1H_RETRY"
TRIGGER_SOURCE_MANUAL_PART = "MANUAL_PART"
TRIGGER_SOURCE_MANUAL_PART_RETRY = "MANUAL_PART_RETRY"


def _retry_source_for(source: Optional[str]) -> str:
    normalized = str(source or TRIGGER_SOURCE_PIPELINE).strip()
    return (
        TRIGGER_SOURCE_MANUAL_PART_RETRY
        if normalized == TRIGGER_SOURCE_MANUAL_PART
        else TRIGGER_SOURCE_PIPELINE_RETRY
    )


@dataclass
class PartScope:
    part_id: int
    mappings: int = 0
    variant_ids: List[str] = field(default_factory=list)
    product_ids: List[str] = field(default_factory=list)
    inactive_mapping_ids: List[str] = field(default_factory=list)
    missing_variant_ids: List[str] = field(default_factory=list)
    deleted_variant_ids: List[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.variant_ids


@dataclass
class PipelineCycleResult:
    cycle_index: int = 0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    sync_result: Optional[Dict[str, Any]] = None
    mapping_result: Optional[Dict[str, Any]] = None
    total_report_rows: int = 0
    total_queued: int = 0
    total_pushed: int = 0
    total_logged: int = 0
    total_skipped_pending: int = 0
    total_skipped_no_part: int = 0
    total_skipped_push: int = 0  # variant yang TIDAK di-push karena stock_sync_push_enabled=false
    total_skipped_freeze: int = 0  # variant DEACTIVATED (product FREEZE): report ditulis, TIDAK queue/push/log
    total_skipped_unchanged: int = 0  # variant dengan previous_stock==target_stock==final_stock: report ditulis, TIDAK queue/log (tidak perlu diproses)
    total_activated: int = 0  # produk INACTIVE yang di-activate sebelum stock update
    # Retry push FAILED di AKHIR cycle (hanya error retryable, mis. HTTP 400):
    total_retry_attempted: int = 0   # jumlah percobaan retry (baris log retry baru)
    total_retry_succeeded: int = 0   # percobaan retry yang SUCCESS
    total_retry_exhausted: int = 0   # entry FAILED yang retry_count-nya habis (>= max)
    push_enabled: bool = True  # snapshot settings.stock_sync_push_enabled saat cycle jalan
    # Statistik inventory cycle GLOBAL (per part_id ASC + Format A/B):
    total_parts_processed: int = 0      # jumlah part_id unik yang diproses cycle ini
    total_push_calls_format_a: int = 0  # jumlah HTTP call batch Format A (multi-SKU)
    total_push_calls_format_b: int = 0  # jumlah HTTP call flat Format B (per-SKU)
    activation_logs: List[Dict[str, Any]] = field(default_factory=list)  # log response APISIX update-status
    reserved_order_log_paths: Dict[str, str] = field(default_factory=dict)
    failures: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        for k in ("started_at", "finished_at"):
            v = payload.get(k)
            if v is not None and hasattr(v, "isoformat"):
                payload[k] = v.isoformat()
        return payload


class StockSyncPipelineEngine:
    """
    Single-cycle orchestrator GLOBAL — memproses SEMUA shop ACTIVE dalam satu
    cycle. Dipanggil oleh StockSyncScheduler SEKALI per hari sesuai jadwal
    (default 23:00, configurable via stock_sync_schedule_time /
    stock_sync_schedule_timezone). Tidak mengelola loop / cancellation — itu
    tanggung jawab scheduler.

    Inventory cycle (steps 4-7) memproses variant lintas marketplace yang
    dikelompokkan per inventree_part_id dan diurutkan part_id ASC.
    """

    def __init__(self, apisix_client: Optional[ApisixClient] = None):
        self._apisix_client = apisix_client or ApisixClient()

    @staticmethod
    def _write_reserved_order_log(
        cycle_index: int,
        sync_result: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Write one order audit file per marketplace in this cycle."""
        orders_by_marketplace: Dict[str, List[Dict[str, Any]]] = {}
        for shop_result in (sync_result or {}).get("shops", []):
            marketplace = str(shop_result.get("marketplace_code") or "UNKNOWN").upper()
            orders_by_marketplace.setdefault(marketplace, [])
            for order in shop_result.get("reserved_order_logs", []):
                order_marketplace = str(order.get("marketplace") or marketplace).upper()
                orders_by_marketplace.setdefault(order_marketplace, []).append(order)

        try:
            local_tz = ZoneInfo(settings.stock_sync_schedule_timezone)
        except Exception:
            local_tz = timezone.utc
        timestamp = datetime.now(local_tz)
        report_dir = Path(__file__).resolve().parents[2] / "e2e-reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        paths: Dict[str, str] = {}
        for marketplace, orders in orders_by_marketplace.items():
            filename = (
                f"LOGS_{marketplace}_{timestamp.month}-{timestamp.day}-{timestamp.year}_"
                f"{timestamp:%H-%M-%S}.JSON"
            )
            path = report_dir / filename
            payload = {
                "pipeline": "stock_sync",
                "cycle_index": cycle_index,
                "marketplace": marketplace,
                "generated_at": timestamp.isoformat(),
                "order_count": len(orders),
                "orders": orders,
            }
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            paths[marketplace] = str(path)
            logger.info(
                "[CYCLE %d] %s ORDER LOG written: %s (%d order(s))",
                cycle_index,
                marketplace,
                path,
                len(orders),
            )
        return paths

    async def aclose(self) -> None:
        """
        Tutup resource HTTP client APISIX (shared AsyncClient).

        Dipanggil oleh StockSyncScheduler saat job di-stop / selesai, supaya
        koneksi pooled dilepas secara eksplisit (tidak menunggu GC).
        """
        try:
            await self._apisix_client.aclose()
        except Exception as exc:
            logger.warning("APISIX client close failed: %s", exc)

    async def run_once(
        self,
        marketplace_code: str = "GLOBAL",
        external_shop_id: str = "*",
        cycle_index: int = 1,
        status_filter: str = "ALL",
        skip_global_sync: bool = False,
        part_ids: Optional[List[int]] = None,
    ) -> PipelineCycleResult:
        """Jalankan 7-step pipeline sekali untuk SEMUA shop ACTIVE.

        Steps 1-3 (sync & mapping) dan steps 4-7 (inventory cycle) kini sama-sama
        global: semua marketplace diproses dalam SATU cycle, dengan step 4-7
        diurutkan per inventree_part_id ASC.

        Args:
            marketplace_code / external_shop_id: Tidak dipakai lagi (compat
                signature lama). Inventory cycle GLOBAL membaca SEMUA shop.
            cycle_index: Nomor cycle (untuk konsistensi prefix log).
            status_filter: 'ALL' atau status product-level tertentu.
            skip_global_sync: Jika True, step 1-3 (sync & mapping SEMUA shop
                ACTIVE + tulis order log) di-SKIP — dipakai mode REPORT-ONLY
                (settings.stock_sync_report_only=true) untuk generate ulang
                inventory_report / stock_sync_queue / stock_sync_log memakai
                data lama tanpa menarik product/order/mapping terbaru.
        """
        # Report-only mode (env STOCK_SYNC_REPORT_ONLY=true): selalu skip
        # sync+mapping, apapun nilai parameter pemanggil.
        if settings.stock_sync_report_only:
            skip_global_sync = True
        if skip_global_sync:
            logger.warning(
                "[CYCLE %d] REPORT-ONLY MODE: steps 1-3 (sync product / order / "
                "mapping) di-SKIP — generate inventory_report / stock_sync_queue "
                "/ stock_sync_log memakai data product/variant/mapping yang "
                "SUDAH ADA di DB (versi lama), tanpa menarik data terbaru.",
                cycle_index,
            )
        result = PipelineCycleResult(cycle_index=cycle_index, started_at=datetime.now(timezone.utc))
        result.push_enabled = settings.stock_sync_push_enabled
        if not result.push_enabled:
            logger.warning(
                "[CYCLE %d] MODE MONITORING: stock_sync_push_enabled=false — "
                "pipeline tetap generate inventory_report tapi TIDAK akan "
                "update-status / update-stock ke APISIX.",
                cycle_index,
            )

        # ------------------------------------------------------------------
        # Validasi minimal: pastikan ada shop ACTIVE di DB (read-only check).
        # Inventory cycle GLOBAL tidak lagi butuh shop target spesifik.
        # ------------------------------------------------------------------
        async with async_session_factory() as session:
            shop_repo = MarketplaceShopRepositoryImpl(session)
            shops = await shop_repo.list_all()
            if not shops:
                result.failures.append("No marketplace_shop rows found in DB")
                result.finished_at = datetime.now(timezone.utc)
                return result

        # ------------------------------------------------------------------
        # Step 1 & 2: GET product baru + SYNC product — PARALEL semua marketplace
        # ------------------------------------------------------------------
        if skip_global_sync:
            logger.info(
                "[CYCLE %d] STEP 1-2 SKIPPED (report-only mode) — langsung ke "
                "inventory cycle GLOBAL (semua shop)",
                cycle_index,
            )
        else:
            try:
                sync_result = await self._sync_all_active_shops(
                    status_filter=status_filter,
                    cycle_index=cycle_index,
                )
                result.sync_result = sync_result
                result.reserved_order_log_paths = self._write_reserved_order_log(
                    cycle_index, sync_result
                )
                logger.info(
                    "[CYCLE %d] STEP 1-2 PARALLEL SYNC OK (status_filter=%s): "
                    "%d shop disync, total_products=%s total_variants=%s failures=%d",
                    cycle_index,
                    status_filter,
                    len(sync_result.get("shops", [])),
                    sync_result.get("total_products", 0),
                    sync_result.get("total_variants", 0),
                    len(sync_result.get("failures", [])),
                )
                # Kegagalan sync shop mana pun TIDAK menghentikan cycle —
                # inventory cycle tetap jalan dengan data shop yang berhasil
                # di-sync (kegagalan tercatat di failures).
            except Exception as exc:
                logger.exception("[CYCLE %d] SYNC PRODUCT failed: %s", cycle_index, exc)
                result.failures.append(f"sync_product: {exc}")
                result.finished_at = datetime.now(timezone.utc)
                return result

        # ------------------------------------------------------------------
        # Step 3: MAPPING product — SEMUA shop ACTIVE (paralel)
        # ------------------------------------------------------------------
        if skip_global_sync:
            logger.info(
                "[CYCLE %d] STEP 3 SKIPPED (report-only mode) — pakai hasil "
                "mapping yang sudah ada",
                cycle_index,
            )
        else:
            try:
                mapping_result = await self._map_all_active_shops(
                    cycle_index=cycle_index,
                )
                result.mapping_result = mapping_result
                logger.info(
                    "[CYCLE %d] STEP 3 MAPPING OK (all shops): total_mapped=%s "
                    "total_not_found=%s total_duplicates=%s total_errors=%s",
                    cycle_index,
                    mapping_result.get("total_mapped", 0),
                    mapping_result.get("total_not_found", 0),
                    mapping_result.get("total_duplicates", 0),
                    mapping_result.get("total_errors", 0),
                )
            except Exception as exc:
                logger.exception("[CYCLE %d] MAPPING PRODUCT failed: %s", cycle_index, exc)
                result.failures.append(f"mapping_product: {exc}")
                # mapping gagal tidak stop pipeline — kita tetap bisa pakai mapping sebelumnya

        # ------------------------------------------------------------------
        # Step 4-7: inventory cycle GLOBAL — per part_id ASC, per-part commit
        # ------------------------------------------------------------------
        try:
            await self._run_inventory_cycle(
                cycle_index=cycle_index,
                result=result,
                status_filter=status_filter,
                part_ids=part_ids,
            )
        except Exception as exc:
            logger.exception("[CYCLE %d] INVENTORY CYCLE failed: %s", cycle_index, exc)
            result.failures.append(f"inventory_cycle: {exc}")

        # ------------------------------------------------------------------
        # Retention cleanup di akhir setiap cycle
        # ------------------------------------------------------------------
        try:
            cleanup_result = await self.cleanup_old_records()
            logger.info("[CYCLE %d] Retention cleanup: %s", cycle_index, cleanup_result)
        except Exception as exc:
            logger.exception("[CYCLE %d] Retention cleanup error: %s", cycle_index, exc)

        result.finished_at = datetime.now(timezone.utc)
        return result

    async def resolve_part_scope(self, part_id: int) -> PartScope:
        """Resolve variants/products represented by one active SKU mapping."""
        async with async_session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        SKUMappingModel.marketplace_variant_id.label("variant_id"),
                        SKUMappingModel.status.label("mapping_status"),
                        MarketplaceProductModel.external_product_id.label("product_id"),
                        MarketplaceVariantModel.status.label("variant_status"),
                    )
                    .outerjoin(
                        MarketplaceVariantModel,
                        MarketplaceVariantModel.marketplace_variant_id
                        == SKUMappingModel.marketplace_variant_id,
                    )
                    .outerjoin(
                        MarketplaceProductModel,
                        MarketplaceProductModel.marketplace_product_id
                        == MarketplaceVariantModel.marketplace_product_id,
                    )
                    .where(SKUMappingModel.inventree_part_id == int(part_id))
                )
            ).all()

        scope = PartScope(part_id=int(part_id), mappings=len(rows))
        for row in rows:
            variant_id = str(getattr(row, "variant_id", "") or "")
            mapping_status = str(getattr(row, "mapping_status", "") or "").upper()
            product_id = getattr(row, "product_id", None)
            variant_status = str(getattr(row, "variant_status", "") or "").upper()
            if not variant_id:
                continue
            if product_id is None:
                scope.missing_variant_ids.append(variant_id)
            elif variant_status == RecordStatus.DELETED.value:
                scope.deleted_variant_ids.append(variant_id)
            elif mapping_status != "ACTIVE":
                scope.inactive_mapping_ids.append(variant_id)
            else:
                scope.variant_ids.append(variant_id)
                product_text = str(product_id)
                if product_text not in scope.product_ids:
                    scope.product_ids.append(product_text)
        return scope

    async def run_once_for_part(
        self,
        part_id: int,
        scope: Optional[PartScope] = None,
    ) -> PipelineCycleResult:
        """Run one inventory cycle restricted to a part ID."""
        scope = scope or await self.resolve_part_scope(part_id)
        if scope.is_empty:
            raise ValueError(f"part_id {part_id} tidak memiliki variant aktif")
        return await self.run_once_for_parts([part_id])

    async def run_once_for_parts(
        self,
        part_ids: List[int],
        full_pipeline: bool = False,
    ) -> PipelineCycleResult:
        """Run one cycle for unique requested part IDs only."""
        unique_parts = []
        for raw_part_id in part_ids:
            try:
                value = int(raw_part_id)
            except (TypeError, ValueError):
                continue
            if value > 0 and value not in unique_parts:
                unique_parts.append(value)
        if not unique_parts:
            raise ValueError("tidak ada part_id yang valid")
        return await self.run_once(
            cycle_index=1,
            skip_global_sync=not full_pipeline,
            part_ids=unique_parts,
        )

    async def _sync_all_active_shops(
        self,
        status_filter: str = "ALL",
        cycle_index: int = 1,
    ) -> Dict[str, Any]:
        """
        Step 1-2 (PARALEL): GET product + SYNC untuk SEMUA shop berstatus ACTIVE.

        Semua shop di-sync sekaligus via ``asyncio.gather`` sehingga sebelum
        step MAPPING (3) dan INVENTORY REPORT (4-7) dijalankan, DB sudah
        memuat product & variant dari SEMUA marketplace — agregasi
        ``reserved_mp`` (SUM reserved stock utk seller_sku yang sama lintas
        marketplace) selalu akurat.

        Returns dict ter-agregasi:
            {
                "status": "success" | "partial",
                "total_products": int,   # SUM lintas shop
                "total_variants": int,   # SUM lintas shop
                "shops": [dict hasil sync_by_shop_id per shop],
                "failures": [str],       # SEMUA kegagalan
            }

        Setiap shop memakai session & SyncEngine sendiri (session SQLAlchemy
        tidak bisa dibagi antar task paralel). Kegagalan satu shop TIDAK
        menggagalkan shop lain — error diakumulasi ke ``failures``.
        """
        # Lock global: serialkan fase sync antar-job scheduler supaya dua job
        # tidak menulis baris yang sama secara bersamaan.
        async with _SYNC_ALL_LOCK:
            # ------------------------------------------------------------------
            # Load semua marketplace + shop (1x, read-only)
            # ------------------------------------------------------------------
            async with async_session_factory() as session:
                mp_repo = MarketplaceRepositoryImpl(session)
                shop_repo = MarketplaceShopRepositoryImpl(session)
                marketplaces = await mp_repo.list_all()
                shops = await shop_repo.list_all()

            mp_by_id = {str(mp.id): mp for mp in marketplaces}
            active_shops = [s for s in shops if s.status == RecordStatus.ACTIVE]

            logger.info(
                "[CYCLE %d] PARALLEL SYNC START: %d shop(s) ACTIVE akan "
                "di-GET PRODUCT & SYNC sekaligus (status_filter=%s)",
                cycle_index,
                len(active_shops),
                status_filter,
            )

            async def _sync_one_shop(shop) -> Dict[str, Any]:
                """Sync 1 shop dengan session & SyncEngine sendiri."""
                mp = mp_by_id.get(str(shop.marketplace_id))
                if mp is None:
                    return {
                        "status": "error",
                        "shop_id": str(shop.id),
                        "external_shop_id": str(shop.external_shop_id),
                        "shop_name": shop.shop_name,
                        "marketplace_code": "UNKNOWN",
                        "total_products": 0,
                        "total_variants": 0,
                        "message": f"Marketplace id={shop.marketplace_id} tidak ditemukan",
                    }
                try:
                    async with async_session_factory() as session:
                        sync_engine = create_sync_engine(
                            session, apisix_client=self._apisix_client
                        )
                        sync_result = await sync_engine.sync_by_shop_id(
                            shop.id, status_filter=status_filter,
                        )
                        await session.commit()

                    sync_result.setdefault("shop_id", str(shop.id))
                    sync_result.setdefault(
                        "external_shop_id", str(shop.external_shop_id)
                    )
                    sync_result.setdefault("shop_name", shop.shop_name)
                    sync_result.setdefault("marketplace_code", mp.code.upper())
                    return sync_result
                except Exception as exc:
                    logger.exception(
                        "[CYCLE %d] Parallel sync failed shop=%s (%s): %s",
                        cycle_index,
                        shop.external_shop_id,
                        mp.code,
                        exc,
                    )
                    return {
                        "status": "error",
                        "shop_id": str(shop.id),
                        "external_shop_id": str(shop.external_shop_id),
                        "shop_name": shop.shop_name,
                        "marketplace_code": mp.code.upper(),
                        "total_products": 0,
                        "total_variants": 0,
                        "message": str(exc),
                    }

            raw_results = await asyncio.gather(
                *[_sync_one_shop(s) for s in active_shops],
                return_exceptions=True,
            )

            # ------------------------------------------------------------------
            # Agregasi hasil per-shop + kumpulkan failures
            # ------------------------------------------------------------------
            shops_result: List[Dict[str, Any]] = []
            failures: List[str] = []
            for raw in raw_results:
                if isinstance(raw, BaseException):
                    failures.append(f"parallel_sync: {raw}")
                    continue
                shops_result.append(raw)
                if raw.get("status") == "error":
                    failures.append(
                        f"{raw.get('marketplace_code')}/{raw.get('external_shop_id')}: "
                        f"{raw.get('message')}"
                    )

            total_products = sum(
                int(r.get("total_products", 0) or 0) for r in shops_result
            )
            total_variants = sum(
                int(r.get("total_variants", 0) or 0) for r in shops_result
            )

            aggregated = {
                "status": "success" if not failures else "partial",
                "total_products": total_products,
                "total_variants": total_variants,
                "shops": shops_result,
                "failures": failures,
            }

            logger.info(
                "[CYCLE %d] PARALLEL SYNC DONE: %d shop, total_products=%d "
                "total_variants=%d, failures=%d",
                cycle_index,
                len(shops_result),
                total_products,
                total_variants,
                len(failures),
            )
            return aggregated

    async def _map_all_active_shops(
        self,
        cycle_index: int = 1,
    ) -> Dict[str, Any]:
        """
        Step 3 (PARALEL): MAPPING untuk SEMUA shop berstatus ACTIVE.

        Mapping dijalankan untuk SEMUA shop sekaligus supaya saat step 4-7
        (inventory cycle) dieksekusi, seluruh variant dari SEMUA marketplace
        sudah ter-mapping. Tanpa ini, agregasi ``part_reserved_converted_map``
        (yang join sku_mapping ACTIVE) akan TIDAK lengkap → stok_di_update
        berbeda antar shop untuk part yang sama.

        Returns dict ter-agregasi:
            {
                "status": "success" | "partial",
                "total_mapped": int,       # SUM lintas shop
                "total_not_found": int,    # SUM lintas shop
                "total_duplicates": int,   # SUM lintas shop
                "total_errors": int,       # SUM lintas shop
                "total_skipped": int,      # SUM lintas shop
                "shops": [dict hasil run_mapping_for_shop per shop],
                "failures": [str],         # SEMUA kegagalan
            }
        """
        # Lock global: serialkan fase mapping antar-job scheduler supaya tidak
        # ada race upsert/remap di sku_mapping.
        async with _MAPPING_ALL_LOCK:
            # ------------------------------------------------------------------
            # Load semua marketplace + shop (1x, read-only)
            # ------------------------------------------------------------------
            async with async_session_factory() as session:
                mp_repo = MarketplaceRepositoryImpl(session)
                shop_repo = MarketplaceShopRepositoryImpl(session)
                marketplaces = await mp_repo.list_all()
                shops = await shop_repo.list_all()

            mp_by_id = {str(mp.id): mp for mp in marketplaces}
            active_shops = [s for s in shops if s.status == RecordStatus.ACTIVE]

            logger.info(
                "[CYCLE %d] PARALLEL MAPPING START: %d shop(s) ACTIVE "
                "akan di-MAPPING sekaligus",
                cycle_index,
                len(active_shops),
            )

            async def _map_one_shop(shop) -> Dict[str, Any]:
                """Mapping 1 shop dengan session & MappingEngine sendiri."""
                mp = mp_by_id.get(str(shop.marketplace_id))
                if mp is None:
                    return {
                        "status": "error",
                        "shop_id": str(shop.id),
                        "external_shop_id": str(shop.external_shop_id),
                        "shop_name": shop.shop_name,
                        "marketplace_code": "UNKNOWN",
                        "total_mapped": 0,
                        "total_not_found": 0,
                        "total_duplicates": 0,
                        "total_errors": 0,
                        "message": f"Marketplace id={shop.marketplace_id} tidak ditemukan",
                    }
                try:
                    async with async_session_factory() as session:
                        mapping_engine = create_mapping_engine(session)
                        mapping_result = await mapping_engine.run_mapping_for_shop(shop.id)
                        await session.commit()

                    mapping_result.setdefault("shop_id", str(shop.id))
                    mapping_result.setdefault(
                        "external_shop_id", str(shop.external_shop_id)
                    )
                    mapping_result.setdefault("shop_name", shop.shop_name)
                    mapping_result.setdefault("marketplace_code", mp.code.upper())
                    return mapping_result
                except Exception as exc:
                    logger.exception(
                        "[CYCLE %d] Parallel mapping failed shop=%s (%s): %s",
                        cycle_index,
                        shop.external_shop_id,
                        mp.code,
                        exc,
                    )
                    return {
                        "status": "error",
                        "shop_id": str(shop.id),
                        "external_shop_id": str(shop.external_shop_id),
                        "shop_name": shop.shop_name,
                        "marketplace_code": mp.code.upper(),
                        "total_mapped": 0,
                        "total_not_found": 0,
                        "total_duplicates": 0,
                        "total_errors": 0,
                        "message": str(exc),
                    }

            raw_results = await asyncio.gather(
                *[_map_one_shop(s) for s in active_shops],
                return_exceptions=True,
            )

            # ------------------------------------------------------------------
            # Agregasi hasil per-shop + kumpulkan failures
            # ------------------------------------------------------------------
            shops_result: List[Dict[str, Any]] = []
            failures: List[str] = []
            for raw in raw_results:
                if isinstance(raw, BaseException):
                    failures.append(f"parallel_mapping: {raw}")
                    continue
                shops_result.append(raw)
                if raw.get("status") == "error":
                    failures.append(
                        f"{raw.get('marketplace_code')}/{raw.get('external_shop_id')}: "
                        f"{raw.get('message')}"
                    )

            total_mapped = sum(
                int(r.get("total_mapped", 0) or 0) for r in shops_result
            )
            total_not_found = sum(
                int(r.get("total_not_found", 0) or 0) for r in shops_result
            )
            total_duplicates = sum(
                int(r.get("total_duplicates", 0) or 0) for r in shops_result
            )
            total_errors = sum(
                int(r.get("total_errors", 0) or 0) for r in shops_result
            )
            total_skipped = sum(
                int(r.get("total_skipped", 0) or 0) for r in shops_result
            )
            # ------------------------------------------------------------------
            # Agregasi hasil validasi (TC-SKU-006) lintas shop (check per-shop:
            # Case 2.1 SKU MP tidak ada di InvenTree).
            # ------------------------------------------------------------------
            total_conflicts = sum(
                int((r.get("validation") or {}).get("total_conflicts", 0) or 0)
                for r in shops_result
            )
            total_warnings = sum(
                int((r.get("validation") or {}).get("total_warnings", 0) or 0)
                for r in shops_result
            )

            # ------------------------------------------------------------------
            # Check GLOBAL (sekali per cycle, bukan per shop):
            #   - Case 1 : SKU_PART_CATEGORY_CONFLICT (part dipakai campuran
            #              SKU_PACK + SKU_S/X/NORMAL)
            #   - Case 2.2: SKU_INVENTREE_ORPHAN (sku_toko InvenTree tanpa SKU MP)
            # Data yang dicek bersifat global — menjalankannya per shop hanya
            # menggandakan kerja & meng-inflasi occurrence_count.
            # ------------------------------------------------------------------
            global_validation = {"total_conflicts": 0, "total_warnings": 0, "issues": []}
            if settings.mapping_validation_enabled:
                try:
                    async with async_session_factory() as v_session:
                        validation_service = create_validation_service(v_session)
                        global_validation = await validation_service.run_global_checks()
                        await v_session.commit()
                except Exception as exc:
                    logger.exception(
                        "[CYCLE %d] Global mapping validation failed: %s",
                        cycle_index, exc,
                    )
                    global_validation = {
                        "total_conflicts": 0,
                        "total_warnings": 0,
                        "issues": [],
                        "error": str(exc),
                    }

            aggregated = {
                "status": "success" if not failures else "partial",
                "total_mapped": total_mapped,
                "total_not_found": total_not_found,
                "total_duplicates": total_duplicates,
                "total_errors": total_errors,
                "total_skipped": total_skipped,
                "total_conflicts": total_conflicts + global_validation.get("total_conflicts", 0),
                "total_warnings": total_warnings + global_validation.get("total_warnings", 0),
                "validation": global_validation,
                "shops": shops_result,
                "failures": failures,
            }

            logger.info(
                "[CYCLE %d] PARALLEL MAPPING DONE: %d shop, total_mapped=%d "
                "total_not_found=%d total_duplicates=%d total_errors=%d "
                "validation_conflicts=%d validation_warnings=%d failures=%d",
                cycle_index,
                len(shops_result),
                total_mapped,
                total_not_found,
                total_duplicates,
                total_errors,
                total_conflicts + global_validation.get("total_conflicts", 0),
                total_warnings + global_validation.get("total_warnings", 0),
                len(failures),
            )
            return aggregated

    async def cleanup_old_records(self) -> Dict[str, int]:
        """
        Hapus rows lama untuk mencegah table membengkak.
        Retention:
          - inventory_report                    : N hari (default 7)
          - stock_sync_log                      : N hari (default 30)
          - stock_sync_queue (status=PROCESSED)  : N hari (default 7)

        Returns dict {table_name: rows_deleted}.
        """
        retention = {
            "inventory_report": settings.stock_sync_report_retention_days,
            "stock_sync_log": settings.stock_sync_log_retention_days,
            "stock_sync_queue_processed": settings.stock_sync_queue_retention_days,
            "validation_issue": settings.validation_issue_retention_days,
        }
        result: Dict[str, int] = {k: 0 for k in retention}

        async with async_session_factory() as session:
            try:
                cutoff_report = datetime.now(timezone.utc) - timedelta(
                    days=retention["inventory_report"]
                )
                del_r = await session.execute(
                    delete(InventoryReportModel).where(
                        InventoryReportModel.generated_at < cutoff_report
                    )
                )
                result["inventory_report"] = del_r.rowcount or 0

                cutoff_log = datetime.now(timezone.utc) - timedelta(
                    days=retention["stock_sync_log"]
                )
                del_l = await session.execute(
                    delete(StockSyncLogModel).where(
                        StockSyncLogModel.created_at < cutoff_log
                    )
                )
                result["stock_sync_log"] = del_l.rowcount or 0

                cutoff_q = datetime.now(timezone.utc) - timedelta(
                    days=retention["stock_sync_queue_processed"]
                )
                # Hanya hapus queue dengan status PROCESSED & processed_at sudah di-set,
                # supaya FAILED/PENDING baris masih tersedia untuk retry / debugging.
                del_q = await session.execute(
                    delete(StockSyncQueueModel).where(
                        StockSyncQueueModel.status == "PROCESSED",
                        StockSyncQueueModel.processed_at < cutoff_q,
                    )
                )
                result["stock_sync_queue_processed"] = del_q.rowcount or 0

                # Retention validation_issue: hanya hapus baris yang sudah RESOLVED
                # (issue OPEN harus dipertahankan untuk tracking aktif).
                cutoff_v = datetime.now(timezone.utc) - timedelta(
                    days=retention["validation_issue"]
                )
                del_v = await session.execute(
                    delete(ValidationIssueModel).where(
                        ValidationIssueModel.status == "RESOLVED",
                        ValidationIssueModel.resolved_at < cutoff_v,
                    )
                )
                result["validation_issue"] = del_v.rowcount or 0

                await session.commit()
                logger.info(
                    "Retention cleanup done: inventory_report=%d stock_sync_log=%d "
                    "stock_sync_queue_processed=%d validation_issue=%d",
                    result["inventory_report"],
                    result["stock_sync_log"],
                    result["stock_sync_queue_processed"],
                    result["validation_issue"],
                )
            except Exception as exc:
                logger.exception("Retention cleanup failed: %s", exc)
                await session.rollback()
        return result

    # =====================================================================
    # Steps 4-7: INVENTORY CYCLE GLOBAL — per part_id ASC, Format A/B
    # =====================================================================

    async def _run_inventory_cycle(
        self,
        cycle_index: int,
        result: PipelineCycleResult,
        status_filter: str = "ALL",
        inventree: Optional[InvenTreeClient] = None,
        part_ids: Optional[List[int]] = None,
    ) -> None:
        """
        Inventory cycle GLOBAL (bukan per-shop):

          1. Baca SEMUA product & variant dari SEMUA shop (2 query, tanpa N+1).
          2. Hitung peta agregat reserved_mp & part_reserved_converted_map
             (2 query) — sama seperti sebelumnya tapi lingkup global.
          3. Resolve mapping ACTIVE per variant + urutan push per part_id ASC
             (_resolve_push_order) — part_id didapat dari sku_mapping yang
             nantinya mengisi stock_sync_queue.
          4. Untuk tiap part (ASC): tulis report/queue/log semua variant
             se-part, lalu push grouped Format A (product multi-variant) atau
             Format B (no-variant/single-variant), per marketplace.
          5. Commit per part — kegagalan satu part tidak menggugurkan part lain.
        """
        inventree = inventree or InvenTreeClient()
        buffer = settings.stock_buffer

        # ------------------------------------------------------------------
        # 4.0 Load SEMUA product & variant (semua shop) dalam SATU session.
        # NOTE: MarketplaceProductModel.shop_id menyimpan external_shop_id
        # (String), bukan UUID PK shop.
        # Product FREEZE TETAP diproses (report-only via _process_part_item);
        # product DEACTIVATED di-skip (tidak relate ke marketplace lagi).
        # Variant DELETED di-skip (dihapus dari marketplace); variant
        # DEACTIVATED (product FREEZE) di-skip dari queue/push/log di
        # _process_part_item (report tetap ditulis).
        # ------------------------------------------------------------------
        async with async_session_factory() as session:
            product_repo = MarketplaceProductRepositoryImpl(session)
            variant_repo = MarketplaceVariantRepositoryImpl(session)

            all_products = await product_repo.list_all()
            matching_products = [
                p for p in all_products
                if p.status != RecordStatus.DEACTIVATED
                and (
                    not status_filter
                    or status_filter.upper() == "ALL"
                    or (p.status and p.status.value == status_filter.upper())
                )
            ]
            all_variants = await variant_repo.list_all()
            active_variants = [
                v for v in all_variants if v.status != RecordStatus.DELETED
            ]
            if len(active_variants) != len(all_variants):
                logger.info(
                    "[CYCLE %d] Skipped %d DELETED variant(s) dari inventory cycle (global)",
                    cycle_index,
                    len(all_variants) - len(active_variants),
                )

            product_by_key = {
                (str(p.shop_id), str(p.external_product_id)): p
                for p in matching_products
            }
            variant_pairs: List[tuple] = []
            for variant in active_variants:
                product = product_by_key.get(
                    (str(variant.shop_id), str(variant.external_product_id))
                )
                if product is None:
                    # Product row hilang (inkonsistensi data): tetap proses
                    # dengan asumsi status ACTIVE bila tidak ada status_filter.
                    # Dengan filter spesifik, status tak bisa diverifikasi → skip.
                    if status_filter and status_filter.upper() not in ("", "ALL"):
                        continue
                    variant_pairs.append((None, variant))
                    continue
                variant_pairs.append((product, variant))

            # Product FREEZE (tidak muncul lagi di GET products terbaru —
            # di-set SyncEngine) + variant DEACTIVATED miliknya: TETAP masuk
            # inventory_report (ikut perhitungan stok), tapi TIDAK masuk
            # stock_sync_queue / push stock / sync log.
            freeze_variants = [
                v for v in all_variants
                if v.status == RecordStatus.DEACTIVATED
            ]
            if freeze_variants:
                logger.info(
                    "[CYCLE %d] %d variant DEACTIVATED (product FREEZE) akan "
                    "ikut inventory_report saja — TIDAK queue/push/log",
                    cycle_index,
                    len(freeze_variants),
                )

            # reserved_mp = SUM(total_reserved_stock) per seller_sku GLOBAL
            # (lintas marketplace), dihitung SEKALI per cycle (1 query).
            reserved_mp_rows = await session.execute(
                select(
                    MarketplaceVariantModel.seller_sku,
                    func.coalesce(
                        func.sum(MarketplaceVariantModel.total_reserved_stock), 0
                    ),
                )
                .where(
                    MarketplaceVariantModel.status != RecordStatus.DELETED.value
                )
                .group_by(MarketplaceVariantModel.seller_sku)
            )
            reserved_mp_map = {
                str(sku): int(total) for sku, total in reserved_mp_rows.all()
            }

            # part_reserved_converted_map = {inventree_part_id: Σ qty_converted}
            # Qty converted per SKU = reserved_mp_raw × faktor_pengali, lalu
            # diagregasi per part (join SKU mapping ACTIVE). Nilai ini dipakai
            # sebagai pengurang di formula stok_di_update: karena SEMUA variasi
            # SKU (x/s/pack) dari part yang sama berbagi stok fisik yang sama,
            # total reservasi terkonversi per part harus dikurangi SEKALI,
            # bukan per SKU (menghindari double-count antar SKU se-part).
            #
            # NOTE scoping: peta ini HANYA menghitung variant dengan mapping
            # ACTIVE (sesuai join + where), sedangkan reserved_mp_map di atas
            # menjumlahkan SEMUA variant (tanpa filter mapping). Perbedaan ini
            # disengaja: hanya variant ter-mapping yang mengonsumsi stok fisik
            # sebuah part, jadi reservasi SKU tanpa mapping aktif TIDAK boleh
            # ikut mengurangi stok part. Jangan "perbaiki" dengan menghapus
            # filter ACTIVE — itu akan double-count reservasi SKU se-part.
            part_reserved_rows = await session.execute(
                select(
                    SKUMappingModel.inventree_part_id,
                    MarketplaceVariantModel.seller_sku,
                    func.coalesce(
                        func.sum(MarketplaceVariantModel.total_reserved_stock), 0
                    ),
                )
                .join(
                    SKUMappingModel,
                    SKUMappingModel.marketplace_variant_id
                    == MarketplaceVariantModel.marketplace_variant_id,
                )
                .where(
                    SKUMappingModel.status == "ACTIVE",
                    MarketplaceVariantModel.status != RecordStatus.DELETED.value,
                )
                .group_by(
                    SKUMappingModel.inventree_part_id,
                    MarketplaceVariantModel.seller_sku,
                )
            )
            part_reserved_converted_map: Dict[int, float] = {}
            for part_id, sku, raw_total in part_reserved_rows.all():
                raw = int(raw_total or 0)
                if raw <= 0:
                    continue
                try:
                    parsed_sku = SkuParser.parse(str(sku))
                    faktor = SkuParser.compute_faktor_pengali(
                        parsed_sku.sku_type, parsed_sku.conversion_qty
                    )
                except Exception:
                    faktor = 1.0
                part_id_int = int(part_id)
                part_reserved_converted_map[part_id_int] = (
                    part_reserved_converted_map.get(part_id_int, 0.0)
                    + raw * faktor
                )

            # ------------------------------------------------------------------
            # 4.1 Resolve mapping + urutan push GLOBAL per part_id ASC.
            # ------------------------------------------------------------------
            push_order = await self._resolve_push_order(
                session=session,
                cycle_index=cycle_index,
                variants=[v for _, v in variant_pairs],
                shop_warehouse_map={},  # warehouse di-resolve per-shop saat push
            )
            if part_ids is not None:
                requested_parts = {int(part_id) for part_id in part_ids}
                push_order = [
                    (part_id, variants)
                    for part_id, variants in push_order
                    if part_id in requested_parts
                ]
        # session read ditutup — per-part session baru dibuka di loop di bawah.

        filtered_out = len(all_products) - len(matching_products)
        if filtered_out > 0:
            logger.info(
                "[CYCLE %d] Filtered out %d products (status != %s) at inventory-cycle stage",
                cycle_index, filtered_out, status_filter,
            )
        total_parts = len(push_order)
        total_variants_mapped = sum(len(vs) for _, vs in push_order)
        logger.info(
            "[CYCLE %d] Inventory cycle GLOBAL: %d part(s) urut ASC, "
            "%d variant ter-mapping (dari %d variant aktif / %d product)",
            cycle_index,
            total_parts,
            total_variants_mapped,
            len(active_variants),
            len(matching_products),
        )

        # ------------------------------------------------------------------
        # 4.2 Phase 1: Hitung stok & tulis report + queue per part (ASC).
        # Commit per part agar snapshot InvenTree & status awal tersimpan.
        # ------------------------------------------------------------------
        all_computed: List[Dict[str, Any]] = []
        for part_id, part_variants in push_order:
            try:
                async with async_session_factory() as session:
                    part_computed = await self._push_part(
                        session=session,
                        part_id=part_id,
                        part_variants=part_variants,
                        cycle_index=cycle_index,
                        result=result,
                        inventree=inventree,
                        buffer=buffer,
                        reserved_mp_map=reserved_mp_map,
                        part_reserved_converted_map=part_reserved_converted_map,
                        product_by_key=product_by_key,
                        push_immediately=False,
                    )
                    await session.commit()
                    if part_computed:
                        all_computed.extend(part_computed)
            except Exception as exc:
                logger.exception(
                    "[CYCLE %d] Per-part failure (part_id=%d): %s",
                    cycle_index,
                    part_id,
                    exc,
                )
                result.failures.append(f"part={part_id}: {exc}")

        # ------------------------------------------------------------------
        # 4.3 Phase 2: Group per PRODUCT LINTAS PART lalu push.
        # Variant-variant dari satu produk yang sama (walaupun beda InvenTree part_id)
        # kini disatukan ke dalam 1 grup product sehingga:
        #   >= 2 variant asli -> Format A (1 batch call update_stock_batch)
        #   1 variant asli    -> Format B (flat update_stock per SKU)
        #   orphan variant    -> Format B (id "0")
        # ------------------------------------------------------------------
        if not settings.stock_sync_push_enabled:
            async with async_session_factory() as session:
                for item in all_computed:
                    q = item["queue_entry"]
                    q.status = "PROCESSED"
                    q.processed_at = datetime.now(timezone.utc)
                    result.total_skipped_push += 1
                    if q.id:
                        try:
                            stmt = (
                                update(StockSyncQueueModel)
                                .where(StockSyncQueueModel.id == q.id)
                                .values(status="PROCESSED", processed_at=q.processed_at)
                            )
                            await session.execute(stmt)
                        except Exception:
                            pass
                await session.commit()
            logger.info(
                "[CYCLE %d] Push SKIPPED (monitoring mode) untuk %d variant",
                cycle_index, len(all_computed),
            )
        else:
            product_groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for item in all_computed:
                key = (
                    str(item["marketplace_code"]),
                    str(item["shop_db_id"]),
                    str(item["product_id"]),
                )
                product_groups.setdefault(key, []).append(item)

            # Tetap urutkan product berdasarkan min(part_id) agar prioritas part ASC terjaga
            sorted_groups = sorted(
                product_groups.items(),
                key=lambda kv: min(it.get("part_id", 0) for it in kv[1]),
            )

            for (_mp, _shop, _pid), items in sorted_groups:
                try:
                    async with async_session_factory() as session:
                        await self._push_product_group(
                            session=session,
                            items=items,
                            cycle_index=cycle_index,
                            result=result,
                        )
                        await session.commit()
                except Exception as exc:
                    first_pid = items[0]["product_id"] if items else "unknown"
                    logger.exception(
                        "[CYCLE %d] Product push failure (product_id=%s): %s",
                        cycle_index,
                        first_pid,
                        exc,
                    )
                    result.failures.append(f"product={first_pid}: {exc}")

        # ------------------------------------------------------------------
        # 4.4 Retry pass DI AKHIR cycle (bukan saat gagal).
        # Semua push normal sudah selesai → baru entry FAILED dgn error
        # retryable (default HTTP 400) di-retry, dengan jeda antar pass,
        # supaya rate limit marketplace tidak menumpuk. Hanya entry cycle ini.
        # ------------------------------------------------------------------
        await self._retry_failed_pushes(
            items=all_computed,
            cycle_index=cycle_index,
            result=result,
        )

        logger.info(
            "[CYCLE %d] Inventory cycle GLOBAL done: parts=%d pushed=%d "
            "formatA_calls=%d formatB_calls=%d skipped_freeze=%d "
            "retry_attempted=%d retry_ok=%d retry_exhausted=%d failures=%d",
            cycle_index,
            result.total_parts_processed,
            result.total_pushed,
            result.total_push_calls_format_a,
            result.total_push_calls_format_b,
            result.total_skipped_freeze,
            result.total_retry_attempted,
            result.total_retry_succeeded,
            result.total_retry_exhausted,
            len(result.failures),
        )

    async def _resolve_push_order(
        self,
        session: AsyncSession,
        cycle_index: int,
        variants: List,
        shop_warehouse_map: Optional[Dict[str, Optional[str]]] = None,
    ) -> List[Tuple[int, List]]:
        """
        Kelompokkan variant per inventree_part_id (dari mapping ACTIVE) dan
        kembalikan daftar ``(part_id, [variant, ...])`` **urut part_id ASC**.

        Inilah urutan eksekusi stock sync queue: part terkecil diselesaikan
        dulu untuk SEMUA toko/marketplace, baru pindah ke part berikutnya.

        Variant tanpa mapping ACTIVE → di-skip di sini (dicatat sebagai
        skipped_no_part oleh pemanggil push).
        """
        part_groups: Dict[int, List] = {}
        for variant in variants:
            seller_sku = (variant.seller_sku or "").strip()
            if not seller_sku:
                continue
            if getattr(variant, "status", None) == RecordStatus.DELETED:
                continue
            mapping_model = (
                await session.execute(
                    select(SKUMappingModel)
                    .where(
                        SKUMappingModel.marketplace_variant_id
                        == variant.marketplace_variant_id,
                        SKUMappingModel.status == "ACTIVE",
                    )
                    .order_by(SKUMappingModel.created_at.asc())
                )
            ).scalars().first()
            if mapping_model is None:
                logger.debug(
                    "[CYCLE %d] Skipping variant (no active mapping): sku=%s",
                    cycle_index,
                    seller_sku,
                )
                continue
            try:
                part_id = int(mapping_model.inventree_part_id)
            except (TypeError, ValueError):
                part_id = 0
            if part_id <= 0:
                logger.warning(
                    "[CYCLE %d] Skip variant — inventree_part_id invalid: sku=%s",
                    cycle_index,
                    seller_sku,
                )
                continue
            part_groups.setdefault(part_id, []).append(variant)

        return sorted(part_groups.items(), key=lambda kv: kv[0])

    async def _push_part(
        self,
        session: AsyncSession,
        part_id: int,
        part_variants: List,
        cycle_index: int,
        result: PipelineCycleResult,
        inventree: InvenTreeClient,
        buffer: int,
        reserved_mp_map: Optional[Dict[str, int]] = None,
        part_reserved_converted_map: Optional[Dict[int, float]] = None,
        product_by_key: Optional[Dict[tuple, Any]] = None,
        push_immediately: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Proses SATU kelompok part (semua variant dari semua marketplace yang
        ter-mapping ke part_id ini), dalam urutan part_id ASC:

          1. Untuk tiap variant: hitung target_stock, tulis report row +
             queue entry (PENDING).
          2. Bila push_immediately=False (default pemanggilan dari _run_inventory_cycle):
             kembalikan computed items untuk di-group per-produk lintas part
             di Phase 2.
          3. Bila push_immediately=True (backward-compatibility / direct call):
             group variant per product lalu push langsung di session ini.
        """
        result.total_parts_processed += 1
        logger.info(
            "[CYCLE %d] PART %d START: %d variant dari %d product",
            cycle_index,
            part_id,
            len(part_variants),
            len({(str(v.shop_id), str(v.external_product_id)) for v in part_variants}),
        )

        snapshot = await inventree.get_stock_snapshot_by_part_id(part_id)
        stok_inventree = float(snapshot.get("stock_available", 0)) if snapshot else 0.0
        reserved_inventree = (
            float(snapshot.get("reserved_inventree", 0)) if snapshot else 0.0
        )
        buffer_effective = (
            int(round(float(snapshot.get("stok_buffer", buffer)))) if snapshot else buffer
        )

        shop_warehouse_map: Dict[str, Optional[str]] = {}
        shop_marketplace_map: Dict[str, str] = {}
        for variant in part_variants:
            shop_key = str(variant.shop_id)
            if shop_key not in shop_warehouse_map:
                shop_warehouse_map[shop_key] = await self._resolve_shop_warehouse(
                    session, shop_key
                )
            if shop_key not in shop_marketplace_map:
                shop_marketplace_map[shop_key] = await self._resolve_marketplace_code(
                    session, shop_key
                )

        # ------------------------------------------------------------------
        # 1. Compute + record report/queue untuk SEMUA variant se-part.
        # ------------------------------------------------------------------
        computed: List[Dict[str, Any]] = []
        for variant in part_variants:
            product = (product_by_key or {}).get(
                (str(variant.shop_id), str(variant.external_product_id))
            )
            product_status_raw = getattr(product, "status", None)
            if product_status_raw is None:
                product_status_value = RecordStatus.ACTIVE.value
            elif hasattr(product_status_raw, "value"):
                product_status_value = product_status_raw.value
            else:
                product_status_value = str(product_status_raw)
            item = await self._process_part_item(
                session=session,
                part_id=part_id,
                variant=variant,
                cycle_index=cycle_index,
                result=result,
                inventree=inventree,
                buffer=buffer,
                stok_inventree=stok_inventree,
                reserved_inventree=reserved_inventree,
                buffer_effective=buffer_effective,
                reserved_mp_map=reserved_mp_map,
                part_reserved_converted_map=part_reserved_converted_map,
                shop_warehouse_map=shop_warehouse_map,
                shop_marketplace_map=shop_marketplace_map,
                product_status_value=product_status_value,
            )
            if item is not None:
                item["product"] = product
                item["part_id"] = part_id
                computed.append(item)

        if not computed:
            return []

        if not push_immediately:
            return computed

        # ------------------------------------------------------------------
        # Direct call compatibility: push langsung per product di session ini.
        # ------------------------------------------------------------------
        push_enabled = settings.stock_sync_push_enabled
        if not push_enabled:
            for item in computed:
                item["queue_entry"].status = "PROCESSED"
                item["queue_entry"].processed_at = datetime.now(timezone.utc)
                result.total_skipped_push += 1
            logger.info(
                "[CYCLE %d] PART %d: push SKIPPED (monitoring mode) untuk %d variant",
                cycle_index, part_id, len(computed),
            )
            return computed

        product_groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        for item in computed:
            key = (
                str(item["marketplace_code"]),
                str(item["shop_db_id"]),
                str(item["product_id"]),
            )
            product_groups.setdefault(key, []).append(item)

        for (_mp, _shop, _pid), items in product_groups.items():
            await self._push_product_group(
                session=session,
                items=items,
                cycle_index=cycle_index,
                result=result,
            )

        return computed

    async def _push_product_group(
        self,
        session: AsyncSession,
        items: List[Dict[str, Any]],
        cycle_index: int,
        result: PipelineCycleResult,
    ) -> None:
        """
        Push stok untuk SATU kelompok produk (seluruh variant dari satu produk
        di toko & marketplace yang sama).

        Logika:
          - variant asli >= 2 -> Format A (batch multi-SKU via update_stock_batch)
          - variant asli == 1 -> Format B (flat per-SKU via update_stock)
          - variant orphan    -> Format B (flat dengan sentinel ID "0")
        """
        if not items:
            return

        first = items[0]
        first_part_id = first.get("part_id", 0)

        await self._execute_product_group_push(
            items=items,
            cycle_index=cycle_index,
            result=result,
            session=session,
        )

        # ------------------------------------------------------------------
        # Finalisasi queue + log berdasarkan hasil push untuk product group ini.
        # ------------------------------------------------------------------
        processed_at_value = datetime.now(timezone.utc)
        for item in items:
            queue_entry = item["queue_entry"]
            push_status = item["push_status"]
            item_part_id = item.get("part_id", first_part_id)

            if push_status == "SKIPPED":
                queue_entry.status = "PROCESSED"
                queue_entry.processed_at = processed_at_value
                result.total_skipped_push += 1
            elif push_status in ("PENDING", "SUCCESS"):
                # PENDING = push dijalankan tanpa exception (status final tak
                # diecho marketplace); SUCCESS = konfirmasi sukses eksplisit.
                queue_entry.status = "PROCESSED"
                queue_entry.processed_at = processed_at_value
                result.total_pushed += 1
            else:  # FAILED
                queue_entry.status = "FAILED"
                queue_entry.processed_at = processed_at_value
                queue_entry.error_message = item["push_error"]
                result.failures.append(
                    f"part={item_part_id} variant={item['variant_id']} "
                    f"sku={item['seller_sku']}: {item['push_error']}"
                )

            if queue_entry.id:
                try:
                    stmt = (
                        update(StockSyncQueueModel)
                        .where(StockSyncQueueModel.id == queue_entry.id)
                        .values(
                            status=queue_entry.status,
                            processed_at=queue_entry.processed_at,
                            error_message=queue_entry.error_message,
                        )
                    )
                    await session.execute(stmt)
                except Exception:
                    pass

            log_entry = StockSyncLogModel(
                marketplace_variant_id=item["variant"].marketplace_variant_id,
                inventree_part_id=item_part_id,
                reference_no=str(queue_entry.id),
                source="POLLING_1H",
                previous_stock=item["previous_stock"],
                target_stock=item["target_stock"],
                final_stock=max(0, int(item["final_stock"])),
                status=push_status if push_status != "PENDING" else "SUCCESS",
                error_message=item["push_error"],
            )
            session.add(log_entry)
            result.total_logged += 1

    async def _execute_product_group_push(
        self,
        items: List[Dict[str, Any]],
        cycle_index: int,
        result: PipelineCycleResult,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Jalankan HTTP push untuk SATU kelompok produk (satu marketplace/shop/
        product) TANPA finalisasi queue/log.

        Hanya meng-update ``item["push_status"]`` / ``push_error`` / ``final_stock``.
        Dipakai bersama oleh push normal (``_push_product_group``) dan pass retry
        di akhir cycle sehingga logika Format A/B tidak terduplikasi.

          - variant asli >= 2 -> Format A (batch multi-SKU via update_stock_batch)
          - variant asli == 1 -> Format B (flat per-SKU via update_stock)
          - variant orphan    -> Format B (flat dengan sentinel ID "0")
        """
        if not items:
            return

        real_variants = [it for it in items if not it["is_orphan"]]
        orphan_items = [it for it in items if it["is_orphan"]]
        first = items[0]

        try:
            if len(real_variants) >= 2:
                # ---------------- Format A (batch multi-SKU) ----------------
                await self._push_format_a(
                    items=real_variants,
                    cycle_index=cycle_index,
                    result=result,
                    session=session,
                )
            elif len(real_variants) == 1:
                # ---------------- Format B (flat per SKU) -------------------
                await self._process_variant_push(
                    item=real_variants[0],
                    cycle_index=cycle_index,
                    result=result,
                    session=session,
                )
            # Orphan (variant_id == product_id) selalu Format B terpisah —
            # id push-nya product_id, tidak boleh tergabung di batch Format A.
            for it in orphan_items:
                await self._process_variant_push(
                    item=it,
                    cycle_index=cycle_index,
                    result=result,
                    session=session,
                )
        except ApisixConfigError as cfg_exc:
            for it in items:
                it["push_status"] = "FAILED"
                it["push_error"] = f"{CONFIG_ERROR_PREFIX}{cfg_exc}"
            logger.warning(
                "[CYCLE %d] Product %s push config error: %s",
                cycle_index, first["product_id"], cfg_exc,
            )
        except httpx.HTTPStatusError as http_exc:
            # Error HTTP dari APISIX/marketplace — simpan response body
            # LENGKAP ke push_error (→ stock_sync_queue.error_message &
            # stock_sync_log.error_message), bukan hanya summary line.
            body_text = _http_error_detail(http_exc)
            for it in items:
                if it["push_status"] == "SUCCESS":
                    continue  # item yang sudah sukses tidak di-override
                it["push_status"] = "FAILED"
                it["push_error"] = body_text
            logger.error(
                "[CYCLE %d] Product %s push HTTP error: %s",
                cycle_index, first["product_id"], body_text,
            )
        except Exception as exc:
            for it in items:
                if it["push_status"] == "SUCCESS":
                    continue  # item yang sudah sukses tidak di-override
                it["push_status"] = "FAILED"
                it["push_error"] = f"{type(exc).__name__}: {exc}"
            logger.exception(
                "[CYCLE %d] Product %s push unexpected error: %s",
                cycle_index, first["product_id"], exc,
            )

    async def _retry_failed_pushes(
        self,
        items: List[Dict[str, Any]],
        cycle_index: int,
        result: PipelineCycleResult,
        session_factory=None,
    ) -> None:
        """
        Pass RETRY di AKHIR cycle untuk queue entry yang berakhir FAILED pada
        cycle ini dengan error retryable (default: HTTP 400).

        Desain (lihat juga docstring module):
          - HANYA entry yang dibuat di cycle ini (``items`` dari inventory cycle)
            dan hanya yang error_message-nya memuat penanda retryable.
          - Retry dilakukan SEKALIGUS di akhir (maks ``stock_sync_max_retry_count``
            attempt), dengan jeda ``stock_sync_retry_backoff_seconds`` sebelum
            tiap pass — bukan saat kegagalan terjadi — supaya rate limit
            marketplace tidak menumpuk dengan push normal.
          - Tiap attempt menaikkan ``retry_count`` dan menulis baris
            ``stock_sync_log`` BARU (audit trail per percobaan).
          - Attempt berhenti untuk entry yang sukses; entry yang habis
            retry_count tetap FAILED (tidak di-retry lagi).
        """
        if not items or not settings.stock_sync_push_enabled:
            return
        if not settings.stock_sync_retry_enabled:
            logger.info("[CYCLE %d] Retry push DISABLED by config", cycle_index)
            return

        max_retries = int(settings.stock_sync_max_retry_count or 0)
        if max_retries <= 0:
            return

        # Kandidat: item cycle ini yang FAILED + error retryable (mis. "400").
        candidates = [
            it for it in items
            if it.get("push_status") == "FAILED"
            and _is_retryable_push_error(it.get("push_error"))
        ]
        if not candidates:
            return

        logger.info(
            "[CYCLE %d] RETRY pass: %d entry FAILED dengan error retryable "
            "(maks %d attempt, backoff %.1fs)",
            cycle_index,
            len(candidates),
            max_retries,
            float(settings.stock_sync_retry_backoff_seconds or 0),
        )

        sf = session_factory or async_session_factory
        backoff = float(settings.stock_sync_retry_backoff_seconds or 0)

        for attempt in range(1, max_retries + 1):
            pending = [
                it for it in candidates
                if it.get("push_status") == "FAILED"
                and int(getattr(it["queue_entry"], "retry_count", 0) or 0) < max_retries
            ]
            if not pending:
                break

            # Jeda SEBELUM pass — inti dari "retry di akhir, bukan saat gagal":
            # push normal sudah selesai, jeda ini mencegah burst ke marketplace.
            if backoff > 0:
                await asyncio.sleep(backoff)

            groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for it in pending:
                key = (
                    str(it.get("marketplace_code")),
                    str(it.get("shop_db_id")),
                    str(it.get("product_id")),
                )
                groups.setdefault(key, []).append(it)

            logger.info(
                "[CYCLE %d] RETRY attempt %d/%d untuk %d entry (%d product group)",
                cycle_index, attempt, max_retries, len(pending), len(groups),
            )

            for group_items in groups.values():
                try:
                    async with sf() as session:
                        await self._retry_product_group(
                            session=session,
                            items=group_items,
                            cycle_index=cycle_index,
                            result=result,
                            attempt=attempt,
                        )
                        await session.commit()
                except Exception as exc:
                    logger.exception(
                        "[CYCLE %d] Retry push failure (product=%s, attempt=%d): %s",
                        cycle_index,
                        group_items[0].get("product_id") if group_items else "unknown",
                        attempt,
                        exc,
                    )

        logger.info(
            "[CYCLE %d] RETRY DONE: attempted=%d succeeded=%d exhausted=%d",
            cycle_index,
            result.total_retry_attempted,
            result.total_retry_succeeded,
            result.total_retry_exhausted,
        )

    async def _retry_product_group(
        self,
        session: AsyncSession,
        items: List[Dict[str, Any]],
        cycle_index: int,
        result: PipelineCycleResult,
        attempt: int,
    ) -> None:
        """Satu attempt retry untuk satu product group + finalisasi queue/log.

        Menulis baris ``stock_sync_log`` BARU tiap attempt (source
        ``POLLING_1H_RETRY``) dan menaikkan ``stock_sync_queue.retry_count``.
        """
        if not items:
            return

        # Reset state in-memory supaya push ulang mengevaluasi dari nol.
        for it in items:
            it["push_status"] = "PENDING"
            it["push_error"] = None

        await self._execute_product_group_push(
            items=items,
            cycle_index=cycle_index,
            result=result,
            session=session,
        )

        max_retries = int(settings.stock_sync_max_retry_count or 0)
        processed_at_value = datetime.now(timezone.utc)

        for item in items:
            queue_entry = item["queue_entry"]
            push_status = item["push_status"]
            queue_entry.retry_count = int(getattr(queue_entry, "retry_count", 0) or 0) + 1
            result.total_retry_attempted += 1

            if push_status in ("PENDING", "SUCCESS"):
                queue_entry.status = "PROCESSED"
                queue_entry.processed_at = processed_at_value
                queue_entry.error_message = None
                result.total_retry_succeeded += 1
                logger.info(
                    "[CYCLE %d] RETRY attempt %d SUCCESS: sku=%s product=%s",
                    cycle_index, queue_entry.retry_count,
                    item["seller_sku"], item["product_id"],
                )
            else:  # FAILED
                queue_entry.status = "FAILED"
                queue_entry.processed_at = processed_at_value
                queue_entry.error_message = item["push_error"]
                if queue_entry.retry_count >= max_retries:
                    result.total_retry_exhausted += 1
                    logger.warning(
                        "[CYCLE %d] RETRY habis (%d/%d) untuk sku=%s product=%s: %s",
                        cycle_index, queue_entry.retry_count, max_retries,
                        item["seller_sku"], item["product_id"],
                        item["push_error"],
                    )

            if queue_entry.id:
                try:
                    stmt = (
                        update(StockSyncQueueModel)
                        .where(StockSyncQueueModel.id == queue_entry.id)
                        .values(
                            status=queue_entry.status,
                            retry_count=queue_entry.retry_count,
                            processed_at=queue_entry.processed_at,
                            error_message=queue_entry.error_message,
                        )
                    )
                    await session.execute(stmt)
                except Exception:
                    pass

            log_entry = StockSyncLogModel(
                marketplace_variant_id=item["variant"].marketplace_variant_id,
                inventree_part_id=item.get("part_id", 0),
                reference_no=str(queue_entry.id),
                source=_retry_source_for(item.get("trigger_source")),
                previous_stock=item["previous_stock"],
                target_stock=item["target_stock"],
                final_stock=max(0, int(item["final_stock"])),
                status=push_status if push_status != "PENDING" else "SUCCESS",
                error_message=item["push_error"],
            )
            session.add(log_entry)
            result.total_logged += 1

    async def _resolve_shop_warehouse(
        self, session: AsyncSession, shop_external_id: str
    ) -> Optional[str]:
        """Resolve per-shop default_warehouse_id (1 query per shop, di-cache
        per kelompok part). None → ApisixClient fallback ke global config."""
        from infrastructure.db.models import MarketplaceShopModel

        row = (
            await session.execute(
                select(MarketplaceShopModel.default_warehouse_id).where(
                    MarketplaceShopModel.external_shop_id == shop_external_id
                )
            )
        ).first()
        if row is None:
            return None
        return (str(row[0]).strip() or None) if row[0] else None

    async def _resolve_marketplace_code(
        self, session: AsyncSession, shop_external_id: str
    ) -> str:
        """Resolve marketplace code ('shopee'/'tiktok') dari DB via JOIN
        marketplace_shop → marketplace (1 query per shop, di-cache per
        kelompok part).

        Ini SOURCE OF TRUTH utk routing APISIX — JANGAN menebak dari string
        external_shop_id: shop TikTok bisa punya ID numerik murni (mis.
        '7494709429666874412') yang secara heuristik terlihat seperti Shopee
        dan membuat request di-route ke adapter yang salah (HTTP 500).
        Fallback ke _marketplace_code_for_shop bila shop tidak ada di DB."""
        from infrastructure.db.models import (
            MarketplaceModel,
            MarketplaceShopModel,
        )

        s = (shop_external_id or "").strip()
        if not s:
            return ""
        row = (
            await session.execute(
                select(MarketplaceModel.code)
                .join(
                    MarketplaceShopModel,
                    MarketplaceShopModel.marketplace_id == MarketplaceModel.id,
                )
                .where(MarketplaceShopModel.external_shop_id == s)
            )
        ).first()
        if row is not None and row[0]:
            return str(row[0]).strip().lower()
        return self._marketplace_code_for_shop(s)

    async def _process_part_item(
        self,
        session: AsyncSession,
        part_id: int,
        variant,
        cycle_index: int,
        result: PipelineCycleResult,
        inventree: InvenTreeClient,
        buffer: int,
        stok_inventree: float,
        reserved_inventree: float,
        buffer_effective: int,
        reserved_mp_map: Optional[Dict[str, int]] = None,
        part_reserved_converted_map: Optional[Dict[int, float]] = None,
        shop_warehouse_map: Optional[Dict[str, Optional[str]]] = None,
        shop_marketplace_map: Optional[Dict[str, str]] = None,
        product_status_value: str = RecordStatus.ACTIVE.value,
    ) -> Optional[Dict[str, Any]]:
        """
        Hitung target_stock untuk 1 variant (dalam kelompok part), tulis report
        row + queue entry (PENDING), dan kembalikan dict item untuk tahap push.

        Returns None bila variant harus di-skip (SKU kosong / pending queue).
        """
        seller_sku = variant.seller_sku or ""
        if not seller_sku.strip():
            return None

        # Defensive guard: variant DELETED tidak boleh masuk report/queue/log.
        if getattr(variant, "status", None) == RecordStatus.DELETED:
            logger.debug(
                "[CYCLE %d] Skipping DELETED variant (sku=%s)", cycle_index, seller_sku
            )
            return None

        # ------------------------------------------------------------------
        # SKU parse + faktor pengali
        # ------------------------------------------------------------------
        try:
            parsed = SkuParser.parse(seller_sku)
        except Exception:
            parsed = None

        if parsed is not None:
            sku_type_value = parsed.sku_type.value
            faktor_pengali = SkuParser.compute_faktor_pengali(
                parsed.sku_type, parsed.conversion_qty
            )
        else:
            # Parse gagal (jarang). conversion_qty di sku_mapping SUDAH berupa
            # FAKTOR pengali — langsung dipakai.
            sku_type_value = "NORMAL"
            faktor_pengali = 1.0

        previous_stock = int(variant.total_available_stock or 0)

        # reserved_mp = SUM(total_reserved_stock) across ALL marketplace variants
        # with the same seller_sku (lintas marketplace). Hot path: peta dihitung
        # sekali per cycle → O(1) lookup. Fallback ke query per-variant bila map
        # tidak diberikan (pemanggilan langsung).
        sku_clean = seller_sku.strip()
        if reserved_mp_map is not None:
            reserved_mp = reserved_mp_map.get(sku_clean, 0)
        else:
            reserved_mp_result = await session.execute(
                select(
                    func.coalesce(func.sum(MarketplaceVariantModel.total_reserved_stock), 0)
                ).where(
                    MarketplaceVariantModel.seller_sku == sku_clean,
                    MarketplaceVariantModel.status != RecordStatus.DELETED.value,
                )
            )
            reserved_mp = reserved_mp_result.scalar() or 0

        # Formula stok_di_update (lihat docstring _run_inventory_cycle):
        #   stok_di_update = (stok_inventree - part_reserved_converted
        #                     - reserved_inventree - buffer) / faktor_pengali
        qty_converted = float(reserved_mp) * faktor_pengali

        if part_reserved_converted_map is not None:
            part_reserved_converted = part_reserved_converted_map.get(part_id, 0.0)
        else:
            # Fallback (pemanggilan langsung/test): pakai qty_converted SKU ini saja.
            part_reserved_converted = qty_converted

        stok_di_update_raw = (
            stok_inventree
            - part_reserved_converted
            - reserved_inventree
            - buffer_effective
        ) / faktor_pengali
        # FLOOR (pembulatan ke bawah) — mencegah overselling; clamp ke 0.
        stok_di_update = math.floor(max(0.0, stok_di_update_raw))
        target_stock = int(stok_di_update)

        # ------------------------------------------------------------------
        # PRODUCT FREEZE (variant DEACTIVATED): product TIDAK muncul lagi di
        # GET products terbaru (di-set SyncEngine). Variant tetap ikut
        # PERHITUNGAN inventory_report (report row tetap ditulis), tapi
        # TIDAK masuk stock_sync_queue, TIDAK di-push ke APISIX, dan TIDAK
        # masuk stock_sync_log.
        # ------------------------------------------------------------------
        variant_status_raw = getattr(variant, "status", None)
        if (
            variant_status_raw == RecordStatus.DEACTIVATED
            or product_status_value == RecordStatus.FREEZE.value
        ):
            report_repo = InventoryReportRepositoryImpl(session)
            await report_repo.create(
                InventoryReport(
                    part_id=part_id,
                    seller_sku=seller_sku,
                    sku_type=sku_type_value,
                    stok_inventree=stok_inventree,
                    buffer=buffer_effective,
                    faktor_pengali=faktor_pengali,
                    reserved_mp=reserved_mp,
                    reserved_inventree=reserved_inventree,
                    qty_converted=qty_converted,
                    stok_di_update=stok_di_update,
                    shop_id=str(variant.shop_id),
                    marketplace_code=(str(variant.shop_id) or "").split("_")[0].upper()
                    if variant.shop_id
                    else None,
                )
            )
            result.total_report_rows += 1
            result.total_skipped_freeze += 1
            logger.info(
                "[CYCLE %d] Variant DEACTIVATED (product FREEZE) — report "
                "ditulis, queue/push/log di-skip: sku=%s product=%s",
                cycle_index,
                seller_sku,
                variant.external_product_id,
            )
            return None

        # ------------------------------------------------------------------
        # Idempotency: skip bila sudah ada queue PENDING utk variant ini.
        # ------------------------------------------------------------------
        if settings.stock_sync_skip_if_pending:
            existing_pending = (
                await session.execute(
                    select(StockSyncQueueModel).where(
                        StockSyncQueueModel.marketplace_variant_id
                        == variant.marketplace_variant_id,
                        StockSyncQueueModel.status == "PENDING",
                    )
                )
            ).scalars().first()
            if existing_pending is not None:
                result.total_skipped_pending += 1
                logger.debug(
                    "[CYCLE %d] Variant dengan pending queue di-skip: sku=%s",
                    cycle_index,
                    seller_sku,
                )
                return None

        # ------------------------------------------------------------------
        # Stok tidak berubah → tidak perlu masuk queue/log/dipush.
        # Report TETAP ditulis (di bawah) agar audit trail formula tetap ada,
        # tapi queue & log tidak dibuat agar tidak meng-genic queue/log sia-sia
        # dan tidak membebani push APISIX.
        # ------------------------------------------------------------------
        # Guard unchanged hanya aktif bila target > 0 (push dengan stock 0
        # tetap bermakna: men-normalisasi stok jadi 0 di marketplace).
        previous_stock = int(variant.total_available_stock or 0)
        if previous_stock == target_stock and target_stock > 0:
            # final_stock = previous_stock (tidak ada perubahan signifikan)
            report_repo = InventoryReportRepositoryImpl(session)
            await report_repo.create(
                InventoryReport(
                    part_id=part_id,
                    seller_sku=seller_sku,
                    sku_type=sku_type_value,
                    stok_inventree=stok_inventree,
                    buffer=buffer_effective,
                    faktor_pengali=faktor_pengali,
                    reserved_mp=reserved_mp,
                    reserved_inventree=reserved_inventree,
                    qty_converted=qty_converted,
                    stok_di_update=stok_di_update,
                    shop_id=str(variant.shop_id),
                    marketplace_code=(str(variant.shop_id) or "").split("_")[0].upper()
                    if variant.shop_id
                    else None,
                )
            )
            result.total_report_rows += 1
            result.total_skipped_unchanged += 1
            logger.info(
                "[CYCLE %d] Stok tidak berubah (previous=target=final=%d) — "
                "report ditulis, queue/log TIDAK dibuat: sku=%s",
                cycle_index,
                target_stock,
                seller_sku,
            )
            return None

        # ------------------------------------------------------------------
        # Report row (InventoryReportModel)
        # ------------------------------------------------------------------
        report_repo = InventoryReportRepositoryImpl(session)
        await report_repo.create(
            InventoryReport(
                part_id=part_id,
                seller_sku=seller_sku,
                sku_type=sku_type_value,
                stok_inventree=stok_inventree,
                buffer=buffer_effective,
                faktor_pengali=faktor_pengali,
                reserved_mp=reserved_mp,
                reserved_inventree=reserved_inventree,
                qty_converted=qty_converted,
                stok_di_update=stok_di_update,
                shop_id=str(variant.shop_id),
                marketplace_code=(str(variant.shop_id) or "").split("_")[0].upper()
                if variant.shop_id
                else None,
            )
        )
        result.total_report_rows += 1

        # ------------------------------------------------------------------
        # Queue entry (PENDING — ditandai PROCESSED/FAILED setelah push)
        # ------------------------------------------------------------------
        queue_entry = StockSyncQueueModel(
            marketplace_variant_id=variant.marketplace_variant_id,
            target_stock=target_stock,
            trigger_source="POLLING_1H",
            status="PENDING",
            retry_count=0,
        )
        session.add(queue_entry)
        await session.flush()
        await session.refresh(queue_entry)
        result.total_queued += 1

        # ------------------------------------------------------------------
        # Identitas push. Variant "orphan" (external_variant_id kosong /
        # == product_id) → id push = "0" (sentinel no-variant, Format B).
        # Product TANPA variant asli di marketplace diwakili model_id "0"
        # (konvensi no-variant APISIX/Shopee); product_id tetap dibawa di
        # path URL /update-stock/{product_id}.
        # ------------------------------------------------------------------
        product_id_value = str(variant.external_product_id or "").strip()
        raw_variant_id = str(variant.external_variant_id or "").strip()
        is_orphan = (not raw_variant_id) or (raw_variant_id == product_id_value)
        variant_id_value = NO_VARIANT_ID if is_orphan else raw_variant_id

        shop_warehouse_id = (shop_warehouse_map or {}).get(str(variant.shop_id))
        variant_warehouse = str(variant.location_id or "").strip() or None
        push_warehouse_id = variant_warehouse or shop_warehouse_id

        return {
            "variant": variant,
            "seller_sku": seller_sku,
            "product_status": product_status_value,
            "product_id": product_id_value,
            "variant_id": variant_id_value,
            "is_orphan": is_orphan,
            "marketplace_code": (shop_marketplace_map or {}).get(
                str(variant.shop_id)
            ) or self._marketplace_code_for_shop(str(variant.shop_id)),
            "shop_db_id": str(variant.shop_id),
            "warehouse_id": push_warehouse_id,
            "target_stock": target_stock,
            "previous_stock": previous_stock,
            "final_stock": previous_stock,
            "push_status": "PENDING",  # resolved saat push / finalisasi
            "push_error": None,
            "queue_entry": queue_entry,
            "queue_entry_id": queue_entry.id,
            "part_id": part_id,
        }

    @staticmethod
    def _marketplace_code_for_shop(shop_external_id: str) -> str:
        """
        Turunkan marketplace code dari external_shop_id.

        Konvensi seed DB: shopee shop pakai external_shop_id numerik murni,
        tiktok shop pakai prefix 'TOKO_TT' / berisi 'tiktok'. Fallback: ''.
        """
        s = (shop_external_id or "").strip().lower()
        if not s:
            return ""
        if "tt" in s or "tiktok" in s:
            return "tiktok"
        if s.isdigit():
            return "shopee"
        if "shopee" in s or "sp" in s:
            return "shopee"
        return ""

    async def _activate_product_if_needed(
        self,
        item: Dict[str, Any],
        cycle_index: int,
        result: PipelineCycleResult,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Auto-activation: product INACTIVE + target_stock > 0 → aktifkan dulu
        via APISIX /update-status sebelum push stock. Kegagalan activation
        TIDAK menghentikan push (perilaku resilient yang sudah ada).

        Bila ``session`` diberikan dan item membawa object ``product``
        (domain entity), status ACTIVE juga dipersist ke DB lokal via
        MarketplaceProductRepositoryImpl.update.
        """
        if item["target_stock"] <= 0:
            return
        if item.get("product_status") != RecordStatus.INACTIVE.value:
            return
        if not item["product_id"]:
            return

        seller_sku = item["seller_sku"]
        product_id_value = item["product_id"]
        marketplace_code = item["marketplace_code"]
        shop_db_id_str = item["shop_db_id"]

        try:
            logger.info(
                "[CYCLE %d] Auto-activating INACTIVE product before stock update: "
                "sku=%s product_id=%s target_stock=%d",
                cycle_index, seller_sku, product_id_value, item["target_stock"],
            )
            activation_response = await self._apisix_client.update_status(
                marketplace=marketplace_code,
                shop_uuid=shop_db_id_str,
                product_id=product_id_value,
                status=RecordStatus.ACTIVE.value,
            )
            result.activation_logs.append(
                {
                    "marketplace_code": marketplace_code,
                    "shop_id": shop_db_id_str,
                    "product_id": product_id_value,
                    "seller_sku": seller_sku,
                    "variant_id": item["variant"].external_variant_id,
                    "target_stock": item["target_stock"],
                    "apisix_response": activation_response,
                }
            )

            # Cek error top-level + nested (raw_response.data.errors[]).
            activation_success = True
            activation_error_msg = None
            if isinstance(activation_response, dict):
                if "error" in activation_response:
                    err_obj = activation_response.get("error") or {}
                    activation_error_msg = (
                        str(err_obj.get("message"))
                        or str(err_obj.get("code"))
                        or "Unknown APISIX activation error"
                    )
                    activation_success = False
                else:
                    raw_resp = activation_response.get("raw_response", {}) or {}
                    if isinstance(raw_resp, dict):
                        data_obj = raw_resp.get("data", {}) or {}
                        errors_list = (
                            data_obj.get("errors", [])
                            if isinstance(data_obj, dict)
                            else []
                        )
                        if errors_list:
                            first_err = (
                                errors_list[0]
                                if isinstance(errors_list, list)
                                else {}
                            )
                            if isinstance(first_err, dict):
                                activation_error_msg = (
                                    str(first_err.get("message"))
                                    or str(first_err.get("code"))
                                    or "Marketplace activation error (unknown)"
                                )
                                activation_success = False

            if activation_success:
                item["product_status"] = RecordStatus.ACTIVE.value
                result.total_activated += 1
                logger.info(
                    "[CYCLE %d] Auto-activation SUCCESS: product %s (sku=%s)",
                    cycle_index, product_id_value, seller_sku,
                )
                # Activation sukses → update status product di DB lokal juga
                # (in-memory object + repo.update) supaya cycle berikutnya
                # tidak meng-activate ulang product yang sama.
                product_obj = item.get("product")
                if product_obj is not None:
                    try:
                        product_obj.status = RecordStatus.ACTIVE
                    except Exception:
                        pass
                    if session is not None:
                        try:
                            product_repo = MarketplaceProductRepositoryImpl(session)
                            await product_repo.update(product_obj)
                            logger.info(
                                "[CYCLE %d] Auto-activation SUCCESS: product %s (sku=%s) "
                                "status -> ACTIVE in local DB",
                                cycle_index, product_id_value, seller_sku,
                            )
                        except Exception as db_exc:
                            logger.warning(
                                "[CYCLE %d] Auto-activation APISIX OK but DB update failed: %s",
                                cycle_index, db_exc,
                            )
            else:
                logger.warning(
                    "[CYCLE %d] Auto-activation FAILED for product %s (sku=%s): %s. "
                    "Proceeding with stock update anyway.",
                    cycle_index, product_id_value, seller_sku, activation_error_msg,
                )
        except ApisixConfigError as cfg_exc:
            logger.warning(
                "[CYCLE %d] Auto-activation config error (proceeding with stock "
                "update): sku=%s product_id=%s err=%s",
                cycle_index, seller_sku, product_id_value, cfg_exc,
            )
        except Exception:
            logger.exception(
                "[CYCLE %d] Auto-activation unexpected error (proceeding with "
                "stock update): sku=%s product_id=%s",
                cycle_index, seller_sku, product_id_value,
            )

    async def _push_format_a(
        self,
        items: List[Dict[str, Any]],
        cycle_index: int,
        result: PipelineCycleResult,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Push SEKALIGUS semua variant asli milik SATU product dalam SATU HTTP
        call Format A (grouped multi-SKU):

            {"skus": [{"id": v1, "inventory": [{"warehouse_id": w, "quantity": n}]}, ...]}

        Pre-condition: semua item satu (marketplace, shop, product_id).
        Bila ada item INACTIVE di grup → auto-activation dijalankan dulu untuk
        product tersebut (sekali per grup), lalu batch dikirim.
        """
        first = items[0]
        marketplace_code = first["marketplace_code"]
        shop_db_id_str = first["shop_db_id"]
        product_id_value = first["product_id"]

        # Auto-activation (sekali per grup product).
        if first.get("product_status") == RecordStatus.INACTIVE.value:
            await self._activate_product_if_needed(first, cycle_index, result, session=session)

        # Activation sukses → semua item grup dianggap aktif (satu product).
        if first.get("product_status") == RecordStatus.ACTIVE.value:
            for it in items:
                it["product_status"] = RecordStatus.ACTIVE.value

        skus_payload = [
            {
                "id": it["variant_id"],
                "quantity": int(it["target_stock"]),
                "warehouse_id": it["warehouse_id"],
            }
            for it in items
        ]

        logger.info(
            "[CYCLE %d] Push Format A batch: marketplace=%s shop=%s product=%s "
            "skus=%d",
            cycle_index,
            marketplace_code,
            shop_db_id_str,
            product_id_value,
            len(skus_payload),
        )

        apisix_response = await self._apisix_client.update_stock_batch(
            marketplace=marketplace_code,
            shop_uuid=shop_db_id_str,
            product_id=product_id_value,
            skus=skus_payload,
            warehouse_id=first["warehouse_id"],
        )
        result.total_push_calls_format_a += 1

        if isinstance(apisix_response, dict) and "error" in apisix_response:
            err_obj = apisix_response.get("error") or {}
            error_msg = (
                str(err_obj.get("message"))
                or str(err_obj.get("code"))
                or "Unknown APISIX marketplace error"
            )
            # Full error envelope di-embed supaya audit trail (stock_sync_queue
            # / stock_sync_log) memuat SELURUH response APISIX.
            error_msg = _format_apisix_error_envelope(error_msg, apisix_response)
            for it in items:
                it["push_status"] = "FAILED"
                it["push_error"] = error_msg
            return

        # Success envelope (atau empty-body neutral SUCCESS). Format A tidak
        # men-echo applied stock — target_stock lokal adalah source of truth.
        for it in items:
            it["push_status"] = "SUCCESS"
            it["final_stock"] = it["target_stock"]

    async def _process_variant_push(
        self,
        item: Dict[str, Any],
        cycle_index: int,
        result: PipelineCycleResult,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Push SATU SKU via Format B (flat) + auto-activation bila perlu.

        Dipakai untuk:
          - product tanpa variant asli (variant_id == product_id),
          - product dengan tepat 1 variant asli.
        """
        seller_sku = item["seller_sku"]
        product_id_value = item["product_id"]
        variant_id_value = item["variant_id"]
        marketplace_code = item["marketplace_code"]
        shop_db_id_str = item["shop_db_id"]

        # Belt-and-suspenders: variant "orphan" (product tanpa variant asli)
        # SELALU di-push dengan id sentinel "0" — bukan product_id (konvensi
        # no-variant marketplace). Normalisasi di sini menutup jalur lama yang
        # masih mengirim item tanpa flag is_orphan yang benar.
        if item.get("is_orphan") and str(variant_id_value).strip() != NO_VARIANT_ID:
            logger.info(
                "[CYCLE %d] Normalisasi push id no-variant: sku=%s product=%s "
                "variant_id=%s -> id=%s",
                cycle_index, seller_sku, product_id_value, variant_id_value,
                NO_VARIANT_ID,
            )
            variant_id_value = NO_VARIANT_ID
            item["variant_id"] = NO_VARIANT_ID

        # Auto-activation: product INACTIVE + stock > 0 → aktifkan dulu.
        await self._activate_product_if_needed(item, cycle_index, result, session=session)

        if not product_id_value:
            item["push_status"] = "FAILED"
            item["push_error"] = (
                "empty external_product_id — cannot route update-stock via APISIX"
            )
            logger.warning(
                "[CYCLE %d] Skip push (no product_id): sku=%s",
                cycle_index, seller_sku,
            )
            return
        if not variant_id_value:
            item["push_status"] = "FAILED"
            item["push_error"] = (
                "empty external_variant_id — cannot route update-stock via APISIX"
            )
            logger.warning(
                "[CYCLE %d] Skip push (no variant_id): sku=%s product=%s",
                cycle_index, seller_sku, product_id_value,
            )
            return

        if not item["warehouse_id"]:
            logger.debug(
                "[CYCLE %d] No per-variant nor per-shop warehouse_id for sku=%s — "
                "will fall back to global config in ApisixClient",
                cycle_index, seller_sku,
            )

        apisix_response = await self._apisix_client.update_stock(
            marketplace=marketplace_code,
            shop_uuid=shop_db_id_str,
            product_id=product_id_value,
            variant_id=variant_id_value,
            stock=item["target_stock"],
            warehouse_id=item["warehouse_id"],
        )
        result.total_push_calls_format_b += 1

        if isinstance(apisix_response, dict) and "error" in apisix_response:
            err_obj = apisix_response.get("error") or {}
            error_msg = (
                str(err_obj.get("message"))
                or str(err_obj.get("code"))
                or "Unknown APISIX marketplace error"
            )
            # Full error envelope di-embed supaya audit trail (stock_sync_queue
            # / stock_sync_log) memuat SELURUH response APISIX.
            error_msg = _format_apisix_error_envelope(error_msg, apisix_response)
            item["push_status"] = "FAILED"
            item["push_error"] = error_msg
            return

        # Success envelope (atau empty-body neutral SUCCESS).
        item["push_status"] = "SUCCESS"
        item["final_stock"] = item["target_stock"]

    async def _process_single_variant(
        self,
        session: AsyncSession,
        marketplace_code: str,
        marketplace_code_value: str,
        shop_db_id_str: str,
        product=None,
        variant=None,
        inventree: Optional[InvenTreeClient] = None,
        buffer: Optional[int] = None,
        cycle_index: int = 1,
        result: Optional[PipelineCycleResult] = None,
        shop_warehouse_id: Optional[str] = None,
        reserved_mp_map: Optional[Dict[str, int]] = None,
        part_reserved_converted_map: Optional[Dict[int, float]] = None,
    ) -> None:
        """
        COMPAT WRAPPER — proses 1 variant (compute + report + queue + push +
        log) persis seperti semula. Inventory cycle GLOBAL tidak lagi memanggil
        method ini per-variant, tapi signature dipertahankan untuk test &
        pemanggilan langsung; push tetap lewat jalur shared Format A/B.

        ``marketplace_code`` / ``marketplace_code_value`` / ``shop_db_id_str``
        meng-override deteksi otomatis dari variant.shop_id.
        """
        assert result is not None, "result (PipelineCycleResult) wajib disediakan"
        assert variant is not None, "variant wajib disediakan"
        inventree = inventree or InvenTreeClient()
        buffer = buffer if buffer is not None else settings.stock_buffer
        part_id = None

        # Mapping lookup untuk part_id variant ini.
        mapping_model = (
            await session.execute(
                select(SKUMappingModel)
                .where(
                    SKUMappingModel.marketplace_variant_id
                    == variant.marketplace_variant_id,
                    SKUMappingModel.status == "ACTIVE",
                )
                .order_by(SKUMappingModel.created_at.asc())
            )
        ).scalars().first()
        if mapping_model is None:
            result.total_skipped_no_part += 1
            logger.debug(
                "[CYCLE %d] Skipping variant (no active mapping): sku=%s",
                cycle_index,
                variant.seller_sku,
            )
            return
        part_id = int(mapping_model.inventree_part_id)
        if part_id <= 0:
            result.total_skipped_no_part += 1
            return

        snapshot = await inventree.get_stock_snapshot_by_part_id(part_id)
        stok_inventree = float(snapshot.get("stock_available", 0)) if snapshot else 0.0
        reserved_inventree = (
            float(snapshot.get("reserved_inventree", 0)) if snapshot else 0.0
        )
        buffer_effective = (
            int(round(float(snapshot.get("stok_buffer", buffer)))) if snapshot else buffer
        )

        shop_warehouse_map = {str(variant.shop_id): shop_warehouse_id}
        # Resolve marketplace code dari DB (source of truth routing APISIX);
        # heuristic fallback dipakai bila shop tidak ada di DB.
        resolved_mp = await self._resolve_marketplace_code(
            session, str(variant.shop_id)
        )
        shop_marketplace_map = {str(variant.shop_id): resolved_mp}
        if product is not None and getattr(product, "status", None) is not None:
            product_status = product.status
            product_status_value = (
                product_status.value
                if hasattr(product_status, "value")
                else str(product_status)
            )
        else:
            product_status_value = RecordStatus.ACTIVE.value

        item = await self._process_part_item(
            session=session,
            part_id=part_id,
            variant=variant,
            cycle_index=cycle_index,
            result=result,
            inventree=inventree,
            buffer=buffer,
            stok_inventree=stok_inventree,
            reserved_inventree=reserved_inventree,
            buffer_effective=buffer_effective,
            reserved_mp_map=reserved_mp_map,
            part_reserved_converted_map=part_reserved_converted_map,
            shop_warehouse_map=shop_warehouse_map,
            shop_marketplace_map=shop_marketplace_map,
            product_status_value=product_status_value,
        )
        if item is None:
            return
        item["product"] = product

        # Override identitas dari pemanggil (kompat lama).
        if marketplace_code:
            item["marketplace_code"] = marketplace_code
        if marketplace_code_value:
            item["marketplace_code"] = marketplace_code_value.lower()
        if shop_db_id_str:
            item["shop_db_id"] = shop_db_id_str
        item["product_status"] = product_status_value

        # ------------------- Push (shared Format A/B path) -------------------
        push_enabled = settings.stock_sync_push_enabled
        if not push_enabled:
            item["push_status"] = "SKIPPED"
            item["push_error"] = (
                "push disabled by config stock_sync_push_enabled=false "
                "(monitoring mode — hasil ada di inventory_report)"
            )
            item["final_stock"] = item["previous_stock"]
            logger.info(
                "[CYCLE %d] Push SKIPPED (monitoring mode): sku=%s target_stock=%d",
                cycle_index, item["seller_sku"], item["target_stock"],
            )
        else:
            try:
                await self._process_variant_push(
                    item=item, cycle_index=cycle_index, result=result
                )
            except ApisixConfigError as cfg_exc:
                item["push_status"] = "FAILED"
                item["push_error"] = f"{CONFIG_ERROR_PREFIX}{cfg_exc}"
                logger.warning(
                    "[CYCLE %d] APISIX update-stock skipped (config): sku=%s err=%s",
                    cycle_index, item["seller_sku"], cfg_exc,
                )
            except httpx.HTTPStatusError as http_exc:
                # Error HTTP dari APISIX/marketplace — simpan response body
                # LENGKAP ke push_error (audit trail stock_sync_queue/log).
                body_text = _http_error_detail(http_exc)
                item["push_status"] = "FAILED"
                item["push_error"] = body_text
                logger.error(
                    "[CYCLE %d] APISIX update-stock HTTP error (sku=%s): %s",
                    cycle_index, item["seller_sku"], body_text,
                )
            except Exception as exc:
                logger.exception(
                    "[CYCLE %d] APISIX update-stock unexpected exception (sku=%s): %s",
                    cycle_index, item["seller_sku"], exc,
                )
                item["push_status"] = "FAILED"
                item["push_error"] = f"{type(exc).__name__}: {exc}"

        # ------------------- Finalisasi queue + log --------------------------
        processed_at_value = datetime.now(timezone.utc)
        queue_entry = item["queue_entry"]
        push_status = item["push_status"]
        if push_status == "SKIPPED":
            queue_entry.status = "PROCESSED"
            queue_entry.processed_at = processed_at_value
            result.total_skipped_push += 1
        elif push_status in ("PENDING", "SUCCESS"):
            queue_entry.status = "PROCESSED"
            queue_entry.processed_at = processed_at_value
            result.total_pushed += 1
        else:  # FAILED
            queue_entry.status = "FAILED"
            queue_entry.processed_at = processed_at_value
            queue_entry.error_message = item["push_error"]
            result.failures.append(
                f"variant={item['variant_id']} sku={item['seller_sku']}: "
                f"{item['push_error']}"
            )

        log_entry = StockSyncLogModel(
            marketplace_variant_id=variant.marketplace_variant_id,
            inventree_part_id=part_id,
            reference_no=str(queue_entry.id),
            source="POLLING_1H",
            previous_stock=item["previous_stock"],
            target_stock=item["target_stock"],
            final_stock=max(0, int(item["final_stock"])),
            status=push_status if push_status != "PENDING" else "SUCCESS",
            error_message=item["push_error"],
        )
        session.add(log_entry)
        await session.flush()
        await session.refresh(log_entry)
        result.total_logged += 1
