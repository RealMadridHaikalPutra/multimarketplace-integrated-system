"""
Router untuk scheduler stock-sync GLOBAL (semua marketplace dalam satu cycle).

Flow pipeline per cycle (GLOBAL — bukan per shop):
  1. GET product baru       (ApisixClient) — SEMUA shop ACTIVE
  2. SYNC product           (SyncEngine)   — SEMUA shop ACTIVE
  3. MAPPING product        (MappingEngine)— SEMUA shop ACTIVE
  4. GENERATE report        (InventoryReportModel)
  5. INSERT stock_sync_queue per variant
  6. UPDATE-STOCK via APISIX — diurutkan per part_id ASC:
       - product multi-variant → Format A (1 call batch multi-SKU)
       - product tanpa variant asli / variant tunggal → Format B (flat per SKU)
  7. INSERT stock_sync_log per result

APISIX (Lua side) handles: token lifecycle (auto-refresh), marketplace
routing (Shopee / TikTok adapter), request transformation, response
normalization, and error mapping. The backend only sends unified fields.

Endpoints:
- POST /api/v2/stock-sync/schedule
    -> start scheduler GLOBAL jadwal harian (default 23:00, configurable)
- GET  /api/v2/stock-sync/schedule
    -> list jobs
- GET  /api/v2/stock-sync/schedule/{job_id}
    -> status job
- POST /api/v2/stock-sync/schedule/{job_id}/stop
    -> stop job
- POST /api/v2/stock-sync/run
    -> manual one-shot (jalankan pipeline sekali, tidak membuat scheduler)
- POST /api/v2/stock-sync/run-part
    -> manual per-part BATCH (BODY JSON {"part_id": [1,2], "full_pipeline": true})
       untuk SEMUA part_id dalam satu cycle — dipakai external task Camunda
       serviceWebhookTest sebagai hit-client
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from application.dto.requests import (
    StockSyncJobResponse,
    StockSyncJobCycleResponse,
    StopStockSyncJobResponse,
    StockSyncManualCycleResponse,
    StockSyncManualPartRequest,
    StockSyncManualPartResponse,
    StockSyncPartScopeResponse,
)
from core.config import settings
from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine
from interfaces.scheduling.stock_sync_scheduler import stock_sync_scheduler

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/stock-sync", tags=["Stock Sync Daily Schedule"])


def _cycle_to_response(cycle_dict) -> StockSyncJobCycleResponse:
    if cycle_dict is None:
        return None
    return StockSyncJobCycleResponse(
        cycle_index=cycle_dict.get("cycle_index", 0),
        started_at=cycle_dict.get("started_at"),
        finished_at=cycle_dict.get("finished_at"),
        total_report_rows=cycle_dict.get("total_report_rows", 0),
        total_queued=cycle_dict.get("total_queued", 0),
        total_pushed=cycle_dict.get("total_pushed", 0),
        total_logged=cycle_dict.get("total_logged", 0),
        total_skipped_pending=cycle_dict.get("total_skipped_pending", 0),
        total_skipped_no_part=cycle_dict.get("total_skipped_no_part", 0),
        total_skipped_push=cycle_dict.get("total_skipped_push", 0),
        total_skipped_unchanged=cycle_dict.get("total_skipped_unchanged", 0),
        total_activated=cycle_dict.get("total_activated", 0),
        total_parts_processed=cycle_dict.get("total_parts_processed", 0),
        total_push_calls_format_a=cycle_dict.get("total_push_calls_format_a", 0),
        total_push_calls_format_b=cycle_dict.get("total_push_calls_format_b", 0),
        push_enabled=cycle_dict.get("push_enabled", True),
        total_retry_attempted=cycle_dict.get("total_retry_attempted", 0),
        total_retry_succeeded=cycle_dict.get("total_retry_succeeded", 0),
        total_retry_exhausted=cycle_dict.get("total_retry_exhausted", 0),
        failures=cycle_dict.get("failures", []) or [],
    )


def _scope_to_response(scope) -> StockSyncPartScopeResponse:
    """Peta PartScope (domain) -> DTO respons (endpoint /run-part)."""
    return StockSyncPartScopeResponse(
        part_id=scope.part_id,
        mappings=scope.mappings,
        variant_ids=list(scope.variant_ids),
        product_ids=list(scope.product_ids),
        inactive_mapping_ids=list(scope.inactive_mapping_ids),
        missing_variant_ids=list(scope.missing_variant_ids),
        deleted_variant_ids=list(scope.deleted_variant_ids),
    )


def _job_to_response(job) -> StockSyncJobResponse:
    last_cycle = job.last_cycle
    return StockSyncJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        schedule_time=job.schedule_time,
        schedule_timezone=job.schedule_timezone,
        status=job.status,
        started_at=job.started_at.isoformat() if job.started_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        cycles_completed=job.cycles_completed,
        cycles_failed=job.cycles_failed,
        last_cycle_at=job.last_cycle_at.isoformat() if job.last_cycle_at else None,
        last_message=job.last_message,
        last_cycle=_cycle_to_response(last_cycle),
        cycles_history=[_cycle_to_response(c) for c in (job.cycles_history or [])],
    )


# ----------------------------------------------------------------------
# Scheduler endpoints
# ----------------------------------------------------------------------


@router.post(
    "/schedule",
    response_model=StockSyncJobResponse,
)
async def start_stock_sync_scheduler(
    status_filter: str = Query(
        "ALL",
        description=(
            "Product-level status filter. Default 'ALL' ensures ALL products "
            "(ACTIVE, INACTIVE, DELETED) are synced and get stock updates. "
            "Even INACTIVE products may need inventory pushed. Pass 'ACTIVE' "
            "to restrict to active products only (rare)."
        ),
        pattern="^(ACTIVE|INACTIVE|DELETED|ALL)$",
    ),
):
    """
    Start scheduler GLOBAL (satu job untuk SEMUA marketplace/shop) — jadwal
    harian tetap.

    Pipeline jalan SEKALI per hari di ``STOCK_SYNC_SCHEDULE_TIME`` (default
    '23:00') pada timezone ``STOCK_SYNC_SCHEDULE_TIMEZONE`` (default
    Asia/Jakarta). Tidak ada cycle langsung saat start — utk run langsung
    gunakan endpoint manual /run.

    Query opsional:
    - status_filter: ALL (default) | ACTIVE | INACTIVE | DELETED.

    Kill-switch: jika settings.stock_sync_enabled = False, endpoint return 503.
    """
    if not settings.stock_sync_enabled:
        raise HTTPException(
            status_code=503,
            detail="Stock sync scheduler disabled via stock_sync_enabled=false",
        )
    try:
        job = await stock_sync_scheduler.start_job(status_filter=status_filter)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return _job_to_response(job)


@router.get(
    "/schedule/{job_id}",
    response_model=StockSyncJobResponse,
)
async def get_stock_sync_job(job_id: str):
    job = stock_sync_scheduler.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Stock-sync job '{job_id}' not found")
    return _job_to_response(job)


@router.post(
    "/schedule/{job_id}/stop",
    response_model=StopStockSyncJobResponse,
)
async def stop_stock_sync_job(job_id: str):
    job = await stock_sync_scheduler.stop_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Stock-sync job '{job_id}' not found")
    return StopStockSyncJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        status=job.status,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        last_message=job.last_message,
    )


@router.get(
    "/schedule",
    response_model=List[StockSyncJobResponse],
)
async def list_stock_sync_jobs():
    """Daftar semua stock-sync job (harusnya maksimal satu — job GLOBAL)."""
    jobs = stock_sync_scheduler.list_jobs()
    return [_job_to_response(j) for j in jobs]


# ----------------------------------------------------------------------
# Manual one-shot cycle (test)
# ----------------------------------------------------------------------


@router.post(
    "/run",
    response_model=StockSyncManualCycleResponse,
)
async def manual_stock_sync_run(
    status_filter: str = Query(
        "ALL",
        description=(
            "Product-level status filter. Default 'ALL' ensures ALL products "
            "(ACTIVE, INACTIVE, DELETED) are synced and get stock updates. "
            "Even INACTIVE products still get inventory pushed. Use 'ACTIVE' "
            "to restrict (rare)."
        ),
        pattern="^(ACTIVE|INACTIVE|DELETED|ALL)$",
    ),
    report_only: bool = Query(
        False,
        description=(
            "REPORT-ONLY mode: skip steps 1-3 (GET product / SYNC product / "
            "MAPPING) dan langsung generate inventory_report / stock_sync_queue "
            "/ stock_sync_log memakai data product/variant/mapping yang SUDAH "
            "ADA di DB (versi lama), tanpa menarik data terbaru dari "
            "marketplace. Nilai global dari env STOCK_SYNC_REPORT_ONLY=true "
            "selalu memaksa mode ini terlepas dari parameter."
        ),
    ),
):
    """
    Trigger 1 cycle pipeline GLOBAL langsung (tidak membuat scheduler).
    Berguna untuk testing / ad-hoc run.

    Satu cycle memproses SEMUA shop ACTIVE lintas marketplace:
      - Step 1-2 (sync) mengambil SEMUA product dari APISIX (termasuk INACTIVE),
      - Inventory cycle memproses SEMUA variant ter-mapping, diurutkan per
        part_id ASC,
      - Push step 6: product multi-variant → Format A (batch), product
        tanpa variant asli / single-variant → Format B (flat).

    `report_only=true` → SKIP sync & mapping (pakai data lama di DB), hanya
    generate inventory_report / stock_sync_queue / stock_sync_log.
    """
    try:
        pipeline = StockSyncPipelineEngine()
        result = await pipeline.run_once(
            cycle_index=1,
            status_filter=status_filter,
            skip_global_sync=report_only,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    finally:
        try:
            await pipeline.aclose()
        except Exception:
            pass

    cycle = result.to_dict()
    return StockSyncManualCycleResponse(
        status="error" if result.failures else "success",
        marketplace_code="GLOBAL",
        shop_id="*",
        cycle=_cycle_to_response(cycle),
        message=(
            f"Cycle GLOBAL selesai. Parts={result.total_parts_processed} "
            f"(urut part_id ASC), Queued={result.total_queued}, "
            f"pushed={result.total_pushed} "
            f"(formatA={result.total_push_calls_format_a}, "
            f"formatB={result.total_push_calls_format_b}), "
            f"skipped_push={result.total_skipped_push}, "
            f"logged={result.total_logged}, reports={result.total_report_rows}."
            + (" MODE MONITORING: stock_sync_push_enabled=false, "
               "tidak ada update-status/update-stock ke APISIX."
               if not result.push_enabled else "")
        ),
    )


# ----------------------------------------------------------------------
# Manual per-part run (hit client dari external task Camunda)
# ----------------------------------------------------------------------


@router.post(
    "/run-part",
    response_model=StockSyncManualPartResponse,
)
async def manual_stock_sync_run_part(request: StockSyncManualPartRequest):
    """Jalankan pipeline stok TER-SCOPE untuk SATU ATAU BANYAK part_id (batch).

    Dipakai dua jalur:
      - External service task Camunda (`serviceWebhookTest`) sebagai HIT CLIENT:
        self-call ``POST /api/v2/stock-sync/run-part`` dengan BODY JSON
                ``{"part_id": [1, 2, 3], "full_pipeline": false}`` — banyak part_id
        dalam SATU request (batch) bila ada order masuk.
      - Operator / dashboard untuk menjalankan satu/banyak part secara manual.

    Menerima part_id sebagai ARRAY di request body (bukan query param),
    sehingga jumlahnya tidak terbatas panjang URL.

    Menulis ``inventory_report`` + ``stock_sync_queue`` + ``stock_sync_log``
    untuk variant yang ter-mapping ACTIVE ke part tsb (dan refresh
    ``marketplace_variant.total_reserved_stock`` bila ``full_pipeline=true``).

    Balasan:
      - 200 : ringkasan scope + hasil cycle.
      - 404 : tidak ada variant yang bisa diproses di ``sku_mapping``.
      - 503 : kill-switch ``stock_sync_enabled=false``.
    """
    if not settings.stock_sync_enabled:
        raise HTTPException(
            status_code=503,
            detail="Stock sync disabled via stock_sync_enabled=false",
        )

    # Normalisasi: buang nilai <= 0 dan duplikat, pertahankan urutan masukan.
    full_pipeline = bool(request.full_pipeline)
    unique_parts: List[int] = []
    for raw in request.part_id or []:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in unique_parts:
            unique_parts.append(value)
    if not unique_parts:
        raise HTTPException(
            status_code=422, detail="part_id wajib berupa angka > 0"
        )

    pipeline = StockSyncPipelineEngine()
    try:
        # Scope per part: untuk respons + deteksi part tanpa variant (=404).
        scopes = []
        for pid in unique_parts:
            scopes.append(await pipeline.resolve_part_scope(pid))
        if not any(not scope.is_empty for scope in scopes):
            raise HTTPException(
                status_code=404,
                detail=(
                    f"part_id {unique_parts} tidak punya variant yang bisa "
                    "diproses di sku_mapping"
                ),
            )

        try:
            result = await pipeline.run_once_for_parts(
                unique_parts, full_pipeline=full_pipeline
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
    finally:
        try:
            await pipeline.aclose()
        except Exception:
            pass

    cycle = result.to_dict()
    primary = next((s for s in scopes if not s.is_empty), scopes[0] if scopes else None)
    return StockSyncManualPartResponse(
        status="error" if result.failures else "success",
        run_mode="MANUAL_PART",
        trigger_source="MANUAL_PART",
        log_source="MANUAL_PART",
        part_id=primary.part_id if primary else None,
        scope=_scope_to_response(primary) if primary else None,
        scopes=[_scope_to_response(s) for s in scopes],
        cycle=cycle,
        message=(
            f"Run part selesai. parts={len(unique_parts)} "
            f"queued={result.total_queued}, pushed={result.total_pushed}, "
            f"logged={result.total_logged}, reports={result.total_report_rows}, "
            f"failures={len(result.failures)} (full_pipeline={full_pipeline})."
        ),
    )
