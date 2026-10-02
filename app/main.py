import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from interfaces.http.routers import (
    mapping,
    sync,
    marketplaces,
    sync_v2,
    stock_sync,
    webhook,
)
from core.config import settings
from infrastructure.db.database import init_db
from core.logging import configure_logging
from interfaces.scheduling.stock_sync_scheduler import stock_sync_scheduler

logger = logging.getLogger(__name__)


async def _start_auto_stock_sync_job() -> str:
    """
    Auto-start SATU scheduler job GLOBAL stock-sync saat aplikasi start.

    Scheduler murni jadwal harian: cycle jalan SEKALI per hari di
    `stock_sync_schedule_time` (mis. 23:00) pada timezone
    `stock_sync_schedule_timezone` (default Asia/Jakarta). Tidak ada cycle
    langsung saat start.

    Satu cycle memproses SEMUA shop ACTIVE lintas marketplace (sync + mapping
    + inventory cycle global, diurutkan per part_id ASC) — tidak lagi satu job
    per shop.
    """
    try:
        job = await stock_sync_scheduler.start_job()
        logger.info(
            "Auto stock-sync started GLOBAL job %s (semua marketplace; "
            "schedule=%s %s harian)",
            job.job_id,
            job.schedule_time,
            job.schedule_timezone,
        )
        return job.job_id
    except ValueError as exc:
        # Contoh: job sudah running atau DB kosong. Tidak fatal.
        logger.warning("Auto stock-sync skip: %s", exc)
        return ""
    except Exception as exc:
        logger.exception("Auto stock-sync failed to start: %s", exc)
        return ""


async def _start_camunda_external_worker() -> None:
    """Auto-start worker external service task Camunda (bila enabled).

    Worker mengambil task bertipe External dari Camunda (fetchAndLock) lalu
    menjalankannya di backend ini. Di-start HANYA bila
    ``external_tasks.enabled=true`` di infrastructure/camunda/config.json —
    task yang terambil langsung dikerjakan, jadi jangan sampai mengubah
    stok/status sebelum konfigurasi diuji. Endpoint manual
    ``/api/v1/webhook/camunda/external-tasks/*`` tetap bisa dipakai walau false.
    """
    try:
        from infrastructure.camunda.external_task import (
            external_tasks_enabled,
            start_worker,
        )
    except Exception as exc:  # noqa: BLE001 — jangan blokir startup
        logger.warning("Camunda external task worker skip (import gagal): %s", exc)
        return

    if not external_tasks_enabled():
        logger.info(
            "Camunda external task worker DISABLED (external_tasks.enabled=false) "
            "— endpoint manual /api/v1/webhook/camunda/external-tasks/* tetap aktif"
        )
        return

    try:
        worker = await start_worker()
        if worker is not None:
            logger.info(
                "Camunda external task worker started (worker_id=%s, topics=%s)",
                worker.worker_id,
                [topic.name for topic in worker.enabled_topics],
            )
    except Exception as exc:  # noqa: BLE001 — jangan matikan aplikasi
        logger.exception("Camunda external task worker gagal start: %s", exc)


async def _stop_camunda_external_worker() -> None:
    """Hentikan worker external service task Camunda saat shutdown."""
    try:
        from infrastructure.camunda.external_task import stop_worker

        await stop_worker()
        logger.info("Camunda external task worker stopped")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Camunda external task worker stop gagal: %s", exc)


async def _stop_all_stock_sync_jobs() -> None:
    """Stop semua stock-sync job scheduler saat shutdown."""
    jobs = stock_sync_scheduler.list_jobs()
    for job in jobs:
        try:
            await stock_sync_scheduler.stop_job(job.job_id)
            logger.info("Auto stock-sync stopped job %s", job.job_id)
        except Exception as exc:
            logger.warning("Auto stock-sync stop job %s failed: %s", job.job_id, exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan — runs on startup and shutdown."""
    configure_logging()
    logger.info("Starting %s v%s", settings.app_name, settings.app_version)

    # ------------------------------------------------------------------
    # PIPELINE MODE banner — supaya operator langsung tahu mode aktif saat
    # server start:
    #   FULL        → steps 1-3 (GET product / SYNC / MAPPING) jalan normal.
    #   REPORT-ONLY → steps 1-3 di-SKIP; hanya generate inventory_report /
    #                 stock_sync_queue / stock_sync_log dari data lama di DB.
    #   PUSH/MONITORING → apakah update-status/update-stock ke APISIX aktif.
    # ------------------------------------------------------------------
    if settings.stock_sync_report_only:
        logger.warning(
            "PIPELINE MODE: REPORT-ONLY (STOCK_SYNC_REPORT_ONLY=true) — "
            "steps 1-3 (GET product / SYNC / MAPPING) DI-SKIP; pipeline hanya "
            "generate inventory_report / stock_sync_queue / stock_sync_log "
            "memakai data product/variant/mapping yang SUDAH ADA di DB "
            "(versi lama), tanpa menarik data terbaru dari marketplace."
        )
    else:
        logger.info(
            "PIPELINE MODE: FULL — steps 1-3 (GET product / SYNC / MAPPING) "
            "dijalankan normal, lalu generate inventory_report / "
            "stock_sync_queue / stock_sync_log."
        )
    if settings.stock_sync_push_enabled:
        logger.info(
            "PIPELINE PUSH: ENABLED — update-status & update-stock akan "
            "dikirim ke APISIX (marketplace). Inventory cycle GLOBAL: urut "
            "part_id ASC, product multi-variant pakai Format A (batch), "
            "single/no-variant pakai Format B."
        )
    else:
        logger.warning(
            "PIPELINE PUSH: MONITORING (STOCK_SYNC_PUSH_ENABLED=false) — "
            "TIDAK ada update-status/update-stock ke APISIX; hasil hanya "
            "tercatat di inventory_report / stock_sync_queue / stock_sync_log."
        )

    if not settings.stock_sync_enabled:
        logger.warning(
            "STOCK_SYNC_DISABLED — stock_sync_enabled=false; "
            "scheduler endpoint /api/v2/stock-sync/schedule/* akan return 503."
        )
    else:
        logger.info(
            "Stock sync scheduler ENABLED (schedule=%s %s harian, skip_if_pending=%s)",
            settings.stock_sync_schedule_time,
            settings.stock_sync_schedule_timezone,
            settings.stock_sync_skip_if_pending,
        )
    await init_db()
    logger.info("Database tables initialized")
    # Worker external service task Camunda (fetchAndLock) — auto-start bila
    # external_tasks.enabled=true. Aman saat Camunda mati: worker hanya gagal
    # fetch & backoff, tidak mematikan aplikasi.
    if settings.camunda_enabled:
        await _start_camunda_external_worker()
    else:
        logger.info("CAMUNDA_ENABLED=false — worker external task tidak di-start")
    if settings.stock_sync_enabled and settings.stock_sync_auto_enabled:
        job_id = await _start_auto_stock_sync_job()
        if job_id:
            logger.info("Auto stock-sync: GLOBAL job started (%s)", job_id)
        else:
            logger.info("Auto stock-sync: tidak ada job global yang bisa distart")
    elif settings.stock_sync_enabled and not settings.stock_sync_auto_enabled:
        logger.info(
            "Auto stock-sync DISABLED — stock_sync_auto_enabled=false; "
            "pipeline tidak auto-start (manual via /api/v2/stock-sync/run)"
        )
    yield
    if settings.camunda_enabled:
        await _stop_camunda_external_worker()
    await _stop_all_stock_sync_jobs()
    logger.info("Shutting down %s", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ------------------------------------------------------------------
# Middleware
# ------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api_allowed_hosts,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------
# Routers
# ------------------------------------------------------------------
app.include_router(mapping.router)
app.include_router(sync.router)
app.include_router(marketplaces.router)
app.include_router(sync_v2.router)
app.include_router(stock_sync.router)

# Webhook receiver (APISIX marketplace webhooks → Camunda).
if settings.camunda_enabled:
    app.include_router(webhook.router)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": settings.app_name, "version": settings.app_version}
