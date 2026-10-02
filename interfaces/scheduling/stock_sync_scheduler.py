"""
StockSyncScheduler — background scheduler GLOBAL untuk StockSyncPipelineEngine,
SEKALI per hari pada jam jadwal (`stock_sync_schedule_time`, default 23:00)
di timezone `stock_sync_schedule_timezone` (default Asia/Jakarta).

MODEL GLOBAL (satu job untuk SEMUA marketplace):
- SATU job scheduler menjalankan pipeline cycle lengkap (steps 1-7) yang
  memproses SEMUA shop ACTIVE lintas marketplace dalam SATU cycle.
- Inventory cycle (steps 4-7) memproses variant dikelompokkan per
  inventree_part_id dan diurutkan part_id ASC: semua toko untuk part terkecil
  diselesaikan dulu, baru pindah ke part berikutnya.

Pattern mengikuti ProductPollingManager yang sudah ada di polling_engine.py:
- start_job()              → return StockSyncJobState (tanpa argumen shop)
- get_job(job_id)
- stop_job(job_id)
- stop_all_jobs()
"""

import asyncio
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from core.config import settings
from application.usecases.stock_sync_pipeline import (
    PipelineCycleResult,
    StockSyncPipelineEngine,
)

logger = logging.getLogger(__name__)

# Placeholder identitas job global (satu job untuk semua marketplace/shop).
GLOBAL_MARKETPLACE_CODE = "GLOBAL"
GLOBAL_SHOP_ID = "*"


def parse_schedule_time(schedule_time: str) -> Tuple[int, int]:
    """
    Parse string jadwal ``'HH:MM'`` (format 24 jam) → ``(hour, minute)``.

    Raise ``ValueError`` dengan pesan jelas bila format salah / di luar
    jangkauan (mis. ``'23:99'``, ``'jam 11'``).
    """
    try:
        hour_str, minute_str = schedule_time.strip().split(":", 1)
        hour, minute = int(hour_str), int(minute_str)
    except (AttributeError, ValueError) as exc:
        raise ValueError(
            f"stock_sync_schedule_time harus format 'HH:MM' (mis. '23:00'), "
            f"got {schedule_time!r}"
        ) from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(
            f"stock_sync_schedule_time di luar jangkauan: {schedule_time!r} "
            "(jam 0-23, menit 0-59)"
        )
    return hour, minute


def next_scheduled_run(
    schedule_time: str,
    timezone_name: str,
    now: Optional[datetime] = None,
) -> datetime:
    """
    Waktu UTC berikutnya utk jadwal harian ``'HH:MM'`` di timezone IANA.

    - Sekarang masih sebelum jam tsb (di timezone lokal) → hari ini.
    - Sudah lewat / sama persis                        → besok.

    Returns aware datetime UTC (selisihnya dipakai utk ``asyncio.sleep``).
    Raise ``ValueError`` bila timezone tidak dikenal (butuh package ``tzdata``
    di server Windows) atau format jam salah.
    """
    try:
        tz = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"Timezone jadwal '{timezone_name}' tidak dikenal — pastikan package "
            f"tzdata terinstall (wajib di Windows) atau ubah "
            f"STOCK_SYNC_SCHEDULE_TIMEZONE."
        ) from exc
    hour, minute = parse_schedule_time(schedule_time)
    now_local = (now or datetime.now(timezone.utc)).astimezone(tz)
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


@dataclass
class StockSyncJobState:
    job_id: str
    # Model GLOBAL: satu job utk semua marketplace. Kolom dipertahankan agar
    # respons API tetap kompatibel — nilai selalu GLOBAL / '*'.
    marketplace_code: str = GLOBAL_MARKETPLACE_CODE
    shop_id: str = GLOBAL_SHOP_ID
    status_filter: str = "ALL"
    # Snapshot jadwal harian saat start (observability via API): jam & timezone
    # diambil dari config dan dibekukan di sini supaya perubahan env saat
    # runtime tidak mengubah job yang sedang berjalan.
    schedule_time: Optional[str] = None
    schedule_timezone: Optional[str] = None
    status: str = "queued"  # queued | running | stopping | stopped | error
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    cycles_completed: int = 0
    cycles_failed: int = 0
    last_cycle_at: Optional[datetime] = None
    last_message: Optional[str] = None
    last_cycle: Optional[Dict[str, Any]] = None
    cycles_history: List[Dict[str, Any]] = field(default_factory=list)
    task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.pop("task", None)
        for k in ("started_at", "finished_at", "last_cycle_at"):
            v = payload.get(k)
            if v is not None and hasattr(v, "isoformat"):
                payload[k] = v.isoformat()
        return payload


class StockSyncScheduler:
    """Job manager GLOBAL untuk scheduler stock sync jadwal harian."""

    def __init__(self):
        self._jobs: Dict[str, StockSyncJobState] = {}

    async def start_job(
        self,
        marketplace_code: Optional[str] = None,
        shop_id: Optional[str] = None,
        status_filter: str = "ALL",
    ) -> StockSyncJobState:
        """Memulai SATU scheduler job GLOBAL (semua marketplace).

        Pipeline jalan SEKALI per hari di `stock_sync_schedule_time` (default
        '23:00') pada timezone `stock_sync_schedule_timezone` (default
        Asia/Jakarta). Tidak ada cycle langsung saat start — untuk run langsung
        gunakan endpoint manual /run.

        ``marketplace_code`` / ``shop_id`` diabaikan (compat argumen lama) —
        cycle kini global. Pemanggilan start_job kedua saat job masih running
        → ValueError (idempotency single-job).

        `status_filter` (default 'ALL') diteruskan ke setiap cycle pipeline.
        Semua produk (ACTIVE / INACTIVE / DELETED) tetap di-sync dan mendapat
        update stock — karena produk non-ACTIVE pun stoknya bisa berubah.
        """
        # Idempotency: SATU job global — tolak start kedua selama masih running.
        for existing in self._jobs.values():
            if existing.status == "running":
                raise ValueError(
                    f"Scheduler stock-sync GLOBAL sudah aktif "
                    f"(job_id={existing.job_id}). Stop dulu jika ingin restart."
                )

        # Snapshot + validasi dini jadwal (format jam & timezone) SEBELUM cek DB
        # supaya error config muncul lebih cepat dan tidak membuang roundtrip DB.
        # `next_scheduled_run` memvalidasi keduanya (parse_schedule_time internal
        # + ZoneInfo).
        schedule_time = settings.stock_sync_schedule_time
        schedule_timezone = settings.stock_sync_schedule_timezone
        next_scheduled_run(schedule_time, schedule_timezone)

        # Validasi dini ketersediaan DB/shop (gagal cepat bila DB kosong/tak
        # bisa dijangkau). Job global tidak butuh shop spesifik.
        await self._validate_database()

        job = StockSyncJobState(
            job_id=str(uuid.uuid4()),
            marketplace_code=GLOBAL_MARKETPLACE_CODE,
            shop_id=GLOBAL_SHOP_ID,
            status_filter=status_filter,
            schedule_time=schedule_time,
            schedule_timezone=schedule_timezone,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        logger.info(
            "Started stock-sync scheduler job %s GLOBAL (semua marketplace; "
            "schedule=%s %s harian)",
            job.job_id,
            schedule_time,
            schedule_timezone,
        )
        self._jobs[job.job_id] = job
        job.task = asyncio.create_task(self._run_loop(job.job_id))
        return job

    def get_job(self, job_id: str) -> Optional[StockSyncJobState]:
        return self._jobs.get(job_id)

    def list_jobs(self) -> List[StockSyncJobState]:
        return list(self._jobs.values())

    async def stop_job(self, job_id: str) -> Optional[StockSyncJobState]:
        job = self._jobs.get(job_id)
        if not job:
            return None
        job.status = "stopping"
        job.last_message = "Stop requested"
        if job.task and not job.task.done():
            job.task.cancel()
            try:
                await job.task
            except (asyncio.CancelledError, Exception):
                pass
        job.status = "stopped"
        job.finished_at = datetime.now(timezone.utc)
        return job

    async def stop_all_jobs(self) -> None:
        """Stop semua job (harusnya maksimal satu — job global)."""
        for job_id in list(self._jobs.keys()):
            await self.stop_job(job_id)

    async def _validate_database(self) -> None:
        """
        Validasi dini: marketplace_shop minimal punya satu baris. Bila DB
        kosong / tidak bisa dijangkau → ValueError sebelum task dibuat.
        """
        from infrastructure.db.database import async_session_factory
        from infrastructure.repositories.marketplace_shop_repository_impl import (
            MarketplaceShopRepositoryImpl,
        )

        async with async_session_factory() as session:
            shop_repo = MarketplaceShopRepositoryImpl(session)
            shops = await shop_repo.list_all()
            if not shops:
                raise ValueError(
                    "Tidak ada marketplace_shop di DB — seed data terlebih dahulu."
                )

    async def _run_loop(self, job_id: str) -> None:
        """Loop utama scheduler — jadwal harian tetap.

        Pipeline jalan SEKALI per hari pada jam ``stock_sync_schedule_time``
        (default 23:00) di timezone ``stock_sync_schedule_timezone``. Tidak ada
        cycle langsung saat start (hindari push stok di luar jam terjadwal).
        """
        job = self._jobs.get(job_id)
        if not job:
            return

        pipeline = StockSyncPipelineEngine()
        cycle_index = 0

        try:
            while job.status == "running":
                # Snapshot jam & timezone dari start_job (konsisten — jam tidak
                # berubah di tengah loop meski env diubah runtime).
                tz_name = job.schedule_timezone or settings.stock_sync_schedule_timezone
                schedule_time = job.schedule_time or settings.stock_sync_schedule_time
                next_run = next_scheduled_run(schedule_time, tz_name)
                job.last_message = (
                    f"Daily schedule {schedule_time} ({tz_name}) — "
                    f"next cycle at {next_run.isoformat()}"
                )
                logger.info(
                    "Stock-sync job %s daily schedule %s (%s) — "
                    "sleeping until %s",
                    job.job_id,
                    schedule_time,
                    tz_name,
                    next_run.isoformat(),
                )
                await asyncio.sleep(
                    max(0.0, (next_run - datetime.now(timezone.utc)).total_seconds())
                )
                cycle_index += 1
                await self._safe_run_cycle(pipeline, job, cycle_index)

        except asyncio.CancelledError:
            logger.info("Stock-sync job %s cancelled by stop request", job.job_id)
            job.status = "stopped"
            job.finished_at = datetime.now(timezone.utc)
        except Exception as exc:
            logger.exception("Stock-sync job %s crashed: %s", job.job_id, exc)
            job.status = "error"
            job.finished_at = datetime.now(timezone.utc)
            job.last_message = f"Critical error: {exc}"
        finally:
            # Lepas resource HTTP client APISIX milik pipeline job ini.
            try:
                await pipeline.aclose()
            except Exception as exc:
                logger.warning("Pipeline aclose failed for job %s: %s", job.job_id, exc)
            job.task = None

    async def _safe_run_cycle(
        self,
        pipeline: StockSyncPipelineEngine,
        job: StockSyncJobState,
        cycle_index: int,
    ) -> None:
        """Jalankan 1 cycle GLOBAL — kalau gagal, log tapi jangan stop loop."""
        try:
            cycle_result: PipelineCycleResult = await pipeline.run_once(
                cycle_index=cycle_index,
                status_filter=job.status_filter,
                skip_global_sync=settings.stock_sync_report_only,
            )
            # Serialisasi sekali, dipakai utk last_cycle + history
            cycle_dict = cycle_result.to_dict()
            job.last_cycle = cycle_dict
            job.cycles_history.append(cycle_dict)
            # Keep history manageable
            if len(job.cycles_history) > 50:
                job.cycles_history = job.cycles_history[-50:]

            if cycle_result.failures:
                job.cycles_failed += 1
                job.last_message = f"Cycle {cycle_index} had failures: {cycle_result.failures}"
            else:
                job.cycles_completed += 1
                mode_note = ""
                if settings.stock_sync_report_only:
                    mode_note = " (REPORT-ONLY: sync/mapping skipped)"
                elif not cycle_result.push_enabled:
                    mode_note = " (MONITORING: push disabled)"
                if not cycle_result.push_enabled:
                    job.last_message = (
                        f"Cycle {cycle_index} OK{mode_note}: "
                        f"parts={cycle_result.total_parts_processed} "
                        f"queued={cycle_result.total_queued} "
                        f"skipped_push={cycle_result.total_skipped_push} "
                        f"logged={cycle_result.total_logged} "
                        f"reports={cycle_result.total_report_rows}"
                    )
                else:
                    job.last_message = (
                        f"Cycle {cycle_index} OK{mode_note}: "
                        f"parts={cycle_result.total_parts_processed} "
                        f"queued={cycle_result.total_queued} "
                        f"pushed={cycle_result.total_pushed} "
                        f"formatA={cycle_result.total_push_calls_format_a} "
                        f"formatB={cycle_result.total_push_calls_format_b} "
                        f"logged={cycle_result.total_logged} "
                        f"reports={cycle_result.total_report_rows}"
                    )
            job.last_cycle_at = datetime.now(timezone.utc)
            logger.info("[JOB %s] %s", job.job_id, job.last_message)
        except Exception as exc:
            job.cycles_failed += 1
            job.last_message = f"Cycle {cycle_index} critical error: {exc}"
            logger.exception("[JOB %s] Critical cycle error: %s", job.job_id, exc)


stock_sync_scheduler = StockSyncScheduler()
