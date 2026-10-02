"""
Unit tests: scheduler stock-sync GLOBAL (satu job untuk semua marketplace).

Mencakup:
- ``parse_schedule_time``: format 'HH:MM' valid / invalid.
- ``next_scheduled_run``: waktu UTC berikutnya utk jadwal harian di timezone
  IANA (hari ini vs besok; custom timezone; timezone tidak dikenal → error).
- ``StockSyncScheduler`` (model GLOBAL): start_job tanpa argumen shop,
  idempotency single-job, validasi dini jadwal, pass-through REPORT-ONLY,
  stop semuanya.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from unittest.mock import AsyncMock

from interfaces.scheduling.stock_sync_scheduler import (
    StockSyncJobState,
    StockSyncScheduler,
    next_scheduled_run,
    parse_schedule_time,
)

WIB = timezone(timedelta(hours=7))
UTC = timezone.utc


# ---------------------------------------------------------------------------
# parse_schedule_time
# ---------------------------------------------------------------------------


class TestParseScheduleTime:
    def test_valid_formats(self):
        assert parse_schedule_time("23:00") == (23, 0)
        assert parse_schedule_time("00:00") == (0, 0)
        assert parse_schedule_time("09:30") == (9, 30)
        assert parse_schedule_time(" 21:15 ") == (21, 15)

    @pytest.mark.parametrize(
        "bad",
        ["25:00", "23:99", "abc", "", "11", "11:60", "-1:00", "1:2:3"],
    )
    def test_invalid_formats_raise(self, bad):
        with pytest.raises(ValueError):
            parse_schedule_time(bad)


# ---------------------------------------------------------------------------
# next_scheduled_run
# ---------------------------------------------------------------------------


class TestNextScheduledRun:
    def test_before_schedule_time_returns_today(self):
        """Sekarang 10:00 WIB → next = hari ini 23:00 WIB (= 16:00 UTC)."""
        now = datetime(2026, 8, 12, 10, 0, tzinfo=WIB)
        nxt = next_scheduled_run("23:00", "Asia/Jakarta", now)
        assert nxt == datetime(2026, 8, 12, 16, 0, tzinfo=UTC)

    def test_after_schedule_time_returns_tomorrow(self):
        """Sekarang 23:30 WIB (sudah lewat 23:00) → next = besok 23:00 WIB."""
        now = datetime(2026, 8, 12, 23, 30, tzinfo=WIB)
        nxt = next_scheduled_run("23:00", "Asia/Jakarta", now)
        assert nxt == datetime(2026, 8, 13, 16, 0, tzinfo=UTC)

    def test_exactly_at_schedule_time_returns_tomorrow(self):
        """Sekarang PERSIS 23:00 WIB → next = besok (candidate <= now)."""
        now = datetime(2026, 8, 12, 23, 0, tzinfo=WIB)
        nxt = next_scheduled_run("23:00", "Asia/Jakarta", now)
        assert nxt == datetime(2026, 8, 13, 16, 0, tzinfo=UTC)

    def test_utc_timezone(self):
        """Timezone UTC: jam 09:00 UTC, sekarang 08:00 UTC → hari ini 09:00 UTC."""
        now = datetime(2026, 8, 12, 8, 0, tzinfo=UTC)
        nxt = next_scheduled_run("09:00", "UTC", now)
        assert nxt == datetime(2026, 8, 12, 9, 0, tzinfo=UTC)

    def test_returns_aware_utc(self):
        nxt = next_scheduled_run("23:00", "Asia/Jakarta")
        assert nxt.tzinfo is not None
        assert nxt.utcoffset() == timedelta(0)

    def test_unknown_timezone_raises(self):
        with pytest.raises(ValueError):
            next_scheduled_run("23:00", "Mars/Olympus")


# ---------------------------------------------------------------------------
# StockSyncScheduler — model GLOBAL (satu job utk semua marketplace)
# ---------------------------------------------------------------------------


def _make_job(marketplace_code: str = "GLOBAL", shop_id: str = "*") -> StockSyncJobState:
    return StockSyncJobState(
        job_id=str(uuid.uuid4()),
        marketplace_code=marketplace_code,
        shop_id=shop_id,
        schedule_time="20:15",
        schedule_timezone="Asia/Jakarta",
        status="running",
        started_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_start_job_rejects_invalid_schedule_time(monkeypatch):
    """
    stock_sync_schedule_time invalid ('25:00') → start_job melempar ValueError
    SEBELUM task dibuat (validasi dini jadwal).
    """
    from core.config import settings

    monkeypatch.setattr(settings, "stock_sync_schedule_time", "25:00")

    scheduler = StockSyncScheduler()

    with pytest.raises(ValueError):
        await scheduler.start_job()


@pytest.mark.asyncio
async def test_start_job_rejects_unknown_timezone(monkeypatch):
    """
    Timezone tidak dikenal → ValueError (butuh tzdata di Windows / typo
    timezone).
    """
    from core.config import settings

    monkeypatch.setattr(settings, "stock_sync_schedule_time", "23:00")
    monkeypatch.setattr(settings, "stock_sync_schedule_timezone", "Mars/Olympus")

    scheduler = StockSyncScheduler()

    with pytest.raises(ValueError):
        await scheduler.start_job()


@pytest.mark.asyncio
async def test_start_job_schedule_snapshot_fields(monkeypatch):
    """
    start_job → job.schedule_time & schedule_timezone tersimpan (snapshot
    dari config, observability via API) dan job berstatus GLOBAL.
    """
    from core.config import settings

    monkeypatch.setattr(settings, "stock_sync_schedule_time", "21:30")
    monkeypatch.setattr(settings, "stock_sync_schedule_timezone", "Asia/Makassar")

    scheduler = StockSyncScheduler()
    scheduler._validate_database = AsyncMock()

    job = await scheduler.start_job()
    try:
        assert job.schedule_time == "21:30"
        assert job.schedule_timezone == "Asia/Makassar"
        assert job.marketplace_code == "GLOBAL"
        assert job.shop_id == "*"
        assert job.status == "running"
    finally:
        await scheduler.stop_job(job.job_id)


@pytest.mark.asyncio
async def test_start_job_is_idempotent_single_global_job(monkeypatch):
    """
    start_job kedua saat job global masih running → ValueError (tidak ada
    job dobel). Setelah stop, start lagi boleh.
    """
    from core.config import settings

    monkeypatch.setattr(settings, "stock_sync_schedule_time", "23:00")
    monkeypatch.setattr(settings, "stock_sync_schedule_timezone", "Asia/Jakarta")

    scheduler = StockSyncScheduler()
    scheduler._validate_database = AsyncMock()

    job1 = await scheduler.start_job()
    try:
        with pytest.raises(ValueError):
            await scheduler.start_job()
    finally:
        await scheduler.stop_job(job1.job_id)

    # Setelah stop → boleh start lagi
    job2 = await scheduler.start_job()
    await scheduler.stop_job(job2.job_id)


@pytest.mark.asyncio
async def test_safe_run_cycle_report_only_passes_skip_flag(monkeypatch):
    """
    REPORT-ONLY mode: cycle global dipanggil dengan skip_global_sync=True —
    pipeline hanya generate inventory_report / stock_sync_queue /
    stock_sync_log dari data lama, tanpa sync & mapping.
    """
    from core.config import settings
    from application.usecases.stock_sync_pipeline import PipelineCycleResult

    monkeypatch.setattr(settings, "stock_sync_report_only", True)

    scheduler = StockSyncScheduler()
    job = _make_job()

    pipeline = AsyncMock()
    pipeline.run_once = AsyncMock(
        return_value=PipelineCycleResult(
            cycle_index=1,
            total_report_rows=10,
            total_queued=10,
            total_pushed=10,
            total_logged=10,
        )
    )

    await scheduler._safe_run_cycle(pipeline, job, cycle_index=1)

    _, kwargs = pipeline.run_once.call_args
    assert kwargs["skip_global_sync"] is True
    assert job.cycles_completed == 1
    assert "REPORT-ONLY" in job.last_message


@pytest.mark.asyncio
async def test_safe_run_cycle_full_mode_runs_global_cycle(monkeypatch):
    """
    FULL mode: satu job global menjalankan run_once dengan
    skip_global_sync=False — sync+mapping+inventory SEMUA shop dalam SATU
    cycle, diurutkan per part_id ASC di inventory cycle.
    """
    from core.config import settings
    from application.usecases.stock_sync_pipeline import PipelineCycleResult

    monkeypatch.setattr(settings, "stock_sync_report_only", False)

    scheduler = StockSyncScheduler()
    job = _make_job()

    pipeline = AsyncMock()
    pipeline.run_once = AsyncMock(
        return_value=PipelineCycleResult(
            cycle_index=1,
            total_report_rows=10,
            total_queued=10,
            total_pushed=10,
            total_logged=10,
        )
    )

    await scheduler._safe_run_cycle(pipeline, job, cycle_index=1)

    _, kwargs = pipeline.run_once.call_args
    assert kwargs["skip_global_sync"] is False
    assert job.cycles_completed == 1


@pytest.mark.asyncio
async def test_safe_run_cycle_failure_does_not_stop_loop(monkeypatch):
    """
    Cycle yang gagal (exception) → cycles_failed naik, job tetap 'running',
    loop tidak mati.
    """
    from core.config import settings

    monkeypatch.setattr(settings, "stock_sync_report_only", False)

    scheduler = StockSyncScheduler()
    job = _make_job()

    pipeline = AsyncMock()
    pipeline.run_once = AsyncMock(side_effect=RuntimeError("boom"))

    await scheduler._safe_run_cycle(pipeline, job, cycle_index=1)

    assert job.cycles_failed == 1
    assert job.status == "running"
    assert "boom" in job.last_message


@pytest.mark.asyncio
async def test_stop_all_stops_every_job():
    scheduler = StockSyncScheduler()
    j1 = _make_job()
    j2 = _make_job()
    scheduler._jobs[j1.job_id] = j1
    scheduler._jobs[j2.job_id] = j2

    await scheduler.stop_all_jobs()

    assert j1.status == "stopped"
    assert j2.status == "stopped"
    # Job tetap tercatat di registry (history/observability), status stopped.
    assert len(scheduler.list_jobs()) == 2
    assert all(j.status == "stopped" for j in scheduler.list_jobs())
