import asyncio
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from infrastructure.db.database import async_session_factory
from infrastructure.repositories.marketplace_repository_impl import (
    MarketplaceRepositoryImpl,
)
from infrastructure.repositories.marketplace_shop_repository_impl import (
    MarketplaceShopRepositoryImpl,
)
from app.container import create_mapping_engine, create_sync_engine

logger = logging.getLogger(__name__)


@dataclass
class PollingJobState:
    job_id: str
    marketplace_code: str
    shop_id: str
    duration_minutes: int
    interval_seconds: int
    status: str = "queued"
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    last_message: Optional[str] = None
    last_sync_result: Optional[Dict[str, Any]] = None
    task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)
    mapping_result: Optional[Dict[str, Any]] = None
    report_result: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.pop("task", None)
        if self.started_at:
            payload["started_at"] = self.started_at.isoformat()
        if self.finished_at:
            payload["finished_at"] = self.finished_at.isoformat()
        return payload


class ProductPollingManager:
    def __init__(self):
        self._jobs: dict[str, PollingJobState] = {}

    async def start_job(
        self,
        marketplace_code: str,
        shop_id: str,
        duration_minutes: int = 60,
        interval_seconds: int = 300,
        with_pipeline: bool = False,
    ) -> PollingJobState:
        """
        Start a polling job.

        Args:
            marketplace_code: Marketplace code (e.g. 'shopee')
            shop_id: External shop ID from marketplace
            duration_minutes: How long to poll for
            interval_seconds: Delay between polls
            with_pipeline: If True, after polling completes, also run mapping
        """
        await self._validate_shop(marketplace_code, shop_id)

        job = PollingJobState(
            job_id=str(uuid.uuid4()),
            marketplace_code=marketplace_code.lower(),
            shop_id=shop_id,
            duration_minutes=duration_minutes,
            interval_seconds=interval_seconds,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        self._jobs[job.job_id] = job

        if with_pipeline:
            job.task = asyncio.create_task(self._run_job_with_pipeline(job.job_id))
        else:
            job.task = asyncio.create_task(self._run_job(job.job_id))
        return job

    def get_job(self, job_id: str) -> Optional[PollingJobState]:
        return self._jobs.get(job_id)

    async def _validate_shop(self, marketplace_code: str, shop_id: str) -> None:
        async with async_session_factory() as session:
            marketplace_repo = MarketplaceRepositoryImpl(session)
            shop_repo = MarketplaceShopRepositoryImpl(session)

            marketplace = await marketplace_repo.get_by_code(marketplace_code.lower())
            if not marketplace:
                raise ValueError(f"Marketplace '{marketplace_code}' not found")

            shop = await shop_repo.get_by_external_id(marketplace.id, shop_id)
            if not shop:
                raise ValueError(
                    f"Shop '{shop_id}' not found for marketplace '{marketplace_code}'"
                )

    async def _run_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return

        deadline = datetime.now(timezone.utc) + timedelta(minutes=job.duration_minutes)

        try:
            while datetime.now(timezone.utc) < deadline:
                job.total_runs += 1
                try:
                    result = await self._run_once(job.marketplace_code, job.shop_id)
                    job.successful_runs += 1
                    job.last_sync_result = result
                    job.last_message = result.get("message") or "Sync completed"
                    logger.info(
                        "Polling job %s run %d completed: %s",
                        job.job_id,
                        job.total_runs,
                        job.last_message,
                    )
                except Exception as exc:
                    job.failed_runs += 1
                    job.last_message = str(exc)
                    logger.exception("Polling job %s failed on run %d", job.job_id, job.total_runs)

                remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    break

                sleep_for = min(job.interval_seconds, max(0, remaining))
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
        finally:
            job.status = "finished"
            job.finished_at = datetime.now(timezone.utc)
            job.task = None

    async def _run_job_with_pipeline(self, job_id: str) -> None:
        """
        Run polling for the configured duration, then run mapping.
        (Inventory report generation moved to separate sprint.)
        """
        job = self._jobs.get(job_id)
        if not job:
            return

        try:
            # Step 1: Polling
            await self._run_job(job_id)

            job = self._jobs.get(job_id)
            if not job:
                return

            job.status = "mapping"
            job.last_message = "Polling complete. Running mapping..."

            # Step 2: Run mapping
            try:
                mapping_result = await self._run_mapping(
                    job.marketplace_code, job.shop_id
                )
                job.mapping_result = mapping_result
                job.last_message = f"Mapping complete: {mapping_result.get('total_mapped', 0)} mapped, {mapping_result.get('total_duplicates', 0)} duplicates"
                logger.info(
                    "Pipeline job %s mapping completed: %s",
                    job.job_id,
                    job.last_message,
                )
            except Exception as exc:
                job.mapping_result = {"status": "error", "message": str(exc)}
                job.last_message = f"Mapping failed: {exc}"
                logger.exception("Pipeline job %s mapping failed", job.job_id)

        finally:
            job.status = "finished"
            job.finished_at = datetime.now(timezone.utc)
            job.task = None

    async def _run_once(self, marketplace_code: str, shop_id: str) -> Dict[str, Any]:
        async with async_session_factory() as session:
            marketplace_repo = MarketplaceRepositoryImpl(session)
            shop_repo = MarketplaceShopRepositoryImpl(session)

            marketplace = await marketplace_repo.get_by_code(marketplace_code)
            if not marketplace:
                raise ValueError(f"Marketplace '{marketplace_code}' not found")

            shop = await shop_repo.get_by_external_id(marketplace.id, shop_id)
            if not shop:
                raise ValueError(f"Shop '{shop_id}' not found for marketplace '{marketplace_code}'")

            engine = create_sync_engine(session)
            result = await engine.sync_by_shop_id(shop.id)
            await session.commit()
            return result

    async def _run_mapping(
        self, marketplace_code: str, shop_id: str
    ) -> Dict[str, Any]:
        async with async_session_factory() as session:
            marketplace_repo = MarketplaceRepositoryImpl(session)
            shop_repo = MarketplaceShopRepositoryImpl(session)

            marketplace = await marketplace_repo.get_by_code(marketplace_code)
            if not marketplace:
                raise ValueError(f"Marketplace '{marketplace_code}' not found")

            shop = await shop_repo.get_by_external_id(marketplace.id, shop_id)
            if not shop:
                raise ValueError(f"Shop '{shop_id}' not found")

            engine = create_mapping_engine(session)
            result = await engine.run_mapping_for_shop(shop.id)
            await session.commit()
            return result


polling_manager = ProductPollingManager()