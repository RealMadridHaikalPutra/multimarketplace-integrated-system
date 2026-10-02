import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.db.models import WebhookLogModel


class WebhookLogRepositoryImpl:
    """Repository untuk membaca dan memperbarui tabel webhook_log."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(
        self,
        marketplace: str,
        signal_code: Optional[str] = None,
        order_id: Optional[str] = None,
        order_status: Optional[str] = None,
        raw_payload: Optional[Dict[str, Any]] = None,
        status: str = "PENDING",
        log_id: Optional[uuid.UUID] = None,
    ) -> WebhookLogModel:
        """Buat entri webhook log baru."""
        model = WebhookLogModel(
            id=log_id or uuid.uuid4(),
            received_at=datetime.now(timezone.utc),
            marketplace=marketplace,
            signal_code=str(signal_code) if signal_code is not None else None,
            order_id=str(order_id) if order_id is not None else None,
            order_status=str(order_status) if order_status is not None else None,
            raw_payload=raw_payload,
            status=status,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return model

    async def get_by_id(self, log_id: uuid.UUID) -> Optional[WebhookLogModel]:
        """Ambil webhook log berdasarkan ID."""
        stmt = select(WebhookLogModel).where(WebhookLogModel.id == log_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def claim_pending_batch(
        self,
        limit: int,
        service_task_id: Optional[str] = None,
    ) -> List[WebhookLogModel]:
        """Claim webhook jobs atomically for one batch service task."""
        if limit <= 0:
            return []

        stmt = (
            select(WebhookLogModel)
            .where(
                WebhookLogModel.status == "PENDING",
                WebhookLogModel.part_ids.is_not(None),
            )
            .order_by(WebhookLogModel.received_at, WebhookLogModel.created_at)
            .limit(int(limit))
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        if not rows:
            return []

        now = datetime.now(timezone.utc)
        for row in rows:
            row.status = "PROCESSED"
            row.service_task_id = service_task_id
            row.updated_at = now
        await self._session.commit()
        return rows

    async def update_order_detail_and_parts(
        self,
        log_id: uuid.UUID,
        order_detail: Optional[Dict[str, Any]],
        part_ids: Optional[List[int]],
        status: Optional[str] = None,
    ) -> None:
        """Update detail order dan resolved part_id setelah diperiksa."""
        values: Dict[str, Any] = {
            "order_detail": order_detail,
            "part_ids": part_ids,
            "updated_at": datetime.now(timezone.utc),
        }
        if status:
            values["status"] = status
        stmt = update(WebhookLogModel).where(WebhookLogModel.id == log_id).values(**values)
        await self._session.execute(stmt)
        await self._session.commit()

    async def update_status(
        self,
        log_id: uuid.UUID,
        status: str,
        error_message: Optional[str] = None,
        processed_at: Optional[datetime] = None,
        service_task_id: Optional[str] = None,
    ) -> None:
        """Update status pemrosesan (PROCESSED, SUCCESS, ON_HOLD)."""
        values: Dict[str, Any] = {
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        }
        if error_message is not None:
            values["error_message"] = error_message
        if processed_at is not None:
            values["processed_at"] = processed_at
        if service_task_id is not None:
            values["service_task_id"] = service_task_id
        stmt = update(WebhookLogModel).where(WebhookLogModel.id == log_id).values(**values)
        await self._session.execute(stmt)
        await self._session.commit()

    async def update_batch_status(
        self,
        log_ids: List[uuid.UUID],
        status: str,
        error_message: Optional[str] = None,
        processed_at: Optional[datetime] = None,
        service_task_id: Optional[str] = None,
    ) -> None:
        """Update status banyak entri webhook log sekaligus."""
        if not log_ids:
            return
        values: Dict[str, Any] = {
            "status": status,
            "updated_at": datetime.now(timezone.utc),
        }
        if error_message is not None:
            values["error_message"] = error_message
        if processed_at is not None:
            values["processed_at"] = processed_at
        if service_task_id is not None:
            values["service_task_id"] = service_task_id
        stmt = (
            update(WebhookLogModel)
            .where(WebhookLogModel.id.in_(log_ids))
            .values(**values)
        )
        await self._session.execute(stmt)
        await self._session.commit()
