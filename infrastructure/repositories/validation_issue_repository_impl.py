from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.validation_issue import ValidationIssue
from domain.repositories.validation_issue_repository import ValidationIssueRepository
from infrastructure.db.models import ValidationIssueModel


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ValidationIssueRepositoryImpl(ValidationIssueRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def upsert_detected(self, issue: ValidationIssue) -> ValidationIssue:
        now = _now()
        values = {
            "check_code": issue.check_code,
            "severity": issue.severity,
            "status": "OPEN",
            "entity_type": issue.entity_type,
            "entity_id": str(issue.entity_id),
            "reference": issue.reference,
            "details": issue.details or {},
            "action_taken": issue.action_taken,
            "suggested_action": issue.suggested_action,
            "marketplace_code": issue.marketplace_code,
            "shop_id": issue.shop_id,
            "occurrence_count": 1,
            "first_seen_at": now,
            "last_seen_at": now,
            "resolved_at": None,
            "created_at": now,
            "updated_at": now,
        }
        stmt = pg_insert(ValidationIssueModel).values(values)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                ValidationIssueModel.check_code,
                ValidationIssueModel.entity_type,
                ValidationIssueModel.entity_id,
            ],
            set_={
                # Reopen — a reappearing issue becomes active again.
                "status": "OPEN",
                "resolved_at": None,
                "occurrence_count": ValidationIssueModel.occurrence_count + 1,
                "last_seen_at": now,
                "updated_at": now,
                # Mutable columns overwritten with the latest values.
                "severity": excluded.severity,
                "reference": excluded.reference,
                "details": excluded.details,
                "action_taken": excluded.action_taken,
                "suggested_action": excluded.suggested_action,
                "marketplace_code": excluded.marketplace_code,
                "shop_id": excluded.shop_id,
            },
        )
        await self._session.execute(stmt)
        result = await self._session.execute(
            select(ValidationIssueModel).where(
                ValidationIssueModel.check_code == issue.check_code,
                ValidationIssueModel.entity_type == issue.entity_type,
                ValidationIssueModel.entity_id == str(issue.entity_id),
            )
        )
        model = result.scalar_one()
        await self._session.flush()
        return self._to_entity(model)

    async def mark_resolved(
        self,
        check_code: str,
        entity_type: str,
        active_entity_ids: List[str],
        shop_id: Optional[str] = None,
    ) -> int:
        now = _now()
        base_conditions = [
            ValidationIssueModel.check_code == check_code,
            ValidationIssueModel.entity_type == entity_type,
            ValidationIssueModel.status == "OPEN",
        ]
        if shop_id:
            base_conditions.append(ValidationIssueModel.shop_id == shop_id)

        if not active_entity_ids:
            result = await self._session.execute(
                update(ValidationIssueModel)
                .where(*base_conditions)
                .values(status="RESOLVED", resolved_at=now, updated_at=now)
            )
            return result.rowcount or 0

        result = await self._session.execute(
            update(ValidationIssueModel)
            .where(
                *base_conditions,
                ~ValidationIssueModel.entity_id.in_(active_entity_ids),
            )
            .values(status="RESOLVED", resolved_at=now, updated_at=now)
        )
        return result.rowcount or 0

    async def list_open(
        self,
        check_code: Optional[str] = None,
        severity: Optional[str] = None,
        shop_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[ValidationIssue]:
        return await self.list_all(
            status="OPEN",
            check_code=check_code,
            severity=severity,
            shop_id=shop_id,
            limit=limit,
        )

    async def list_all(
        self,
        status: Optional[str] = None,
        check_code: Optional[str] = None,
        severity: Optional[str] = None,
        shop_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[ValidationIssue]:
        query = select(ValidationIssueModel)
        if status:
            query = query.where(ValidationIssueModel.status == status)
        if check_code:
            query = query.where(ValidationIssueModel.check_code == check_code)
        if severity:
            query = query.where(ValidationIssueModel.severity == severity)
        if shop_id:
            query = query.where(ValidationIssueModel.shop_id == shop_id)
        query = query.order_by(
            ValidationIssueModel.last_seen_at.desc(),
            ValidationIssueModel.occurrence_count.desc(),
        ).limit(limit)
        result = await self._session.execute(query)
        return [self._to_entity(m) for m in result.scalars().all()]

    @staticmethod
    def _to_entity(model: ValidationIssueModel) -> ValidationIssue:
        return ValidationIssue(
            id=model.id,
            check_code=model.check_code,
            severity=model.severity,
            status=model.status,
            entity_type=model.entity_type,
            entity_id=model.entity_id,
            reference=model.reference,
            details=model.details or {},
            action_taken=model.action_taken,
            suggested_action=model.suggested_action,
            marketplace_code=model.marketplace_code,
            shop_id=model.shop_id,
            occurrence_count=model.occurrence_count,
            first_seen_at=model.first_seen_at,
            last_seen_at=model.last_seen_at,
            resolved_at=model.resolved_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
