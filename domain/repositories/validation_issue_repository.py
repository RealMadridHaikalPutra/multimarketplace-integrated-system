from abc import ABC, abstractmethod
from typing import List, Optional

from domain.entities.validation_issue import ValidationIssue


class ValidationIssueRepository(ABC):
    """Repository interface for the dynamic ``validation_issue`` table."""

    @abstractmethod
    async def upsert_detected(self, issue: ValidationIssue) -> ValidationIssue:
        """Insert a new issue OR dedupe an identical one (natural key).

        Natural key: (check_code, entity_type, entity_id). On re-detection:
        bump ``occurrence_count``, refresh ``last_seen_at``/``updated_at``,
        reopen to OPEN (clear ``resolved_at``), and overwrite mutable columns.
        """
        raise NotImplementedError

    @abstractmethod
    async def mark_resolved(
        self,
        check_code: str,
        entity_type: str,
        active_entity_ids: List[str],
        shop_id: Optional[str] = None,
    ) -> int:
        """Mark RESOLVED all OPEN issues of a check whose entity_id is absent.

        ``shop_id`` scopes the operation for per-shop checks so a run of another
        shop cannot resolve this shop's issues.

        Returns the number of updated rows.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_open(
        self,
        check_code: Optional[str] = None,
        severity: Optional[str] = None,
        shop_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[ValidationIssue]:
        raise NotImplementedError

    @abstractmethod
    async def list_all(
        self,
        status: Optional[str] = None,
        check_code: Optional[str] = None,
        severity: Optional[str] = None,
        shop_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[ValidationIssue]:
        raise NotImplementedError
