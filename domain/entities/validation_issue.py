from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ValidationIssue:
    """
    Domain entity for ValidationIssue — a dynamically-tracked error/warning row.

    One table tracks ALL problem kinds (mapping conflicts, SKU parity, sync
    failures, ...) without schema changes. See ``infrastructure/db/models.py``
    for the physical schema.
    """

    check_code: str = ""
    severity: str = "WARNING"
    status: str = "OPEN"
    entity_type: str = ""
    entity_id: str = ""
    reference: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    action_taken: Optional[str] = None
    suggested_action: Optional[str] = None
    marketplace_code: Optional[str] = None
    shop_id: Optional[str] = None
    occurrence_count: int = 1
    id: Optional[int] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "check_code": self.check_code,
            "severity": self.severity,
            "status": self.status,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "reference": self.reference,
            "details": self.details,
            "action_taken": self.action_taken,
            "suggested_action": self.suggested_action,
            "marketplace_code": self.marketplace_code,
            "shop_id": self.shop_id,
            "occurrence_count": self.occurrence_count,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "last_seen_at": self.last_seen_at.isoformat() if self.last_seen_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @staticmethod
    def issue_list_to_dicts(issues: List["ValidationIssue"]) -> List[dict]:
        return [i.to_dict() for i in issues]
