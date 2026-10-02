"""Create webhook_log table.

Mencatat webhook masuk beserta detail order, part_id, dan status pemrosesan
service task (PENDING, PROCESSED, SUCCESS, ON_HOLD).

Revision chain:
    8cdb59d8d312 -> c9f3a1b2d4e5 -> d5e6f7a8b9c0 -> e6a5f4b3c2d1 -> f7b8a9c0d1e2 (ini)
"""

revision = "f7b8a9c0d1e2"
down_revision = "e6a5f4b3c2d1"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


def upgrade() -> None:
    op.create_table(
        "webhook_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("marketplace", sa.String(length=50), nullable=False),
        sa.Column("signal_code", sa.String(length=100), nullable=True),
        sa.Column("order_id", sa.String(length=100), nullable=True),
        sa.Column("order_status", sa.String(length=50), nullable=True),
        sa.Column("raw_payload", JSONB(), nullable=True),
        sa.Column("order_detail", JSONB(), nullable=True),
        sa.Column("part_ids", JSONB(), nullable=True),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="PENDING"),
        sa.Column("service_task_id", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        op.f("ix_webhook_log_received_at"), "webhook_log", ["received_at"], unique=False
    )
    op.create_index(
        op.f("ix_webhook_log_marketplace"), "webhook_log", ["marketplace"], unique=False
    )
    op.create_index(
        op.f("ix_webhook_log_order_id"), "webhook_log", ["order_id"], unique=False
    )
    op.create_index(
        op.f("ix_webhook_log_order_status"), "webhook_log", ["order_status"], unique=False
    )
    op.create_index(
        op.f("ix_webhook_log_status"), "webhook_log", ["status"], unique=False
    )
    op.create_index(
        op.f("ix_webhook_log_service_task_id"), "webhook_log", ["service_task_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_webhook_log_service_task_id"), table_name="webhook_log")
    op.drop_index(op.f("ix_webhook_log_status"), table_name="webhook_log")
    op.drop_index(op.f("ix_webhook_log_order_status"), table_name="webhook_log")
    op.drop_index(op.f("ix_webhook_log_order_id"), table_name="webhook_log")
    op.drop_index(op.f("ix_webhook_log_marketplace"), table_name="webhook_log")
    op.drop_index(op.f("ix_webhook_log_received_at"), table_name="webhook_log")
    op.drop_table("webhook_log")
