"""Create validation_issue table — dynamic error/warning tracking.

Tabel tunggal untuk tracking SEMUA jenis masalah (mapping conflict, SKU
parity, sync failure, push failure, dsb) tanpa perlu tabel baru per kategori:

- Kolom tetap hanya untuk filter/agregasi (check_code, severity, status,
  entity_type/entity_id, occurrence_count, timestamps).
- Payload bebas disimpan di kolom JSONB ``details`` — isi per-check apa saja.
- Dedupe natural key (check_code, entity_type, entity_id): issue yang sama
  terdeteksi ulang menaikkan ``occurrence_count``, bukan menambah baris.

Revision chain:
    8cdb59d8d312 (initial) → c9f3a1b2d4e5 (conversion_qty→Float)
    → d5e6f7a8b9c0 (drop inventory_report.conversion_qty) → e6a5f4b3c2d1 (ini)
"""

revision = "e6a5f4b3c2d1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


def upgrade() -> None:
    op.create_table(
        "validation_issue",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("check_code", sa.String(length=50), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("entity_type", sa.String(length=30), nullable=False),
        sa.Column("entity_id", sa.String(length=100), nullable=False),
        sa.Column("reference", sa.String(length=255), nullable=True),
        sa.Column("details", JSONB(), nullable=False),
        sa.Column("action_taken", sa.String(length=255), nullable=True),
        sa.Column("suggested_action", sa.Text(), nullable=True),
        sa.Column("marketplace_code", sa.String(length=50), nullable=True),
        sa.Column("shop_id", sa.String(length=100), nullable=True),
        sa.Column("occurrence_count", sa.Integer(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "check_code", "entity_type", "entity_id", name="uq_validation_issue_natural_key"
        ),
    )
    op.create_index(
        op.f("ix_validation_issue_check_code"), "validation_issue", ["check_code"], unique=False
    )
    op.create_index(
        op.f("ix_validation_issue_status"), "validation_issue", ["status"], unique=False
    )
    op.create_index(
        op.f("ix_validation_issue_marketplace_code"),
        "validation_issue", ["marketplace_code"], unique=False,
    )
    op.create_index(
        op.f("ix_validation_issue_shop_id"), "validation_issue", ["shop_id"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_validation_issue_shop_id"), table_name="validation_issue")
    op.drop_index(op.f("ix_validation_issue_marketplace_code"), table_name="validation_issue")
    op.drop_index(op.f("ix_validation_issue_status"), table_name="validation_issue")
    op.drop_index(op.f("ix_validation_issue_check_code"), table_name="validation_issue")
    op.drop_table("validation_issue")
