"""Drop inventory_report.conversion_qty.

User requirement: kolom conversion_qty di inventory_report DIHAPUS — faktor
pengali sudah diwakili oleh kolom ``faktor_pengali`` yang terpisah, dan qty
mentah SKU tidak lagi perlu disimpan di report (SKU dapat di-parse ulang).

Revision chain:
    8cdb59d8d312 (initial) → c9f3a1b2d4e5 (conversion_qty→Float di sku_mapping)
    → d5e6f7a8b9c0 (ini)
"""

revision = "d5e6f7a8b9c0"
down_revision = "c9f3a1b2d4e5"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.drop_column("inventory_report", "conversion_qty")


def downgrade() -> None:
    op.add_column(
        "inventory_report",
        sa.Column("conversion_qty", sa.Integer(), nullable=False, server_default="1"),
    )
