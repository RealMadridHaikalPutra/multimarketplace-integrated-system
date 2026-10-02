"""sku_mapping.conversion_qty: Integer → Float (faktor pengali).

Sebelum: conversion_qty menyimpan qty mentah dari string SKU
        (mis. ``1A1-50x`` → 50, ``1A1-50s`` → 50).

Sesudah: conversion_qty menyimpan FAKTOR PENGALI mengikuti aturan yang sama
        dengan ``faktor_pengali`` di inventory_report:
          SKU_X    (50x)      → 1/qty   = 0.02
          SKU_S    (50s)      → 1.0     (1:1 dengan stok fisik)
          SKU_PACK (Pack-50)  → qty     = 50
          NORMAL   (1A1)      → 1.0

Karena SKU_X menghasilkan pecahan (0.02), tipe kolom diubah ke Float.
Backfill row lama dilakukan dalam migrasi ini (bukan menunggu remap).
"""

revision = "c9f3a1b2d4e5"
down_revision = "8cdb59d8d312"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    # Ubah tipe dulu (Integer → Float) — `1.0 / int` menghasilkan numeric,
    # yang tidak bisa ditulis balik ke kolom Integer (PostgreSQL error).
    op.alter_column(
        "sku_mapping",
        "conversion_qty",
        existing_type=sa.Integer(),
        type_=sa.Float(),
        existing_nullable=False,
    )
    # Backfill nilai yang sudah ada ke faktor pengali.
    op.execute(
        """
        UPDATE sku_mapping
        SET conversion_qty = CASE
            WHEN sku_type = 'SKU_X' AND conversion_qty > 0
                THEN 1.0 / conversion_qty
            WHEN sku_type = 'SKU_PACK'
                THEN conversion_qty
            ELSE 1.0
        END
        """
    )


def downgrade() -> None:
    # NOTE: downgrade ini LOSSY — nilai faktor pecahan (mis. 0.02 utk SKU_X)
    # akan di-round ke Integer (0.02 → 0). Nilai qty mentah asli sudah tidak
    # tersimpan, jadi tidak bisa dipulihkan. Downgrade hanya berguna untuk
    # rollback schema darurat, bukan untuk memulihkan data.
    op.alter_column(
        "sku_mapping",
        "conversion_qty",
        existing_type=sa.Float(),
        type_=sa.Integer(),
        existing_nullable=False,
    )
