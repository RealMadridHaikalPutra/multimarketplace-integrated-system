# Modul: infrastructure/inventory/inventree

## Asal implementasi
- `infrastructure/inventree/client.py` → `infrastructure/inventory/inventree/client.py`.

## Perubahan
- Hanya path package & import `config` yang disesuaikan; logika (cache
  in-memory, index O(1), fallback REST legacy) **tidak diubah**.
- Ditambah export publik di `__init__.py`.

## Alasan
- Menempatkan semua adapter eksternal di bawah `infrastructure/` yang konsisten
  (`marketplace/` untuk APISIX, `inventory/` untuk InvenTree).

## Compatibility
- Method identik: `find_part_by_sku`, `get_stock_by_sku`, `get_stock_by_part_id`,
  `get_all_stock`, `find_part_by_keywords`, `get_stock_snapshot_by_part_id`,
  `get_stock_at_location`, `create_part`, `aclose`.
- Struktur data hasil (`dict`) identik.

## TODO / perbaikan lanjutan
- `create_part` belum terimplementasi (sesuai lama).
- `find_part_by_keywords` & `get_stock_at_location` tampak tidak dipakai jalur
  inti — evaluasi apakah masih diperlukan.
