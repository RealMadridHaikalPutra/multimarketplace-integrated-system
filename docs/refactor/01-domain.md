# Modul: domain

## Asal implementasi
- `domain/enums.py`, `domain/entities/*`, `domain/repositories/*` (dari project lama, path tetap).
- `domain/exceptions.py` → `domain/errors.py`.
- `infrastructure/mapping/sku_parser.py` → `domain/rules/sku.py`.
- Aturan konflik (dulu tersebar di `application/services/mapping_service.py`
  dan `mapping_validation_service.py` sebagai `_category_family` + deteksi) →
  `domain/rules/sku_conflict.py`.
- Formula stok (dulu di dalam `synchronization/stock_sync_pipeline.py`) →
  `domain/rules/stock.py`.

## Perubahan
- Lapisan domain kini **murni**: tidak mengimpor infrastructure/config.
- `SkuParser`, faktor pengali, aturan keluarga konflik, deteksi konflik
  produk, dan formula stok diekstrak menjadi fungsi/kelas yang dapat diuji
  tanpa DB/HTTP.
- `detect_product_pack_sx_conflict` menerima daftar `seller_sku` (bukan objek
  `ParsedProduct` infrastructure) → memutus ketergantungan domain→infrastructure.
- Ditambah `ConfigurationError(ValueError)` sebagai error generik client config
  (pengganti peran `ApisixConfigError`; alias tetap ada di adapter).

## Alasan
- Business rule inti (source of truth perilaku) harus independen dan mudah
  dites; sekaligus menghapus duplikasi `_category_family`.
- Mendukung prinsip dependency rule Clean Architecture.

## Compatibility
- Nama entity/field & enum **identik** (dataclass sama).
- Perilaku `SkuParser`/faktor pengali/konflik/formula **identik** dengan lama.
- `domain/errors.py` mempertahankan nama kelas lama (`DomainError`,
  `DuplicateMappingError`, `EntityNotFoundError`, `InvenTreePartNotFoundError`).

## TODO / perbaikan lanjutan
- `SKUMapping` masih membawa kolom hasil JOIN (`shop_id`, `external_product_id`)
  — idealnya dipisah ke read-model, tetapi ditahan agar perilaku & query tetap.
- `marketplace_sku.py` (entity deprecated) sengaja TIDAK diport (tidak dipakai).
