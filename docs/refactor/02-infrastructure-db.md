# Modul: infrastructure/db + repositories

## Asal implementasi
- `infrastructure/database/{base,models,database,seed}.py` → `infrastructure/db/*`.
- `infrastructure/repositories/*_impl.py` → `infrastructure/repositories/*_impl.py`.
- `alembic/` + `alembic.ini` disalin apa adanya.

## Perubahan
- Path package `infrastructure/database` → `infrastructure/db`; import di model
  & session disesuaikan.
- Implementasi repository: hanya import yang berubah (`infrastructure.database.models`
  → `infrastructure.db.models`). Query & semantik **tidak diubah**.
- `MarketplaceProductRepository` mendapat metode abstrak `list_all` (lama hanya
  ada di impl) agar kontrak lengkap.

## Alasan
- Penamaan `db` lebih ringkas & konsisten; memisahkan DB dari adapter eksternal.
- Melengkapi interface agar implementasi tidak "menambah" method diam-diam.

## Compatibility
- **Schema DB tidak berubah**: tabel/kolom/constraint/tipe sama persis.
- Migrations Alembic disalin identik → `alembic upgrade head` menghasilkan schema sama.
- Semantik `upsert_many` tetap **full overwrite** (bukan COALESCE), sesuai
  perilaku nyata project lama (docstring lama menyebut COALESCE, namun kode
  menimpa penuh — yang dipakai adalah perilaku kode).

## TODO / perbaikan lanjutan
- Migration masih sinkron (psycopg2) sedangkan app async (asyncpg) — biarkan
  sesuai project lama; dokumentasikan bila mau disatukan.
- `ValidationIssueRepositoryImpl.upsert_detected` melakukan SELECT tambahan
  setelah upsert (untuk mengembalikan entitas) — dapat dioptimalkan dengan
  `RETURNING`, tetapi ditahan demi paritas.
