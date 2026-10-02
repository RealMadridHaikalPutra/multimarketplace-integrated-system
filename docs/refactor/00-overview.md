# Refactor Overview — backend-sinkronisasi-referensi → backend new

Dokumen ini merangkum hasil audit, keputusan arsitektur, pemetaan modul lama→baru,
dan status pengerjaan. Referensi utama: `backend-sinkronisasi-referensi`
(source of truth). Tujuan: perilaku sistem **tidak berubah**.

## Ringkasan sistem lama
FastAPI async + SQLAlchemy 2.0 (asyncpg) + PostgreSQL. Alur inti:
`GET products (APISIX) → SYNC → MAPPING (InvenTree) → VALIDASI → REPORT → QUEUE → PUSH (APISIX) → LOG`,
dijalankan oleh satu job scheduler harian global. Terdapat subsistem
`camunda/` + `api/routers/webhook.py` yang ada + ber-test tetapi **tidak
dimount** dan setting-nya tidak ada di `config.py` (dormant).

## Temuan audit utama (code smells)
1. God-file `synchronization/stock_sync_pipeline.py` (2662 baris).
2. Pelanggaran dependency: `application → infrastructure` (mapping/validation).
3. Ketergantungan global `config.settings` di semua layer.
4. Dormant subsystem `camunda/` (~6479 baris) + `webhook.py` (539) + test-nya.
5. Error handling tidak konsisten (`except Exception: pass`, logging tak seragam).
6. Duplikasi aturan (`_category_family` di 2 file).
7. Hardcode/berisiko (default DB URL, IP LAN, default jadwal vs dokumentasi).
8. Kode legacy/deprecated (marketplace_sku, sku_service, inventory_engine, handlers, sync router).

## Keputusan (dikonfirmasi user)
- Camunda/webhook **ikut direfactor & diaktifkan**.
- Struktur baru: `app/ core/ domain/ application/ infrastructure/ interfaces/`.
- Strategi: **restrukturisasi per modul**, perilaku diutamakan.
- Folder tujuan tetap `backend new`.

## Pemetaan lama → baru
| Lama | Baru | Status |
|---|---|---|
| `config.py` | `core/config.py` (+ semua setting camunda/webhook) | ✅ |
| `logging_config.py` | `core/logging.py` | ✅ |
| `domain/exceptions.py` | `domain/errors.py` (+ `ConfigurationError`) | ✅ |
| `infrastructure/mapping/sku_parser.py` | `domain/rules/sku.py` | ✅ |
| aturan konflik di `mapping_service`/`mapping_validation_service` | `domain/rules/sku_conflict.py` | ✅ |
| formula stok di pipeline | `domain/rules/stock.py` | ✅ |
| `domain/entities/*`, `domain/enums.py`, `domain/repositories/*` | sama (path tetap) | ✅ |
| `infrastructure/database/{base,models,database,seed}.py` | `infrastructure/db/*` | ✅ |
| `infrastructure/repositories/*` | `infrastructure/repositories/*` | ✅ |
| `infrastructure/apisix/client.py` | `infrastructure/marketplace/apisix/{client,pagination,constants}.py` | ✅ |
| `infrastructure/apisix/parsers/*` | `infrastructure/marketplace/apisix/parsers/*` | ✅ |
| `infrastructure/inventree/client.py` | `infrastructure/inventory/inventree/client.py` | ✅ |
| `application/dto/requests.py` | `application/dto/requests.py` | ✅ |
| `application/services/*` | `application/services/*` (dep ke ports) | ✅ |
| — | `application/ports/` (MarketplaceGateway, InventoryProvider) | ✅ |
| `application/events/*` | `application/events/*` | ✅ |
| `application/services/sku_service.py` | `application/services/sku_service.py` (shim deprecated) | ✅ |
| `synchronization/{sync_engine,mapping_engine}.py` | `application/usecases/*` | ✅ |
| `synchronization/stock_sync_pipeline.py` | `application/usecases/stock_sync_pipeline.py` | ✅ |
| `synchronization/{stock_sync_scheduler,polling_engine}.py` | `interfaces/scheduling/*` | ✅ |
| `synchronization/handlers/*` | `application/handlers/*` | ✅ |
| `api/deps.py` | `interfaces/http/deps.py` | ✅ |
| `api/routers/*` | `interfaces/http/routers/*` | ✅ |
| `synchronization/engine_factory.py` | `app/container.py` | ✅ |
| `camunda/*` | `infrastructure/camunda/*` | ✅ (aktif) |
| `api/routers/webhook.py` | `interfaces/http/routers/webhook.py` | ✅ (aktif) |
| `main.py` | `app/main.py` | ✅ |
| `alembic/*`, `alembic.ini` | sama | ✅ |
| `tests/*` | `tests/*` | ✅ |
| `reset_db.py`, `Dockerfile`, `docker-compose.yml`, `README.md` | sama (path diupdate) | ✅ |

## Status
- ✅ Selesai: SELURUH modul diport. `python -m compileall` bersih, import smoke test lulus.
- ✅ Test: **333 passed** (`pytest tests --ignore=tests/test_stock_sync_manual_part.py`).
  File `test_stock_sync_manual_part.py` sengaja dilewati karena **juga gagal collect
  di repo referensi** (DTO `StockSyncManualPartResponse`/`StockSyncPartScopeResponse`
  tidak pernah ada di source manapun — WIP test). Bukan regresi.
- ✅ Perbaikan saat porting: method `ApisixClient.fetch_order_detail` yang dipanggil
  `camunda/service.py` & `task_handlers.py` **belum ada** di client lama sehingga 125
  test error di repo referensi; diimplementasi di proyek baru (kontrak dari test
  `test_camunda_order_detail.py`) sehingga seluruh test lulus.

## Catatan struktur pipeline
`stock_sync_pipeline.py` diport **utuh sebagai satu modul** (perilaku identik).
Pemecahan halus (calc/queue/push/retry/retention) ditandai sebagai TODO lanjutan di
`docs/refactor/06-usecases-interfaces-camunda.md` — belum dilakukan agar risiko
parity minimal.

## Prinsip yang dijaga
- Schema DB **tidak berubah** (models + migrations disalin apa adanya).
- Endpoint tetap kompatibel (DTO & path dipertahankan).
- Business rule dipindah **tanpa** mengubah semantik (faktor pengali, konflik PACK+S/X, formula stok, freeze/stale, retry).
