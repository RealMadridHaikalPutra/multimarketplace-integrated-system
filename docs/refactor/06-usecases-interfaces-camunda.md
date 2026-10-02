# Modul: use cases, events, interfaces (HTTP/scheduling), Camunda & container

Dokumen ini melengkapi `00-overview.md` untuk modul yang diport pada tahap akhir.

## Asal implementasi → tujuan
| Lama | Baru |
|---|---|
| `synchronization/sync_engine.py` | `application/usecases/sync_engine.py` |
| `synchronization/mapping_engine.py` | `application/usecases/mapping_engine.py` |
| `synchronization/stock_sync_pipeline.py` | `application/usecases/stock_sync_pipeline.py` |
| `application/events/*` | `application/events/*` |
| `synchronization/handlers/*` | `application/handlers/*` |
| `synchronization/stock_sync_scheduler.py` | `interfaces/scheduling/stock_sync_scheduler.py` |
| `synchronization/polling_engine.py` | `interfaces/scheduling/polling_engine.py` |
| `api/deps.py` | `interfaces/http/deps.py` |
| `api/routers/*` | `interfaces/http/routers/*` |
| `synchronization/engine_factory.py` | `app/container.py` |
| `main.py` | `app/main.py` |
| `camunda/*` | `infrastructure/camunda/*` |
| `api/routers/webhook.py` | `interfaces/http/routers/webhook.py` |

## Perubahan yang dilakukan

### `app/container.py` (menggantikan `synchronization/engine_factory.py`)
- Satu composition root: `create_sync_engine`, `create_mapping_engine`,
  `create_validation_service`. Semua pemanggil (pipeline, polling, router v2,
  deps) memakai helper yang sama.
- **Adapter konkret hanya dibuat di sini** (`ApisixClient`, `InvenTreeClient`).
  Use case menerima port/kontrak, bukan implementasi.
- `create_sync_engine(session, apisix_client=None)` mempertahankan nama argumen
  lama demi kompatibilitas pemanggil; nilai diteruskan sebagai `gateway=` dan
  `parser_factory=get_parser` ke `SyncEngine` baru.

### `application/usecases/sync_engine.py`
- Bergantung pada port `MarketplaceGateway` & `InventoryProvider`.
- Gateway & `parser_factory` di-*inject* (bukan `import infrastructure.apisix`).
- Logika freeze/stale/conflict/reserved **tidak berubah**.

### `application/usecases/stock_sync_pipeline.py`
- Diport utuh; import diarahkan ke path baru (`core.config`,
  `infrastructure.db`, `infrastructure.marketplace.apisix`,
  `infrastructure.inventory.inventree`, `domain.rules.sku`, `app.container`).
- `report_dir` memakai `Path(__file__).resolve().parents[2] / "e2e-reports"`
  (modul kini 2 level lebih dalam), sehingga output tetap di root proyek.
- **TODO lanjutan (belum):** pecah file ini menjadi modul calc/queue/push-FormatA-B/
  retry/retention. Saat ini dibiarkan utuh untuk menjamin parity.

### `infrastructure/camunda/*` (DIKATIFKAN)
- Diport dengan import diarahkan ke `infrastructure.camunda.*`,
  `core.config`, `infrastructure.marketplace.apisix.client`,
  `infrastructure.db`.
- `PROJECT_ROOT` diperbaiki menjadi `parents[2]` (paket kini di
  `infrastructure/camunda/`) sehingga `camunda_rules_file` &
  `camunda_log_dir` tetap relatif ke root proyek.
- `config.json` & `README.md` disalin ke `infrastructure/camunda/`.

### `infrastructure/marketplace/apisix/client.py`
- **Ditambah** `fetch_order_detail(marketplace, shop_uuid, order_id, translate, timeout)`.
  Method ini dipanggil `camunda/service.py` & `camunda/task_handlers.py` tetapi
  **tidak pernah ada** di client referensi (menyebabkan 125 test error di repo
  lama). Kontrak diambil dari `tests/test_camunda_order_detail.py`:
  `GET {APISIX_ORDER_BASE_URL|APISIX_BASE_URL}{APISIX_ORDER_PATH}` dengan param
  `marketplace`, `shop_uuid`, `ids=<order_id>`, `translate=true|false`;
  **fail-open** (kembalikan `None` saat error / input tak lengkap).
- `CONFIG_ERROR_PREFIX` di-*re-export* dari `constants` agar impor lama tetap jalan.

### `app/main.py`
- Import diarahkan ke path baru (`interfaces.http.routers.*`,
  `infrastructure.db.database`, `core.logging`,
  `interfaces.scheduling.stock_sync_scheduler`).
- Router **webhook diaktifkan** (di-guard `settings.camunda_enabled`).

### `interfaces/http/*`, `interfaces/scheduling/*`, `application/events/*`,
### `application/handlers/*`
- Diport dengan import diarahkan ke `interfaces.http.deps`, `app.container`,
  `application.usecases.*`, `application.handlers.*`, `core.config`.
- Perilaku endpoint & scheduling identik.

## Compatibility
- Semua endpoint/path & DTO tetap sama.
- Constructor `SyncEngine` berubah (internal): `gateway` + `parser_factory`
  (dulu `apisix_client` + parser internal). Test disesuaikan; pemanggil
  produksi memakai `app/container.py`.
- `MappingService`/`MappingValidationService` memakai argumen
  `inventory_provider` (dulu `inventree_client`).

## Verifikasi
- `python -m compileall` bersih.
- Import smoke test untuk seluruh modul lulus.
- `pytest tests --ignore=tests/test_stock_sync_manual_part.py` → **333 passed**
  (repo referensi: 208 passed, 125 error).
