# Synchronization Service

Backend FastAPI untuk sinkronisasi produk, SKU, stok InvenTree, Shopee, dan TikTok melalui APISIX. Sistem memiliki dua jalur pipeline yang berbeda:

1. **Global scheduler**: menjalankan pipeline penuh untuk seluruh marketplace dan shop ACTIVE.
2. **Webhook Camunda**: memproses hanya `part_id` yang berasal dari order dan service task Camunda. Beberapa service task dapat digabung menjadi satu batch dan `part_id` yang sama diproses satu kali.

Dokumen ini adalah panduan setup, operasi, alur sistem, testing, dan troubleshooting untuk repository `backend new`.

---

## Daftar Isi

- [Ringkasan Sistem](#ringkasan-sistem)
- [Komponen Eksternal](#komponen-eksternal)
- [Struktur Repository](#struktur-repository)
- [Prasyarat](#prasyarat)
- [Setup Windows](#setup-windows)
- [Konfigurasi Environment](#konfigurasi-environment)
- [Menjalankan Backend](#menjalankan-backend)
- [Database dan Migration](#database-dan-migration)
- [Arsitektur Pipeline](#arsitektur-pipeline)
- [Flow Scheduler Global](#flow-scheduler-global)
- [Flow Webhook Camunda](#flow-webhook-camunda)
- [Batch Service Task](#batch-service-task)
- [Status Webhook](#status-webhook)
- [API Utama](#api-utama)
- [Testing](#testing)
- [E2E Webhook](#e2e-webhook)
- [Observability dan Audit](#observability-dan-audit)
- [Troubleshooting](#troubleshooting)
- [Aturan Perubahan Kode](#aturan-perubahan-kode)

---

## Ringkasan Sistem

Marketplace menyimpan produk dan varian jualan. InvenTree menyimpan stok fisik dan `part_id`. Backend menghubungkan keduanya melalui `sku_mapping`.

```mermaid
flowchart LR
    MP[Shopee / TikTok] <--> AX[APISIX Gateway]
    AX <--> API[FastAPI Backend]
    API <--> DB[(PostgreSQL)]
    API <--> IT[InvenTree]
    API <--> C[Camunda]
    API --> AX
```

### Istilah penting

| Istilah | Arti |
|---|---|
| `marketplace` | Platform, misalnya `shopee` atau `tiktok`. |
| `shop` | Akun toko pada marketplace. |
| `product` | Produk marketplace. |
| `variant` | Varian produk dan seller SKU. |
| `seller_sku` | SKU yang dijual di marketplace. |
| `part_id` | ID part fisik di InvenTree. |
| `sku_mapping` | Mapping variant marketplace ke `part_id`. |
| `reserved stock` | Stok yang sudah terpakai/terreservasi oleh order. |
| `inventory_report` | Hasil kalkulasi stok sebelum push. |
| `stock_sync_queue` | Antrian dan status pekerjaan update stok. |
| `stock_sync_log` | Audit hasil update ke marketplace. |
| `webhook_log` | Audit webhook, detail order, part ID, service task, dan status proses. |
| `serviceWebhookTest` | Topic external service task Camunda untuk webhook order. |

---

## Komponen Eksternal

Backend ini tidak berdiri sendiri. Untuk operasi penuh, komponen berikut harus tersedia:

| Komponen | Fungsi | Contoh |
|---|---|---|
| PostgreSQL | Database aplikasi | `localhost:5432/sync_service` |
| APISIX | Gateway API marketplace dan InvenTree tertentu | `http://localhost:9080` |
| InvenTree | Sumber stok fisik dan lookup SKU ke part | URL internal InvenTree |
| Camunda 7 | Start process, service task, signal | `http://localhost:8080` |
| Backend API | Self-call dari external task ke `/run-part` | `http://127.0.0.1:8000` |

Backend hanya mengirim format unified ke APISIX. Token, routing marketplace, format payload native, dan refresh token dikelola oleh APISIX.

---

## Struktur Repository

```text
app/
  main.py                         # FastAPI app, lifespan, router, worker startup
  container.py                    # Dependency/container aplikasi

core/
  config.py                       # Settings dari .env
  logging.py                      # Konfigurasi logging

domain/
  enums.py                        # RecordStatus dan enum domain
  entities/                       # Entity bisnis
  repositories/                   # Kontrak repository
  rules/                          # Aturan SKU/domain

application/
  dto/requests.py                 # DTO request/response
  services/                       # Mapping, product, variant, validation
  usecases/stock_sync_pipeline.py # Orkestrasi pipeline stok
  usecases/sync_engine.py         # Sinkronisasi product/variant
  usecases/mapping_engine.py      # Mapping SKU ke InvenTree

infrastructure/
  db/                             # SQLAlchemy model, session, seed
  repositories/                   # Implementasi repository SQL
  inventory/inventree/             # Client InvenTree
  marketplace/apisix/              # Client dan parser APISIX
  camunda/                        # Client, rules, worker, handler

interfaces/
  http/routers/                   # Endpoint FastAPI
  scheduling/                     # Scheduler stock sync

alembic/                          # Migration database
tests/                            # Unit/integration test dan runner E2E
main.py                           # Compatibility entrypoint: uvicorn main:app
```

Entrypoint resmi aplikasi adalah `app.main:app`. File root `main.py` adalah compatibility shim agar command `uvicorn main:app` tetap dapat dipakai.

---

## Prasyarat

- Windows 10/11 atau Linux.
- Python 3.11 atau 3.12.
- PostgreSQL dapat diakses.
- APISIX dapat diakses untuk test update.
- Camunda dapat diakses untuk E2E webhook.
- Git dan PowerShell.
- Token marketplace APISIX masih valid.

> Untuk test unit, PostgreSQL, APISIX, InvenTree, dan Camunda tidak selalu diperlukan. Untuk E2E execute, semuanya diperlukan.

---

## Setup Windows

### 1. Masuk ke repository

```powershell
cd "C:\mirorim3\backend new"
```

### 2. Buat virtual environment

```powershell
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
```

Jika PowerShell menolak aktivasi script:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```

### 3. Install dependency

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Buat `.env`

```powershell
Copy-Item .env.example .env
```

Edit `.env` sesuai database, APISIX, InvenTree, dan Camunda yang dipakai.

### 5. Pastikan database tersedia

Contoh PostgreSQL lokal:

```text
Host: localhost
Port: 5432
Database: sync_service
User: postgres
Password: 1234
```

### 6. Jalankan backend

Keduanya valid:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

atau:

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Expected response:

```json
{
  "status": "healthy",
  "service": "Synchronization Service",
  "version": "0.1.0"
}
```

Swagger tersedia di:

```text
http://127.0.0.1:8000/docs
```

---

## Konfigurasi Environment

File `.env.example` adalah template. Jangan commit `.env` karena dapat berisi password, token, atau credential gateway.

### Database

```dotenv
DATABASE_URL=postgresql+asyncpg://postgres:1234@localhost:5432/sync_service
DATABASE_ECHO=false
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20
```

`DATABASE_URL` dipakai SQLAlchemy async runtime. Alembic memakai URL sync dari `alembic.ini` atau konfigurasi yang tersedia di environment.

### Backend

```dotenv
API_HOST=0.0.0.0
API_PORT=8000
BACKEND_BASE_URL=http://127.0.0.1:8000
BACKEND_SELF_CALL_TIMEOUT=30
```

`BACKEND_BASE_URL` sangat penting karena handler Camunda memanggil endpoint `/api/v2/stock-sync/run-part` melalui HTTP.

### APISIX

```dotenv
APISIX_BASE_URL=http://localhost:9080
APISIX_PRODUCTS_PATH=/products
APISIX_ORDER_PATH=/order
APISIX_RESERVED_STOCK_PATH=/order
APISIX_UPDATE_STOCK_PATH=/update-stock
APISIX_UPDATE_STATUS_PATH=/update-status
APISIX_UPDATE_STOCK_TIMEOUT=30
APISIX_UPDATE_STATUS_TIMEOUT=30
```

### Push dan scheduler

```dotenv
STOCK_SYNC_ENABLED=true
STOCK_SYNC_AUTO_ENABLED=true
STOCK_SYNC_PUSH_ENABLED=true
STOCK_SYNC_REPORT_ONLY=false
STOCK_SYNC_SKIP_IF_PENDING=true
STOCK_SYNC_SCHEDULE_TIME=09:41
STOCK_SYNC_SCHEDULE_TIMEZONE=Asia/Jakarta
```

Arti penting:

- `STOCK_SYNC_ENABLED=false`: endpoint stock sync menolak pekerjaan.
- `STOCK_SYNC_PUSH_ENABLED=false`: kalkulasi tetap dibuat, tetapi tidak ada update ke marketplace.
- `STOCK_SYNC_REPORT_ONLY=true`: skip sync/mapping dan hanya memakai data yang sudah ada di DB.
- `STOCK_SYNC_AUTO_ENABLED=true`: scheduler global di-start saat aplikasi startup.
- `STOCK_SYNC_SKIP_IF_PENDING=true`: mencegah pekerjaan bertumpuk jika masih ada queue pending.

Untuk update marketplace nyata, gunakan:

```dotenv
STOCK_SYNC_PUSH_ENABLED=true
STOCK_SYNC_REPORT_ONLY=false
```

### Camunda

```dotenv
CAMUNDA_ENABLED=true
CAMUNDA_BASE_URL=http://localhost:8080
CAMUNDA_VERSION=7
CAMUNDA_REST_PATH=
CAMUNDA_RULES_FILE=infrastructure/camunda/config.json
CAMUNDA_TIMEOUT_SECONDS=10
CAMUNDA_MAX_ATTEMPTS=3
CAMUNDA_TYPED_VARIABLES=true
```

### Webhook

```dotenv
WEBHOOK_LOG_ENABLED=true
WEBHOOK_LOG_DIR=logs/webhook
WEBHOOK_LOG_FILENAME=order_webhook.log
WEBHOOK_ORDER_DETAIL_ENABLED=true
WEBHOOK_ORDER_DETAIL_TIMEOUT=30
WEBHOOK_PART_ID_ENABLED=true
WEBHOOK_RESERVED_ENABLED=true
WEBHOOK_ORDER_STATUS_ALLOWLIST=
```

Biarkan `WEBHOOK_ORDER_STATUS_ALLOWLIST` kosong jika status signal seperti `READY_TO_SHIP`, `ON_HOLD`, atau `CANCELLED` tetap harus diteruskan ke proses yang sudah berjalan.

---

## Database dan Migration

### Inisialisasi otomatis

Saat startup, `app.main` menjalankan `init_db()` dan seed data dasar. `Base.metadata.create_all` membuat tabel yang belum ada.

### Migration normal

```powershell
alembic current
alembic heads
alembic upgrade head
```

Migration terakhir saat dokumentasi ini dibuat adalah migration `webhook_log`.

### Cek tabel webhook

```sql
SELECT to_regclass('public.webhook_log');

SELECT id, order_id, status, part_ids, service_task_id, processed_at
FROM webhook_log
ORDER BY created_at DESC
LIMIT 20;
```

### Reset database

`reset_db.py` bersifat destruktif. Jangan jalankan pada database yang berisi data penting.

```powershell
python reset_db.py
```

Gunakan hanya untuk development/test database.

---

## Arsitektur Pipeline

Ada dua pipeline berbeda dan keduanya tidak boleh disamakan.

### Pipeline global scheduler

Memproses semua shop ACTIVE dan seluruh katalog yang relevan:

```text
GET product marketplace
  -> sync product dan variant
  -> mapping seller_sku ke InvenTree
  -> validation
  -> inventory report global
  -> stock_sync_queue
  -> update-status/update-stock APISIX
  -> stock_sync_log
```

### Pipeline webhook `/run-part`

Memproses hanya part ID dari service task Camunda:

```text
webhook order
  -> detail order
  -> product_id line item
  -> sku_mapping ACTIVE
  -> part_id unik
  -> Camunda service task
  -> POST /api/v2/stock-sync/run-part
  -> inventory cycle untuk part tersebut saja
  -> update marketplace terkait
  -> webhook_log SUCCESS / ON_HOLD
```

`/run-part` menerima array walaupun hanya satu part:

```json
{
  "part_id": [2001549, 3000100, 3000101],
  "full_pipeline": false
}
```

`full_pipeline=false` berarti tidak melakukan sync product dan mapping seluruh marketplace. Data product, variant, dan mapping yang sudah ada digunakan, lalu inventory cycle difilter berdasarkan `part_id`.

---

## Flow Scheduler Global

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant P as StockSyncPipeline
    participant A as APISIX
    participant I as InvenTree
    participant D as PostgreSQL

    S->>P: run_once() global
    P->>A: GET products semua shop ACTIVE
    P->>D: upsert product dan variant
    P->>I: mapping seller SKU ke part
    P->>D: simpan sku_mapping dan validation_issue
    P->>I: ambil snapshot stok per part
    P->>D: tulis inventory_report dan stock_sync_queue
    P->>A: update-stock grouped per product/marketplace
    P->>D: finalisasi queue dan stock_sync_log
```

Scheduler global berjalan pada waktu `STOCK_SYNC_SCHEDULE_TIME`. Aplikasi memulai satu job global saat startup jika `STOCK_SYNC_AUTO_ENABLED=true`.

---

## Flow Webhook Camunda

```mermaid
sequenceDiagram
    participant M as Marketplace/APISIX
    participant B as Backend Webhook
    participant D as PostgreSQL
    participant C as Camunda
    participant W as External Task Worker
    participant P as Scoped Pipeline
    participant A as APISIX Update

    M->>B: POST /api/v1/webhook/order/status
    B->>D: insert webhook_log PENDING
    B->>B: fetch detail order
    B->>D: simpan order_detail + part_ids
    B->>C: start process serviceWebhook
    C-->>W: serviceWebhookTest task
    W->>W: collect task dalam satu poll
    W->>W: merge partId dan deduplicate
    W->>B: POST /api/v2/stock-sync/run-part
    B->>P: run_once_for_parts(part_ids, full_pipeline=false)
    P->>A: update hanya variant/product untuk part tersebut
    P-->>B: cycle result
    B->>D: webhook_log SUCCESS atau ON_HOLD
    W->>C: complete/failure external task
```

### Resolusi `part_id`

1. Webhook membawa `order_id` dan status.
2. Backend mengambil detail order.
3. Detail order berisi line item dan `product_id` marketplace.
4. `product_id` dicocokkan ke product/variant di database.
5. Variant ACTIVE dicocokkan ke `sku_mapping` ACTIVE.
6. Semua `inventree_part_id` dikumpulkan.
7. Duplikasi dibuang sebelum masuk ke Camunda/service task.

Contoh:

```text
Order A -> [2001549, 3000100]
Order B -> [2001549, 3000101]
Order C -> [2001549]

Batch service task -> [2001549, 3000100, 3000101]
```

`2001549` hanya dijalankan satu kali dalam batch tersebut.

---

## Batch Service Task

Konfigurasi berada di [`infrastructure/camunda/config.json`](infrastructure/camunda/config.json):

```json
{
  "external_tasks": {
    "enabled": true,
    "max_tasks": 10,
    "poll_interval_seconds": 2,
    "idle_backoff_seconds": 60,
    "topics": [
      {
        "topic": "serviceWebhookTest",
        "handler": "service_webhook_test",
        "batch": true,
        "variables": ["partId", "webhookLogId", "orderStatus", "e2eTest"]
      }
    ]
  }
}
```

Perilaku worker:

- Saat ada task: fetch dan lock hingga `max_tasks`.
- Task dengan topic `serviceWebhookTest` digabung dalam satu handler call.
- `partId` scalar, JSON string, dan array dinormalisasi.
- Part ID duplikat dibuang dengan urutan kemunculan pertama.
- Semua task tetap di-complete satu per satu ke Camunda setelah satu aksi bisnis batch selesai.
- Semua `webhookLogId` dibawa ke virtual task agar semua row webhook menerima status terminal.
- Saat tidak ada task: worker idle selama `idle_backoff_seconds`, default 60 detik.

Log monitoring:

```text
Camunda serviceWebhook poll: jobs=5 unique_part_ids=[2001549, 3000100]
Camunda external task BATCH merge: 5 task -> partId=[2001549, 3000100]
```

Jangan menjalankan dua worker dengan `worker_id` dan topic yang sama untuk tujuan test yang sama, karena task akan dibagi antar-worker melalui mekanisme lock Camunda.

---

## Status Webhook

| Status | Arti |
|---|---|
| `PENDING` | Webhook sudah diterima dan menunggu detail/service task. |
| `PROCESSED` | Row sudah di-claim oleh batch worker dan sedang diproses. |
| `SUCCESS` | Pipeline scoped selesai tanpa failure. |
| `ON_HOLD` | Tidak dapat selesai otomatis, misalnya mapping kosong, token marketplace expired, APISIX error, atau pipeline failure. |

Contoh query:

```sql
SELECT status, COUNT(*)
FROM webhook_log
GROUP BY status
ORDER BY status;
```

Error `401 refresh_token_expired` dari Shopee harus diperbaiki pada credential/APISIX. Jangan menandainya sebagai sukses secara manual.

---

## API Utama

### Health

```http
GET /health
```

### Webhook order status

```http
POST /api/v1/webhook/order/status?marketplace=shopee
Content-Type: application/json
```

Endpoint menerima payload raw dan mengembalikan response cepat agar APISIX tidak retry karena timeout backend.

### Status external task worker

```http
GET /api/v1/webhook/camunda/external-tasks/status
```

### Poll manual sekali

```http
POST /api/v1/webhook/camunda/external-tasks/poll?limit=1
```

### Start/stop worker

```http
POST /api/v1/webhook/camunda/external-tasks/start
POST /api/v1/webhook/camunda/external-tasks/stop
```

### Run part scoped

```http
POST /api/v2/stock-sync/run-part
Content-Type: application/json
```

Body:

```json
{
  "part_id": [2001549, 3000100],
  "full_pipeline": false
}
```

`full_pipeline=false` adalah mode webhook scoped. `full_pipeline=true` hanya gunakan bila memang ingin sync/mapping product terlebih dahulu untuk kebutuhan manual operator.

### Scheduler stock sync

```http
POST /api/v2/stock-sync/schedule
GET  /api/v2/stock-sync/schedule
GET  /api/v2/stock-sync/schedule/{job_id}
POST /api/v2/stock-sync/schedule/{job_id}/stop
POST /api/v2/stock-sync/run
```

### Camunda config

```http
GET /api/v1/webhook/camunda/config
```

Swagger menyediakan schema dan try-out untuk endpoint lain:

```text
http://127.0.0.1:8000/docs
```

---

## Testing

### Unit dan integration test

Menjalankan seluruh test:

```powershell
pytest -q
```

Test webhook dan Camunda:

```powershell
pytest -q tests/test_webhook_receiver.py
pytest -q tests/test_camunda_external_task.py
pytest -q tests/test_external_task_batch.py
pytest -q tests/test_stock_sync_run_part_endpoint.py
```

Test pipeline manual-part:

```powershell
pytest -q tests/test_stock_sync_manual_part.py
```

Compile tanpa menjalankan server:

```powershell
python -m compileall -q app application core domain infrastructure interfaces tests
```

### Test mode monitoring tanpa push

Gunakan environment berikut jika hanya ingin menguji kalkulasi:

```dotenv
STOCK_SYNC_PUSH_ENABLED=false
```

Pada mode ini report dan log internal dapat dibuat, tetapi APISIX tidak menerima update stock.

---

## E2E Webhook

Runner tersedia di [`tests/run_e2e_webhook.py`](tests/run_e2e_webhook.py).

Runner membaca product, variant, seller SKU, mapping, dan part ID asli dari database. Payload webhook dan detail order dibuat dummy. Runner tidak melakukan truncate product/variant/mapping.

### Dry-run

Tidak mengirim ke Camunda dan tidak mengubah stok:

```powershell
python tests/run_e2e_webhook.py --dry-run --limit 5
```

Filter part tertentu:

```powershell
python tests/run_e2e_webhook.py --dry-run --part-id 2001549
```

### E2E normal

Pastikan backend sudah berjalan:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Lalu di terminal lain:

```powershell
python tests/run_e2e_webhook.py --execute --limit 5
```

Alur normal runner memproses setiap order secara berurutan agar business key dan hasilnya mudah diaudit.

### E2E batch demo

Untuk membuat banyak order sekaligus dan menguji deduplikasi part ID:

```powershell
python tests/run_e2e_webhook.py --execute --batch-demo 10 --limit 5
```

Mode ini:

1. Membuat 10 order dummy dari product nyata.
2. Mengulang sebagian product dan part ID agar terjadi duplikasi.
3. Menghentikan worker backend yang sedang berjalan agar tidak berebut task.
4. Mengirim semua process ke Camunda.
5. Mengambil task dengan worker test.
6. Menjalankan satu batch handler.
7. Mengirim satu array part ID unik ke `/run-part`.
8. Menyalakan kembali worker backend.
9. Mencetak status semua `webhook_log` dan stock result.

Contoh kombinasi filter:

```powershell
python tests/run_e2e_webhook.py --execute --batch-demo 6 --marketplace shopee --limit 5
python tests/run_e2e_webhook.py --execute --batch-demo 5 --part-id 2001549
```

### Syarat E2E execute

- PostgreSQL hidup.
- Backend aktif di `BACKEND_BASE_URL`.
- Camunda reachable.
- Process definition `serviceWebhook` sudah deploy.
- BPMN memiliki external service task topic `serviceWebhookTest`.
- APISIX reachable.
- Token marketplace valid.
- `STOCK_SYNC_ENABLED=true`.
- `STOCK_SYNC_PUSH_ENABLED=true` jika update nyata diinginkan.
- `STOCK_SYNC_REPORT_ONLY=false` jika pipeline tidak boleh report-only.

Perintah E2E dapat mengubah stok marketplace nyata. Jalankan dry-run lebih dahulu.

---

## Audit dan Observability

### File log

Lokasi umum:

```text
logs/webhook/order_webhook.log
logs/camunda/camunda_trigger.log
e2e-reports/
```

### Audit database

```sql
SELECT id, order_id, marketplace, status, part_ids,
       service_task_id, error_message, processed_at
FROM webhook_log
ORDER BY created_at DESC
LIMIT 50;
```

```sql
SELECT inventree_part_id, marketplace_variant_id,
       source, status, error_message, created_at
FROM stock_sync_log
ORDER BY created_at DESC
LIMIT 50;
```

```sql
SELECT marketplace_variant_id, target_stock,
       trigger_source, status, retry_count, error_message
FROM stock_sync_queue
ORDER BY created_at DESC
LIMIT 50;
```

### Cara membaca hasil E2E

- `webhook_log=SUCCESS` + `stock_sync_log=SUCCESS`: update berhasil.
- `webhook_log=SUCCESS` + tidak ada `stock_sync_log`: kemungkinan stok target sama dengan stok sebelumnya sehingga push di-skip sebagai unchanged.
- `webhook_log=ON_HOLD`: cek `error_message`.
- `stock_sync_log=FAILED`: pipeline berjalan, tetapi update variant tertentu gagal.
- `SUCCESS` tidak selalu berarti ada HTTP push; bisa berarti pipeline selesai dengan `skipped_unchanged`.

---

## Troubleshooting

### `Could not import module "main"`

Gunakan root repository dan command berikut:

```powershell
cd "C:\mirorim3\backend new"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Jika tetap gagal, gunakan:

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### `relation "webhook_log" does not exist`

Jalankan:

```powershell
alembic upgrade head
```

Kemudian restart backend. Startup aplikasi juga menjalankan `init_db()` untuk membuat tabel yang belum tersedia.

### Camunda task gagal memanggil backend

Error:

```text
gagal hit backend http://127.0.0.1:8000/api/v2/stock-sync/run-part
```

Periksa:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

Pastikan `BACKEND_BASE_URL` sama dengan alamat backend yang sedang hidup.

### Task Camunda tidak masuk batch

Periksa:

1. `external_tasks.enabled=true`.
2. Topic BPMN benar-benar `serviceWebhookTest`.
3. `batch=true` pada `config.json`.
4. Handler `service_webhook_test` terdaftar.
5. Tidak ada worker lain yang mengambil task lebih dahulu.
6. `max_tasks` cukup besar untuk jumlah task yang ingin diuji.

Lihat endpoint:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/webhook/camunda/external-tasks/status
```

### Tidak ada update ke marketplace

Periksa:

```dotenv
STOCK_SYNC_PUSH_ENABLED=true
STOCK_SYNC_REPORT_ONLY=false
```

Kemudian lihat `inventory_report`:

- Jika `previous_stock == target_stock`, push memang di-skip.
- Jika ada `stock_sync_log` status `FAILED`, baca `error_message`.
- Jika ada `401 refresh_token_expired`, perbarui credential marketplace di APISIX.

### Shopee `refresh_token_expired`

Ini bukan error pipeline. Token refresh Shopee di APISIX sudah expired. Perbarui token/credential Shopee di APISIX, lalu ulangi hanya part atau product yang gagal.

### Pipeline terlalu lama

Bedakan jalur yang dipakai:

- Scheduler global memang dapat lama karena sync semua shop, mapping, validation, dan inventory global.
- Webhook `/run-part` harus memakai `full_pipeline=false` dan hanya memproses part terpilih.
- Jangan mengirim `full_pipeline=true` dari handler webhook.
- Pastikan tidak ada dua scheduler global yang aktif bersamaan.

### Webhook tetap `PENDING`

Periksa:

1. Apakah process Camunda berhasil dibuat.
2. Apakah external task worker berjalan.
3. Apakah topic BPMN benar.
4. Apakah task terkunci worker lain.
5. Apakah `webhookLogId` ikut di-fetch pada topic.
6. Log Camunda dan `service_task_id` di `webhook_log`.

---

## Docker

`docker-compose.yml` saat ini mendefinisikan service aplikasi, bukan PostgreSQL, APISIX, atau Camunda lengkap. Pastikan dependency eksternal sudah hidup sebelum menjalankan compose.

```powershell
docker compose up --build
```

Untuk background:

```powershell
docker compose up -d --build
```

Cek log:

```powershell
docker compose logs -f app
```

Health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

---

## Aturan Perubahan Kode

- Jangan mengubah scheduler global ketika hanya memperbaiki webhook scoped.
- Jangan menjalankan `full_pipeline=true` dari handler `serviceWebhookTest`.
- Pertahankan body `/run-part` berbentuk array agar satu atau banyak part ID dapat diproses.
- Deduplikasi part ID sebelum pipeline dijalankan.
- Setiap perubahan status webhook harus tetap dapat diaudit di `webhook_log`.
- Error marketplace harus menjadi `ON_HOLD` atau `FAILED`, bukan dipalsukan menjadi sukses.
- Tambahkan atau perbarui test ketika mengubah batching, status, handler, atau kontrak endpoint.
- Jangan menjalankan `reset_db.py` pada database production.
- Jangan commit `.env`, token, password, atau log yang berisi credential.

---

## Referensi Internal

- [Dokumentasi refactor](docs/refactor/00-overview.md)
- [Dokumentasi Camunda](infrastructure/camunda/README.md)
- [Konfigurasi Camunda](infrastructure/camunda/config.json)
- [Runner E2E webhook](tests/run_e2e_webhook.py)
- [Test external-task batch](tests/test_external_task_batch.py)
- [Test webhook receiver](tests/test_webhook_receiver.py)
- [Test endpoint run-part](tests/test_stock_sync_run_part_endpoint.py)
