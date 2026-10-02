# Dummy Harness — Verifikasi E2E Webhook -> Camunda -> part_id -> Service Task

Folder ini dipakai untuk menguji alur **webhook marketplace** dan **external
service task** TANPA APISIX asli dan TANPA Camunda asli (keduanya "mati").
Kode produksi tetap dijalankan utuh — hanya kedua service eksternal itu yang
diganti server palsu (dummy) berbasis `http.server` (stdlib, tanpa dependensi).

## Isi folder

| File | Fungsi |
|---|---|
| `dummy_apisix.py` | Server APISIX palsu: `/order` (detail order + reserved stock), `/products`, `/update-stock`, `/update-status` |
| `dummy_camunda.py` | Server Camunda palsu (engine-rest): start process, message, signal, `external-task/fetchAndLock`, `complete`, `failure`, `bpmnError` |
| `verify_webhook_e2e.py` | Verifier otomatis: jalankan kode ASLI terhadap dua dummy di atas, cek part_id & service task |
| `__init__.py` | Penanda package |

## Cara menjalankan verifier

Dari root project:

```bash
python dummy/verify_webhook_e2e.py
```

Exit code `0` = semua PASS, `1` = ada FAIL. **Tidak butuh PostgreSQL, APISIX,
maupun Camunda.**

### Apa yang diperiksa

| Fase | Isi |
|---|---|
| FASE 0 | Guardrail statis dari kode sumber (tanpa service) |
| FASE 1 | Nyalakan Dummy APISIX + Dummy Camunda + stub `/run-part` |
| FASE 2 | `camunda_webhook_trigger.trigger_all(...)` ASLI -> cek start event & variabel `orderItems`, `partId`, `total_reserved` |
| FASE 3 | `ExternalTaskWorker.poll_once()` ASLI -> handler `service_webhook_test` -> self-call `/run-part` -> cek `complete` + part_id |

### Arti hasil (kondisi kode saat ini)

```
[FAIL] A.  Impl repository punya method part_id        -> method HILANG
[FAIL] A2. Interface mendeklarasikan method part_id     -> HILANG
[FAIL] B.  app/main.py memulai worker saat startup      -> TIDAK dijalankan
[FAIL] C.  fetch_reserved_stock menerima product_id      -> TIDAK (TypeError)
[FAIL]     Variabel total_reserved ada                   -> MISSING (akibat C)
```

Catatan penting soal FASE 2:

- FASE 2 menampilkan `partId` = `[321]` **PASS**, tetapi itu karena lapisan DB
  (`resolve_part_ids_for_products`) sengaja di-stub supaya harness bebas
  PostgreSQL. Jadi FASE 2 hanya membuktikan **wiring** (payload -> Camunda start),
  BUKAN pembacaan `sku_mapping` sungguhan.
- Kebenaran pembacaan DB dari `sku_mapping` dicek oleh **FASE 0 A/A2**: selama
  method `find_active_part_ids_by_external_product_ids` belum ada, `part_id`
  SELALU kosong di produksi, sehingga service task `serviceWebhookTest` selalu
  kena `PART_ID_MISSING`.

### Cara memverifikasi setelah diperbaiki

Jalankan lagi perintah yang sama. Harapan setelah semua diperbaiki:

```
FASE 0: A, A2, B, C  -> [ OK ]
FASE 2: orderItems, partId, total_reserved -> [ OK ]
FASE 3: complete + /run-part part_id + pipelineStatus=success -> [ OK ]
HASIL: SEMUA PASS
```

## Menjalankan dummy secara mandiri (untuk uji manual / curl)

```bash
# Terminal 1 — Camunda palsu di :8080
python -m dummy.dummy_camunda --port 8080 --topics serviceWebhookTest

# Terminal 2 — APISIX palsu di :9080
python -m dummy.dummy_apisix --port 9080 --marketplace tiktok \
    --order-id ORDER-DUMMY-1 --product-id PROD-DUMMY-1 --seller-sku 1A1
```

Lalu arahkan `.env` aplikasi ke dummy:

```env
APISIX_BASE_URL=http://127.0.0.1:9080
APISIX_ORDER_BASE_URL=
CAMUNDA_ENABLED=true
CAMUNDA_BASE_URL=http://127.0.0.1:8080
CAMUNDA_VERSION=7
CAMUNDA_REST_PATH=
BACKEND_BASE_URL=http://localhost:8000
```

Jalankan aplikasi, lalu kirim webhook mentah (contoh TikTok):

```bash
curl -X POST "http://localhost:8000/api/v1/webhook/order/status?marketplace=tiktok" \
  -H "Content-Type: application/json" \
  -d '{"type":1,"shop_id":"7494709429666874412","timestamp":1790059971,
       "data":{"order_id":"ORDER-DUMMY-1","order_status":"UNPAID"}}'
```

Inspeksi apa yang diterima Camunda palsu:

```bash
curl http://127.0.0.1:8080/__state
curl http://127.0.0.1:9080/__state
```

- Start event + variabel (`partId`, `orderItems`, ...) tampak di `starts`.
- Nyalakan worker & jalankan satu siklus task:
  ```bash
  curl -X POST "http://localhost:8000/api/v1/webhook/camunda/external-tasks/poll?limit=1"
  ```
- Hasil `complete`/`failure` tampak di `completions` / `failures` / `bpmn_errors`.

> Catatan: worker external task **tidak** auto-start (temuan B), jadi saat uji
> manual worker harus dinyalakan lewat endpoint `.../external-tasks/start` atau
> `.../poll`.

## Kenapa dummy ini dibuat

Karena APISIX dan Camunda di lingkungan Anda sedang mati, tanpa dummy alur
`webhook -> start event -> part_id -> service task` tidak bisa diuji sama sekali.
Dengan menukar hanya dua service tersebut, kita bisa membuktikan apakah masalah
ada di **kode kita** (bukan di service eksternal) dan memverifikasinya setelah
perbaikan.
