# 🔗 Integrasi Camunda — Webhook APISIX → Start Event / Signal / External Task

Folder ini meneruskan **webhook mentah** yang masuk dari APISIX menjadi
**start event / message / SIGNAL Camunda**, dan menjalankan **external service
task** yang diminta BPMN — seluruh perilakunya dapat diubah tanpa menyentuh
kode.

```
APISIX (webhook-config.json)
   │  POST /api/v1/webhook/order/status   (payload_mode = "raw")
   ▼
api/routers/webhook.py ──► catat payload mentah (JSONL)
   │  BackgroundTask (APISIX sudah menerima 200)
   ▼
camunda/service.py  ──► ambil DETAIL ORDER dari APISIX
   │                    GET /order?ids=<order id>  → produk & jumlah yang di-order
   │                    pilih rule (camunda/config.json)
   │                    HIT SIGNAL bila `signal.send_on_webhook=true`
   │                    bangun variabel dari payload mentah + detail order
   ▼
camunda/client.py   ──► POST start process / message / signal ke Camunda
   │
   ▼
logs/camunda/camunda_trigger.log   (hasil trigger, JSONL)

BPMN (service task External) ──► camunda/external_task.py
   fetchAndLock → handler (camunda/task_handlers.py) → complete / failure / bpmnError
```

## Isi folder

| File | Fungsi |
|---|---|
| `config.json` | **ATURAN**: webhook mana → proses Camunda mana + mapping variabel + peta status→signal + topic external task. Diedit langsung, berlaku tanpa restart (worker daur ulang lewat endpoint start). |
| `rules.py` | Loader + matcher (marketplace / signal code / conditions) + mapper variabel (dot-path, `{{template}}`, literal, `first`, `var.*`). |
| `signals.py` | Peta **STATUS ORDER → NAMA SIGNAL** (bagian `signal`), resolusi + ringkasan untuk endpoint debug. |
| `order_detail.py` | Ambil order id & shop_uuid dari payload webhook, lalu normalisasi DETAIL ORDER APISIX (produk apa saja + jumlahnya) menjadi konteks `order.*`. |
| `variables.py` | Normalisasi tipe (`"string"` → `String`), inferensi, dan coercion nilai↔tipe (anti HTTP 400 Camunda 7). |
| `client.py` | HTTP client Camunda 7 (`/engine-rest`) & Camunda 8 (`/v2`), termasuk retry, timeout, auth, variabel bertipe, **signal**, dan operasi external task. |
| `service.py` | Orkestrasi: resolve rule → kirim ke Camunda (start/message/signal) → tulis log hasil. Dipanggil router webhook sebagai background task. |
| `external_task.py` | **Worker external service task**: fetchAndLock → handler → complete/failure/bpmnError, konfigurasi dari `external_tasks` di `config.json`. |
| `task_handlers.py` | Handler bawaan topic (`order_detail`, `send_signal`, `stock_sync_part`, `stock_sync_global`) + registry untuk handler kustom. |
| `models.py` | DTO hasil (`CamundaTriggerResult`) dan rencana request (`CamundaPlan`). |

## Konfigurasi lewat `.env`

Semua di bawah ini punya default aman; cukup isi yang perlu. Ini **default
global** — rule di `config.json` boleh menimpa per marketplace/signal.

```env
# ── Saklar & alamat Camunda ─────────────────────────────────────────────
CAMUNDA_ENABLED=true                    # false = semua trigger dilewati
CAMUNDA_BASE_URL=http://localhost:8080  # mis. http://192.168.1.50:8080
#   Boleh juga langsung memuat rest path (gaya default library external task
#   client): CAMUNDA_BASE_URL=http://localhost:8080/engine-rest — tidak akan
#   dobel, backend mendeteksi dan tidak menempelkan /engine-rest lagi.
CAMUNDA_VERSION=7                       # 7 (Platform/engine-rest) | 8 (Zeebe REST /v2)
CAMUNDA_REST_PATH=                      # kosong = default per versi
CAMUNDA_MATCH_MODE=all                  # all = semua rule cocok dijalankan | first

# ── Target proses default (dipakai bila rule tidak menyebut) ─────────────
CAMUNDA_PROCESS_DEFINITION_KEY=
CAMUNDA_PROCESS_DEFINITION_ID=
CAMUNDA_MESSAGE_NAME=                   # diisi → default kirim message (v7)
CAMUNDA_TENANT_ID=
CAMUNDA_BUSINESS_KEY_TEMPLATE=          # mis. {{marketplace}}-{{data.ordersn}}
CAMUNDA_TYPED_VARIABLES=true            # v7: kirim {value, type}; v8 selalu polos

# ── Ketahanan & auth ────────────────────────────────────────────────────
CAMUNDA_TIMEOUT_SECONDS=10
CAMUNDA_MAX_ATTEMPTS=3                  # 1 = tanpa retry
CAMUNDA_RETRY_DELAY_SECONDS=1
CAMUNDA_RETRY_ON_STATUS=429,500,502,503,504
CAMUNDA_AUTH_TYPE=none                  # none | basic | bearer
CAMUNDA_USERNAME=
CAMUNDA_PASSWORD=
CAMUNDA_TOKEN=
CAMUNDA_VERIFY_SSL=true

# ── File aturan & log ───────────────────────────────────────────────────
CAMUNDA_RULES_FILE=camunda/config.json
CAMUNDA_LOG_ENABLED=true
CAMUNDA_LOG_DIR=logs/camunda
CAMUNDA_LOG_FILENAME=camunda_trigger.log

# ── DETAIL ORDER via APISIX (produk apa saja yang di-order + jumlahnya) ──
# Endpoint order APISIX (docs §6.4): list tanpa `ids`, DETAIL dengan `ids=<order id>`.
APISIX_ORDER_PATH=/order
# Base URL khusus panggilan ORDER (kosong = pakai APISIX_BASE_URL).
# Berguna bila gateway publik (https) tidak terjangkau dari host ini tapi LAN bisa,
# mis. APISIX_ORDER_BASE_URL=http://192.168.1.185:9080
APISIX_ORDER_BASE_URL=
# Shop UUID untuk GET /order (kosong = pakai `shop_id` dari payload webhook;
# di credentials.json APISIX shop_uuid umumnya == shop_id).
APISIX_SHOPEE_SHOP_UUID=
APISIX_TIKTOK_SHOP_UUID=
WEBHOOK_ORDER_DETAIL_ENABLED=true     # false = tanpa fetch detail order
WEBHOOK_ORDER_DETAIL_TRANSLATE=false  # false = raw (ada NAMA produk) | true = reserved_stock
WEBHOOK_ORDER_DETAIL_TIMEOUT=30

# ── TOTAL RESERVED STOCK per produk (APISIX /order?product_id=…) ─────
# Diambil setelah detail order, untuk SETIAP produk yang ada di order tsb.
WEBHOOK_RESERVED_ENABLED=true
WEBHOOK_RESERVED_MAX_PRODUCTS=20     # batas panggilan per order (pengaman)
WEBHOOK_RESERVED_RETRY_DELAY=3       # detik; retry bila order pemicu belum
                                     # terindeks di snapshot (0 = nonaktif)

# ── PART ID dari sku_mapping (ikut variabel start event) ────────────
# SETELAH reserved: product_id line item → sku_mapping ACTIVE → inventree_part_id
# (fail-open: DB gagal / tidak ada mapping → partId dilewati, start tetap jalan).
# Diekspos sebagai order.part_id (ARRAY semua part) → variabel `partId`
# berisi TEKS JSON ARRAY (variable_types.partId = "string") yang di-fetch
# service task BPMN `serviceWebhookTest`.
# Satu product marketplace bisa memetakan ke BANYAK part_id, dan SEMUA part
# itu yang dijalankan pipeline-nya — bukan hanya yang pertama.
# CATATAN: jangan diubah ke tipe "json" — tipe Json Camunda (plugin Spin)
# tidak round-trip lewat fetchAndLock external task (nilainya kembali jadi
# objek deskriptor, isi array hilang → PART_ID_MISSING). Dilindungi 2 lapis:
# (1) handler menulis `partId` kembali dgn variable_types={"partId": "String"}
#     (tanpa tipe, list di-inferensi Camunda sebagai Json → variabel TERMUTASI);
# (2) topic serviceWebhookTest pakai `deserialize_values: false` supaya Json →
#     teks JSON. Tanpa ini, task ke-2 (serviceWebhookCancel, dijalankan setelah
#     signal cancel) kena PART_ID_MISSING walau variabelnya jelas ada.
WEBHOOK_PART_ID_ENABLED=true

# ── GATE STATUS ORDER saat start event ──────────────────────────────
# Hanya status ini yang boleh men-start proses (dipisah koma). Config bawaan
# kini UNPAID saja — status lain TIDAK start instance baru, hanya mengirim
# signal. Kosong = tanpa gate. AWAS: gate .env ini menyaring SEBELUM
# auto-signal, jadi kalau diisi sinyal status di luar daftar ikut terblokir.
# Gate per aturan (hot-reload, dievaluasi SETELAH auto-signal) di camunda/config.json.
WEBHOOK_ORDER_STATUS_ALLOWLIST=
```

> **Catatan shop_uuid:** APISIX memvalidasi `shop_uuid` terhadap
> `apisix-conf/credentials/credentials.json`. Bila `shop_id` pada webhook tidak
> sama dengan `shop_uuid` di APISIX, endpoint membalas `SHOP_NOT_FOUND` — isi
> `APISIX_SHOPEE_SHOP_UUID` / `APISIX_TIKTOK_SHOP_UUID` (nilainya terlihat di
> log warning `APISIX order detail HTTP 404 (order_id=…)`).

Cek konfigurasi efektif tanpa mengirim apa pun:

```bash
curl http://localhost:8000/api/v1/webhook/camunda/config
```

## Menulis `config.json` (bentuk dinamis)

Hanya kunci yang dinamis: target proses + pemetaan variabel. Tidak ada filter
marketplace/signal code — **setiap webhook yang masuk dijalankan**.

```jsonc
{
  "id": "retur-from-order-status",
  "enabled": true,
  "process_definition_key": "Mirorim_Operasional.Trigger_Retur",
  "business_key": "{{var.resi}}:{{var.courier}}:{{var.retur_date}}",
  "variables": {
    "resi":    ["data.tracking_number", "data.ordersn", "data.order_id"],
    "courier": ["data.shipping_carrier", "data.courier", {"literal": ""}],
    "marketplace": "{{marketplace}}",
    "signalCode":  "{{signal_code}}",
    "shopId":      "shop_id",

    // ── DETAIL ORDER dari APISIX (namespace order.*) ──
    "orderItems":         { "path": "order.items" },
    "orderProducts":      { "path": "order.products_summary" },
    "orderItemCount":     { "path": "order.item_count" },
    "orderTotalQuantity": { "path": "order.total_quantity" },
    "orderDetailOk":      { "path": "order.ok" }
  },
  "variable_types": {
    "resi": "string", "courier": "string",
    "orderItems": "json", "orderItemCount": "long", "orderTotalQuantity": "long",
    "orderDetailOk": "boolean"
  }
}
```

`process_definition_key` boleh **array** → satu process instance per key:

```jsonc
"process_definition_key": ["Mirorim_Operasional.Trigger_Retur", "Mirorim_Operasional.Trigger_Retur_2"]
```

### Bentuk spesifikasi nilai variabel

| Bentuk | Arti |
|---|---|
| `"data.ordersn"` | Dot-path payload; **dilewati** bila tidak ada |
| `"shop_id"` | Kunci level atas payload (tanpa titik); dilewati bila tidak ada |
| `["data.tracking_number", "data.ordersn", {"literal": ""}]` | **Array = fallback berantai**, dibaca kiri→kanan sampai ada nilainya (inilah yang membuat konfigurasi jalan di semua marketplace) |
| `{"literal": 12}` | Nilai tetap (angka tetap angka) |
| `{"literal": ["a", "b"]}` | Kirim **array** sebagai nilai variabel |
| `{"path": "...", "default": "-"}` | Pakai `default` bila path kosong |
| `{"template": "{{marketplace}}-{{data.ordersn}}"}` | Gabung beberapa nilai |
| `{"first": [ ... ]}` | Sama dengan bentuk array |
| `"{{marketplace}}"` | Placeholder tunggal → **tipe asli** dipertahankan |

> String polos **selalu** diartikan sebagai path (bukan literal). Untuk nilai
tetap pakai `{"literal": "..."}` — supaya salah tulis nama field tidak diam-diam
terkirim sebagai nilai (mis. `retur_date` berisi teks `"timestamp"`).

### Format lanjutan (opsional): `rules[]` berfilter

Masih didukung bila suatu saat perlu filter marketplace/signal code/conditions —
lihat riwayat contoh di bagian ini: `defaults` + `rules[]` dengan
`marketplace`, `signal_codes`, `conditions`, `condition_mode`, dan `mode`
(`all`/`first`). Bila `rules[]` ada, bentuk dinamis di atas tidak dipakai.

### Placeholder yang tersedia

`{{marketplace}}`, `{{signal_code}}`, `{{received_at}}`,
`{{headers.NamaHeader}}` (mis. `{{headers.X-Webhook-Target}}`),
`{{var.namaVariabel}}` (variabel hasil `variables` — dipakai untuk business key),
dot-path payload mentah (mis. `{{data.ordersn}}`, `{{shop_id}}`, `{{data.0.item_id}}`),
dan detail order (mis. `{{order.products_summary}}`, `{{order.total_quantity}}`).

---

## Detail order: produk apa saja yang di-order & berapa banyak

Saat webhook masuk, backend mengambil **detail order** ke APISIX sebelum start
event:

```
GET {APISIX_BASE_URL}{APISIX_ORDER_PATH}?marketplace=…&shop_uuid=…&ids=<order id>&translate=false
```

- `ids` diisi → mode **DETAIL** (Shopee `get_order_detail`, TikTok `/orders?ids=`).
- `translate=false` → response **mentah marketplace**, sehingga **nama produk**
  dan jumlah per line item tersedia. Kalau diubah ke `true`, APISIX
  mengembalikan agregasi `reserved_stock` (tetap didukung, tapi tanpa nama produk).

Order id diambil dari payload webhook (`data.ordersn` Shopee / `data.order_id`
TikTok) dan `shop_uuid` dari `shop_id` payload (bisa di-override lewat setting).

Hasilnya dinormalisasi jadi konteks `order.*`:

| Path | Isi | Contoh |
|---|---|---|
| `order.ok` | berhasil/tidak fetch detail | `true` |
| `order.order_id` / `order.order_status` | id & status order | `586196597913388837` / `UNPAID` |
| `order.items` | **array** line item (mudah dibaca) | `[{"product_name": "Kaos Polos", "variant_name": "M", "seller_sku": "1A1-M", "quantity": 2, …}]` |
| `order.item_count` | jumlah baris (produk berbeda) | `2` |
| `order.total_quantity` | **total unit** yang di-order | `3` |
| `order.products_summary` | ringkasan teks siap dibaca manusia | `Kaos Polos (M) x2, Celana Chino (L) x1` |
| `order.product_names` | daftar nama unik | `["Kaos Polos (M)", "Celana Chino (L)"]` |
| `order.total_reserved` | **total reserved stock** semua produk di order ini (dari `/order?product_id=…`) | `8` |
| `order.reserved_by_product` | map **NESTED** per produk: total + rincian **per VARIANT** (`variant_id`, `seller_sku`, `total_reserved_stock`, `variant_name`) | `{"1736…": {"total_reserved": 9, "variants": {"1736672772166353964": {"variant_id": "…", "seller_sku": "8F14", "total_reserved_stock": 3, "variant_name": "Single Pack"}}}}` |
| `order.part_id` | **SEMUA part InvenTree** pada order ini (ARRAY, unik) — dari `sku_mapping` (product_id → variant → mapping ACTIVE), dibaca SETELAH reserved; menjadi variabel `partId` (String berisi teks JSON array) utk service task `serviceWebhookTest`, yang menjalankan pipeline dari awal untuk TIAP part | `[7, 9]` → `"[7, 9]"` |
| `order.error` | alasan kegagalan (bila ada) | `fetch_failed` |

Contoh variabel Camunda yang dihasilkan (`camunda/config.json` bawaan):

```json
{
  "orderItems":         {"value": "[{\"product_id\": \"1736…\", \"product_name\": \"Kaos Polos\",
                                    \"variant_name\": \"M\", \"seller_sku\": \"1A1-M\",
                                    \"quantity\": 2, \"unit_price\": \"29900\"}]",
                         "type": "Json"},
  "orderProducts":      {"value": "Kaos Polos (M) x2, Celana Chino (L) x1", "type": "String"},
  "orderItemCount":     {"value": 2, "type": "Long"},
  "orderTotalQuantity": {"value": 3, "type": "Long"},
  "orderDetailOk":      {"value": true, "type": "Boolean"},
  "total_reserved":     {"value": 8, "type": "Long"},
  "reserved_products":  {"value": "{\"1736…\": {\"total_reserved\": 9, \"variants\": {\"1736672772166353964\":
                                    {\"variant_id\": \"…\", \"seller_sku\": \"8F14\",
                                     \"total_reserved_stock\": 3, \"variant_name\": \"Single Pack\"}}}}",
                         "type": "Json"}
}
```

> **Catatan tipe `Json`:** Camunda 7 **mewajibkan** nilai tipe `Json` berupa
> **teks JSON (string)** — mengirim array/objek langsung membalas HTTP 400:
> `Must provide 'null' or String value for value of SerializableValue type 'Json'.`
> Backend meng-serialize otomatis (`camunda/variables.py`), jadi di `config.json`
> Anda cukup menulis `{"path": "order.items"}` apa adanya. Di sisi proses,
> `JSON.parse(orderItems)` mengembalikan array yang sama. Kalau engine Anda
> tidak memakai Spin, ubah saja tipenya di config.json: `"orderItems": "string"`
> (nilai tetap teks JSON, tipe jadi `String`).

**FAIL-OPEN:** bila APISIX error / order tidak ketemu / `shop_uuid` salah,
start event **tetap dijalankan** dan variabel order yang kosong otomatis
**dilewati** (hanya `orderDetailOk: false` yang terkirim). Alasannya tercatat di
`logs/camunda/camunda_trigger.log` pada blok `order_detail`:

```json
"order_detail": {"ok": true, "source": "order_detail", "order_id": "586196597913388837",
                 "order_status": "UNPAID", "item_count": 2, "total_quantity": 3,
                 "products_summary": "Kaos Polos (M) x2, Celana Chino (L) x1", "error": null}
```

Line item **identik** (produk + SKU + variant sama) otomatis digabung dan
jumlahnya diakumulasi. TikTok yang tidak mengirim `quantity` dianggap **1 unit
per line item** (sesuai perilaku marketplace).

### Total reserved stock per produk (`total_reserved`)

Setelah detail order terbaca, backend mengambil **total reserved stock untuk tiap
produk yang ada di order tsb** — memakai **filter gateway** yang baru:

```
GET {APISIX_BASE_URL}{APISIX_ORDER_PATH}?marketplace=…&shop_uuid=…&product_id=<pid>
```

- **Status order tetap sama** dengan `/order` biasa (TikTok `UNPAID`/`ON_HOLD`/
  `AWAITING_SHIPMENT`, Shopee `UNPAID`/`READY_TO_SHIP`) — jadi maknanya identik
  dengan reserved stock yang selama ini dipakai pipeline.
- **Satu panggilan per produk** (batas `WEBHOOK_RESERVED_MAX_PRODUCTS`), bukan
  satu sweep seluruh toko.
- Hasilnya dua variabel: `total_reserved` (Long = jumlah semua produk di order)
  dan `reserved_products` (Json = map **NESTED** `product_id` →
  `{total_reserved, variants}`). Karena satu listing marketplace bisa punya
  BANYAK variant (mis. `8F14` + `8F14-Pack-100`), tiap variant dilaporkan
  sendiri: `variant_id`, `seller_sku`, `total_reserved_stock`, dan
  `variant_name` (nama diambil dari DB backend `marketplace_variant.variant_name`
  — response APISIX `/order` tidak memuat nama variant; bila DB belum tersedia,
  hanya `variant_name` yang kosong).
- **FAIL-OPEN**: gagal per produk → cuma produk itu dilewati; gagal semua →
  kedua variabel tidak dikirim, start event **tetap jalan**.
- Response gateway `filtered_by` dipakai sebagai penanda; kalau **belum ada**
  (plugin `request-transformer` gateway belum di-reload), total tetap benar karena
  backend menyaring ulang per produk + mencatat warning di log.
- **Retry order pemicu** (`WEBHOOK_RESERVED_RETRY_DELAY`, default 3 detik):
  webhook tiba hanya 1–4 detik setelah order dibuat, sementara agregasi `/order`
  gateway membaca daftar order marketplace (`orders/search`) yang indeksnya bisa
  tertinggal beberapa detik — order pemicu bisa **belum** ikut terhitung (kasus
  nyata: `total_reserved` = 4 padahal gateway beberapa saat kemudian bilang 5).
  Bila `order_id` pemicu tidak ada di snapshot, backend menunggu sekian detik
  lalu mengambil ulang **sekali**; bila tetap tidak ada → warning eksplisit di log
  (angka yang terkirim kemungkinan kurang, bukan gagal diam-diam).
- Matikan dengan `WEBHOOK_RESERVED_ENABLED=false`.

Contoh isi variabel di Camunda:

```json
"total_reserved":    {"value": 8, "type": "Long"},
"reserved_products": {"value": "{\"1736033390941733932\": {\"total_reserved\": 3, \"variants\": {\"1736672772166353964\": {\"variant_id\": \"1736672772166353964\", \"seller_sku\": \"8F14\", \"total_reserved_stock\": 3, \"variant_name\": \"Single Pack\"}}}, \"1736268650108650540\": {\"total_reserved\": 5, \"variants\": {...}}}", "type": "Json"}
```

Contoh membaca di BPMN (Camunda 7 / Spin): `JSON.parse(reserved_products)` →
objek; total per produk: `obj[productId].total_reserved`; detail variant:
`obj[productId].variants[variantId]`.

> **Catatan bentuk (PERUBAHAN):** sebelumnya `reserved_products` hanya berisi
> satu angka per produk (`{"<product_id>": 3}`) sehingga produk multi-variant
> tampil seolah cuma punya satu variant. Sekarang nilainya **nested** — BPMN yang
> membaca angka langsung dari `reserved_products[productId]` perlu diubah menjadi
> `reserved_products[productId].total_reserved`. Total keseluruhan tetap
> tersedia di variabel `total_reserved`.

Mau mengubah bentuk/nama variabelnya? Cukup edit mapping `order.*` di
`camunda/config.json` — tanpa restart. Untuk mematikan fitur ini seluruhnya:
`WEBHOOK_ORDER_DETAIL_ENABLED=false`.

### Gate status order (hanya status tertentu yang men-start proses)

Supaya webhook dengan status yang tidak relevan tidak ikut men-start proses,
ada dua lapis filter STATUS ORDER — statusnya dibaca dari payload **webhook
mentah** (Shopee `data.status`, TikTok `data.order_status`), bukan dari detail
order:

1. **Per aturan** (`camunda/config.json`, hot-reload tanpa restart):

   ```json
   "order_status": ["UNPAID"],
   "exclude_order_status": ["CANCELLED"]
   ```

   Webhook dengan status di luar daftar (atau status yang tidak ada di payload)
   dilewati dengan `skipped_reason: "order_status_not_allowed"` — pengecekan ini
   hanya menahan START EVENT; auto-signal (bagian `signal`) dievaluasi lebih
   dulu, jadi sinyal status di luar daftar tetap terkirim. Daftar bawaan kini
   `UNPAID` saja: instance dibuat saat order masih UNPAID, status berikutnya
   cukup mengirim signal (readyToShip / awaitingCollection / cancel).
2. **Global** (`WEBHOOK_ORDER_STATUS_ALLOWLIST` di .env, dipisah koma) — berlaku
   untuk SEMUA aturan dan menjadi **lantai keras**: aturan hanya bisa
   mempersempit, bukan melonggarkan. Kosong = tidak ada gate global.

Gate ini dicek **sebelum** detail order diambil, jadi webhook yang dilewati
sama sekali tidak memakai panggilan APISIX. Contoh nyata: `total_reserved`,
`orderItems`, dan start event hanya dijalankan untuk order berstatus `UNPAID`
(bucket awal yang memang diurus proses), bukan setiap perubahan status.
Mau menambah status start lain (mis. `AWAITING_SHIPMENT`)? Cukup tambahkan di
`order_status` — status di luar daftar tetap mengirim signal.

Di sisi gateway, pemisahan ini berpasangan dengan **satu target webhook per
marketplace** di `apisix/webhook-config.json`
(`order-status-backend-shopee` → Shopee code=3, `order-status-backend-tiktok`
→ TikTok type=1) sehingga signal code pun tidak bisa tertukar antar marketplace.

## Hit signal: status order → signal Camunda

Selain men**start** proses, webhook bisa mengirim **SIGNAL** — dipakai untuk
memberi tahu proses yang sudah berjalan (mis. branch retur) tanpa membuat
instance baru. Pemetaannya ada di `config.json` bagian `signal` dan
**hot-reload** (edit file → langsung berlaku):

```json
"signal": {
  "enabled": true,
  "send_on_webhook": true,
  "by_status": {
    "ON_HOLD": "readyToShip${invoice}",
    "READY_TO_SHIP": "readyToShip${invoice}",
    "AWAITING_SHIPMENT": "readyToShip${invoice}",
    "AWAITING_COLLECTION": "awaitingCollection${invoice}",
    "CANCELLED": "cancel${invoice}",
    "CANCEL": "cancel${invoice}"
  },
  "by_marketplace": { "tiktok": {} },
  "default_signal": "",
  "execution_lookup": {
    "enabled": true,
    "business_key_template": "{{var.invoice}}:%",
    "fallback": "broadcast"
  },
  "status_fallback": { "enabled": true, "skip_statuses": [] },
  "variables": { "invoice": ["data.tracking_number", "data.ordersn", "data.order_id"] },
  "variable_types": { "orderTotalQuantity": "long" }
}
```

Nama sinyal di atas SENGJA memakai `${invoice}` — sisi BPMN
(`<signal name="readyToShip${invoice}">`) mengevaluasi ekspresi dari process
variable `invoice`, dan backend merender placeholder dari `signal.variables`
dengan fallback yang **harus identik** dengan `variables.invoice` di atas.

> **Kenapa harus dinamis (bukan `"cancel"` statis)?**
> Karena boundary event di-subprocess memegang subscription pada **scope
> execution tanpa `activityId`**, sehingga Camunda **menolak**
> `POST /execution/{id}/signal` dengan error
> `cannot signal execution ...: it has no current activity`. Satu-satunya
> pengiriman yang valid adalah **broadcast**, dan broadcast hanya aman bila
> nama sinyal **unik per order**. Nama statis akan mengenai SEMUA instance yang
> sedang menunggu sinyal itu. `execution_lookup` di bawah dipakai untuk
> **memverifikasi** subscriber (dan menangkap silent-drop) serta memilih jalur
> per-execution bila execution-nya memang layak di-signal (mis. intermediate
> catch event).

Start event sendiri kini hanya menerima **UNPAID** (gate `order_status`):
instance dibuat sekali saat order masih UNPAID, lalu menunggu di catch event —
webhook status lain (ON_HOLD / READY_TO_SHIP / AWAITING_SHIPMENT / CANCELLED)
TIDAK membuat instance baru, hanya mengirim signal ke instance yang sudah jalan.

Presedensi nama sinyal: **`by_marketplace[<mp>][STATUS]` → `by_status[STATUS]` →
`default_signal`** (status dibandingkan case-insensitive, spasi tepi dibuang).
`default_signal` kosong = status yang tidak dipetakan **tidak** mengirim apa pun
(tercatat `skipped_reason: signal_not_configured`, `ok: true` karena bukan error)
— kecuali `status_fallback` aktif (lihat di bawah).

**Status efektif dari detail order (`status_fallback`).** Status dari payload
**webhook** selalu dipakai lebih dulu. Bila status itu tidak punya mapping
di `by_status`/`by_marketplace` — kasus nyata: webhook Shopee `code=3` datang
saat `data.status = UNPAID` (hanya memicu START), sementara pembacaan gateway
pada request yang sama sudah melihat `READY_TO_SHIP` — maka status dari **DETAIL
ORDER APISIX** (`order.order_status` → variabel `orderDetailStatus`) dipakai
sebagai status efektif: sinyal yang dikirim tetap `readyToShip${invoice}`,
bukan dilewati. `skip_statuses` (mis. `["CANCELLED"]`) melarang status tertentu
dipakai sebagai fallback, dan `enabled: false` mengembalikan perilaku lama.
Backend hanya mengambil detail order sendiri bila pemanggil belum menyuplai
(bila status webhook sudah dipetakan, **tidak ada** panggilan APISIX tambahan).
Nama sinyal eksplisit (`signal_name`, mis. dari topic `send-signal`) tidak pernah
ditimpa fallback ini.

Tiga cara memicunya:

| Cara | Kapan dipakai |
|---|---|
| `POST /api/v1/webhook/camunda/signal?marketplace=tiktok&status=ON_HOLD` | Uji coba / kirim manual dari operator-dashboard. Body JSON opsional (payload dipakai untuk variabel `data.*` / `order.*`). `dry_run=true` = catat tanpa kirim. |
| Otomatis saat webhook masuk | Set `"send_on_webhook": true`. Sinyal dikirim **sebelum** aturan start event dievaluasi, jadi status yang hanya perlu signal (tanpa start proses) tetap terkirim. Gate status order tetap berlaku untuk start event. |
| External service task topic `send-signal` | BPMN sendiri yang memutuskan kapan sinyal dikirim (lihat bagian worker di bawah). |

Detail penting:

- **Nama sinyal dinamis**: nilai `by_status` / `by_marketplace` / `default_signal`
  boleh memuat placeholder `${invoice}` (gaya JS template literal) **atau**
  `{{invoice}}` — dua-duanya diisi dari **VARIABEL signal** (`signal.variables`
  yang sudah di-resolve), bukan payload mentah. Contoh: `"READY_TO_SHIP":
  "readyToShip${invoice}"` + `"variables": {"invoice": ["data.ordersn"]}` →
  sinyal yang dikirim `readyToShipSH-123`. Awalan `var.` boleh ditulis
  (`${var.invoice}` sama dengan `${invoice}`). Variabel tidak ketemu →
  placeholder dibiarkan apa adanya + warning di log; hasil render kosong →
  sinyal tidak dikirim (`signal_not_configured`). Helper-nya:
  `camunda/signals.py::render_signal_name`.
- Variabel: `signal.variables` memakai sintaks yang sama dengan `variables` aturan
  (path / template / literal / array + namespace `order.*`), lalu **ditimpa**
  argumen pemanggil (task/variabel eksplisit). Backend selalu menambahkan
  `signalName`, `signalOrderStatus`, `signalMarketplace`, dan
  `signalStatusSource` (`"webhook"` | `"order_detail"` — dari mana status yang
  memilih sinyal berasal).
- **Camunda 7**: REST `/signal` tidak punya field `businessKey`. Nilai
  `signal.business_key` (mis. `"{{var.signalName}}:{{var.orderId}}"`) dikirim
  sebagai **VARIABEL** `businessKey` supaya proses penerima tetap bisa mengaitkan.
- **`execution_lookup` (verifikasi subscriber, Camunda 7)** — direkomendasikan.
  Sebelum mengirim, backend mencari execution yang menunggu sinyal ini pada
  process instance dengan business key cocok `business_key_template` (LIKE, `%`
  = wildcard). Hasilnya:
  - ketemu + execution layak → `POST /execution/{id}/signal` (per-execution);
  - ketemu tapi execution **tidak layak** (boundary event, tanpa `activityId`)
    → tetap broadcast — aman karena nama sinyal per-order;
  - **tidak ketemu** → silent-drop terdeteksi: `fallback: "broadcast"` = kirim
    broadcast + **WARNING**; `fallback: "skip"` = tidak dikirim
    (`skipped_reason: signal_no_subscriber`).
  Helper: `camunda/client.py::match_signal_subscription`.
- `execution_id` (Camunda 7 saja) mengirim sinyal ke SATU eksekusi yang sudah
  diketahui statis (`POST /execution/{id}/signal`); kosong = broadcast.
- Camunda 8 memakai `POST /v2/signals/broadcast` (tersedia sejak Camunda 8.6);
  signal per-eksekusi tidak didukung.
- Hasil sinyal dicatat di `logs/camunda/camunda_trigger.log` dengan penanda
  `"event": "signal"` + `signal_name`, sehingga tidak tertukar dengan baris
  start event.
- Aturan `config.json` juga boleh bertipe signal: `{"type": "signal"}` dengan
  `signal_name` eksplisit, atau dibiarkan kosong agar diambil dari peta status.

## External service task (worker)

Worker mengambil service task bertipe **External** dari Camunda, menjalankan
handler Python di backend ini, lalu melaporkan hasilnya:

```
POST /engine-rest/external-task/fetchAndLock
        │  (task + variabel yang diminta)
        ▼
camunda/task_handlers.py — handler per topic
        │
        ▼
POST /engine-rest/external-task/{id}/complete   (variabel hasil)
POST /engine-rest/external-task/{id}/failure    (error teknis → retry)
POST /engine-rest/external-task/{id}/bpmnError  (error bisnis → boundary event)
```

Konfigurasi di `config.json` bagian `external_tasks` (hot-reload saat worker
distart / lewat endpoint start):

```json
"external_tasks": {
  "enabled": false,
  "worker_id": "sync-service-1",
  "max_tasks": 5,
  "poll_interval_seconds": 2,
  "idle_backoff_seconds": 5,
  "default_lock_duration_ms": 60000,
  "default_retries": 3,
  "topics": [
    {"topic": "order-detail", "handler": "order_detail",
     "variables": ["marketplace", "orderId", "shopId"]},
    {"topic": "send-signal", "handler": "send_signal"},
    {"topic": "stock-sync-part", "handler": "stock_sync_part", "variables": ["partId"]},
    {"topic": "serviceWebhookTest", "handler": "service_webhook_test",
     "batch": true, "variables": ["partId"]}
  ]
}
```

### Mode BATCH vs SATU PER SATU (configurable per topic)

Tiap topik bisa memuat kunci **`batch`** (hot-reload saat worker distart):

| `batch` | Perilaku |
|---|---|
| `true` | SEMUA task topik tsb yang terkumpul dalam **SATU poll** di-merge jadi **SATU** aksi handler — mis. satu `POST /api/v2/stock-sync/run-part` dengan body `part_id` **ARRAY** gabungan (dedup). Tiap task lalu di-`complete` dengan hasil yang sama. |
| `false` (default / kunci dihapus) | Dikerjakan **SATU PER SATU** per task — satu panggilan handler per task. |

Syarat: handler harus menandai dirinya `handler.batch_capable = True`. Handler
**tanpa** flag ini otomatis jatuh ke mode per-task walaupun `batch: true`
(aman — tidak memicu error). Handler `service_webhook_test` sudah
`batch_capable`; handler lain (`stock_sync_part`, `webhook_part_batch`, dst.)
dijalankan per-task.

Mengubah perilaku = edit `infrastructure/camunda/config.json` lalu restart
worker (`POST /api/v1/webhook/camunda/external-tasks/stop` → `/start`),
tanpa menyentuh kode.

Handler bawaan (boleh juga `"handler": "paket.modul:fungsi"` untuk kode sendiri):

| Handler | Yang dikerjakan | Variabel INPUT | Variabel hasil |
|---|---|---|---|
| `order_detail` | Ambil detail order dari APISIX (produk + jumlah) | `marketplace`, `orderId`, `shopId` | `orderItems`, `orderProducts`, `orderItemCount`, `orderTotalQuantity`, `orderDetailStatus`, `orderDetailOk` |
| `send_signal` | Hit signal sesuai status order (peta `signal`) | `marketplace`, `orderStatus`, `signalName` (opsional) | `signalName`, `signalSent`, `signalStatus`, `signalSkippedReason`, `signalError` |
| `stock_sync_part` | Stock sync untuk SEMUA `part_id` yang diberikan (scope `sku_mapping`) | `partId` (array / skalar) | `partId`, `partIds`, `syncReportRows`, `syncQueued`, `syncPushed`, `syncLogged`, `syncFailures`, `syncRunMode` |
| `stock_sync_global` | Satu cycle pipeline stok GLOBAL | `statusFilter` (opsional) | idem + `syncPartsProcessed` |
| `service_webhook_test` | **HIT backend sendiri** → `POST /api/v2/stock-sync/run-part` dengan BODY JSON `{"part_id": [1, 2], "full_pipeline": true}` (banyak part_id dalam SATU request) — menjalankan **pipeline DARI AWAL** (GET product / SYNC / MAPPING semua marketplace → inventory_report / stock_sync_queue / stock_sync_log) untuk **SETIAP** `partId`; COMPLETED hanya bila sukses | `partId` (teks JSON array dari `sku_mapping` saat webhook — lihat `WEBHOOK_PART_ID_ENABLED`) | `partId`, `partIds`, `pipelineStatus`, `pipelineRunMode`, `pipelineMessage`, `pipelineVariantCount`, `pipelineReportRows`, `pipelineQueued`, `pipelinePushed`, `pipelineLogged`, `pipelineSkippedUnchanged`, `pipelineSkippedPending`, `pipelineFailures`, `pipelinePushEnabled` |

Perilaku penting:

- Handler mengembalikan `ExternalTaskResult` (complete / failure / bpmnError),
  `dict` variabel, atau `None`. Exception → task dilaporkan **FAILURE** dengan
  `retries` diturunkan (default: `task.retries - 1`) + traceback di
  `errorDetails`, sehingga bisa di-retry engine / terlihat di Cockpit.
- Topic tanpa handler tidak dibiarkan menggantung: task langsung dilaporkan
  failure (`no handler registered for topic ...`).
- Variabel di-`complete` memakai konversi tipe yang sama dengan start event
  (array/objek → teks JSON untuk Camunda 7).
- `handle_send_signal` meneruskan variabel task (selain variabel kontrol
  `signalName`/`executionId`/`signalVariables`/`forwardVariables`) sebagai
  variabel sinyal.
- `service_webhook_test` (topic `serviceWebhookTest` — BPMN *TEST WEBHOOK DAPET
  APA KAGA*) meng-HIT endpoint manual pipeline milik backend ini sendiri,
  base URL dari `.env` **`BACKEND_BASE_URL`** (default `http://localhost:8000`;
  worker di container → pakai `http://host.docker.internal:8000`) + timeout
  **`BACKEND_SELF_CALL_TIMEOUT`** (default `120` detik). Pemetaan hasil ke aksi
  Camunda: `2xx status=success` → **complete**; `404`/`503`/`5xx`/timeout/
  jaringan/`2xx status=error` → **failure** (retry engine — instance TIDAK
  diakhiri; dulu `404`/`503` dipetakan ke BPMN error sehingga instance bisa
  "hilang" sebelum pipeline jalan); `partId` kosong / tanpa angka > 0 → BPMN
  error `PART_ID_MISSING`. `partId` berupa
  **JSON array** (boleh juga skalar) — SEMUA part_id diteruskan dalam SATU
  request dengan BODY JSON `{"part_id": [3000100, 3000101], "full_pipeline": true}`.
  `part_id` adalah ARRAY sehingga banyak part_id bisa dikirim sekaligus untuk
  batch (bukan query param yang dibatasi panjang URL).

Endpoint operasional (uji & kontrol tanpa restart):

| Method | Endpoint | Fungsi |
|---|---|---|
| GET | `/api/v1/webhook/camunda/external-tasks/status` | Konfigurasi efektif + statistik worker (fetched/completed/failed/bpmn_errors). |
| POST | `/api/v1/webhook/camunda/external-tasks/poll?limit=1` | Jalankan siklus poll SEKARANG (task benar-benar dikerjakan). |
| POST | `/api/v1/webhook/camunda/external-tasks/start?force=false` | Nyalakan worker (loop). `force=true` mengabaikan `enabled=false`. |
| POST | `/api/v1/webhook/camunda/external-tasks/stop` | Hentikan worker (task yang sedang diproses diselesaikan dulu). |

Worker auto-start saat aplikasi start **hanya** bila `external_tasks.enabled=true`
(alasan: task yang terambil langsung dikerjakan — jangan sampai tidak sengaja
mengubah stok/status sebelum konfigurasi diuji). Semua endpoint di atas tetap
bisa dipakai walau `enabled=false`.

#### Kenapa worker-nya ditulis sendiri (bukan library eksternal)?

`requirements.txt` masih memuat `camunda-external-task-client-python3>=4.5.0`
(opsional). Worker di paket ini (`camunda/external_task.py`) ditulis sendiri
agar memakai konfigurasi, auth, retry, dan konversi tipe yang SAMA dengan jalur
start/message/signal — sehingga tidak ada dua set aturan kredensial. Library
eksternal boleh dipakai sebagai pembanding, tetapi `subscribe()`-nya memblokir
(loop panjang) dan tidak tahu konfigurasi repo ini.

## Mode: satu webhook → beberapa proses

`config.json` punya kunci `mode`:

- `"all"` (**default**) — SEMUA rule yang cocok dijalankan, jadi satu webhook
  bisa men-start lebih dari satu proses (mis. retur + order-status). Satu baris
  log ditulis per rule.
- `"first"` — hanya rule pertama yang cocok (perilaku paling konservatif).

Bisa juga diatur lewat env `CAMUNDA_MATCH_MODE=first` (dipakai bila config.json
 tidak menulis `mode`).

## Contoh nyata: order-status → `Mirorim_Operasional.Trigger_Retur`

Rule `retur-from-order-status` mencocokkan **Shopee `code=3`** dan **TikTok `type=1`**.
Payload mentah yang diterima backend hanya berisi data order:

| Yang dibutuhkan | Shopee (`code=3`) | TikTok (`type=1`) |
|---|---|---|
| order id | `data.ordersn` | `data.order_id` |
| status | `data.status` | `data.order_status` |
| waktu update | `data.update_time` | `data.update_time` |
| shop | `shop_id` (angka) | `shop_id` (string) |
| `resi` / `courier` | **tidak ada** | **tidak ada** |

Karena `resi`/`courier` tidak dikirim, tiap variabel memakai `first` (fallback
berantai antar marketplace/versi API) + `default` supaya proses BPMN selalu
menerima variabel lengkap. Hasil nyata (diverifikasi dengan payload asli):

```json
// POST /engine-rest/process-definition/key/Mirorim_Operasional.Trigger_Retur/start
{
  "variables": {
    "source":      {"value": "apisix-webhook", "type": "String"},
    "resi":        {"value": "2501080NKAMXA8", "type": "String"},
    "courier":     {"value": "", "type": "String"},
    "retur_date":  {"value": "1736323997", "type": "String"},
    "marketplace": {"value": "shopee", "type": "String"},
    "orderId":     {"value": "2501080NKAMXA8", "type": "String"},
    "orderStatus": {"value": "UNPAID", "type": "String"},
    "shopId":      {"value": "341431138", "type": "String"},
    "signalCode":  {"value": 3, "type": "Long"},
    "updateTime":  {"value": "1736323997", "type": "String"}
  },
  "businessKey": "2501080NKAMXA8::1736323997"
}
```

Setara dengan helper referensi (`type: "start"` + `variables.resi` +
`variables.courier` + `businessKey: resi:courier:retur_date`): nama proses ada di
`process_definition_key`, `type: "start"` dikenali sebagai alias
`start_process`, dan `businessKey` ditulis `{{var.resi}}:{{var.courier}}:{{var.retur_date}}`.
Karena `courier` tidak ada di webhook, segmen itu kosong (`resi::retur_date`) —
ubah `"default"` courier di `config.json` bila ingin nilai lain (mis. `"UNKNOWN"`).

Bila `resi`/`courier` nanti tersedia (mis. push tracking number Shopee,
`data.tracking_number`), mapping otomatis memakainya karena sudah jadi kandidat
pertama di rantai `first` — tanpa perlu ubah kode.

### Camunda 7 vs 8

Bagian ini berlaku untuk SEMUA jalur (start event, signal, external task).

| | Camunda 7 | Camunda 8 (Zeebe REST / Orchestration Cluster) |
|---|---|---|
| Start process | `/engine-rest/process-definition/key/{key}/start` | `/v2/process-instances` |
| Message | `/engine-rest/message` | (tidak dipakai di sini) |
| Signal | `/engine-rest/signal` (broadcast), `/engine-rest/execution/{id}/signal` | `/v2/signals/broadcast` (sejak 8.6; per-eksekusi tidak ada) |
| External task | `/engine-rest/external-task/fetchAndLock`, `/{id}/complete`, `/{id}/failure`, `/{id}/bpmnError`, `/{id}/extendLock` | `/v2/jobs/activation`, `/v2/jobs/{key}/completion`, `/failure`, `/error` (extend lock tidak ada) |
| Variabel | `{"value": ..., "type": "String"}` (bisa dimatikan lewat `CAMUNDA_TYPED_VARIABLES=false`) | nilai polos |
| Tenant | `tenantId` | `tenantId` |

`variable_types` (opsional, v7) **memaksa sekaligus menyelaraskan tipe** nilai:
`shopId: 2778101` dengan `{"shopId": "String"}` dikirim sebagai
`{"value": "2778101", "type": "String"}`. Bila nilai tidak bisa dikonversi
(mis. `"abc"` diminta `Long`), tipe yang diminta diabaikan dan dipakai tipe
hasil inferensi + warning di log — supaya Camunda tidak membalas HTTP 400.
Tipe `Json` diterima sebagai nilai array/objek di config, tapi **selalu dikirim
sebagai teks JSON** karena Camunda 7 menolak nilai non-string untuk tipe ini.

## Log & audit

- `logs/camunda/camunda_trigger.log` — 1 baris JSON per webhook: rule yang
  dipakai, variabel final, status HTTP Camunda, jumlah percobaan retry,
  durasi, error, dan payload mentah pemicunya.
- `logs/webhook/order_webhook.log` — payload mentah dari APISIX (lihat
  `api/routers/webhook.py`).

## Catatan penting

- Trigger dijalankan sebagai **background task** setelah response 200 dikirim
  ke APISIX, sehingga retry APISIX tidak menunggu Camunda.
- Bila `mode: "all"` dan beberapa rule cocok, semua proses dicoba; satu yang
  gagal (mis. process key belum di-deploy → HTTP 404) TIDAK membatalkan rule
  lain.
- Bila Camunda mati atau membalas non-2xx setelah semua percobaan, webhook
  **tetap** dianggap diterima (200 ke APISIX) tetapi tercatat `ok: false` di
  log Camunda — aman untuk audit & replay manual.
- `CAMUNDA_ENABLED=false` atau tidak ada rule cocok → tercatat
  `skipped_reason` (`camunda_disabled` / `no_matching_rule`), tanpa HTTP call.
