# Modul: infrastructure/marketplace/apisix

## Asal implementasi
- `infrastructure/apisix/client.py` (1525 baris) → dipecah menjadi:
  - `constants.py` — `STATUS_*`, `STOCK_MIN/MAX`, `TIKTOK_LISTING_PLATFORMS`,
    `CONFIG_ERROR_PREFIX`, `HTTP_ERROR_BODY_MAX_CHARS`, `ApisixConfigError`,
    `raise_http_error_with_body()`.
  - `pagination.py` — helper murni: `normalize_path`, `parse_pagination`,
    `pagination_has_next`, `pagination_has_more`, `product_id_key`,
    `is_cursor_marketplace`, `next_page_plan`.
  - `client.py` — `ApisixClient` (I/O HTTP).
- `infrastructure/apisix/parsers/*` → `infrastructure/marketplace/apisix/parsers/*`.

## Perubahan
- Logika pagination murni dipindah ke `pagination.py`; `ApisixClient` memakai
  fungsi tersebut (bukan static method tersembunyi).
- Konstanta & error helper dipusatkan di `constants.py`.
- `ShopeeParser._parse_single_item` dipecah jadi `_parse_normalized_item` /
  `_parse_raw_item` (logika sama).
- Import path parser disesuaikan.

## Alasan
- File 1525 baris mencampur aturan pagination (murni) dengan I/O HTTP.
- Memisahkan yang bisa diuji tanpa jaringan dari yang butuh jaringan.

## Compatibility
- Kontrak method `ApisixClient` **identik**: `fetch_products`,
  `fetch_reserved_stock`, `update_status`, `update_stock`, `update_stock_batch`,
  `aclose`. Body/param HTTP tidak berubah.
- Export `CONFIG_ERROR_PREFIX` & `ApisixConfigError` tetap tersedia (kini dari
  `infrastructure.marketplace.apisix`).
- Parser menghasilkan `ParsedProduct`/`ParsedVariant` yang sama.

## TODO / perbaikan lanjutan
- `TIKTOK_LISTING_PLATFORMS` bisa dipindah ke konfigurasi bila nilainya mungkin
  berubah per-deployment.
- Heuristik `_marketplace_code_for_shop` masih ada di pipeline (bukan di client);
  perlu perbaikan agar tidak menebak marketplace dari pola shop id.
