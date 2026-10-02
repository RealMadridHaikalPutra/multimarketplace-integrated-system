# Modul: application (ports, dto, services)

## Asal implementasi
- `application/services/*` — ProductService, VariantService, MappingService,
  MappingValidationService.
- `application/dto/requests.py` — semua DTO.
- Port `application/ports/` — **baru** (memutus `application → infrastructure`).

## Perubahan
- `MappingService` & `MappingValidationService` tidak lagi mengimpor
  `infrastructure.inventree.client` / `infrastructure.apisix.parsers`: keduanya
  bergantung pada port `InventoryProvider` (Protocol). `ApisixClient` &
  `InvenTreeClient` memenuhi port ini secara struktural.
- Aturan konflik & parsing SKU memakai `domain.rules` (bukan definisi lokal).
- Deteksi konflik produk (`check_parsed_products_for_conflicts`) memakai
  `domain.rules.sku_conflict.detect_product_pack_sx_conflict` dengan daftar
  `seller_sku` (duck-typed product), menghapus impor infrastructure.
- `settings` kini dari `core.config`.

## Alasan
- Menegakkan dependency rule: lapisan application hanya tahu kontrak.
- Menghapus duplikasi aturan konflik.
- Memudahkan pengujian (port dapat di-*fake*).

## Compatibility
- Signature publik service **sama**.
- Perilaku TC-SKU-006, idempotent create variant, upsert batch, dsb. identik.
- DTO tidak berubah (schema request/response API tetap).

## TODO / perbaikan lanjutan
- DTO masih satu file besar; dapat dipecah per domain (`marketplace_dto.py`,
  `sync_dto.py`, ...) tanpa mengubah kelas.
- `sku_service.py` (alias deprecated) tidak diport.
- Beberapa `except Exception` di service masih menelan detail → evaluasi
  konsistensi error handling saat porting use case.
