"""
Unit tests for the SyncEngine reserved-stock merge logic.

Covers:
- build_reserved_stock_map(): parse APISIX /order payload → {variant_id: total}
- SyncEngine._apply_reserved_stock_overrides(): override ParsedVariant.reserved_stock
  by matching variant_id (external_variant_id)
- SyncEngine._zero_absent_reserved_stock(): set reserved_stock=0 utk variant yang
  TIDAK muncul di /order (hanya dipanggil saat sweep /order tuntas)
- SyncEngine._fetch_reserved_stock_map(): delegates to ApisixClient dan returns
  (reserved_map, order_sweep_complete) — empty dict + False pada kegagalan
  (graceful fallback)
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.parsers.base import ParsedProduct, ParsedVariant
from application.usecases.sync_engine import SyncEngine, build_reserved_stock_map
from infrastructure.marketplace.apisix.parsers.factory import get_parser


def _variant_repo():
    """MarketplaceVariantRepository mock dgn list_by_product kosong (default)."""
    repo = MagicMock()
    repo.list_by_product = AsyncMock(return_value=[])
    repo.soft_delete = AsyncMock()
    return repo


# ======================================================================
# build_reserved_stock_map()
# ======================================================================


class TestBuildReservedStockMap:
    def test_valid_tiktok_payload(self):
        """Payload persis seperti API /order TikTok → map per variant_id."""
        raw = {
            "marketplace": "tiktok",
            "reserved_stock": [
                {
                    "product_id": "1736672569117213740",
                    "variants": [
                        {
                            "seller_sku": "8F14-Pack-100",
                            "variant_id": "1736672772166353964",
                            "total_reserved_stock": 3,
                            "orders": [],
                        },
                        {
                            "seller_sku": "8F14",
                            "variant_id": "1736672772166288428",
                            "total_reserved_stock": 3,
                            "orders": [],
                        },
                    ],
                },
                {
                    "product_id": "1736672745058173996",
                    "variants": [
                        {
                            "seller_sku": "2A4-100s",
                            "variant_id": "1736672744536507436",
                            "total_reserved_stock": 2,
                            "orders": [],
                        },
                    ],
                },
            ],
        }
        result = build_reserved_stock_map(raw)
        assert result == {
            "1736672772166353964": 3,
            "1736672772166288428": 3,
            "1736672744536507436": 2,
        }

    def test_empty_and_malformed_payloads(self):
        assert build_reserved_stock_map(None) == {}
        assert build_reserved_stock_map({}) == {}
        assert build_reserved_stock_map({"reserved_stock": []}) == {}
        assert build_reserved_stock_map({"reserved_stock": "nope"}) == {}
        assert build_reserved_stock_map("not-a-dict") == {}

    def test_skips_incomplete_variants(self):
        """Variant tanpa variant_id / total_reserved_stock / int invalid dilewati."""
        raw = {
            "reserved_stock": [
                {
                    "variants": [
                        {"variant_id": "v1"},  # no total_reserved_stock
                        {"total_reserved_stock": 5},  # no variant_id
                        {"variant_id": "v2", "total_reserved_stock": "abc"},  # bad int
                        {"variant_id": "v3", "total_reserved_stock": 7},  # valid
                    ]
                }
            ]
        }
        assert build_reserved_stock_map(raw) == {"v3": 7}


# ======================================================================
# SyncEngine._apply_reserved_stock_overrides()
# ======================================================================


def _make_product(variant_ids):
    return ParsedProduct(
        external_product_id="p1",
        product_name="P1",
        product_image=None,
        variants=[
            ParsedVariant(
                external_variant_id=vid,
                variant_name=None,
                seller_sku=f"sku-{vid}",
                reserved_stock=0,
            )
            for vid in variant_ids
        ],
    )


# ======================================================================
# SyncEngine._zero_absent_reserved_stock()
# ======================================================================


class TestZeroAbsentReservedStock:
    def _product_with_reserved(self, pairs):
        """pairs: [(variant_id, reserved_stock)] — nilai awal meniru hasil parser."""
        return ParsedProduct(
            external_product_id="p1",
            product_name="P1",
            product_image=None,
            variants=[
                ParsedVariant(
                    external_variant_id=vid,
                    variant_name=None,
                    seller_sku=f"sku-{vid}" if vid else "",
                    reserved_stock=reserved,
                )
                for vid, reserved in pairs
            ],
        )

    def test_zeros_only_variants_absent_from_map(self):
        """Variant di map dipertahankan; variant TIDAK ada di /order → 0."""
        product = self._product_with_reserved(
            [("v1", 3), ("v2", 40), ("v3", 7)]
        )

        zeroed = SyncEngine._zero_absent_reserved_stock([product], {"v1": 3})

        assert zeroed == 2
        by_id = {v.external_variant_id: v.reserved_stock for v in product.variants}
        assert by_id["v1"] == 3  # ada di /order → tidak disentuh
        assert by_id["v2"] == 0  # absent (stale 40) → di-reset ke 0
        assert by_id["v3"] == 0

    def test_empty_map_zeros_all_variants(self):
        """/order sukses tapi TIDAK ada entri sama sekali → semua variant = 0."""
        product = self._product_with_reserved([("v1", 5), ("v2", 5)])

        zeroed = SyncEngine._zero_absent_reserved_stock([product], {})

        assert zeroed == 2
        assert all(v.reserved_stock == 0 for v in product.variants)

    def test_variants_in_map_are_untouched(self):
        product = self._product_with_reserved([("v1", 3), ("v2", 40)])

        zeroed = SyncEngine._zero_absent_reserved_stock([product], {"v1": 3, "v2": 40})

        assert zeroed == 0
        by_id = {v.external_variant_id: v.reserved_stock for v in product.variants}
        assert by_id == {"v1": 3, "v2": 40}

    def test_variant_without_id_is_skipped(self):
        product = self._product_with_reserved([(None, 5)])

        zeroed = SyncEngine._zero_absent_reserved_stock([product], {})

        assert zeroed == 0
        assert product.variants[0].reserved_stock == 5


class TestApplyReservedStockOverrides:
    def test_overrides_matching_variants_only(self):
        product = _make_product(["v1", "v2", "v3"])
        reserved_map = {"v1": 3, "v3": 7}

        matched = SyncEngine._apply_reserved_stock_overrides([product], reserved_map)

        assert matched == 2
        by_id = {v.external_variant_id: v.reserved_stock for v in product.variants}
        assert by_id["v1"] == 3
        assert by_id["v3"] == 7
        # v2 tidak ada di map → nilai parser dipertahankan
        assert by_id["v2"] == 0

    def test_no_match_keeps_original_values(self):
        product = _make_product(["v1"])
        matched = SyncEngine._apply_reserved_stock_overrides([product], {"nope": 9})
        assert matched == 0
        assert product.variants[0].reserved_stock == 0

    def test_empty_map_is_noop(self):
        product = _make_product(["v1", "v2"])
        matched = SyncEngine._apply_reserved_stock_overrides([product], {})
        assert matched == 0
        assert all(v.reserved_stock == 0 for v in product.variants)


# ======================================================================
# SyncEngine._fetch_reserved_stock_map()
# ======================================================================


def _make_engine(mock_client):
    return SyncEngine(
        gateway=mock_client,
        parser_factory=get_parser,
        product_service=None,
        variant_service=None,
        mapping_service=None,
        shop_repo=None,
        product_repo=None,
        variant_repo=None,
        marketplace_repo=None,
    )


@pytest.mark.asyncio
async def test_fetch_reserved_stock_map_delegates_to_client():
    mock_client = MagicMock()
    mock_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "tiktok",
            "reserved_stock": [
                {"variants": [{"variant_id": "v1", "total_reserved_stock": 4}]}
            ],
        }
    )
    engine = _make_engine(mock_client)

    shop = MagicMock()
    shop.external_shop_id = "7494709429666874412"

    result = await engine._fetch_reserved_stock_map("tiktok", shop)

    assert result == ({"v1": 4}, True), result
    mock_client.fetch_reserved_stock.assert_awaited_once_with(
        "tiktok", "7494709429666874412"
    )


@pytest.mark.asyncio
async def test_fetch_reserved_stock_map_failure_returns_empty():
    """Client mengembalikan None (endpoint gagal) → ({}, False) — TIDAK boleh
    zeroing karena data /order tidak tersedia."""
    mock_client = MagicMock()
    mock_client.fetch_reserved_stock = AsyncMock(return_value=None)
    engine = _make_engine(mock_client)

    shop = MagicMock()
    shop.external_shop_id = "shop1"

    result = await engine._fetch_reserved_stock_map("tiktok", shop)
    assert result == ({}, False)


@pytest.mark.asyncio
async def test_fetch_reserved_stock_map_partial_sweep_flag():
    """Sweep /order TIDAK lengkap (sweep_complete=False) → map parsial tetap
    dikembalikan tapi flag False → caller TIDAK boleh zeroing variant absent."""
    mock_client = MagicMock()
    mock_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "tiktok",
            "reserved_stock": [
                {"variants": [{"variant_id": "v1", "total_reserved_stock": 4}]}
            ],
            "sweep_complete": False,
            "pages_fetched": 2,
        }
    )
    engine = _make_engine(mock_client)

    shop = MagicMock()
    shop.external_shop_id = "shop1"

    result = await engine._fetch_reserved_stock_map("tiktok", shop)
    assert result == ({"v1": 4}, False), result

# ======================================================================
# sync_by_shop_id() — end-to-end: reserved /order di-match per variant_id
# ======================================================================


@pytest.mark.asyncio
async def test_sync_by_shop_id_saves_reserved_stock_from_order_endpoint():
    """
    Payload product (inventory[0]) memberi reserved_stock=1 dan 0, tetapi
    endpoint /order memberi total_reserved_stock=3 untuk kedua variant.

    Expected: total_reserved_stock yang tersimpan di MarketplaceVariant
    adalah 3 (dari /order, di-match per variant_id), bukan nilai payload.
    """
    shop = MagicMock()
    shop.id = "shop-uuid-1"
    shop.external_shop_id = "7494709429666874412"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-1"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "id": "1736672569117213740",
                    "title": "Produk 1",
                    "status": "ACTIVATE",
                    "skus": [
                        {
                            "id": "1736672772166353964",
                            "seller_sku": "8F14-Pack-100",
                            "inventory": [
                                {"available_stock": 10, "reserved_stock": 1, "location_id": "WH1"}
                            ],
                        },
                        {
                            "id": "1736672772166288428",
                            "seller_sku": "8F14",
                            "inventory": [
                                {"available_stock": 20, "reserved_stock": 0, "location_id": "WH1"}
                            ],
                        },
                    ],
                }
            ]
        }
    )
    apisix_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "tiktok",
            "reserved_stock": [
                {
                    "product_id": "1736672569117213740",
                    "variants": [
                        {
                            "seller_sku": "8F14-Pack-100",
                            "variant_id": "1736672772166353964",
                            "total_reserved_stock": 3,
                        },
                        {
                            "seller_sku": "8F14",
                            "variant_id": "1736672772166288428",
                            "total_reserved_stock": 3,
                        },
                    ],
                }
            ],
        }
    )

    upserted_variants = []
    variant_service = MagicMock()

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="7494709429666874412_1736672569117213740",
            shop_id="7494709429666874412",
            external_product_id="1736672569117213740",
            product_name="Produk 1",
            status=RecordStatus.ACTIVE,
        )
    )

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=_variant_repo(),
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants"] == 2
    assert len(upserted_variants) == 2

    by_id = {v.external_variant_id: v.total_reserved_stock for v in upserted_variants}
    assert by_id["1736672772166353964"] == 3, (
        f"reserved stock harus dari /order (3), bukan payload (1); got {by_id['1736672772166353964']}"
    )
    assert by_id["1736672772166288428"] == 3, (
        f"reserved stock harus dari /order (3), bukan payload (0); got {by_id['1736672772166288428']}"
    )

    variant_service.upsert_variants.assert_awaited_once()
    apisix_client.fetch_reserved_stock.assert_awaited_once_with(
        "tiktok", "7494709429666874412"
    )


@pytest.mark.asyncio
async def test_sync_by_shop_id_saves_reserved_stock_from_order_endpoint_shopee():
    """
    Shopee juga memakai endpoint /order untuk reserved stock (sama seperti
    TikTok). Payload product (summary_info.total_reserved_stock) memberi 0,
    tetapi /order memberi 5 untuk model_id 11250495.

    Expected: total_reserved_stock yang tersimpan = 5 (dari /order,
    di-match per variant_id = model_id), bukan nilai payload.
    """
    shop = MagicMock()
    shop.id = "shop-uuid-2"
    shop.external_shop_id = "227674818"
    shop.shop_name = "Shopee Shop"
    shop.marketplace_id = "mp-uuid-2"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "SHOPEE"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "external_product_id": "802023254",
                    "product_name": "Produk Shopee",
                    "status": "NORMAL",
                    "skus": [
                        {
                            "external_variant_id": "11250495",
                            "seller_sku": "8F14",
                            "total_reserved_stock": 0,
                            "total_available_stock": 200,
                        },
                        {
                            "external_variant_id": "11250496",
                            "seller_sku": "8F14-Pack-100",
                            "total_reserved_stock": 0,
                            "total_available_stock": 44,
                        },
                    ],
                }
            ]
        }
    )
    apisix_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "shopee",
            "reserved_stock": [
                {
                    "product_id": "802023254",
                    "variants": [
                        {
                            "seller_sku": "8F14",
                            "variant_id": "11250495",
                            "total_reserved_stock": 5,
                        },
                    ],
                }
            ],
        }
    )

    upserted_variants = []
    variant_service = MagicMock()

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="227674818_802023254",
            shop_id="227674818",
            external_product_id="802023254",
            product_name="Produk Shopee",
            status=RecordStatus.ACTIVE,
        )
    )

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=_variant_repo(),
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-2", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants"] == 2
    assert len(upserted_variants) == 2

    by_id = {v.external_variant_id: v.total_reserved_stock for v in upserted_variants}
    assert by_id["11250495"] == 5, (
        f"reserved stock harus dari /order (5), bukan payload (0); got {by_id['11250495']}"
    )
    # Variant tanpa entri /order + sweep tuntas → di-set 0 (bukan nilai payload)
    assert by_id["11250496"] == 0

    variant_service.upsert_variants.assert_awaited_once()
    apisix_client.fetch_reserved_stock.assert_awaited_once_with(
        "shopee", "227674818"
    )


@pytest.mark.asyncio
async def test_sync_by_shop_id_zeros_variant_absent_from_order():
    """
    Kasus user: variant yang tidak lagi muncul di /order (order kemarin sudah
    selesai) TIDAK boleh mengendap dengan total_reserved_stock stale.

    Sweep /order tuntas tapi hanya memuat v1 (total 3). Payload product
    memberi reserved_stock v2 = 5 (nilai lama). Expected: v1 → 3 (override),
    v2 → 0 (absent + sweep tuntas → tanpa reservasi).
    """
    shop = MagicMock()
    shop.id = "shop-uuid-3"
    shop.external_shop_id = "7494709429666874412"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-1"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "id": "1736672569117213740",
                    "title": "Produk 1",
                    "status": "ACTIVATE",
                    "skus": [
                        {
                            "id": "1736672772166353964",
                            "seller_sku": "8F14-Pack-100",
                            "inventory": [
                                {"available_stock": 10, "reserved_stock": 1, "location_id": "WH1"}
                            ],
                        },
                        {
                            "id": "1736672772166288428",
                            "seller_sku": "8F14",
                            "inventory": [
                                {"available_stock": 20, "reserved_stock": 5, "location_id": "WH1"}
                            ],
                        },
                    ],
                }
            ]
        }
    )
    # Sweep tuntas (tanpa key sweep_complete = dianggap tuntas) tapi hanya v1
    # yang punya reservasi aktif — v2 sudah tidak ada orderan.
    apisix_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "tiktok",
            "reserved_stock": [
                {
                    "product_id": "1736672569117213740",
                    "variants": [
                        {
                            "seller_sku": "8F14-Pack-100",
                            "variant_id": "1736672772166353964",
                            "total_reserved_stock": 3,
                        },
                    ],
                }
            ],
        }
    )

    upserted_variants = []
    variant_service = MagicMock()

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="7494709429666874412_1736672569117213740",
            shop_id="7494709429666874412",
            external_product_id="1736672569117213740",
            product_name="Produk 1",
            status=RecordStatus.ACTIVE,
        )
    )

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=_variant_repo(),
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-3", status_filter="ALL")

    assert result["status"] == "success"
    assert len(upserted_variants) == 2

    by_id = {v.external_variant_id: v.total_reserved_stock for v in upserted_variants}
    assert by_id["1736672772166353964"] == 3, (
        f"variant di /order → override 3; got {by_id['1736672772166353964']}"
    )
    assert by_id["1736672772166288428"] == 0, (
        f"variant absent dari /order + sweep tuntas → 0 (bukan stale 5); "
        f"got {by_id['1736672772166288428']}"
    )


@pytest.mark.asyncio
async def test_sync_by_shop_id_soft_deletes_stale_variant_not_in_payload():
    """
    Kasus baru: product MASIH ada di payload marketplace, tapi variant DB
    (external_variant_id) TIDAK lagi relate ke marketplace (variant dihapus/
    diarsip). Variant tsb harus di-set DELETED (soft delete), dan mapping-nya
    ikut di-soft-delete — supaya tidak ter-mapping / tidak terhitung di
    inventory report / tidak masuk sync log & queue.

    Scope: HANYA variant milik product yang masih ada di payload yang dicek;
    variant milik product lain tidak disentuh.
    """
    shop = MagicMock()
    shop.id = "shop-uuid-5"
    shop.external_shop_id = "7494709429666874412"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-1"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    # Payload hanya memuat v1 (v2 dihapus/diarsip di marketplace).
    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "id": "1736672569117213740",
                    "title": "Produk 1",
                    "status": "ACTIVATE",
                    "skus": [
                        {
                            "id": "v1",
                            "seller_sku": "8F14",
                            "inventory": [
                                {"available_stock": 10, "reserved_stock": 0, "location_id": "WH1"}
                            ],
                        },
                    ],
                }
            ]
        }
    )
    apisix_client.fetch_reserved_stock = AsyncMock(return_value=None)

    upserted_variants = []
    variant_service = MagicMock()

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="7494709429666874412_1736672569117213740",
            shop_id="7494709429666874412",
            external_product_id="1736672569117213740",
            product_name="Produk 1",
            status=RecordStatus.ACTIVE,
        )
    )

    # Variant lama di DB utk product ini: v1 (masih ada) & v2 (stale).
    stale_v2 = MarketplaceVariant(
        marketplace_variant_id="7494709429666874412_v2",
        marketplace_product_id="7494709429666874412_1736672569117213740",
        shop_id="7494709429666874412",
        external_product_id="1736672569117213740",
        external_variant_id="v2",
        seller_sku="8F14-Pack-100",
        status=RecordStatus.ACTIVE,
    )
    variant_repo = _variant_repo()
    variant_repo.list_by_product = AsyncMock(return_value=[stale_v2])

    mapping_service = MagicMock()
    mapping_service.soft_delete_mapping = AsyncMock()

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=mapping_service,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=variant_repo,
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-5", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants_stale_deleted"] == 1
    variant_repo.soft_delete.assert_awaited_once_with(
        "7494709429666874412", "1736672569117213740", "v2"
    )
    mapping_service.soft_delete_mapping.assert_awaited_once_with(
        "7494709429666874412", "1736672569117213740", "v2"
    )


@pytest.mark.asyncio
async def test_sync_by_shop_id_skips_stale_cleanup_when_sweep_incomplete():
    """
    Sweep products TIDAK tuntas (sweep_complete=False) → stale-variant
    cleanup TIDAK boleh jalan (payload parsial bisa salah menganggap variant
    hilang). Variant lama tetap dipertahankan.
    """
    shop = MagicMock()
    shop.id = "shop-uuid-6"
    shop.external_shop_id = "7494709429666874412"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-1"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "id": "p1",
                    "title": "Produk 1",
                    "status": "ACTIVATE",
                    "skus": [{"id": "v1", "seller_sku": "8F14",
                               "inventory": [{"available_stock": 10, "reserved_stock": 0}]}],
                }
            ],
            "sweep_complete": False,
            "pages_fetched": 1,
        }
    )
    apisix_client.fetch_reserved_stock = AsyncMock(return_value=None)

    variant_service = MagicMock()
    variant_service.upsert_variants = AsyncMock(return_value=1)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="7494709429666874412_p1",
            shop_id="7494709429666874412",
            external_product_id="p1",
            product_name="Produk 1",
            status=RecordStatus.ACTIVE,
        )
    )

    variant_repo = _variant_repo()
    variant_repo.list_by_product = AsyncMock(
        return_value=[MarketplaceVariant(
            marketplace_variant_id="7494709429666874412_v2",
            marketplace_product_id="7494709429666874412_p1",
            shop_id="7494709429666874412",
            external_product_id="p1",
            external_variant_id="v2",
            seller_sku="8F14-Pack-100",
            status=RecordStatus.ACTIVE,
        )]
    )

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=MagicMock(),
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=variant_repo,
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-6", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants_stale_deleted"] == 0
    variant_repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_by_shop_id_keeps_value_when_order_sweep_incomplete():
    """
    Sweep /order TIDAK lengkap (sweep_complete=False) → variant yang tidak
    muncul di /order TIDAK di-zero-kan; nilai payload dipertahankan (perilaku
    lama). Zeroing hanya aman saat data /order meyakinkan.
    """
    shop = MagicMock()
    shop.id = "shop-uuid-4"
    shop.external_shop_id = "7494709429666874412"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-1"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(
        return_value={
            "products": [
                {
                    "id": "1736672569117213740",
                    "title": "Produk 1",
                    "status": "ACTIVATE",
                    "skus": [
                        {
                            "id": "1736672772166353964",
                            "seller_sku": "8F14-Pack-100",
                            "inventory": [
                                {"available_stock": 10, "reserved_stock": 1, "location_id": "WH1"}
                            ],
                        },
                        {
                            "id": "1736672772166288428",
                            "seller_sku": "8F14",
                            "inventory": [
                                {"available_stock": 20, "reserved_stock": 5, "location_id": "WH1"}
                            ],
                        },
                    ],
                }
            ]
        }
    )
    apisix_client.fetch_reserved_stock = AsyncMock(
        return_value={
            "marketplace": "tiktok",
            "reserved_stock": [
                {
                    "product_id": "1736672569117213740",
                    "variants": [
                        {
                            "seller_sku": "8F14-Pack-100",
                            "variant_id": "1736672772166353964",
                            "total_reserved_stock": 3,
                        },
                    ],
                }
            ],
            "sweep_complete": False,
            "pages_fetched": 2,
        }
    )

    upserted_variants = []
    variant_service = MagicMock()

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)
    product_service = MagicMock()
    product_service.create_product = AsyncMock(
        return_value=MarketplaceProduct(
            marketplace_product_id="7494709429666874412_1736672569117213740",
            shop_id="7494709429666874412",
            external_product_id="1736672569117213740",
            product_name="Produk 1",
            status=RecordStatus.ACTIVE,
        )
    )

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=_variant_repo(),
        marketplace_repo=marketplace_repo,
    )

    result = await engine.sync_by_shop_id("shop-uuid-4", status_filter="ALL")

    assert result["status"] == "success"
    assert len(upserted_variants) == 2

    by_id = {v.external_variant_id: v.total_reserved_stock for v in upserted_variants}
    assert by_id["1736672772166353964"] == 3  # tetap di-override dari /order
    assert by_id["1736672772166288428"] == 5, (
        f"sweep parsial → jangan zeroing; nilai payload 5 dipertahankan; "
        f"got {by_id['1736672772166288428']}"
    )


# ======================================================================
# Stale product → FREEZE + variant DEACTIVATED (_freeze_stale_products)
# ======================================================================


def _make_db_product(external_product_id, status=RecordStatus.ACTIVE):
    p = MarketplaceProduct(
        marketplace_product_id="shop_" + external_product_id,
        shop_id="shop123",
        external_product_id=external_product_id,
        product_name="Produk " + external_product_id,
        status=status,
    )
    return p


def _make_db_variant(external_product_id, variant_id, status=RecordStatus.ACTIVE):
    v = MarketplaceVariant(
        marketplace_variant_id="shop_" + variant_id,
        marketplace_product_id="shop_" + external_product_id,
        shop_id="shop123",
        external_product_id=external_product_id,
        external_variant_id=variant_id,
        variant_name="Variant " + variant_id,
        seller_sku="SKU-" + variant_id,
        status=status,
    )
    return v


@pytest.mark.asyncio
async def test_freeze_stale_products_deactivates_absent_product():
    """Product DB yang TIDAK ada di payload → status FREEZE, variant-nya
    di-set DEACTIVATED (mapping DIPERTAHANKAN supaya tetap terhitung di
    inventory report — hanya dikecualikan dari queue/push/log)."""
    shop = MagicMock()
    shop.external_shop_id = "shop123"
    shop.marketplace_id = "mp1"

    product_repo = MagicMock()
    stale = _make_db_product("prod-stale")
    present = _make_db_product("prod-present")
    product_repo.list_by_shop = AsyncMock(return_value=[stale, present])
    product_repo.update = AsyncMock()

    variant_repo = MagicMock()
    stale_variant = _make_db_variant("prod-stale", "var-stale")
    variant_repo.list_by_product = AsyncMock(return_value=[stale_variant])
    variant_repo.update = AsyncMock()
    variant_repo.soft_delete = AsyncMock()

    mapping_service = MagicMock()
    mapping_service.soft_delete_mapping = AsyncMock()

    engine = SyncEngine(
        gateway=MagicMock(),
        parser_factory=get_parser,
        product_service=None,
        variant_service=None,
        mapping_service=mapping_service,
        shop_repo=None,
        product_repo=product_repo,
        variant_repo=variant_repo,
        marketplace_repo=None,
    )

    parsed_present = ParsedProduct(
        external_product_id="prod-present",
        product_name="Present",
        product_image=None,
        status=RecordStatus.ACTIVE,
        variants=[],
    )

    count = await engine._freeze_stale_products(shop, [parsed_present])

    assert count == 1
    # Hanya product absent yang di-update statusnya → FREEZE
    assert product_repo.update.await_count == 1
    assert stale.status == RecordStatus.FREEZE
    # Product present tidak tersentuh
    assert present.status == RecordStatus.ACTIVE
    # Variant product stale di-set DEACTIVATED via repo.update (bukan
    # soft-delete) — status tetap terhitung di inventory report.
    variant_repo.update.assert_awaited_once_with(stale_variant)
    assert stale_variant.status == RecordStatus.DEACTIVATED
    # Mapping TIDAK di-soft-delete & variant TIDAK di-soft-delete.
    variant_repo.soft_delete.assert_not_awaited()
    mapping_service.soft_delete_mapping.assert_not_awaited()


@pytest.mark.asyncio
async def test_freeze_stale_products_skips_already_frozen():
    """Product yang SUDAH FREEZE tidak di-update ulang (idempotent)."""
    shop = MagicMock()
    shop.external_shop_id = "shop123"

    product_repo = MagicMock()
    already = _make_db_product("prod-already", status=RecordStatus.FREEZE)
    product_repo.list_by_shop = AsyncMock(return_value=[already])
    product_repo.update = AsyncMock()

    variant_repo = MagicMock()
    variant_repo.list_by_product = AsyncMock(return_value=[])
    variant_repo.update = AsyncMock()
    variant_repo.soft_delete = AsyncMock()

    engine = SyncEngine(
        gateway=MagicMock(),
        parser_factory=get_parser,
        product_service=None,
        variant_service=None,
        mapping_service=MagicMock(),
        shop_repo=None,
        product_repo=product_repo,
        variant_repo=variant_repo,
        marketplace_repo=None,
    )

    count = await engine._freeze_stale_products(shop, [])
    assert count == 0
    product_repo.update.assert_not_awaited()
    variant_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_freeze_stale_products_all_in_payload():
    """Semua product DB ada di payload → tidak ada yang di-freeze."""
    shop = MagicMock()
    shop.external_shop_id = "shop123"

    product_repo = MagicMock()
    a = _make_db_product("prod-a")
    b = _make_db_product("prod-b")
    product_repo.list_by_shop = AsyncMock(return_value=[a, b])
    product_repo.update = AsyncMock()

    variant_repo = MagicMock()
    variant_repo.list_by_product = AsyncMock(return_value=[])
    variant_repo.update = AsyncMock()
    variant_repo.soft_delete = AsyncMock()

    engine = SyncEngine(
        gateway=MagicMock(),
        parser_factory=get_parser,
        product_service=None,
        variant_service=None,
        mapping_service=MagicMock(),
        shop_repo=None,
        product_repo=product_repo,
        variant_repo=variant_repo,
        marketplace_repo=None,
    )

    parsed = [
        ParsedProduct(
            external_product_id="prod-a", product_name="A",
            product_image=None, status=RecordStatus.ACTIVE, variants=[],
        ),
        ParsedProduct(
            external_product_id="prod-b", product_name="B",
            product_image=None, status=RecordStatus.ACTIVE, variants=[],
        ),
    ]

    count = await engine._freeze_stale_products(shop, parsed)
    assert count == 0
    product_repo.update.assert_not_awaited()


# ======================================================================
# sync_by_shop_id() — TikTok listing platform: product TIDAK ada di sweep
# TIKTOK_SHOP → FREEZE + variant DEACTIVATED (walau ada di TOKOPEDIA)
# ======================================================================


def _tiktok_listing_payload():
    """Payload gabungan sweep TOKOPEDIA + TIKTOK_SHOP (via ApisixClient):
    - prod-tp-only : hanya di sweep TOKOPEDIA → harus di-FREEZE
    - prod-both    : di KEDUA sweep → tetap sesuai status payload (ACTIVE)
    """
    return {
        "products": [
            {
                "id": "prod-tp-only",
                "title": "Hanya Tokopedia",
                "status": "ACTIVATE",
                "skus": [
                    {
                        "id": "v-tp-1",
                        "seller_sku": "SKU-TP-1",
                        "inventory": [
                            {"available_stock": 5, "reserved_stock": 1, "location_id": "WH1"}
                        ],
                    },
                ],
            },
            {
                "id": "prod-both",
                "title": "Cross-listed",
                "status": "ACTIVATE",
                "skus": [
                    {
                        "id": "v-both-1",
                        "seller_sku": "SKU-BOTH-1",
                        "inventory": [
                            {"available_stock": 7, "reserved_stock": 2, "location_id": "WH1"}
                        ],
                    },
                ],
            },
        ],
        "product_listing_platforms": {
            "prod-tp-only": ["TOKOPEDIA"],
            "prod-both": ["TOKOPEDIA", "TIKTOK_SHOP"],
        },
    }


def _make_listing_sync_setup(payload):
    """Setup engine + mocks utk sync TikTok dgn product_listing_platforms."""
    shop = MagicMock()
    shop.id = "shop-uuid-tp"
    shop.external_shop_id = "7496101136776792121"
    shop.shop_name = "TikTok Shop"
    shop.marketplace_id = "mp-uuid-tp"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    marketplace = MagicMock()
    marketplace.code = "TIKTOK"
    marketplace_repo = MagicMock()
    marketplace_repo.get_by_id = AsyncMock(return_value=marketplace)

    apisix_client = MagicMock()
    apisix_client.fetch_products = AsyncMock(return_value=payload)
    apisix_client.fetch_reserved_stock = AsyncMock(return_value=None)

    upserted_variants = []

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service = MagicMock()
    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)

    product_service = MagicMock()
    product_service.create_product = AsyncMock(side_effect=lambda p: p)
    product_service.update_product = AsyncMock(side_effect=lambda p: p)

    product_repo = MagicMock()
    product_repo.list_by_shop = AsyncMock(return_value=[])

    variant_repo = _variant_repo()

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=product_repo,
        variant_repo=variant_repo,
        marketplace_repo=marketplace_repo,
    )
    return {
        "engine": engine,
        "product_service": product_service,
        "upserted_variants": upserted_variants,
    }


@pytest.mark.asyncio
async def test_sync_forces_freeze_when_product_missing_from_tiktok_shop_sweep():
    """
    Product yang ada di payload gabungan TAPI TIDAK ada di sweep TIKTOK_SHOP
    (hanya TOKOPEDIA) → status product di DB FREEZE + variant-nya
    DEACTIVATED, WALAU status payload marketplace masih ACTIVATE.
    Product cross-listed (ada di TIKTOK_SHOP) tetap ACTIVE.
    """
    setup = _make_listing_sync_setup(_tiktok_listing_payload())
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-tp", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_products"] == 2

    # Variant milik product yang tidak ada di TIKTOK_SHOP → DEACTIVATED.
    by_variant = {
        v.external_variant_id: v.status for v in setup["upserted_variants"]
    }
    assert by_variant["v-tp-1"] == RecordStatus.DEACTIVATED
    # Variant product cross-listed tetap ACTIVE (dari payload).
    assert by_variant["v-both-1"] == RecordStatus.ACTIVE

    # update_product dipanggil untuk memaksa status FREEZE di DB.
    updated = [
        call.args[0]
        for call in setup["product_service"].update_product.await_args_list
    ]
    frozen_ids = {p.external_product_id for p in updated}
    assert frozen_ids == {"prod-tp-only"}, (
        f"hanya prod-tp-only yang di-freeze; got {frozen_ids}"
    )
    assert all(p.status == RecordStatus.FREEZE for p in updated)


@pytest.mark.asyncio
async def test_sync_keeps_status_when_all_products_on_tiktok_shop():
    """Semua product ada di sweep TIKTOK_SHOP → tidak ada yang di-freeze."""
    payload = _tiktok_listing_payload()
    payload["product_listing_platforms"] = {
        "prod-tp-only": ["TOKOPEDIA", "TIKTOK_SHOP"],
        "prod-both": ["TIKTOK_SHOP"],
    }
    setup = _make_listing_sync_setup(payload)

    result = await setup["engine"].sync_by_shop_id("shop-uuid-tp", status_filter="ALL")

    assert result["status"] == "success"
    setup["product_service"].update_product.assert_not_awaited()
    by_variant = {
        v.external_variant_id: v.status for v in setup["upserted_variants"]
    }
    assert by_variant["v-tp-1"] == RecordStatus.ACTIVE
    assert by_variant["v-both-1"] == RecordStatus.ACTIVE


@pytest.mark.asyncio
async def test_sync_skips_listing_freeze_when_sweep_incomplete():
    """Sweep tidak tuntas → paksaan FREEZE TIDAK dijalankan (data mungkin
    terlewat; hindari freeze palsu)."""
    payload = _tiktok_listing_payload()
    payload["sweep_complete"] = False
    setup = _make_listing_sync_setup(payload)

    result = await setup["engine"].sync_by_shop_id("shop-uuid-tp", status_filter="ALL")

    assert result["status"] == "success"
    setup["product_service"].update_product.assert_not_awaited()
    by_variant = {
        v.external_variant_id: v.status for v in setup["upserted_variants"]
    }
    assert by_variant["v-tp-1"] == RecordStatus.ACTIVE
