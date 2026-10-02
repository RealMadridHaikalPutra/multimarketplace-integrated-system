"""
Unit tests: TC-SKU-006 — BLOCK duplikasi kategori SKU (Case 1) &
deteksi paritas SKU MP vs InvenTree (Case 2) via validation_issue.

Case 1: SKU_PACK vs SKU_S/SKU_X pada part yang sama HANYA konflik jika
        ANGKA qty-nya SAMA (Pack-50 vs 50s → BLOCK PACK; Pack-50 vs 2s
        → BOLEH). NORMAL + PACK selalu boleh berbagi part.
Case 2: SKU MP != SKU InvenTree (dua arah) → dicatat di validation_issue.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from application.services.mapping_service import (
    MappingService,
    MAPPING_STATUS_CONFLICT,
)
from application.services.mapping_validation_service import (
    MappingValidationService,
    CHECK_PART_CATEGORY_CONFLICT,
    CHECK_MP_NOT_IN_INVENTREE,
    CHECK_INVENTREE_ORPHAN,
)
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.entities.sku_mapping import SKUMapping
from domain.enums import RecordStatus, SkuType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_variant(seller_sku: str, variant_id: str = "shop123_variant001") -> MarketplaceVariant:
    return MarketplaceVariant(
        marketplace_variant_id=variant_id,
        shop_id="shop123",
        external_product_id="prod456",
        external_variant_id="variant001",
        variant_name="Variant A",
        seller_sku=seller_sku,
        total_reserved_stock=5,
        total_available_stock=100,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )


def _make_mapping(
    sku_type,
    part_id: int,
    variant_id: str = "shop123_variant001",
    status: str = "ACTIVE",
    conversion_qty: float = 1.0,
    seller_sku: str = "",
) -> SKUMapping:
    return SKUMapping(
        sku_mapping_id=f"mapping-{variant_id}",
        marketplace_variant_id=variant_id,
        inventree_part_id=part_id,
        sku_type=sku_type,
        conversion_qty=conversion_qty,
        status=status,
        seller_sku=seller_sku,
    )


def _make_service(active_on_part=None) -> MappingService:
    """MappingService dengan repo mock; find_active_by_part_id configurable."""
    mapping_repo = MagicMock()
    mapping_repo.get_by_variant_id = AsyncMock(return_value=[])
    mapping_repo.create = AsyncMock(side_effect=lambda m: m)
    mapping_repo.update = AsyncMock(side_effect=lambda m: m)
    mapping_repo.find_active_by_part_id = AsyncMock(
        return_value=active_on_part or []
    )

    variant_repo = MagicMock()
    variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("PLACEHOLDER")
    )

    inventree_client = MagicMock()
    inventree_client.find_part_by_sku = AsyncMock(return_value=2001549)

    return MappingService(
        mapping_repo=mapping_repo,
        variant_repo=variant_repo,
        inventory_provider=inventree_client,
    )


# ---------------------------------------------------------------------------
# Case 1 — BLOCK SKU_PACK vs SKU_S/SKU_X HANYA jika ANGKA qty SAMA
# (Pack-50 vs 50s → KONFLIK; Pack-50 vs 2s → BOLEH; NORMAL + PACK selalu
# boleh berbagi part — contoh 1A1 + 1A1-Pack-50)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_pack_when_part_used_by_s_same_qty():
    """
    create_mapping utk SKU_PACK (1A1-Pack-50) pada part yang sudah punya
    ACTIVE SKU_S dengan ANGKA SAMA (1A1-50s, qty=50) → status CONFLICT.
    """
    existing_s = _make_mapping(
        SkuType.SKU_S, 2001549, variant_id="other_variant", seller_sku="1A1-50s"
    )
    service = _make_service(active_on_part=[existing_s])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-Pack-50")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == MAPPING_STATUS_CONFLICT, (
        f"PACK-50 pada part yg dipakai 50s (angka sama) harus CONFLICT, got {mapping.status}"
    )


@pytest.mark.asyncio
async def test_block_pack_when_part_used_by_x_same_qty():
    """
    create_mapping utk SKU_PACK (1A1-Pack-50) pada part yang sudah punya
    ACTIVE SKU_X dengan ANGKA SAMA (1A1-50x, qty=50) → status CONFLICT.
    """
    existing_x = _make_mapping(
        SkuType.SKU_X, 2001549, variant_id="other_variant", seller_sku="1A1-50x"
    )
    service = _make_service(active_on_part=[existing_x])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-Pack-50")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == MAPPING_STATUS_CONFLICT, (
        f"PACK-50 pada part yg dipakai 50x (angka sama) harus CONFLICT, got {mapping.status}"
    )


@pytest.mark.asyncio
async def test_pack_allowed_when_part_sx_diff_qty():
    """
    RULES TERBARU: PACK-50 + SKU_S dengan ANGKA BEDA (1A1-2s, qty=2)
    → BOLEH berbagi part, status ACTIVE (bukan CONFLICT).
    """
    existing_s = _make_mapping(
        SkuType.SKU_S, 2001549, variant_id="other_variant", seller_sku="1A1-2s"
    )
    service = _make_service(active_on_part=[existing_s])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-Pack-50")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == "ACTIVE", (
        f"PACK-50 vs 2s (angka beda) harus BOLEH — ACTIVE, got {mapping.status}"
    )


@pytest.mark.asyncio
async def test_pack_allowed_when_part_used_by_normal():
    """
    SKU_PACK pada part yang sudah punya ACTIVE NORMAL → status ACTIVE
    (NORMAL + PACK BOLEH berbagi part_id — mis. 1A1 & 1A1-Pack-50).
    """
    existing_normal = _make_mapping(
        SkuType.NORMAL, 2001549, variant_id="normal_variant", seller_sku="1A1"
    )
    service = _make_service(active_on_part=[existing_normal])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-Pack-50")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == "ACTIVE", (
        f"NORMAL + PACK boleh berbagi part — harus ACTIVE, got {mapping.status}"
    )


@pytest.mark.asyncio
async def test_pack_allowed_when_part_unused():
    """
    SKU_PACK pada part yang belum dipakai mapping lain → status ACTIVE.
    """
    service = _make_service(active_on_part=[])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-Pack-50")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == "ACTIVE"


@pytest.mark.asyncio
async def test_s_mapping_demotes_existing_pack_same_qty():
    """
    Mapping SKU_S (1A1-50s) pada part yang sudah punya ACTIVE PACK dengan
    ANGKA SAMA (1A1-Pack-50) → PACK lama di-demote ke CONFLICT, mapping
    SKU_S baru tetap ACTIVE.
    """
    existing_pack = _make_mapping(
        SkuType.SKU_PACK, 2001549, variant_id="pack_variant", seller_sku="1A1-Pack-50"
    )
    service = _make_service(active_on_part=[existing_pack])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-50s")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == "ACTIVE", "Mapping SKU_S baru harus tetap ACTIVE"
    # PACK lama harus di-demote via repo.update
    update_calls = service._mapping_repo.update.await_args_list
    assert update_calls, "repo.update harus dipanggil utk demote PACK"
    demoted = update_calls[0].args[0]
    assert demoted.status == MAPPING_STATUS_CONFLICT, (
        f"PACK lama harus CONFLICT, got {demoted.status}"
    )


@pytest.mark.asyncio
async def test_s_mapping_keeps_pack_with_diff_qty():
    """
    RULES TERBARU: SKU_S (1A1-2s, qty=2) pada part yang sudah punya PACK
    dengan ANGKA BEDA (1A1-Pack-50, qty=50) → PACK TIDAK di-demote, mapping
    SKU_S baru tetap ACTIVE (keduanya sah berbagi part).
    """
    existing_pack = _make_mapping(
        SkuType.SKU_PACK, 2001549, variant_id="pack_variant", seller_sku="1A1-Pack-50"
    )
    service = _make_service(active_on_part=[existing_pack])
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant("1A1-2s")
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.status == "ACTIVE", "Mapping SKU_S baru harus ACTIVE"
    # Tidak boleh ada demote (repo.update tidak dipanggil di jalur demote).
    demote_calls = [
        c.args[0]
        for c in service._mapping_repo.update.await_args_list
        if c.args[0].sku_mapping_id == "mapping-pack_variant"
    ]
    assert not demote_calls, (
        f"PACK dengan angka beda tidak boleh di-demote, got {len(demote_calls)} update call(s)"
    )


# ---------------------------------------------------------------------------
# Case 2 — Paritas SKU MP vs InvenTree
# ---------------------------------------------------------------------------


def _make_validation_service(
    mappings=None,
    shop_variants=None,
    all_mp_skus=None,
    inventree_items=None,
):
    mapping_repo = MagicMock()
    mapping_repo.list_all = AsyncMock(return_value=mappings or [])

    variant_repo = MagicMock()
    variant_repo.list_by_shop = AsyncMock(return_value=shop_variants or [])
    variant_repo.list_all_seller_skus = AsyncMock(return_value=all_mp_skus or [])

    inventree_client = MagicMock()
    inventree_client.get_all_stock = AsyncMock(return_value=inventree_items or [])

    validation_repo = MagicMock()
    validation_repo.upsert_detected = AsyncMock(side_effect=lambda i: i)
    validation_repo.mark_resolved = AsyncMock(return_value=0)

    return MappingValidationService(
        validation_repo=validation_repo,
        mapping_repo=mapping_repo,
        variant_repo=variant_repo,
        inventory_provider=inventree_client,
    )


@pytest.mark.asyncio
async def test_detect_part_category_conflict_same_qty():
    """
    Satu part_id dipakai SKU_PACK (1A1-Pack-50) dan SKU_S dengan ANGKA SAMA
    (1A1-50s) → issue CONFLICT (check_code SKU_PART_CATEGORY_CONFLICT,
    check GLOBAL).
    """
    mappings = [
        _make_mapping(SkuType.SKU_S, 2001549, variant_id="v1", seller_sku="1A1-50s"),
        _make_mapping(
            SkuType.SKU_PACK, 2001549, variant_id="v2",
            conversion_qty=50.0, seller_sku="1A1-Pack-50",
        ),
    ]
    service = _make_validation_service(mappings=mappings)

    summary = await service.run_global_checks()

    assert summary["total_conflicts"] == 1
    assert summary["total_warnings"] == 0
    issue = summary["issues"][0]
    assert issue["check_code"] == CHECK_PART_CATEGORY_CONFLICT
    assert issue["severity"] == "CONFLICT"
    assert issue["entity_type"] == "PART"
    assert issue["entity_id"] == "2001549"
    assert issue["action_taken"] == "mapping_blocked"
    assert issue["details"]["families"] == ["PACK", "SX"]
    # Detail harus memuat ANGKA qty per mapping utk audit.
    qty_by_sku = {d["seller_sku"]: d["qty"] for d in issue["details"]["involved_mappings"]}
    assert qty_by_sku == {"1A1-50s": 50, "1A1-Pack-50": 50}


@pytest.mark.asyncio
async def test_no_conflict_when_pack_and_sx_diff_qty():
    """
    RULES TERBARU: PACK-50 (qty=50) + SKU_S 2s (qty=2) pada part yang sama
    → ANGKA BEDA → TIDAK ada konflik (keduanya sah berbagi part).
    """
    mappings = [
        _make_mapping(SkuType.SKU_S, 2001549, variant_id="v1", seller_sku="1A1-2s"),
        _make_mapping(
            SkuType.SKU_PACK, 2001549, variant_id="v2",
            conversion_qty=50.0, seller_sku="1A1-Pack-50",
        ),
    ]
    service = _make_validation_service(mappings=mappings)

    summary = await service.run_global_checks()

    assert summary["total_conflicts"] == 0, (
        "PACK-50 vs 2s (angka beda) tidak boleh memicu conflict issue"
    )


@pytest.mark.asyncio
async def test_no_conflict_when_part_single_family():
    """Part yang hanya dipakai S/X/NORMAL (atau hanya PACK) → tidak ada issue."""
    mappings = [
        _make_mapping(SkuType.SKU_S, 2001549, variant_id="v1", seller_sku="1A1-50s"),
        _make_mapping(SkuType.SKU_X, 2001549, variant_id="v2", seller_sku="1A1-2x"),
        _make_mapping(
            SkuType.SKU_PACK, 3000001, variant_id="v3",
            conversion_qty=50.0, seller_sku="1A1-Pack-50",
        ),
    ]
    service = _make_validation_service(mappings=mappings)

    summary = await service.run_global_checks()

    assert summary["total_conflicts"] == 0


@pytest.mark.asyncio
async def test_no_conflict_when_part_normal_and_pack():
    """
    NORMAL + PACK pada part yang sama → TIDAK ada konflik (sah berbagi
    part_id — contoh 1A1 & 1A1-Pack-50).
    """
    mappings = [
        _make_mapping(SkuType.NORMAL, 2001549, variant_id="v1", seller_sku="1A1"),
        _make_mapping(
            SkuType.SKU_PACK, 2001549, variant_id="v2",
            conversion_qty=50.0, seller_sku="1A1-Pack-50",
        ),
    ]
    service = _make_validation_service(mappings=mappings)

    summary = await service.run_global_checks()

    assert summary["total_conflicts"] == 0, (
        "NORMAL + PACK boleh berbagi part — tidak boleh ada conflict issue"
    )


@pytest.mark.asyncio
async def test_detect_mp_sku_not_in_inventree():
    """
    SKU MP '1A1-50s' tidak ada di sku_toko InvenTree → issue WARNING
    (check_code SKU_MP_NOT_IN_INVENTREE), policy report-only.
    """
    shop_variants = [_make_variant("1A1-50s", variant_id="v-missing")]
    inventree_items = [
        {"part_id": 2001549, "sku_toko": "1A1-50x", "name": "Barang A"},
    ]
    service = _make_validation_service(
        shop_variants=shop_variants,
        all_mp_skus=["1A1-50s", "1A1-50x"],
        inventree_items=inventree_items,
    )

    summary = await service.run_checks_for_shop("shop123")

    mp_issues = [i for i in summary["issues"] if i["check_code"] == CHECK_MP_NOT_IN_INVENTREE]
    assert len(mp_issues) == 1
    issue = mp_issues[0]
    assert issue["severity"] == "WARNING"
    assert issue["entity_type"] == "VARIANT"
    assert issue["reference"] == "1A1-50s"
    assert issue["action_taken"] == "reported_only"


@pytest.mark.asyncio
async def test_detect_inventree_orphan():
    """
    sku_toko InvenTree '9Z9-99x' tidak dipakai SKU MP manapun → issue WARNING
    (check_code SKU_INVENTREE_ORPHAN).
    """
    shop_variants = [_make_variant("1A1-50s", variant_id="v1")]
    inventree_items = [
        {"part_id": 2001549, "sku_toko": "1A1-50s", "name": "Barang A"},
        {"part_id": 7777777, "sku_toko": "9Z9-99x", "name": "Orphan Part"},
    ]
    service = _make_validation_service(
        shop_variants=shop_variants,
        all_mp_skus=["1A1-50s"],
        inventree_items=inventree_items,
    )

    summary = await service.run_global_checks()

    orphan_issues = [i for i in summary["issues"] if i["check_code"] == CHECK_INVENTREE_ORPHAN]
    assert len(orphan_issues) == 1
    issue = orphan_issues[0]
    assert issue["severity"] == "WARNING"
    assert issue["entity_type"] == "PART"
    assert issue["entity_id"] == "7777777"
    assert issue["reference"] == "9Z9-99x"


@pytest.mark.asyncio
async def test_mapping_engine_skips_deleted_variants():
    """
    Variant berstatus DELETED (dihapus dari marketplace / tidak relate lagi)
    TIDAK boleh ikut ter-mapping — MappingEngine harus men-skip-nya.
    """
    from application.usecases.mapping_engine import MappingEngine

    shop = MagicMock()
    shop.id = "shop-uuid-1"
    shop.external_shop_id = "shop123"
    shop.shop_name = "Test Shop"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    product_repo = MagicMock()
    product_repo.list_by_shop = AsyncMock(return_value=[])

    variant_active = _make_variant("1A1", variant_id="v-active")
    variant_deleted = _make_variant("9Z9-99x", variant_id="v-deleted")
    variant_deleted.status = RecordStatus.DELETED

    variant_repo = MagicMock()
    variant_repo.list_by_shop = AsyncMock(
        return_value=[variant_active, variant_deleted]
    )

    mapping_service = MagicMock()
    mapping_service.update_mapping_for_variant = AsyncMock(
        return_value=_make_mapping(
            SkuType.NORMAL, 2001549, variant_id="v-active", seller_sku="1A1"
        )
    )
    validation_service = None  # validasi nonaktif — fokus ke skip DELETED

    engine = MappingEngine(
        mapping_service=mapping_service,
        variant_service=MagicMock(),
        variant_repo=variant_repo,
        product_repo=product_repo,
        shop_repo=shop_repo,
        validation_service=validation_service,
    )

    summary = await engine.run_mapping_for_shop("shop-uuid-1")

    assert summary["total_skipped_deleted"] == 1, summary
    assert summary["total_mapped"] == 1, summary
    # Mapping HANYA dipanggil utk variant ACTIVE — DELETED di-skip.
    mapping_service.update_mapping_for_variant.assert_awaited_once_with(
        "shop123", "prod456", "variant001"
    )


@pytest.mark.asyncio
async def test_mapping_engine_skips_variants_of_deactivated_products():
    """
    Variant milik product berstatus DEACTIVATED (product tidak relate ke
    marketplace) TIDAK boleh ikut ter-mapping — MappingEngine harus men-skip-nya.
    """
    from application.usecases.mapping_engine import MappingEngine

    shop = MagicMock()
    shop.id = "shop-uuid-1"
    shop.external_shop_id = "shop123"
    shop.shop_name = "Test Shop"

    shop_repo = MagicMock()
    shop_repo.get_by_id = AsyncMock(return_value=shop)

    product_repo = MagicMock()
    deactivated_product = MagicMock()
    deactivated_product.external_product_id = "prod-deactivated"
    deactivated_product.status = RecordStatus.DEACTIVATED
    product_repo.list_by_shop = AsyncMock(return_value=[deactivated_product])

    variant_of_deactivated = _make_variant(
        "8P8", variant_id="v-of-deactivated"
    )
    variant_of_deactivated.external_product_id = "prod-deactivated"
    # Meskipun variant ini masih berstatus ACTIVE, produk induknya
    # DEACTIVATED → tetap di-skip dari mapping.
    variant_active = _make_variant("1A1", variant_id="v-active")

    variant_repo = MagicMock()
    variant_repo.list_by_shop = AsyncMock(
        return_value=[variant_active, variant_of_deactivated]
    )

    mapping_service = MagicMock()
    mapping_service.update_mapping_for_variant = AsyncMock(
        return_value=_make_mapping(
            SkuType.NORMAL, 2001549, variant_id="v-active", seller_sku="1A1"
        )
    )
    validation_service = None

    engine = MappingEngine(
        mapping_service=mapping_service,
        variant_service=MagicMock(),
        variant_repo=variant_repo,
        product_repo=product_repo,
        shop_repo=shop_repo,
        validation_service=validation_service,
    )

    summary = await engine.run_mapping_for_shop("shop-uuid-1")

    assert summary["total_skipped_deleted"] == 1, summary
    assert summary["total_mapped"] == 1, summary
    mapping_service.update_mapping_for_variant.assert_awaited_once_with(
        "shop123", "prod456", "variant001"
    )


@pytest.mark.asyncio
async def test_upsert_dedupe_called_with_same_natural_key():
    """
    Issue yang sama (check_code + entity_type + entity_id) terdeteksi ulang
    harus lewat upsert_detected (dedupe via natural key di repo), bukan
    membuat baris baru — verifikasi service memanggil upsert_detected.
    """
    mappings = [
        _make_mapping(SkuType.SKU_S, 2001549, variant_id="v1", seller_sku="1A1-50s"),
        _make_mapping(
            SkuType.SKU_PACK, 2001549, variant_id="v2",
            conversion_qty=50.0, seller_sku="1A1-Pack-50",
        ),
    ]
    service = _make_validation_service(mappings=mappings)

    await service.run_global_checks()

    # Global checks: 1 konflik kategori + 0 orphan → 1 upsert, 2 mark_resolved
    # (kategori + orphan).
    assert service._validation_repo.upsert_detected.await_count == 1
    assert service._validation_repo.mark_resolved.await_count == 2


@pytest.mark.asyncio
async def test_mp_check_mark_resolved_scoped_to_shop():
    """
    Check per-shop (SKU_MP_NOT_IN_INVENTREE) harus memanggil mark_resolved
    DENGAN shop_id scope supaya run shop lain tidak mereset issue shop ini.
    """
    shop_variants = [_make_variant("1A1-50s", variant_id="v-missing")]
    service = _make_validation_service(
        shop_variants=shop_variants,
        all_mp_skus=["1A1-50s"],
        inventree_items=[{"part_id": 2001549, "sku_toko": "1A1-50x", "name": "A"}],
    )

    await service.run_checks_for_shop("shop123")

    assert service._validation_repo.upsert_detected.await_count == 1
    assert service._validation_repo.mark_resolved.await_count == 1
    call_kwargs = service._validation_repo.mark_resolved.await_args.kwargs
    assert call_kwargs.get("shop_id") == "shop123", (
        f"mark_resolved harus di-scope per shop, got {call_kwargs}"
    )
