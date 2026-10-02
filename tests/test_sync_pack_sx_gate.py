"""
Unit tests: Gate TC-SKU-006 saat GET PRODUCT (sync dari APISIX).

Rules terbaru: variant ``SKU_PACK`` + ``SKU_S``/``SKU_X`` dengan BASE SKU &
ANGKA qty SAMA (mis. ``1A1-Pack-50`` + ``1A1-50s``) — KEDUA variant pasangan
konflik TIDAK di-insert ke ``marketplace_variant``; product & variant lain
yang tidak konflik TETAP di-insert. Dicatat WARNING
``SKU_PRODUCT_PACK_SX_CONFLICT`` di ``validation_issue``.

Cakupan test:
- ``_detect_product_pack_sx_conflict``: deteksi murni dari payload (base SKU +
  qty sama → konflik; qty beda / NORMAL+PACK / sesama SX → tidak).
- ``SyncEngine.sync_by_shop_id``: variant pasangan konflik di-skip, product &
  variant lain tetap tersimpan, warning di-upsert.
- Mode ``SYNC_BLOCK_PACK_SX_CONFLICT=false`` → report-only: semua variant tetap
  di-insert, warning tetap dicatat.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from application.services.mapping_validation_service import (
    MappingValidationService,
    _detect_product_pack_sx_conflict,
    CHECK_PRODUCT_PACK_SX_CONFLICT,
)
from core.config import settings
from domain.enums import RecordStatus
from infrastructure.marketplace.apisix.parsers.base import ParsedProduct, ParsedVariant
from application.usecases.sync_engine import SyncEngine
from infrastructure.marketplace.apisix.parsers.factory import get_parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_parsed_product(
    external_product_id: str,
    skus: list,
    product_name: str = "Produk",
) -> ParsedProduct:
    """Buat ParsedProduct dari list (seller_sku, variant_id)."""
    return ParsedProduct(
        external_product_id=external_product_id,
        product_name=product_name,
        product_image=None,
        variants=[
            ParsedVariant(
                external_variant_id=vid,
                variant_name=None,
                seller_sku=sku,
                reserved_stock=0,
            )
            for sku, vid in skus
        ],
    )


def _make_validation_service():
    """MappingValidationService asli dengan repo mock (hanya validation_repo dipakai)."""
    validation_repo = MagicMock()
    validation_repo.upsert_detected = AsyncMock(side_effect=lambda i: i)
    validation_repo.mark_resolved = AsyncMock(return_value=0)
    return MappingValidationService(
        validation_repo=validation_repo,
        mapping_repo=MagicMock(),
        variant_repo=MagicMock(),
        inventory_provider=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Deteksi murni: _detect_product_pack_sx_conflict
# ---------------------------------------------------------------------------


class TestDetectProductPackSxConflict:
    def test_conflict_pack_and_s_same_qty(self):
        """1A1-Pack-50 + 1A1-50s (base sama, qty 50==50) → konflik."""
        product = _make_parsed_product(
            "prod-1", [("1A1-Pack-50", "v1"), ("1A1-50s", "v2")]
        )
        info = _detect_product_pack_sx_conflict(product)
        assert info is not None
        assert info["conflict_pairs"] == [{"base_sku": "1A1", "qty": 50}]
        assert info["pack_skus"] == ["1A1-Pack-50"]
        assert info["sx_skus"] == ["1A1-50s"]
        # Kedua sisi pasangan konflik dianggap conflict (tidak di-insert).
        assert info["conflict_skus"] == ["1A1-Pack-50", "1A1-50s"]

    def test_conflict_pack_and_x_same_qty(self):
        """1A1-Pack-50 + 1A1-50x (base sama, qty 50==50) → konflik."""
        product = _make_parsed_product(
            "prod-1", [("1A1-Pack-50", "v1"), ("1A1-50x", "v2")]
        )
        info = _detect_product_pack_sx_conflict(product)
        assert info is not None
        assert info["conflict_pairs"] == [{"base_sku": "1A1", "qty": 50}]
        assert set(info["conflict_skus"]) == {"1A1-Pack-50", "1A1-50x"}

    def test_allowed_pack_and_s_diff_qty(self):
        """RULES TERBARU: 1A1-Pack-50 + 1A1-2s (50 != 2) → BOLEH (None)."""
        product = _make_parsed_product(
            "prod-1", [("1A1-Pack-50", "v1"), ("1A1-2s", "v2")]
        )
        assert _detect_product_pack_sx_conflict(product) is None

    def test_allowed_normal_and_pack(self):
        """1A1 (NORMAL) + 1A1-Pack-50 → BOLEH berbagi (None)."""
        product = _make_parsed_product(
            "prod-1", [("1A1", "v1"), ("1A1-Pack-50", "v2")]
        )
        assert _detect_product_pack_sx_conflict(product) is None

    def test_allowed_pack_pack_and_sx_only(self):
        """PACK+PACK sesama & S+X sesama → tidak pernah konflik."""
        pack_pack = _make_parsed_product(
            "prod-1", [("1A1-Pack-50", "v1"), ("1A1-Pack-100", "v2")]
        )
        assert _detect_product_pack_sx_conflict(pack_pack) is None

        sx_only = _make_parsed_product(
            "prod-2", [("1A1-50s", "v1"), ("1A1-50x", "v2"), ("1A1-2s", "v3")]
        )
        assert _detect_product_pack_sx_conflict(sx_only) is None

    def test_empty_or_bad_variants(self):
        """Variant kosong / tidak bisa diparse → di-skip, tidak konflik."""
        product = _make_parsed_product("prod-1", [])
        assert _detect_product_pack_sx_conflict(product) is None
        product.variants = [ParsedVariant(None, None, "")]
        assert _detect_product_pack_sx_conflict(product) is None


# ---------------------------------------------------------------------------
# SyncEngine.sync_by_shop_id — gate skip variant pasangan konflik
# ---------------------------------------------------------------------------


def _make_sync_setup(payload):
    """Setup engine + mocks untuk payload APISIX TikTok."""
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
    apisix_client.fetch_products = AsyncMock(return_value=payload)
    # Tidak ada override reserved stock — fallback ke nilai payload.
    apisix_client.fetch_reserved_stock = AsyncMock(return_value=None)

    created_products = []

    def _capture_create(product):
        created_products.append(product)
        return product

    product_service = MagicMock()
    product_service.create_product = AsyncMock(side_effect=_capture_create)

    upserted_variants = []

    def _capture_upsert(variants):
        upserted_variants.extend(variants)
        return len(variants)

    variant_service = MagicMock()
    variant_service.upsert_variants = AsyncMock(side_effect=_capture_upsert)

    variant_repo = MagicMock()
    variant_repo.list_by_product = AsyncMock(return_value=[])
    variant_repo.soft_delete = AsyncMock()

    validation_service = _make_validation_service()

    engine = SyncEngine(
        gateway=apisix_client,
        parser_factory=get_parser,
        product_service=product_service,
        variant_service=variant_service,
        mapping_service=None,
        shop_repo=shop_repo,
        product_repo=MagicMock(),
        variant_repo=variant_repo,
        marketplace_repo=marketplace_repo,
        validation_service=validation_service,
    )
    return {
        "engine": engine,
        "created_products": created_products,
        "upserted_variants": upserted_variants,
        "validation_service": validation_service,
        "variant_repo": variant_repo,
    }


def _tiktok_payload():
    return {
        "products": [
            {
                "id": "prod-conflict",
                "title": "Product Konflik PACK+S",
                "status": "ACTIVATE",
                "skus": [
                    {
                        "id": "v1",
                        "seller_sku": "1A1-Pack-50",
                        "inventory": [{"available_stock": 10, "reserved_stock": 1, "location_id": "WH1"}],
                    },
                    {
                        "id": "v2",
                        "seller_sku": "1A1-50s",
                        "inventory": [{"available_stock": 20, "reserved_stock": 0, "location_id": "WH1"}],
                    },
                ],
            },
            {
                "id": "prod-ok",
                "title": "Product Sehat",
                "status": "ACTIVATE",
                "skus": [
                    {
                        "id": "v3",
                        "seller_sku": "2A4-20s",
                        "inventory": [{"available_stock": 30, "reserved_stock": 0, "location_id": "WH1"}],
                    },
                ],
            },
        ]
    }


@pytest.mark.asyncio
async def test_sync_skips_only_conflicting_variants(monkeypatch):
    """
    Product berisi 1A1-Pack-50 + 1A1-50s (base sama, qty sama) → KEDUA
    variant pasangan konflik TIDAK di-insert; product TETAP dibuat dan
    variant lain (2A4-20s) tetap di-insert. WARNING di-upsert.
    """
    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", True)

    setup = _make_sync_setup(_tiktok_payload())
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants_skipped_conflict"] == 2, (
        "kedua variant pasangan konflik harus di-skip (Pack-50 + 50s)"
    )
    assert result["total_products"] == 2
    # Hanya variant non-konflik yang tersimpan (2A4-20s dari prod-ok).
    assert result["total_variants"] == 1

    # marketplace_product: KEDUA product tetap dibuat (hanya variant yang di-skip).
    created_ids = {p.external_product_id for p in setup["created_products"]}
    assert created_ids == {"prod-conflict", "prod-ok"}, (
        f"product tetap di-insert, variant konflik saja yang di-skip; got {created_ids}"
    )

    # marketplace_variant: variant konflik (Pack-50, 50s) tidak ikut ter-upsert.
    assert [v.seller_sku for v in setup["upserted_variants"]] == ["2A4-20s"]

    # WARNING di-upsert ke validation_issue (entity_id di-prefix shop supaya
    # natural key tidak bentrok antar shop).
    upserts = setup["validation_service"]._validation_repo.upsert_detected.await_args_list
    assert len(upserts) == 1, "harus ada 1 issue untuk product konflik"
    issue = upserts[0].args[0]
    assert issue.check_code == CHECK_PRODUCT_PACK_SX_CONFLICT
    assert issue.severity == "WARNING"
    assert issue.entity_type == "PRODUCT"
    assert issue.entity_id == "7494709429666874412_prod-conflict"
    assert issue.action_taken == "variant_skipped"
    assert set(issue.details["conflict_variants"]) == {"1A1-Pack-50", "1A1-50s"}

    # detail product konflik menandai conflict_action + daftar variant.
    conflict_detail = next(
        d for d in result["details"] if d.get("external_product_id") == "prod-conflict"
    )
    assert conflict_detail["conflict_action"] == "variant_skipped"
    assert set(conflict_detail["conflict_variants"]) == {"1A1-Pack-50", "1A1-50s"}
    assert conflict_detail["conflict_pairs"] == [{"base_sku": "1A1", "qty": 50}]


@pytest.mark.asyncio
async def test_sync_soft_deletes_only_stale_conflicting_variants(monkeypatch):
    """
    Variant pasangan konflik yang sudah terlanjur tersimpan dari sync
    SEBELUMNYA → di-soft-delete (tidak boleh tetap ter-map / ter-push stok).
    Variant NON-konflik lama TIDAK disentuh.

    NOTE (aturan baru): variant lama yang TIDAK lagi muncul di payload
    marketplace (mis. vx/1A1 yang bukan bagian product ini di marketplace)
    JUGA di-soft-delete oleh stale-variant cleanup — kebijakan "variant DB
    yang tidak relate ke marketplace → DELETED".
    """
    from domain.entities.marketplace_variant import MarketplaceVariant

    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", True)

    setup = _make_sync_setup(_tiktok_payload())
    stale_conflict = MarketplaceVariant(
        marketplace_variant_id="7494709429666874412_v1",
        marketplace_product_id="7494709429666874412_prod-conflict",
        shop_id="7494709429666874412",
        external_product_id="prod-conflict",
        external_variant_id="v1",
        seller_sku="1A1-Pack-50",
        status=RecordStatus.ACTIVE,
    )
    stale_normal = MarketplaceVariant(
        marketplace_variant_id="7494709429666874412_vx",
        marketplace_product_id="7494709429666874412_prod-conflict",
        shop_id="7494709429666874412",
        external_product_id="prod-conflict",
        external_variant_id="vx",
        seller_sku="1A1",
        status=RecordStatus.ACTIVE,
    )
    # list_by_product di-mock per product: hanya prod-conflict yang punya
    # variant lama di DB.
    async def _list_by_product(shop_id, external_product_id):
        if external_product_id == "prod-conflict":
            return [stale_conflict, stale_normal]
        return []

    setup["variant_repo"].list_by_product = AsyncMock(
        side_effect=_list_by_product
    )

    engine = setup["engine"]
    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["total_variants_skipped_conflict"] == 2
    # v1 (pasangan konflik) di-soft-delete oleh conflict gate; vx (variant
    # lama yang tidak relate ke marketplace) di-soft-delete oleh stale cleanup.
    deleted_calls = sorted(
        c.args[2] for c in setup["variant_repo"].soft_delete.await_args_list
    )
    assert deleted_calls == ["v1", "vx"], deleted_calls


@pytest.mark.asyncio
async def test_sync_report_only_mode_does_not_skip(monkeypatch):
    """
    SYNC_BLOCK_PACK_SX_CONFLICT=false → variant pasangan konflik TETAP
    di-insert (report-only), warning tetap dicatat ke validation_issue.
    """
    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", False)

    setup = _make_sync_setup(_tiktok_payload())
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants_skipped_conflict"] == 0, (
        "mode report-only tidak boleh skip variant"
    )
    assert result["total_variants"] == 3, "semua variant (2+1) tetap di-insert"

    created_ids = [p.external_product_id for p in setup["created_products"]]
    assert set(created_ids) == {"prod-conflict", "prod-ok"}

    upserts = setup["validation_service"]._validation_repo.upsert_detected.await_args_list
    assert len(upserts) == 1
    assert upserts[0].args[0].action_taken == "reported_only"

    conflict_detail = next(
        d for d in result["details"] if d.get("external_product_id") == "prod-conflict"
    )
    assert conflict_detail["conflict_action"] == "reported_only"


@pytest.mark.asyncio
async def test_sync_allows_pack_sx_diff_qty(monkeypatch):
    """
    Product berisi 1A1-Pack-50 + 1A1-2s (angka BEDA) → TIDAK di-skip,
    TIDAK ada warning (keduanya sah berbagi).
    """
    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", True)

    payload = _tiktok_payload()
    # Ganti variant 50s → 2s di product konflik.
    payload["products"][0]["skus"][1]["seller_sku"] = "1A1-2s"

    setup = _make_sync_setup(payload)
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["total_variants_skipped_conflict"] == 0
    assert result["total_variants"] == 3
    assert setup["validation_service"]._validation_repo.upsert_detected.await_count == 0, (
        "PACK-50 vs 2s (beda angka) tidak boleh memicu warning"
    )


@pytest.mark.asyncio
async def test_sync_does_not_resolve_issues_when_sweep_incomplete(monkeypatch):
    """
    Sweep pagination TIDAK tuntas (sweep_complete=False) → mark_resolved TIDAK
    boleh dipanggil (payload parsial; issue yang masih ada jangan premature-resolve).
    Variant pasangan konflik tetap di-skip & warning tetap di-upsert.
    """
    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", True)

    payload = _tiktok_payload()
    payload["sweep_complete"] = False
    payload["pages_fetched"] = 1

    setup = _make_sync_setup(payload)
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["total_variants_skipped_conflict"] == 2
    validation_repo = setup["validation_service"]._validation_repo
    assert validation_repo.upsert_detected.await_count == 1, "warning tetap dicatat"
    assert validation_repo.mark_resolved.await_count == 0, (
        "sweep tidak tuntas → tidak boleh resolve issue"
    )


@pytest.mark.asyncio
async def test_gate_off_when_validation_disabled(monkeypatch):
    """
    MAPPING_VALIDATION_ENABLED=false → gate mati total: tidak ada skip,
    tidak ada warning (semua product & variant di-insert normal).
    """
    monkeypatch.setattr(settings, "sync_block_pack_sx_conflict", True)
    monkeypatch.setattr(settings, "mapping_validation_enabled", False)

    setup = _make_sync_setup(_tiktok_payload())
    engine = setup["engine"]

    result = await engine.sync_by_shop_id("shop-uuid-1", status_filter="ALL")

    assert result["status"] == "success"
    assert result["total_variants_skipped_conflict"] == 0
    assert result["total_variants"] == 3
    assert setup["validation_service"]._validation_repo.upsert_detected.await_count == 0, (
        "validasi off → tidak boleh ada issue"
    )
    assert len(setup["created_products"]) == 2
