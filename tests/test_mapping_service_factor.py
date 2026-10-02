"""
Unit tests: MappingService menyimpan FAKTOR PENGALI di sku_mapping.conversion_qty.

Aturan (mengikuti faktor_pengali di inventory_report):
    SKU_X    (1A1-50x)     → 1/50  = 0.02
    SKU_S    (1A1-50s)     → 1.0   (1:1 dengan stok fisik)
    SKU_PACK (1A1-Pack-50) → 50.0  (isi paket)
    NORMAL   (1A1)         → 1.0
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from application.services.mapping_service import MappingService
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus
from domain.rules.sku import SkuParser


def _make_variant(seller_sku: str) -> MarketplaceVariant:
    return MarketplaceVariant(
        marketplace_variant_id="shop123_variant001",
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


def _make_service() -> MappingService:
    """MappingService dengan repo & InvenTree client mock (part selalu ketemu)."""
    mapping_repo = MagicMock()
    mapping_repo.get_by_variant_id = AsyncMock(return_value=[])
    mapping_repo.create = AsyncMock(
        side_effect=lambda m: m  # return entity apa adanya
    )
    mapping_repo.update = AsyncMock(side_effect=lambda m: m)

    variant_repo = MagicMock()
    variant_repo.get_by_id = AsyncMock(side_effect=lambda *a: _make_variant("PLACEHOLDER"))

    inventree_client = MagicMock()
    inventree_client.find_part_by_sku = AsyncMock(return_value=2001549)

    service = MappingService(
        mapping_repo=mapping_repo,
        variant_repo=variant_repo,
        inventory_provider=inventree_client,
    )
    return service


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seller_sku, expected_factor",
    [
        ("1A1-50x", 0.02),
        ("1A1-50s", 1.0),
        ("1A1-Pack-50", 50.0),
        ("1A1-Pack-20", 20.0),
        ("1A1", 1.0),
    ],
)
async def test_create_mapping_stores_faktor_pengali(seller_sku, expected_factor):
    """
    create_mapping harus menyimpan FAKTOR PENGALI (bukan qty mentah SKU)
    ke kolom conversion_qty:
        - 1A1-50x  → 0.02   (bukan 50)
        - 1A1-50s  → 1.0    (bukan 50)
        - Pack-50  → 50.0
        - Pack-20  → 20.0
    """
    service = _make_service()
    # Sinkronkan get_by_id dengan SKU yang sedang diuji
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant(seller_sku)
    )

    mapping = await service.create_mapping("shop123", "prod456", "variant001")

    assert mapping.sku_type.value == SkuParser.parse(seller_sku).sku_type.value
    assert abs(mapping.conversion_qty - expected_factor) < 1e-9, (
        f"SKU {seller_sku}: conversion_qty expected={expected_factor} "
        f"(faktor pengali), got {mapping.conversion_qty}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seller_sku, expected_factor",
    [
        ("1A1-50x", 0.02),
        ("1A1-50s", 1.0),
        ("1A1-Pack-50", 50.0),
    ],
)
async def test_update_mapping_recomputes_faktor_pengali(seller_sku, expected_factor):
    """
    update_mapping_for_variant (remap) juga harus menulis ulang conversion_qty
    sebagai faktor pengali terbaru.
    """
    service = _make_service()
    service._variant_repo.get_by_id = AsyncMock(
        return_value=_make_variant(seller_sku)
    )

    existing_mapping = MagicMock()
    existing_mapping.sku_mapping_id = "mapping-001"
    service._mapping_repo.get_by_variant_id = AsyncMock(return_value=[existing_mapping])

    mapping = await service.update_mapping_for_variant("shop123", "prod456", "variant001")

    assert abs(mapping.conversion_qty - expected_factor) < 1e-9, (
        f"SKU {seller_sku}: conversion_qty expected={expected_factor}, got {mapping.conversion_qty}"
    )
