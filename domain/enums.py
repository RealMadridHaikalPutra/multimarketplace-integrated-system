from enum import Enum


class RecordStatus(str, Enum):
    """Status for all domain entities."""

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    INACTIVE = "INACTIVE"
    DEACTIVATED = "DEACTIVATED"
    # Product hilang dari payload GET products terbaru (mis. listing TikTok
    # berstatus FREEZE / tidak ikut ter-listing). Bukan DELETED — product
    # masih dimiliki toko dan bisa muncul lagi di sync berikutnya. Variant
    # milik product FREEZE di-set DEACTIVATED: tetap ikut perhitungan
    # inventory_report, tapi tidak ikut stock_sync_queue / push / log.
    FREEZE = "FREEZE"


class MarketplaceStatus(str, Enum):
    """Status for marketplace."""

    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class SkuType(str, Enum):
    """Type of SKU based on pattern analysis."""

    NORMAL = "NORMAL"
    SKU_X = "SKU_X"
    SKU_S = "SKU_S"
    SKU_PACK = "SKU_PACK"
