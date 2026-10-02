from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# ============================================================
# Marketplace & Shop CRUD
# ============================================================


class CreateMarketplaceRequest(BaseModel):
    code: str = Field(..., description="Marketplace code (e.g. 'shopee', 'tiktok')", examples=["shopee"])
    name: str = Field(..., description="Marketplace display name", examples=["Shopee"])
    status: str = Field("ACTIVE", description="Status: ACTIVE or INACTIVE")


class UpdateMarketplaceRequest(BaseModel):
    name: Optional[str] = Field(None, description="Marketplace display name")
    status: Optional[str] = Field(None, description="Status: ACTIVE or INACTIVE")


class MarketplaceResponse(BaseModel):
    id: str
    code: str
    name: str
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class CreateMarketplaceShopRequest(BaseModel):
    marketplace_code: str = Field(..., description="Marketplace code (e.g. 'shopee', 'tiktok')", examples=["shopee"])
    external_shop_id: str = Field(..., description="Shop ID from marketplace", examples=["227674818"])
    shop_name: str = Field(..., description="Shop display name", examples=["Toko Saya"])
    default_warehouse_id: Optional[str] = Field(
        None,
        description=(
            "Per-shop warehouse_id for APISIX update-stock Format A. NULL = "
            "fall back to global apisix_*_default_warehouse_id config (Shopee "
            "'IDZ', TikTok empty = no default)."
        ),
        examples=["7650040435037325077"],
    )
    status: str = Field("ACTIVE", description="Status: ACTIVE or INACTIVE")


class UpdateMarketplaceShopRequest(BaseModel):
    shop_name: Optional[str] = None
    default_warehouse_id: Optional[str] = Field(
        None,
        description=(
            "Per-shop warehouse_id for APISIX update-stock. Pass null to "
            "explicitly remove (falls back to global config)."
        ),
    )
    status: Optional[str] = None


class MarketplaceShopResponse(BaseModel):
    id: str
    marketplace_code: str
    marketplace_id: str
    external_shop_id: str
    shop_name: str
    default_warehouse_id: Optional[str] = None
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ============================================================
# Sync Products & Mapping Run
# ============================================================


class SyncProductsRequest(BaseModel):
    status: str = Field("ALL", description="Filter by product status: 'ACTIVE', 'INACTIVE', or 'ALL'", examples=["ACTIVE"])


class SyncProductsResponse(BaseModel):
    status: str
    shop_id: str
    shop_name: str
    marketplace_code: str
    total_products: int = 0
    total_variants: int = 0
    # TC-SKU-006 gate GET PRODUCT: jumlah VARIANT yang di-SKIP karena masuk
    # pasangan konflik PACK + SKU_S/SKU_X ber-base SKU & ANGKA qty sama
    # (SKU_PRODUCT_PACK_SX_CONFLICT → validation_issue). Product & variant
    # lain tetap di-insert.
    total_variants_skipped_conflict: int = 0
    message: Optional[str] = None
    details: List[Dict[str, Any]] = []


class StartProductPollingRequest(BaseModel):
    marketplace_code: str = Field(..., description="Marketplace code, e.g. 'shopee' or 'tiktok'")
    shop_id: str = Field(..., description="External shop identifier used by APISIX")
    duration_minutes: int = Field(60, ge=1, le=24 * 60, description="Polling duration in minutes")
    interval_seconds: int = Field(300, ge=10, le=3600, description="Delay between polls in seconds")


class ProductPollingJobResponse(BaseModel):
    job_id: str
    marketplace_code: str
    shop_id: str
    status: str
    duration_minutes: int
    interval_seconds: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    last_message: Optional[str] = None
    last_sync_result: Optional[Dict[str, Any]] = None


class MappingRunResult(BaseModel):
    shop_id: Optional[str] = None
    external_product_id: Optional[str] = None
    external_variant_id: Optional[str] = None
    seller_sku: str = ""
    sku_type: Optional[str] = None
    # Faktor pengali (SKU_X=1/qty, SKU_S=1, SKU_PACK=qty, NORMAL=1)
    conversion_qty: Optional[float] = None
    status: str = ""
    message: Optional[str] = None
    inventree_part_id: Optional[str] = None
    mapping_id: Optional[str] = None


class MappingRunResponse(BaseModel):
    status: str
    shop_id: str
    total_variants: int = 0
    total_mapped: int = 0
    total_not_found: int = 0
    total_duplicates: int = 0
    total_errors: int = 0
    total_skipped: int = 0
    total_conflicts: int = 0  # TC-SKU-006 Case 1: part dipakai campuran kategori SKU
    total_warnings: int = 0  # TC-SKU-006 Case 2: paritas SKU MP vs InvenTree
    message: Optional[str] = None
    results: List[MappingRunResult] = []
    validation: List[Dict[str, Any]] = []  # issue validation terbaru


class ValidationIssueResponse(BaseModel):
    """Satu baris dari tabel dinamis validation_issue."""
    id: Optional[int] = None
    check_code: str = ""
    severity: str = "WARNING"
    status: str = "OPEN"
    entity_type: str = ""
    entity_id: str = ""
    reference: Optional[str] = None
    details: Dict[str, Any] = {}
    action_taken: Optional[str] = None
    suggested_action: Optional[str] = None
    marketplace_code: Optional[str] = None
    shop_id: Optional[str] = None
    occurrence_count: int = 1
    first_seen_at: Optional[str] = None
    last_seen_at: Optional[str] = None
    resolved_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ValidationIssueListResponse(BaseModel):
    total: int = 0
    status: Optional[str] = None
    check_code: Optional[str] = None
    severity: Optional[str] = None
    shop_id: Optional[str] = None
    issues: List[ValidationIssueResponse] = []


# ============================================================
# Update Product Status (ACTIVE / INACTIVE) via APISIX
# ============================================================


class UpdateProductStatusRequest(BaseModel):
    """Body unified utk update status produk di Shopee & TikTok.

    APISIX yang memetakan status per marketplace:
      - Shopee: ACTIVE → NORMAL, INACTIVE → UNLIST
      - TikTok: ACTIVE → activate, INACTIVE → deactivate
    """
    status: Literal["ACTIVE", "INACTIVE"] = Field(
        "ACTIVE",
        description="Target status: 'ACTIVE' atau 'INACTIVE'",
        examples=["ACTIVE"],
    )


class UpdateProductStatusResponse(BaseModel):
    marketplace_code: str
    shop_id: str
    product_id: str
    status: str
    apisix_response: Dict[str, Any] = {}


# ============================================================
# Product & Variant List
# ============================================================


class VariantItemResponse(BaseModel):
    shop_id: str = ""
    external_product_id: str = ""
    external_variant_id: Optional[str] = None
    marketplace_variant_id: Optional[str] = None
    variant_name: Optional[str] = None
    seller_sku: str = ""
    total_reserved_stock: Optional[int] = None
    total_available_stock: Optional[int] = None
    location_id: Optional[str] = None
    status: str = ""


class ProductWithVariantsResponse(BaseModel):
    shop_id: str = ""
    external_product_id: str = ""
    product_name: str = ""
    product_image: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    status: str = ""
    variants: List[VariantItemResponse] = []


class ShopProductsResponse(BaseModel):
    shop_id: str
    shop_name: str
    marketplace_code: str
    external_shop_id: str
    total_products: int = 0
    products: List[ProductWithVariantsResponse] = []


# ============================================================
# SKU Mapping
# ============================================================


class SKUMappingResponse(BaseModel):
    sku_mapping_id: str
    shop_id: str = ""
    external_product_id: str = ""
    external_variant_id: str = ""
    inventree_part_id: str
    sku_type: str
    # Faktor pengali (SKU_X=1/qty, SKU_S=1, SKU_PACK=qty, NORMAL=1)
    conversion_qty: float
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class SKUMappingListResponse(BaseModel):
    total: int
    mappings: List[SKUMappingResponse]


# ============================================================
# Webhook Events
# ============================================================


class VariantRequest(BaseModel):
    external_variant_id: Optional[str] = Field(None, description="Variant ID from marketplace (nullable)")
    variant_name: Optional[str] = Field(None, description="Variant name (nullable)")
    seller_sku: str = Field(..., description="Seller SKU — source of truth for mapping")
    reserved_stock: Optional[int] = Field(None, description="Total reserved stock quantity")
    available_stock: Optional[int] = Field(None, description="Total available stock quantity")
    location_id: Optional[str] = Field(None, description="Warehouse location ID (first warehouse of inventory[])")


class ProductCreatedRequest(BaseModel):
    marketplace_code: str = Field(..., description="Marketplace code (e.g. 'TOKOPEDIA')")
    shop_external_id: str = Field(..., description="External shop ID from marketplace")
    shop_name: str = Field(..., description="Shop name")
    external_product_id: str = Field(..., description="Product ID from marketplace")
    product_name: str = Field(..., description="Product name")
    product_image: Optional[str] = Field(None, description="Product image URL")
    category: Optional[str] = Field(None, description="Product category")
    description: Optional[str] = Field(None, description="Product description")
    variants: List[VariantRequest] = Field(default_factory=list, description="List of variants")


class ProductUpdatedRequest(BaseModel):
    marketplace_code: str
    shop_external_id: str
    external_product_id: str
    product_name: Optional[str] = None
    product_image: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    variants: List[VariantRequest] = []


class ProductDeletedRequest(BaseModel):
    marketplace_code: str
    shop_external_id: str
    external_product_id: str


class SkuCreatedRequest(BaseModel):
    marketplace_code: str
    shop_external_id: str
    external_product_id: str
    external_variant_id: Optional[str] = None
    variant_name: Optional[str] = None
    seller_sku: str
    reserved_stock: Optional[int] = None
    available_stock: Optional[int] = None
    location_id: Optional[str] = None


class SkuUpdatedRequest(BaseModel):
    marketplace_code: str
    shop_external_id: str
    external_product_id: str
    external_variant_id: Optional[str] = None
    variant_name: Optional[str] = None
    seller_sku: Optional[str] = None
    reserved_stock: Optional[int] = None
    available_stock: Optional[int] = None
    location_id: Optional[str] = None


class SkuDeletedRequest(BaseModel):
    marketplace_code: str
    shop_external_id: str
    external_product_id: str
    external_variant_id: Optional[str] = None


# ============================================================
# Inventory Report (KEPT — table not dropped)
# ============================================================


class InventoryReportRowResponse(BaseModel):
    id: int
    part_id: int
    seller_sku: str
    sku_type: str
    stok_inventree: float
    buffer: int
    faktor_pengali: float
    reserved_mp: int
    reserved_inventree: float = 0.0
    qty_converted: float
    stok_di_update: float
    shop_id: Optional[str] = None
    marketplace_code: Optional[str] = None
    generated_at: Optional[str] = None


class InventoryReportResponse(BaseModel):
    status: str
    shop_id: str
    total_rows: int
    message: Optional[str] = None
    rows: List[InventoryReportRowResponse] = []


class InventoryPollJobResponse(BaseModel):
    job_id: str
    marketplace_code: str
    shop_id: str
    external_shop_id: str
    status: str
    duration_minutes: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    poll_runs: int = 0
    poll_successful: int = 0
    poll_failed: int = 0
    mapping_result: Optional[Dict[str, Any]] = None
    report_result: Optional[Dict[str, Any]] = None
    last_message: Optional[str] = None


# ============================================================
# Daily Stock-Sync Scheduler (jadwal harian)
# ============================================================


class StockSyncJobCycleResponse(BaseModel):
    cycle_index: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    total_report_rows: int = 0
    total_queued: int = 0
    total_pushed: int = 0
    total_logged: int = 0
    total_skipped_pending: int = 0
    total_skipped_no_part: int = 0
    total_skipped_push: int = 0  # variant di-skip karena stock_sync_push_enabled=false
    total_skipped_unchanged: int = 0  # variant dengan previous_stock==target_stock==final_stock: report ditulis, TIDAK queue/log
    total_activated: int = 0
    # Statistik inventory cycle GLOBAL (urut part_id ASC + Format A/B):
    total_parts_processed: int = 0      # jumlah part_id unik yang diproses
    total_push_calls_format_a: int = 0  # HTTP call batch Format A (multi-SKU)
    total_push_calls_format_b: int = 0  # HTTP call flat Format B (per-SKU)
    push_enabled: bool = True  # snapshot stock_sync_push_enabled saat cycle jalan
    # Retry push FAILED di akhir cycle (hanya error retryable, mis. HTTP 400):
    total_retry_attempted: int = 0
    total_retry_succeeded: int = 0
    total_retry_exhausted: int = 0
    failures: List[str] = []


class StockSyncJobResponse(BaseModel):
    job_id: str
    marketplace_code: str
    shop_id: str
    # Jadwal harian: cycle SEKALI per hari di schedule_time (timezone
    # schedule_timezone). Tidak ada cycle langsung saat start — utk run
    # langsung gunakan endpoint manual /run/*.
    schedule_time: Optional[str] = None
    schedule_timezone: Optional[str] = None
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    cycles_completed: int = 0
    cycles_failed: int = 0
    last_cycle_at: Optional[str] = None
    last_message: Optional[str] = None
    last_cycle: Optional[StockSyncJobCycleResponse] = None
    cycles_history: List[StockSyncJobCycleResponse] = []


class StopStockSyncJobResponse(BaseModel):
    job_id: str
    marketplace_code: str
    shop_id: str
    status: str
    finished_at: Optional[str] = None
    last_message: Optional[str] = None


class StockSyncManualCycleResponse(BaseModel):
    status: str
    marketplace_code: str
    shop_id: str
    cycle: Optional[StockSyncJobCycleResponse] = None
    message: Optional[str] = None


class StockSyncPartScopeResponse(BaseModel):
    part_id: int
    mappings: int = 0
    variant_ids: List[str] = Field(default_factory=list)
    product_ids: List[str] = Field(default_factory=list)
    inactive_mapping_ids: List[str] = Field(default_factory=list)
    missing_variant_ids: List[str] = Field(default_factory=list)
    deleted_variant_ids: List[str] = Field(default_factory=list)


class StockSyncManualPartResponse(BaseModel):
    status: str
    run_mode: str = "MANUAL_PART"
    trigger_source: str = "MANUAL_PART"
    log_source: str = "MANUAL_PART"
    part_id: Optional[int] = None
    scope: Optional[StockSyncPartScopeResponse] = None
    scopes: Optional[List[StockSyncPartScopeResponse]] = None
    cycle: Optional[Dict[str, Any]] = None
    message: str = ""


class StockSyncManualPartRequest(BaseModel):
    """Body ``POST /api/v2/stock-sync/run-part`` — BATCH part_id per request.

    Menggantikan format query lama (``?part_id=1&part_id=2``) supaya banyak
    part_id bisa dikirim sekaligus dalam SATU body JSON array — dipakai
    external task Camunda ``serviceWebhookTest`` sebagai hit-client batch.
    """

    part_id: List[int] = Field(
        ...,
        description=(
            "Daftar part_id InvenTree (bisa banyak). Semua diproses dalam satu "
            "cycle pipeline. Nilai <= 0 atau duplikat dibuang di endpoint."
        ),
    )
    full_pipeline: bool = Field(
        default=False,
        description=(
            "true = jalankan pipeline DARI AWAL (GET product / SYNC / MAPPING) "
            "sebelum inventory cycle; false = langsung inventory cycle memakai "
            "data yang sudah ada di DB."
        ),
    )

