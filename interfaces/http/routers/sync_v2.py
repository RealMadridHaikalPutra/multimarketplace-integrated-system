import logging
from typing import List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from application.dto.requests import (
    SyncProductsRequest,
    SyncProductsResponse,
    MappingRunResponse,
    MappingRunResult,
    ValidationIssueResponse,
    ValidationIssueListResponse,
    ProductWithVariantsResponse,
    VariantItemResponse,
    ShopProductsResponse,
    SKUMappingResponse,
    SKUMappingListResponse,
    StartProductPollingRequest,
    ProductPollingJobResponse,
    InventoryPollJobResponse,
    UpdateProductStatusRequest,
    UpdateProductStatusResponse,
)
from domain.repositories.marketplace_product_repository import MarketplaceProductRepository
from domain.repositories.marketplace_repository import MarketplaceRepository
from domain.repositories.marketplace_shop_repository import MarketplaceShopRepository
from domain.repositories.marketplace_variant_repository import MarketplaceVariantRepository
from domain.repositories.sku_mapping_repository import SKUMappingRepository
from domain.repositories.validation_issue_repository import ValidationIssueRepository
from infrastructure.db.database import get_db_session
from infrastructure.marketplace.apisix.client import ApisixClient, ApisixConfigError
from infrastructure.repositories.marketplace_product_repository_impl import (
    MarketplaceProductRepositoryImpl,
)
from infrastructure.repositories.marketplace_repository_impl import (
    MarketplaceRepositoryImpl,
)
from infrastructure.repositories.marketplace_shop_repository_impl import (
    MarketplaceShopRepositoryImpl,
)
from infrastructure.repositories.marketplace_variant_repository_impl import (
    MarketplaceVariantRepositoryImpl,
)
from infrastructure.repositories.sku_mapping_repository_impl import (
    SKUMappingRepositoryImpl,
)
from infrastructure.repositories.validation_issue_repository_impl import (
    ValidationIssueRepositoryImpl,
)
from app.container import create_mapping_engine, create_sync_engine
from interfaces.scheduling.polling_engine import polling_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v2", tags=["Sync v2"])


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _get_mp_repo(session: AsyncSession) -> MarketplaceRepository:
    return MarketplaceRepositoryImpl(session)


def _get_shop_repo(session: AsyncSession) -> MarketplaceShopRepository:
    return MarketplaceShopRepositoryImpl(session)


def _get_product_repo(session: AsyncSession) -> MarketplaceProductRepository:
    return MarketplaceProductRepositoryImpl(session)


def _get_variant_repo(session: AsyncSession) -> MarketplaceVariantRepository:
    return MarketplaceVariantRepositoryImpl(session)


def _get_sku_mapping_repo(session: AsyncSession) -> SKUMappingRepository:
    return SKUMappingRepositoryImpl(session)


def _get_validation_issue_repo(session: AsyncSession) -> ValidationIssueRepository:
    return ValidationIssueRepositoryImpl(session)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _variant_to_response(variant):
    return VariantItemResponse(
        shop_id=str(variant.shop_id) if variant.shop_id else "",
        external_product_id=variant.external_product_id,
        external_variant_id=variant.external_variant_id,
        variant_name=variant.variant_name,
        seller_sku=variant.seller_sku,
        total_reserved_stock=variant.total_reserved_stock,
        total_available_stock=variant.total_available_stock,
        status=variant.status.value if hasattr(variant.status, "value") else variant.status,
    )


def _product_to_response(product):
    return ProductWithVariantsResponse(
        shop_id=str(product.shop_id) if product.shop_id else "",
        external_product_id=product.external_product_id,
        product_name=product.product_name,
        product_image=product.product_image,
        category=product.category,
        description=product.description,
        status=product.status.value if hasattr(product.status, "value") else product.status,
        variants=[],
    )


async def _resolve_shop(
    session: AsyncSession, marketplace_code: str, external_shop_id: str
):
    mp_repo = _get_mp_repo(session)
    shop_repo = _get_shop_repo(session)

    marketplace = await mp_repo.get_by_code(marketplace_code)
    if not marketplace:
        raise HTTPException(
            status_code=404,
            detail=f"Marketplace with code '{marketplace_code}' not found",
        )

    shop = await shop_repo.get_by_external_id(marketplace.id, external_shop_id)
    if not shop:
        raise HTTPException(
            status_code=404,
            detail=f"Shop with external_id '{external_shop_id}' not found for marketplace '{marketplace_code}'",
        )

    return marketplace, shop


# ---------------------------------------------------------------------------
# Sync Products from APISIX
# ---------------------------------------------------------------------------


@router.post(
    "/sync/products/{marketplace_code}/{external_shop_id}",
    response_model=SyncProductsResponse,
)
async def sync_products(
    marketplace_code: str,
    external_shop_id: str,
    payload: SyncProductsRequest = SyncProductsRequest(),
    session: AsyncSession = Depends(get_db_session),
):
    """
    Sync products & variants from APISIX for a specific shop.

    Path parameters:
    - `marketplace_code`: e.g. 'shopee', 'tiktok'
    - `external_shop_id`: Shop ID from the marketplace (e.g. '227674818')
    """
    marketplace, shop = await _resolve_shop(session, marketplace_code, external_shop_id)

    engine = create_sync_engine(session)
    result = await engine.sync_by_shop_id(shop.id, status_filter=payload.status)
    await session.commit()

    return SyncProductsResponse(
        status=result.get("status", "error"),
        shop_id=str(shop.external_shop_id),
        shop_name=result.get("shop_name", shop.shop_name),
        marketplace_code=marketplace_code,
        total_products=result.get("total_products", 0),
        total_variants=result.get("total_variants", 0),
        total_variants_skipped_conflict=result.get(
            "total_variants_skipped_conflict", 0
        ),
        message=result.get("message"),
        details=result.get("details", []),
    )


@router.post(
    "/polling/products/start",
    response_model=ProductPollingJobResponse,
)
async def start_product_polling(
    payload: StartProductPollingRequest,
):
    """
    Start background polling for products and variants.
    """
    job = await polling_manager.start_job(
        marketplace_code=payload.marketplace_code,
        shop_id=payload.shop_id,
        duration_minutes=payload.duration_minutes,
        interval_seconds=payload.interval_seconds,
    )

    return ProductPollingJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        status=job.status,
        duration_minutes=job.duration_minutes,
        interval_seconds=job.interval_seconds,
        started_at=job.started_at.isoformat() if job.started_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        total_runs=job.total_runs,
        successful_runs=job.successful_runs,
        failed_runs=job.failed_runs,
        last_message=job.last_message,
        last_sync_result=job.last_sync_result,
    )


@router.get(
    "/polling/products/{job_id}",
    response_model=ProductPollingJobResponse,
)
async def get_product_polling_job(job_id: str):
    job = polling_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Polling job '{job_id}' not found",
        )

    return ProductPollingJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        status=job.status,
        duration_minutes=job.duration_minutes,
        interval_seconds=job.interval_seconds,
        started_at=job.started_at.isoformat() if job.started_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        total_runs=job.total_runs,
        successful_runs=job.successful_runs,
        failed_runs=job.failed_runs,
        last_message=job.last_message,
        last_sync_result=job.last_sync_result,
    )


# ---------------------------------------------------------------------------
# Run Mapping from Existing Data
# ---------------------------------------------------------------------------


@router.post(
    "/mapping/run/{marketplace_code}/{external_shop_id}",
    response_model=MappingRunResponse,
)
async def run_mapping(
    marketplace_code: str,
    external_shop_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """
    Run mapping for ALL variants in a shop.

    Path parameters:
    - `marketplace_code`: e.g. 'shopee', 'tiktok'
    - `external_shop_id`: Shop ID from the marketplace
    """
    marketplace, shop = await _resolve_shop(session, marketplace_code, external_shop_id)

    engine = create_mapping_engine(session)
    result = await engine.run_mapping_for_shop(shop.id)
    await session.commit()

    validation = result.get("validation") or {}
    return MappingRunResponse(
        status=result.get("status", "error"),
        shop_id=str(shop.external_shop_id),
        total_variants=result.get("total_variants", 0),
        total_mapped=result.get("total_mapped", 0),
        total_not_found=result.get("total_not_found", 0),
        total_duplicates=result.get("total_duplicates", 0),
        total_errors=result.get("total_errors", 0),
        total_skipped=result.get("total_skipped", 0),
        total_conflicts=validation.get("total_conflicts", 0),
        total_warnings=validation.get("total_warnings", 0),
        message=result.get("message"),
        results=[MappingRunResult(**r) for r in result.get("results", [])],
        validation=validation.get("issues", []),
    )


# ---------------------------------------------------------------------------
# Validation Issues (TC-SKU-006 dynamic error tracking)
# ---------------------------------------------------------------------------


def _validation_issue_to_response(issue) -> ValidationIssueResponse:
    return ValidationIssueResponse(
        id=issue.id,
        check_code=issue.check_code,
        severity=issue.severity,
        status=issue.status,
        entity_type=issue.entity_type,
        entity_id=issue.entity_id,
        reference=issue.reference,
        details=issue.details or {},
        action_taken=issue.action_taken,
        suggested_action=issue.suggested_action,
        marketplace_code=issue.marketplace_code,
        shop_id=issue.shop_id,
        occurrence_count=issue.occurrence_count,
        first_seen_at=issue.first_seen_at.isoformat() if issue.first_seen_at else None,
        last_seen_at=issue.last_seen_at.isoformat() if issue.last_seen_at else None,
        resolved_at=issue.resolved_at.isoformat() if issue.resolved_at else None,
        created_at=issue.created_at.isoformat() if issue.created_at else None,
        updated_at=issue.updated_at.isoformat() if issue.updated_at else None,
    )


@router.get(
    "/validation/issues",
    response_model=ValidationIssueListResponse,
)
async def list_validation_issues(
    status: Optional[str] = Query(None, description="Filter status: OPEN / RESOLVED"),
    check_code: Optional[str] = Query(None, description="Filter check_code"),
    severity: Optional[str] = Query(None, description="Filter severity: CONFLICT / WARNING / INFO"),
    shop_id: Optional[str] = Query(None, description="Filter shop_id"),
    limit: int = Query(500, ge=1, le=5000),
    session: AsyncSession = Depends(get_db_session),
):
    """
    List issue dari tabel dinamis ``validation_issue``.

    Satu tabel untuk SEMUA jenis masalah (mapping conflict TC-SKU-006, SKU
    parity, dan check lain di masa depan). ``details`` berisi payload bebas
    per check_code; ``occurrence_count`` menandakan berapa kali issue yang
    sama terdeteksi ulang.
    """
    repo = _get_validation_issue_repo(session)
    issues = await repo.list_all(
        status=status,
        check_code=check_code,
        severity=severity,
        shop_id=shop_id,
        limit=limit,
    )
    return ValidationIssueListResponse(
        total=len(issues),
        status=status,
        check_code=check_code,
        severity=severity,
        shop_id=shop_id,
        issues=[_validation_issue_to_response(i) for i in issues],
    )


# ---------------------------------------------------------------------------
# List SKU Mappings
# ---------------------------------------------------------------------------


def _mapping_to_response(mapping):
    return SKUMappingResponse(
        sku_mapping_id=str(mapping.sku_mapping_id),
        shop_id=str(mapping.shop_id) if mapping.shop_id else "",
        external_product_id=mapping.external_product_id,
        external_variant_id=mapping.external_variant_id,
        inventree_part_id=str(mapping.inventree_part_id) if mapping.inventree_part_id else "",
        sku_type=mapping.sku_type.value if hasattr(mapping.sku_type, "value") else mapping.sku_type,
        conversion_qty=mapping.conversion_qty,
        status=mapping.status,
        created_at=str(mapping.created_at) if mapping.created_at else None,
        updated_at=str(mapping.updated_at) if mapping.updated_at else None,
    )


@router.get(
    "/mappings",
    response_model=SKUMappingListResponse,
)
async def list_sku_mappings(
    session: AsyncSession = Depends(get_db_session),
):
    """
    List all SKU mappings, ordered by created_at descending.
    """
    mapping_repo = _get_sku_mapping_repo(session)
    mappings = await mapping_repo.list_all()

    return SKUMappingListResponse(
        total=len(mappings),
        mappings=[_mapping_to_response(m) for m in mappings],
    )


# ---------------------------------------------------------------------------
# List Products + Variants for a Shop
# ---------------------------------------------------------------------------


@router.get(
    "/shop/{marketplace_code}/{external_shop_id}/products",
    response_model=ShopProductsResponse,
)
async def list_shop_products(
    marketplace_code: str,
    external_shop_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """
    List all products with their variants for a specific shop.
    """
    shop_repo = _get_shop_repo(session)
    product_repo = _get_product_repo(session)
    variant_repo = _get_variant_repo(session)

    marketplace, shop = await _resolve_shop(session, marketplace_code, external_shop_id)

    # Get all products for this shop (keyed by external_shop_id)
    # NOTE: MarketplaceProductModel.shop_id adalah String FK ke external_shop_id,
    # bukan UUID PK MarketplaceShop.id.
    products = await product_repo.list_by_shop(shop.external_shop_id)

    product_responses = []
    for product in products:
        prod_resp = _product_to_response(product)

        variants = await variant_repo.list_by_product(product.shop_id, product.external_product_id)
        prod_resp.variants = [_variant_to_response(v) for v in variants]

        product_responses.append(prod_resp)

    return ShopProductsResponse(
        shop_id=str(shop.id),
        shop_name=shop.shop_name,
        marketplace_code=marketplace.code,
        external_shop_id=shop.external_shop_id,
        total_products=len(product_responses),
        products=product_responses,
    )


# ---------------------------------------------------------------------------
# Update Product Status (ACTIVE / INACTIVE) via APISIX
# ---------------------------------------------------------------------------


@router.post(
    "/products/status/{marketplace_code}/{external_shop_id}/{external_product_id}",
    response_model=UpdateProductStatusResponse,
)
async def update_product_status(
    marketplace_code: str,
    external_shop_id: str,
    external_product_id: str,
    payload: UpdateProductStatusRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """
    Update status produk (ACTIVE / INACTIVE) di marketplace melalui APISIX.

    Path parameters:
    - `marketplace_code`: 'shopee' atau 'tiktok'
    - `external_shop_id`: Shop ID dari marketplace (== shop_uuid APISIX)
    - `external_product_id`: Product ID native marketplace

    Request body (sama utk Shopee & TikTok):
        {"status": "ACTIVE"}  atau  {"status": "INACTIVE"}

    Contoh Shopee:
        POST /api/v2/products/status/shopee/227674818/123456789
        {"status": "ACTIVE"}

    Contoh TikTok:
        POST /api/v2/products/status/tiktok/7494709429666874412/987654321
        {"status": "INACTIVE"}

    Backend hanya mengirim field unified; pemetaan status per marketplace
    (Shopee NORMAL/UNLIST, TikTok activate/deactivate) ditangani APISIX.
    """
    # Validasi marketplace & shop terdaftar (konsisten dengan endpoint lain).
    await _resolve_shop(session, marketplace_code, external_shop_id)

    client = ApisixClient()
    try:
        apisix_result = await client.update_status(
            marketplace=marketplace_code,
            shop_uuid=external_shop_id,
            product_id=external_product_id,
            status=payload.status,
        )
    except ApisixConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                f"APISIX update-status gagal: "
                f"{exc.response.status_code} {exc.response.text[:500]}"
            ),
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Gagal koneksi ke APISIX: {exc}")
    finally:
        await client.aclose()

    return UpdateProductStatusResponse(
        marketplace_code=marketplace_code,
        shop_id=external_shop_id,
        product_id=external_product_id,
        status=payload.status,
        apisix_response=apisix_result or {},
    )


# ---------------------------------------------------------------------------
# Inventory Pipeline
# ---------------------------------------------------------------------------


@router.post(
    "/inventory/poll/{marketplace_code}/{external_shop_id}",
    response_model=InventoryPollJobResponse,
)
async def start_inventory_pipeline(
    marketplace_code: str,
    external_shop_id: str,
    duration_minutes: int = 60,
    interval_seconds: int = 300,
):
    """
    Start the full inventory pipeline for a shop:

    1. Poll APISIX for products & variants
    2. Run mapping to update sku_mapping

    Path parameters:
    - `marketplace_code`: e.g. 'shopee', 'tiktok'
    - `external_shop_id`: Shop ID from the marketplace (e.g. '227674818')

    Query parameters:
    - `duration_minutes`: How long to poll (default 60)
    - `interval_seconds`: Delay between polls (default 300)
    """
    job = await polling_manager.start_job(
        marketplace_code=marketplace_code,
        shop_id=external_shop_id,
        duration_minutes=duration_minutes,
        interval_seconds=interval_seconds,
        with_pipeline=True,
    )

    return InventoryPollJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        external_shop_id=external_shop_id,
        status=job.status,
        duration_minutes=job.duration_minutes,
        started_at=job.started_at.isoformat() if job.started_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        poll_runs=job.total_runs,
        poll_successful=job.successful_runs,
        poll_failed=job.failed_runs,
        mapping_result=job.mapping_result,
        report_result=job.report_result,
        last_message=job.last_message,
    )


@router.get(
    "/inventory/poll/{job_id}",
    response_model=InventoryPollJobResponse,
)
async def get_inventory_pipeline_job(job_id: str):
    job = polling_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Inventory pipeline job '{job_id}' not found",
        )
    return InventoryPollJobResponse(
        job_id=job.job_id,
        marketplace_code=job.marketplace_code,
        shop_id=job.shop_id,
        external_shop_id=job.shop_id,
        status=job.status,
        duration_minutes=job.duration_minutes,
        started_at=job.started_at.isoformat() if job.started_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        poll_runs=job.total_runs,
        poll_successful=job.successful_runs,
        poll_failed=job.failed_runs,
        mapping_result=job.mapping_result,
        report_result=job.report_result,
        last_message=job.last_message,
    )
