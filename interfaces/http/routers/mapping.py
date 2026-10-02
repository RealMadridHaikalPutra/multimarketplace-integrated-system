import logging

from fastapi import APIRouter, Depends

from application.dto.requests import (
    ProductCreatedRequest,
    ProductUpdatedRequest,
    ProductDeletedRequest,
    SkuCreatedRequest,
    SkuUpdatedRequest,
    SkuDeletedRequest,
)
from application.handlers.product_handler import InventoryHandler
from application.events.product_events import ProductEventHandler
from application.events.sku_events import SkuEventHandler
from interfaces.http.deps import (
    get_inventory_handler,
    get_product_event_handler,
    get_sku_event_handler,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/mapping", tags=["Mapping"])


# ---------------------------------------------------------------------------
# Product Events
# ---------------------------------------------------------------------------


@router.post("/product/created")
async def product_created(
    payload: ProductCreatedRequest,
    handler: ProductEventHandler = Depends(get_product_event_handler),
):
    """
    Event 1: Product Created

    Marketplace sends webhook/API when a new product is created.
    """
    logger.info("Event 1: Product Created — product=%s", payload.external_product_id)
    return await handler.handle_product_created(payload)


@router.post("/product/updated")
async def product_updated(
    payload: ProductUpdatedRequest,
    handler: ProductEventHandler = Depends(get_product_event_handler),
):
    """
    Event 2: Product Updated

    Marketplace sends webhook/API when a product is updated.
    """
    logger.info("Event 2: Product Updated — product=%s", payload.external_product_id)
    return await handler.handle_product_updated(payload)


@router.post("/product/deleted")
async def product_deleted(
    payload: ProductDeletedRequest,
    handler: ProductEventHandler = Depends(get_product_event_handler),
):
    """
    Event 3: Product Deleted

    Marketplace sends webhook/API when a product is deleted.
    Soft-delete is used — no data is removed.
    """
    logger.info("Event 3: Product Deleted — product=%s", payload.external_product_id)
    return await handler.handle_product_deleted(payload)


# ---------------------------------------------------------------------------
# Variant Events (formerly SKU)
# ---------------------------------------------------------------------------


@router.post("/variant/created")
async def variant_created(
    payload: SkuCreatedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    """
    Event 4: Variant Created

    Marketplace sends webhook/API when a new variant is created.

    Business Rule: Seller SKU must not have >1 active mapping.
    If duplicate is detected, mapping is skipped and error is returned.
    """
    logger.info("Event 4: Variant Created — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_created(payload)


@router.post("/variant/updated")
async def variant_updated(
    payload: SkuUpdatedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    """
    Event 6: Variant Updated

    Marketplace sends webhook/API when a variant is updated.
    - Recalculates sku_type and conversion_qty.
    - Updates sku_mapping accordingly.
    """
    logger.info("Event 6: Variant Updated — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_updated(payload)


@router.post("/variant/deleted")
async def variant_deleted(
    payload: SkuDeletedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    """
    Event 7: Variant Deleted

    Marketplace sends webhook/API when a variant is deleted.
    Soft-delete is used — data is preserved with status DELETED.
    """
    logger.info("Event 7: Variant Deleted — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_deleted(payload)


# ---------------------------------------------------------------------------
# Legacy SKU event aliases (backward compatibility)
# ---------------------------------------------------------------------------


@router.post("/sku/created")
async def sku_created(
    payload: SkuCreatedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    logger.info("Event 4 (legacy): SKU Created — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_created(payload)


@router.post("/sku/updated")
async def sku_updated(
    payload: SkuUpdatedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    logger.info("Event 6 (legacy): SKU Updated — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_updated(payload)


@router.post("/sku/deleted")
async def sku_deleted(
    payload: SkuDeletedRequest,
    handler: SkuEventHandler = Depends(get_sku_event_handler),
):
    logger.info("Event 7 (legacy): SKU Deleted — variant=%s", payload.external_variant_id)
    return await handler.handle_variant_deleted(payload)


# ---------------------------------------------------------------------------
# Inventory Events (Placeholder — not yet implemented)
# ---------------------------------------------------------------------------


@router.post("/inventory/updated")
async def inventory_updated(
    payload: dict,
    handler: InventoryHandler = Depends(get_inventory_handler),
):
    """
    Event 8: Inventory Updated (Placeholder)

    Will be implemented in Sprint 2 (Inventory Synchronization).
    """
    logger.info("Event 8: Inventory Updated — placeholder")
    return await handler.handle_inventory_updated(payload)


@router.post("/inventory/qty-sync-completed")
async def qty_sync_completed(
    payload: dict,
    handler: InventoryHandler = Depends(get_inventory_handler),
):
    """
    Event 9: Quantity Synchronization Completed (Placeholder)
    """
    logger.info("Event 9: Qty Sync Completed — placeholder")
    return await handler.handle_qty_sync_completed(payload)


@router.post("/inventory/qty-sync-failed")
async def qty_sync_failed(
    payload: dict,
    handler: InventoryHandler = Depends(get_inventory_handler),
):
    """
    Event 10: Quantity Synchronization Failed (Placeholder)
    """
    logger.info("Event 10: Qty Sync Failed — placeholder")
    return await handler.handle_qty_sync_failed(payload)
