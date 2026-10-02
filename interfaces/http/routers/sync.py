from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/sync", tags=["Synchronization"])


@router.get("/status")
async def sync_status():
    """
    Simple health/status endpoint for the sync engine.

    Returns the current sync capabilities.
    """
    return {
        "status": "ready",
        "supported_marketplaces": ["shopee", "tiktok"],
        "sync_mode": "manual_trigger_v2",
    }
