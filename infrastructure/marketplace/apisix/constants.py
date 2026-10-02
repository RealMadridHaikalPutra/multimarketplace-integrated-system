"""APISIX adapter constants and shared error helpers."""

from __future__ import annotations

import httpx

# APISIX update-status status values.
STATUS_ACTIVE = "ACTIVE"
STATUS_INACTIVE = "INACTIVE"

# APISIX update-stock quantity constraints (per APISIX docs). APISIX clamps
# server-side; we log a WARNING for out-of-range values so operators spot
# silent semantic drift.
STOCK_MIN = 1
STOCK_MAX = 99_999

# TikTok: sweep one ``listing_platforms`` filter per request, then merge.
# ORDER MATTERS: TOKOPEDIA first, TIKTOK_SHOP last — SyncEngine uses the
# TIKTOK_SHOP sweep as the "presence list" for the TikTok Shop FREEZE logic.
TIKTOK_LISTING_PLATFORMS = ["TOKOPEDIA", "TIKTOK_SHOP"]

# Prefix used by callers to tag config-caused failures (grep-able convention).
CONFIG_ERROR_PREFIX = "CONFIG: "

# Max characters of the upstream response body embedded into the
# HTTPStatusError message (and therefore into stock_sync_queue /
# stock_sync_log error_message).
HTTP_ERROR_BODY_MAX_CHARS = 2000


class ApisixConfigError(ValueError):
    """Raised when ApisixClient is misconfigured for a call.

    Subclass of ValueError so callers using broad ``except ValueError`` keep
    working; new code should catch this explicitly to distinguish client-side
    config failures from unrelated ValueError sources.
    """

    pass


def raise_http_error_with_body(e: httpx.HTTPStatusError) -> None:
    """Re-raise an httpx.HTTPStatusError with the FULL upstream body embedded.

    ``str(HTTPStatusError)`` contains only the summary line; the response body
    (APISIX error code + marketplace message) would otherwise be lost for
    callers that only persist the exception text.
    """
    response = e.response
    status = getattr(response, "status_code", "?")
    try:
        body = (response.text or "").strip()
    except Exception:  # defensive: unreadable/non-text body
        body = ""
    if body:
        if len(body) > HTTP_ERROR_BODY_MAX_CHARS:
            body = body[:HTTP_ERROR_BODY_MAX_CHARS] + "...[truncated]"
        e.args = (f"{e} | HTTP {status} response body: {body}",)
    raise e
