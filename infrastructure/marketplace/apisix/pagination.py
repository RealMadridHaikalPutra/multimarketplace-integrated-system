"""APISIX pagination & path helpers — pure functions (no I/O).

APISIX returns a ``pagination`` metadata object on every list response.
Gateway variations (verified against real runs):

  - Shopee products: {"total", "page", "page_size", "has_next"} — page-based.
  - TikTok:          {"has_next", "next_page_token"} — cursor-based.
  - legacy variants: "has_next_page" / "next_offset" are accepted defensively.

Request param mapping:
  - Shopee products & orders: ``page`` (gateway computes offset itself; the
    raw ``offset`` param from the client is ignored). ``next_page_token`` in a
    Shopee product response echoes the offset and MUST be ignored.
  - TikTok products & orders: ``next_page_token`` (the ``page_token`` param is
    ignored by the gateway).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


# Git Bash / MSYS2 can silently convert POSIX paths (e.g. "/products") into
# Windows absolute paths when set as env vars. Detect the corruption.
_INVALID_PATH_RE = re.compile(r"[:\\ ]")


def normalize_path(raw: str, default: str, name: str = "") -> str:
    """Return a clean URL path segment from *raw*.

    Rejects anything containing ``:``, ``\\`` or whitespace (MSYS2 corruption)
    and falls back to *default*, logging a warning.
    """
    raw_s = str(raw or "").strip()
    if len(raw_s) < 2 or _INVALID_PATH_RE.search(raw_s):
        if raw_s and name:
            logger.warning(
                "%s=%r rejected (MSYS2 path corruption?) — fallback to %r",
                name,
                raw_s,
                default,
            )
        result = str(default or "/").lstrip("/")
        return result if result else "products"
    return raw_s.lstrip("/")


def parse_pagination(data: Any) -> Dict[str, Any]:
    """Extract the ``pagination`` object from an APISIX response (defensive)."""
    if not isinstance(data, dict):
        return {}
    pagination = data.get("pagination")
    return pagination if isinstance(pagination, dict) else {}


def pagination_has_next(pagination: Dict[str, Any]) -> bool:
    """Normalize APISIX ``has_next`` / ``has_next_page`` flags into a bool."""
    raw = pagination.get("has_next")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes")
    if isinstance(raw, int):
        return raw != 0

    raw_page = pagination.get("has_next_page")
    if isinstance(raw_page, bool):
        return raw_page
    if isinstance(raw_page, str):
        return raw_page.strip().lower() in ("true", "1", "yes")
    if isinstance(raw_page, int):
        return raw_page != 0
    return False


def product_id_key(item: Any) -> Optional[str]:
    """Best-effort dedup key for a product list entry (all APISIX formats).

    Returns None when the entry is not a dict / has no identifier — such an
    entry is still kept but cannot be deduped.
    """
    if not isinstance(item, dict):
        return None
    for key in ("external_product_id", "id", "item_id"):
        value = item.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def pagination_has_more(pagination: Dict[str, Any]) -> bool:
    """True when the response claims more pages exist.

    Accepts bool ``has_next`` / ``has_next_page`` / ``next_offset`` and a
    non-empty ``next_page_token`` (TikTok).
    """
    if pagination_has_next(pagination):
        return True
    if pagination.get("next_page_token") not in (None, ""):
        return True
    if pagination.get("next_offset") not in (None, ""):
        return True
    return False


def is_cursor_marketplace(marketplace: str) -> bool:
    """Marketplaces whose pagination is cursor-based (not page/offset)."""
    return (marketplace or "").lower() in ("tiktok", "tiktoksandbox")


def next_page_plan(
    pagination: Dict[str, Any],
    marketplace: str,
    seen_tokens: set,
) -> Tuple[bool, Optional[str], bool, bool]:
    """Decide how to continue a sweep after one page (products & /order).

    Returns ``(has_more, next_token, repeated, unsupported)``:
      - has_more=False                    → stop normally (last page).
      - has_more=True + next_token=None   → continue via incrementing ``page``
        (Shopee & legacy — gateway computes offset from page).
      - has_more=True + next_token=X      → next request uses ``next_page_token=X``.
      - repeated=True                     → cursor repeated (TikTok only);
        sweep stopped & flagged incomplete (anti infinite-loop).
      - unsupported=True                  → TikTok sent ``has_next`` without a
        cursor; cannot continue → incomplete.
    """
    if not pagination_has_more(pagination):
        return False, None, False, False

    if not is_cursor_marketplace(marketplace):
        # Page-based: ignore any token/offset in the response.
        return True, None, False, False

    token_raw = pagination.get("next_page_token")
    if token_raw in (None, ""):
        return False, None, False, True

    token = str(token_raw)
    if token in seen_tokens:
        return False, None, True, False
    seen_tokens.add(token)
    return True, token, False, False
