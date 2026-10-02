"""TC-SKU-006 conflict rules — pure domain logic.

"Shared part" decisions (unchanged from the reference implementation):

- ``NORMAL`` MAY share a part with anyone (including ``PACK``) — e.g.
  ``1A1`` + ``1A1-Pack-50`` is valid.
- ``SKU_PACK`` MUST NOT share a part with ``SKU_S`` / ``SKU_X`` ONLY IF the
  qty NUMBER is equal (``1A1-Pack-50`` vs ``1A1-50s`` → 50 == 50 → conflict).
  Different numbers (``Pack-50`` vs ``2s``) are allowed.
- ``SKU_S`` / ``SKU_X`` may share a part with each other.

``NORMAL`` never triggers a conflict.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from domain.enums import SkuType
from domain.rules.sku import SkuParser

FAMILY_PACK = "PACK"
FAMILY_SX = "SX"
FAMILY_NORMAL = "NORMAL"


def category_family(sku_type) -> str:
    """Return the conflict family for an SKU type: ``PACK`` | ``SX`` | ``NORMAL``.

    Accepts a :class:`SkuType` enum or a raw string (``"SKU_PACK"``, ...).
    """
    type_str = sku_type.value if hasattr(sku_type, "value") else str(sku_type or "")
    type_str = str(type_str).upper()
    if type_str == SkuType.SKU_PACK.value:
        return FAMILY_PACK
    if type_str in (SkuType.SKU_S.value, SkuType.SKU_X.value):
        return FAMILY_SX
    return FAMILY_NORMAL


def detect_product_pack_sx_conflict(seller_skus: List[str]) -> Optional[dict]:
    """Detect PACK vs SKU_S/SKU_X conflict within ONE product payload.

    Conflict = a ``SKU_PACK`` and a ``SKU_S``/``SKU_X`` share the same base SKU
    AND the same qty number, e.g. ``1A1-Pack-50`` + ``1A1-50s`` (base ``1A1``,
    qty 50 == 50). Detection is based on ``seller_sku`` only (the InvenTree part
    is not known yet at GET-product time).

    Returns ``None`` when there is no conflict, otherwise a dict with the same
    shape as the reference implementation:
        {
            "conflict_pairs": [{"base_sku": str, "qty": int}, ...],
            "pack_skus": [str, ...],
            "sx_skus": [str, ...],
            "conflict_skus": [str, ...],  # all involved seller_skus (deduped)
        }
    """
    packs: Dict[Tuple[str, int], List[str]] = {}
    sxs: Dict[Tuple[str, int], List[str]] = {}
    base_names: Dict[Tuple[str, int], str] = {}

    for sku in seller_skus or []:
        sku_clean = (sku or "").strip()
        if not sku_clean:
            continue
        try:
            parsed = SkuParser.parse(sku_clean)
        except Exception:
            continue
        family = category_family(parsed.sku_type)
        key = (parsed.base_sku.strip().lower(), parsed.conversion_qty)
        base_names.setdefault(key, parsed.base_sku)
        if family == FAMILY_PACK:
            packs.setdefault(key, []).append(sku_clean)
        elif family == FAMILY_SX:
            sxs.setdefault(key, []).append(sku_clean)

    conflicts = sorted(set(packs.keys()) & set(sxs.keys()))
    if not conflicts:
        return None

    pack_skus = [sku for key in conflicts for sku in packs[key]]
    sx_skus = [sku for key in conflicts for sku in sxs[key]]
    return {
        "conflict_pairs": [
            {"base_sku": base_names[key], "qty": key[1]} for key in conflicts
        ],
        "pack_skus": pack_skus,
        "sx_skus": sx_skus,
        # Both sides of the conflict pair are skipped (business decision).
        # Dedupe with dict.fromkeys — duplicate seller_skus in one product
        # must not inflate counters.
        "conflict_skus": list(dict.fromkeys(pack_skus + sx_skus)),
    }
