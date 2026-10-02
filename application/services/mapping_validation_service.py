"""MappingValidationService — detection & recording of mapping validation issues.

Serves the two TC-SKU-006 test cases:

Case 1 (PACK + S/X share a part, same qty number) → ``SKU_PART_CATEGORY_CONFLICT``
    (severity CONFLICT). The system already BLOCKS the PACK mapping
    (status CONFLICT); here the issue is recorded so it can be tracked.

Case 0 (GET-product gate) → ``SKU_PRODUCT_PACK_SX_CONFLICT`` (severity WARNING):
    a product payload containing PACK + S/X with the same base SKU AND the same
    qty number is recorded; in BLOCK mode the conflicting variants are skipped.

Case 2 (SKU parity):
    2.1 ``SKU_MP_NOT_IN_INVENTREE`` (per shop) — report-only + retry.
    2.2 ``SKU_INVENTREE_ORPHAN`` (global) — report-only.

All issues go to the dynamic ``validation_issue`` table (natural key:
check_code + entity_type + entity_id), occurrence-count deduped and auto-resolved.
"""

import logging
from typing import Any, Dict, List, Optional, Set

from application.ports import InventoryProvider
from domain.entities.validation_issue import ValidationIssue
from domain.repositories.marketplace_variant_repository import (
    MarketplaceVariantRepository,
)
from domain.repositories.sku_mapping_repository import SKUMappingRepository
from domain.repositories.validation_issue_repository import ValidationIssueRepository
from domain.rules.sku import SkuParser, sku_qty_of
from domain.rules.sku_conflict import (
    FAMILY_PACK,
    FAMILY_SX,
    category_family,
    detect_product_pack_sx_conflict,
)

logger = logging.getLogger(__name__)


CHECK_PART_CATEGORY_CONFLICT = "SKU_PART_CATEGORY_CONFLICT"
CHECK_MP_NOT_IN_INVENTREE = "SKU_MP_NOT_IN_INVENTREE"
CHECK_INVENTREE_ORPHAN = "SKU_INVENTREE_ORPHAN"
CHECK_PRODUCT_PACK_SX_CONFLICT = "SKU_PRODUCT_PACK_SX_CONFLICT"

_SEVERITY_CONFLICT = "CONFLICT"
_SEVERITY_WARNING = "WARNING"


def product_entity_id(shop_external_id, external_product_id) -> str:
    """Natural-key ``entity_id`` for a per-product issue — prefixed with shop.

    External product ids are not unique across shops, so without the prefix the
    natural key ``(check_code, entity_type, entity_id)`` could collide.

    Shared by ``check_parsed_products_for_conflicts`` and the SyncEngine skip
    lookup — do not duplicate the formula elsewhere.
    """
    return f"{shop_external_id}_{external_product_id}"


def _seller_skus_of(parsed_product) -> List[str]:
    """Extract non-empty seller_skus from a duck-typed parsed product."""
    return [
        (v.seller_sku or "").strip()
        for v in (getattr(parsed_product, "variants", None) or [])
        if (v.seller_sku or "").strip()
    ]


def _detect_product_pack_sx_conflict(parsed_product) -> Optional[dict]:
    """Backward-compatible wrapper around the pure domain rule.

    Kept so callers/tests that used the old private helper (which accepted a
    duck-typed parsed product) keep working; the logic itself now lives in
    ``domain.rules.sku_conflict.detect_product_pack_sx_conflict``.
    """
    return detect_product_pack_sx_conflict(_seller_skus_of(parsed_product))


class MappingValidationService:
    def __init__(
        self,
        validation_repo: ValidationIssueRepository,
        mapping_repo: SKUMappingRepository,
        variant_repo: MarketplaceVariantRepository,
        inventory_provider: InventoryProvider,
    ):
        self._validation_repo = validation_repo
        self._mapping_repo = mapping_repo
        self._variant_repo = variant_repo
        self._inventory = inventory_provider

    # ------------------------------------------------------------------
    # Public entries
    # ------------------------------------------------------------------
    async def run_checks_for_shop(self, shop_external_id: str) -> Dict[str, Any]:
        """Run PER-SHOP checks (Case 2.1) for one shop.

        Returns ``{"total_conflicts", "total_warnings", "issues"}``.
        """
        issues = await self._detect_mp_not_in_inventree(shop_external_id)

        total_conflicts = sum(1 for i in issues if i.severity == _SEVERITY_CONFLICT)
        total_warnings = sum(1 for i in issues if i.severity == _SEVERITY_WARNING)

        logger.info(
            "Validation done shop=%s: %d issue(s) (%d conflict, %d warning)",
            shop_external_id,
            len(issues),
            total_conflicts,
            total_warnings,
        )
        return {
            "total_conflicts": total_conflicts,
            "total_warnings": total_warnings,
            "issues": ValidationIssue.issue_list_to_dicts(issues),
        }

    async def run_global_checks(self) -> Dict[str, Any]:
        """Run GLOBAL checks once per cycle (Case 1 + Case 2.2)."""
        issues: List[ValidationIssue] = []
        issues += await self._detect_part_category_conflicts()
        issues += await self._detect_inventree_orphans()

        total_conflicts = sum(1 for i in issues if i.severity == _SEVERITY_CONFLICT)
        total_warnings = sum(1 for i in issues if i.severity == _SEVERITY_WARNING)

        logger.info(
            "Global validation done: %d issue(s) (%d conflict, %d warning)",
            len(issues),
            total_conflicts,
            total_warnings,
        )
        return {
            "total_conflicts": total_conflicts,
            "total_warnings": total_warnings,
            "issues": ValidationIssue.issue_list_to_dicts(issues),
        }

    async def check_parsed_products_for_conflicts(
        self,
        parsed_products: List[Any],
        shop_external_id: str,
        marketplace_code: str,
        block: bool = True,
        resolve_stale: bool = True,
    ) -> Dict[str, dict]:
        """GET-product gate for the PACK + S/X conflict (Case 0).

        ``block=True``  → action ``variant_skipped`` (caller must not insert the
                          conflicting seller_skus).
        ``block=False`` → action ``reported_only`` (all variants inserted).

        Returns ``{entity_id_prefixed: conflict_info}`` for problematic products.
        """
        conflicts_by_product: Dict[str, dict] = {}
        active_product_ids: Set[str] = set()

        for parsed_product in parsed_products or []:
            seller_skus = _seller_skus_of(parsed_product)
            info = detect_product_pack_sx_conflict(seller_skus)
            if not info:
                continue
            product_id = str(getattr(parsed_product, "external_product_id", "") or "")
            if not product_id:
                continue
            entity_id = product_entity_id(shop_external_id, product_id)
            conflicts_by_product[entity_id] = info
            active_product_ids.add(entity_id)

            issue = ValidationIssue(
                check_code=CHECK_PRODUCT_PACK_SX_CONFLICT,
                severity="WARNING",
                entity_type="PRODUCT",
                entity_id=entity_id,
                reference=getattr(parsed_product, "product_name", None) or product_id,
                details={
                    "external_product_id": product_id,
                    "product_name": getattr(parsed_product, "product_name", None),
                    "conflict_pairs": info["conflict_pairs"],
                    "pack_skus": info["pack_skus"],
                    "sx_skus": info["sx_skus"],
                    "conflict_variants": info["conflict_skus"],
                    "rule": (
                        "PACK vs SKU_S/SKU_X dengan BASE SKU & ANGKA qty SAMA "
                        "(1A1-Pack-50 vs 1A1-50s) → variant pasangan konflik "
                        "di-skip saat sync. Angka BEDA (1A1-Pack-50 vs 1A1-2s) "
                        "& NORMAL + PACK boleh."
                    ),
                    "policy": (
                        "Variant pasangan konflik TIDAK di-insert ke "
                        "marketplace_variant (product & variant lain tetap "
                        "di-insert) sampai SKU diperbaiki."
                        if block
                        else "report-only: semua variant tetap di-insert, warning dicatat."
                    ),
                },
                action_taken="variant_skipped" if block else "reported_only",
                suggested_action=(
                    f"Product '{getattr(parsed_product, 'product_name', None)}' berisi "
                    f"variant PACK dan SKU_S/SKU_X dengan base SKU & ANGKA qty sama "
                    f"({', '.join(info['pack_skus'])} vs {', '.join(info['sx_skus'])}). "
                    "Ubah salah satu SKU (mis. ganti angka qty / hapus variant), lalu "
                    "sync ulang product."
                ),
                marketplace_code=marketplace_code,
                shop_id=str(shop_external_id),
            )
            try:
                await self._validation_repo.upsert_detected(issue)
            except Exception as exc:
                logger.warning(
                    "upsert issue %s (product=%s) gagal: %s",
                    issue.check_code,
                    product_id,
                    exc,
                )

        if resolve_stale:
            try:
                await self._validation_repo.mark_resolved(
                    CHECK_PRODUCT_PACK_SX_CONFLICT,
                    "PRODUCT",
                    list(active_product_ids),
                    shop_id=str(shop_external_id),
                )
            except Exception as exc:
                logger.warning(
                    "mark_resolved (%s) gagal (shop=%s): %s",
                    CHECK_PRODUCT_PACK_SX_CONFLICT,
                    shop_external_id,
                    exc,
                )

        if conflicts_by_product:
            total_conflict_variants = sum(
                len(info.get("conflict_skus", []))
                for info in conflicts_by_product.values()
            )
            logger.warning(
                "GET PRODUCT conflict gate: %d product(s), %d variant pasangan "
                "konflik (PACK + S/X ber-qty sama) shop=%s — %s",
                len(conflicts_by_product),
                total_conflict_variants,
                shop_external_id,
                "variant di-SKIP" if block else "report-only",
            )
        return conflicts_by_product

    # ------------------------------------------------------------------
    # Case 1 — Duplicate SKU category on one part_id
    # ------------------------------------------------------------------
    async def _detect_part_category_conflicts(self) -> List[ValidationIssue]:
        try:
            mappings = await self._mapping_repo.list_all()
        except Exception as exc:
            logger.warning("list_all mappings failed: %s", exc)
            return []

        by_part: Dict[int, List] = {}
        for m in mappings:
            if m.status not in ("ACTIVE", "CONFLICT"):
                continue
            if not m.inventree_part_id:
                continue
            by_part.setdefault(int(m.inventree_part_id), []).append(m)

        active_part_ids: Set[str] = set()
        issues: List[ValidationIssue] = []

        for part_id, part_mappings in by_part.items():
            packs = [
                (m, sku_qty_of(m))
                for m in part_mappings
                if category_family(m.sku_type) == FAMILY_PACK
            ]
            sxs = [
                (m, sku_qty_of(m))
                for m in part_mappings
                if category_family(m.sku_type) == FAMILY_SX
            ]
            if not packs or not sxs:
                continue

            conflict_found = any(
                pack_qty is not None and pack_qty == sx_qty
                for (_, pack_qty) in packs
                for (_, sx_qty) in sxs
            )
            if not conflict_found:
                continue
            active_part_ids.add(str(part_id))

            families = {category_family(m.sku_type) for m in part_mappings}
            involved = [
                {
                    "sku_mapping_id": str(m.sku_mapping_id),
                    "marketplace_variant_id": str(m.marketplace_variant_id),
                    "seller_sku": m.seller_sku or "",
                    "sku_type": m.sku_type.value
                    if hasattr(m.sku_type, "value")
                    else str(m.sku_type),
                    "qty": sku_qty_of(m),
                    "faktor_pengali": m.conversion_qty,
                    "status": m.status,
                }
                for m in part_mappings
            ]
            issues.append(
                ValidationIssue(
                    check_code=CHECK_PART_CATEGORY_CONFLICT,
                    severity=_SEVERITY_CONFLICT,
                    entity_type="PART",
                    entity_id=str(part_id),
                    reference=(
                        f"part_id={part_id} dipakai SKU_PACK + SKU_S/SKU_X ber-qty sama"
                    ),
                    details={
                        "part_id": part_id,
                        "families": sorted(families),
                        "involved_mappings": involved,
                        "rule": (
                            "PACK vs SKU_S/SKU_X pada part yang sama hanya konflik "
                            "jika ANGKA qty SAMA (Pack-50 vs 50s). BEDA ANGKA boleh "
                            "(Pack-50 vs 2s); NORMAL + PACK selalu boleh."
                        ),
                        "policy": (
                            "BLOCK: mapping SKU_PACK di-set status CONFLICT "
                            "(tidak dipakai pipeline) sampai part dipisah / di-review."
                        ),
                    },
                    action_taken="mapping_blocked",
                    suggested_action=(
                        f"Part {part_id} dipakai oleh SKU_PACK dan SKU_S/SKU_X dengan "
                        "ANGKA qty yang sama. Pisahkan mapping: SKU_PACK harus "
                        "memetakan ke part pack tersendiri, atau hapus salah satu "
                        "mapping. Setelah part dipisah, mapping akan otomatis "
                        "di-reprocess di cycle berikutnya."
                    ),
                )
            )

        try:
            await self._validation_repo.mark_resolved(
                CHECK_PART_CATEGORY_CONFLICT, "PART", list(active_part_ids)
            )
        except Exception as exc:
            logger.warning("mark_resolved (%s) failed: %s", CHECK_PART_CATEGORY_CONFLICT, exc)

        persisted: List[ValidationIssue] = []
        for issue in issues:
            try:
                persisted.append(await self._validation_repo.upsert_detected(issue))
            except Exception as exc:
                logger.warning("upsert issue %s failed: %s", issue.check_code, exc)
        return persisted or issues

    # ------------------------------------------------------------------
    # Case 2.1 — SKU MP exists, InvenTree doesn't (PER-SHOP)
    # ------------------------------------------------------------------
    async def _detect_mp_not_in_inventree(
        self, shop_external_id: str
    ) -> List[ValidationIssue]:
        try:
            inventree_items = await self._inventory.get_all_stock() or []
            shop_variants = await self._variant_repo.list_by_shop(shop_external_id)
        except Exception as exc:
            logger.warning("SKU parity data gathering failed: %s", exc)
            return []

        inventree_sku_set: Set[str] = {
            str(item.get("sku_toko") or "").strip().lower()
            for item in inventree_items
            if str(item.get("sku_toko") or "").strip()
        }

        issues: List[ValidationIssue] = []
        active_mp_ids: Set[str] = set()

        for variant in shop_variants:
            sku = (variant.seller_sku or "").strip()
            if not sku:
                continue
            if sku.lower() in inventree_sku_set:
                continue
            active_mp_ids.add(str(variant.marketplace_variant_id))
            issues.append(
                ValidationIssue(
                    check_code=CHECK_MP_NOT_IN_INVENTREE,
                    severity=_SEVERITY_WARNING,
                    entity_type="VARIANT",
                    entity_id=str(variant.marketplace_variant_id),
                    reference=sku,
                    details={
                        "seller_sku": sku,
                        "sku_type": _parse_sku_type(sku),
                        "shop_id": str(variant.shop_id or ""),
                        "external_product_id": variant.external_product_id or "",
                        "external_variant_id": variant.external_variant_id or "",
                        "policy": "report-only + retry (tanpa auto-create part)",
                    },
                    action_taken="reported_only",
                    suggested_action=(
                        f"SKU '{sku}' tidak ditemukan di vwStockAvailableInventree. "
                        "Buat part di InvenTree dengan sku_toko = SKU ini, atau "
                        "perbaiki SKU di marketplace. Mapping akan dicoba ulang "
                        "otomatis di cycle berikutnya."
                    ),
                    shop_id=str(variant.shop_id or ""),
                )
            )

        try:
            await self._validation_repo.mark_resolved(
                CHECK_MP_NOT_IN_INVENTREE,
                "VARIANT",
                list(active_mp_ids),
                shop_id=str(shop_external_id),
            )
        except Exception as exc:
            logger.warning("mark_resolved (%s) failed: %s", CHECK_MP_NOT_IN_INVENTREE, exc)

        persisted: List[ValidationIssue] = []
        for issue in issues:
            try:
                persisted.append(await self._validation_repo.upsert_detected(issue))
            except Exception as exc:
                logger.warning("upsert issue %s failed: %s", issue.check_code, exc)
        return persisted or issues

    # ------------------------------------------------------------------
    # Case 2.2 — SKU InvenTree exists, SKU MP doesn't (GLOBAL)
    # ------------------------------------------------------------------
    async def _detect_inventree_orphans(self) -> List[ValidationIssue]:
        try:
            inventree_items = await self._inventory.get_all_stock() or []
            all_mp_skus = await self._variant_repo.list_all_seller_skus()
        except Exception as exc:
            logger.warning("InvenTree orphan data gathering failed: %s", exc)
            return []

        mp_sku_set: Set[str] = {
            str(s).strip().lower() for s in all_mp_skus if str(s).strip()
        }

        issues: List[ValidationIssue] = []
        active_orphan_ids: Set[str] = set()

        for item in inventree_items:
            sku = str(item.get("sku_toko") or "").strip()
            part_id = str(item.get("part_id") or "").strip()
            if not sku or not part_id:
                continue
            if sku.lower() in mp_sku_set:
                continue
            active_orphan_ids.add(part_id)
            issues.append(
                ValidationIssue(
                    check_code=CHECK_INVENTREE_ORPHAN,
                    severity=_SEVERITY_WARNING,
                    entity_type="PART",
                    entity_id=part_id,
                    reference=sku,
                    details={
                        "sku_toko": sku,
                        "part_id": part_id,
                        "name": item.get("name") or "",
                        "policy": "report-only (tidak bisa auto-create SKU MP)",
                    },
                    action_taken="reported_only",
                    suggested_action=(
                        f"InvenTree punya sku_toko '{sku}' (part {part_id}) tapi tidak "
                        "ada seller_sku MP yang memakainya. Cek apakah part ini masih "
                        "dipakai; kalau obsolete, nonaktifkan di InvenTree."
                    ),
                )
            )

        try:
            await self._validation_repo.mark_resolved(
                CHECK_INVENTREE_ORPHAN, "PART", list(active_orphan_ids)
            )
        except Exception as exc:
            logger.warning("mark_resolved (%s) failed: %s", CHECK_INVENTREE_ORPHAN, exc)

        persisted: List[ValidationIssue] = []
        for issue in issues:
            try:
                persisted.append(await self._validation_repo.upsert_detected(issue))
            except Exception as exc:
                logger.warning("upsert issue %s failed: %s", issue.check_code, exc)
        return persisted or issues


def _parse_sku_type(seller_sku: str) -> str:
    """Parse sku_type from an SKU string (simple fallback)."""
    try:
        return SkuParser.parse(seller_sku).sku_type.value
    except Exception:
        return "NORMAL"
