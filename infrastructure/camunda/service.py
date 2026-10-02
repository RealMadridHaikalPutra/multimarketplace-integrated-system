"""
Service utama: WEBHOOK MENTAH dari APISIX → START EVENT / MESSAGE ke Camunda.

Alur satu webhook:
    0. Ambil DETAIL ORDER dari APISIX berdasarkan order id webhook
       (GET /order?ids=… , docs §6.4) → produk apa saja yang di-order + jumlahnya
       (camunda/order_detail.py). FAIL-OPEN: gagal fetch ⇒ start event tetap jalan.
    1. Baca aturan dari camunda/config.json (hot-reload) → pilih SEMUA aturan
       yang cocok (marketplace + signal_codes + conditions), sesuai
       `mode` di config.json: \"all\" = jalankan semua rule yang cocok
       (satu webhook bisa start beberapa proses), \"first\" = rule pertama saja.
    2. Bangun variabel Camunda dari payload mentah + detail order
       (dot-path / template / literal / first, namespace `order.*` —
       lihat camunda/rules.py).
    3. Kirim start-process / message ke Camunda lewat CamundaClient
       (retry + timeout dari settings).
    4. Catat hasilnya sebagai JSONL di settings.camunda_log_dir
       (1 baris per rule per webhook).

Fungsi ini SENGAJA dipanggil sebagai FastAPI BackgroundTask dari
api/routers/webhook.py: APISIX tetap menerima balasan 2xx dengan cepat,
sementara trigger Camunda berjalan setelah response terkirim.
"""

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from infrastructure.camunda.client import CamundaClient, normalize_request_type
from infrastructure.camunda.models import CamundaPlan, CamundaTriggerResult
from infrastructure.camunda.order_detail import (
    extract_order_id,
    extract_webhook_order_status,
    normalize_order_detail,
    resolve_shop_uuid,
)
from infrastructure.camunda.rules import (
    load_rules,
    order_status_matches,
    parse_status_list,
    render_template,
    resolve_match_mode,
    resolve_variable_types,
    resolve_variables,
    rule_matches,
    rules_file_path,
)
from infrastructure.camunda.signals import (
    describe_signal_config,
    execution_lookup_section,
    render_signal_name,
    resolve_signal_name,
    resolve_signal_target,
    signal_config,
    signal_is_enabled,
    signal_send_on_webhook,
    status_fallback_section,
    status_fallback_skip_statuses,
)
from core.config import settings
from infrastructure.marketplace.apisix.client import ApisixClient

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Kunci aturan yang khusus GATE STATUS ORDER (lihat
# camunda/rules.py::order_status_matches). Dipakai juga oleh `_skip_reason`
# untuk membedakan "tidak ada aturan cocok" dari "status order ditolak".
ORDER_STATUS_RULE_KEYS = (
    "order_status",
    "statuses",
    "exclude_order_status",
    "exclude_statuses",
)

# Kunci level atas yang diperlakukan sebagai SATU aturan ketika config.json
# ditulis dalam bentuk ringkas (tanpa `rules`) — semua kunci lain (version,
# mode, settings, defaults, _comment) diabaikan untuk pembentukan aturan.
FLAT_RULE_KEYS = (
    "id",
    "enabled",
    "type",
    "process_definition_key",
    "process_definition_id",
    "message_name",
    "tenant_id",
    "business_key",
    "variables",
    "variable_types",
    "extra_body",
    # type="signal": nama sinyal + (opsional, Camunda 7) target eksekusi.
    "signal_name",
    "execution_id",
    # Gate status order boleh ditulis di config.json bentuk flat → hot-reload
    # tanpa restart.
) + ORDER_STATUS_RULE_KEYS

# Lock append log — trigger dipanggil dari banyak request async.
_LOG_LOCK = threading.Lock()


def _log_file_path() -> Path:
    """Path file log hasil trigger Camunda."""
    base = Path(settings.camunda_log_dir)
    if not base.is_absolute():
        base = PROJECT_ROOT / base
    return base / settings.camunda_log_filename


def append_camunda_log(entry: Dict[str, Any]) -> None:
    """Append 1 baris JSONL hasil trigger ke file log (best-effort)."""
    if not settings.camunda_log_enabled:
        return
    try:
        path = _log_file_path()
        line = json.dumps(entry, ensure_ascii=False, default=str)
        with _LOG_LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
    except Exception as exc:  # noqa: BLE001 — log tidak boleh fatal
        logger.warning("Camunda: gagal menulis log trigger: %s", exc)


async def resolve_part_ids_for_products(product_ids: List[str]) -> List[int]:
    """part_id InvenTree ACTIVE dari ``sku_mapping`` untuk product_id order.

    Jalur: ``marketplace_product.external_product_id`` → ``marketplace_variant``
    → ``sku_mapping`` (ACTIVE) → ``inventree_part_id`` (baca DB backend ini
    sendiri, urut sesuai urutan product_id, duplikat dibuang).

    Dipisah jadi fungsi modul supaya test bisa memalsukannya tanpa menyentuh
    DB (monkeypatch ``camunda.service.resolve_part_ids_for_products``).
    """
    from infrastructure.db.database import async_session_factory
    from infrastructure.repositories.sku_mapping_repository_impl import (
        SKUMappingRepositoryImpl,
    )

    async with async_session_factory() as session:
        repository = SKUMappingRepositoryImpl(session)
        return await repository.find_active_part_ids_by_external_product_ids(
            product_ids
        )


async def resolve_variant_names_for_products(
    product_ids: List[str],
    shop_uuid: str = "",
) -> Dict[tuple, str]:
    """Nama variant dari DB (``marketplace_variant.variant_name``) per product order.

    Key hasil: ``(external_product_id, external_variant_id)`` → nama variant.
    Dipakai untuk memperkaya variabel Camunda ``reserved_products``: breakdown
    per variant memuat ``variant_id``, ``seller_sku``, jumlah reserved, dan
    ``variant_name``. Response APISIX ``/order`` TIDAK memuat nama variant,
    jadi nama diambil dari DB backend ini (satu query untuk semua produk order).

    Dipisah jadi fungsi modul supaya test bisa dipalsukan tanpa DB
    (monkeypatch ``camunda.service.resolve_variant_names_for_products``).
    FAIL-OPEN: DB gagal / belum tersedia → dict kosong (hanya nama variant yang
    hilang; ``variant_id``/``seller_sku``/jumlah tetap terkirim).
    """
    cleaned = [str(p).strip() for p in (product_ids or []) if str(p or "").strip()]
    if not cleaned:
        return {}

    try:
        from sqlalchemy import select

        from infrastructure.db.database import async_session_factory
        from infrastructure.db.models import (
            MarketplaceProductModel,
            MarketplaceVariantModel,
        )

        async with async_session_factory() as session:
            stmt = (
                select(
                    MarketplaceProductModel.external_product_id,
                    MarketplaceVariantModel.external_variant_id,
                    MarketplaceVariantModel.variant_name,
                )
                .join(
                    MarketplaceVariantModel,
                    MarketplaceVariantModel.marketplace_product_id
                    == MarketplaceProductModel.marketplace_product_id,
                )
                .where(
                    MarketplaceProductModel.external_product_id.in_(cleaned)
                )
            )
            # ``shop_id`` menyimpan external_shop_id (bukan UUID PK shop).
            if shop_uuid:
                stmt = stmt.where(
                    MarketplaceProductModel.shop_id == str(shop_uuid)
                )
            rows = (await session.execute(stmt)).all()
    except Exception as exc:  # noqa: BLE001 — webhook tidak boleh gagal
        logger.warning(
            "Camunda: gagal membaca nama variant dari DB (product=%s): %s: %s "
            "— reserved_products tanpa variant_name",
            cleaned,
            type(exc).__name__,
            str(exc) or repr(exc),
        )
        return {}

    names: Dict[tuple, str] = {}
    for external_product_id, external_variant_id, variant_name in rows:
        key = (
            str(external_product_id or "").strip(),
            str(external_variant_id or "").strip(),
        )
        if key[0] and key[1]:
            names[key] = variant_name
    return names


class CamundaWebhookTrigger:
    """Orkestrasi webhook → Camunda (start process / message) yang configurable."""

    def __init__(self, client_factory=None, apisix_client_factory=None):
        # factory dipakai test untuk menyuntik client dengan MockTransport.
        self._client_factory = client_factory or CamundaClient
        # factory APISIX dipakai untuk fetch DETAIL ORDER (produk yang di-order).
        self._apisix_client_factory = apisix_client_factory or ApisixClient

    # ------------------------------------------------------------------
    # Status / ringkasan konfigurasi
    # ------------------------------------------------------------------

    def is_enabled(self) -> bool:
        """True bila env CAMUNDA_ENABLED tidak dimatikan."""
        return bool(settings.camunda_enabled)

    def describe_config(self) -> Dict[str, Any]:
        """Ringkasan konfigurasi efektif (untuk endpoint debug & troubleshooting)."""
        config = load_rules()
        defaults = config.get("defaults") or {}
        flat_rule = self._flat_rule(config) or {}
        return {
            "enabled": self.is_enabled(),
            "base_url": settings.camunda_base_url,
            "version": str(settings.camunda_version),
            "rest_path": str(settings.camunda_rest_path) or "(default per versi)",
            "auth_type": str(settings.camunda_auth_type),
            "timeout_seconds": settings.camunda_timeout_seconds,
            "max_attempts": settings.camunda_max_attempts,
            "retry_delay_seconds": settings.camunda_retry_delay_seconds,
            "retry_on_status": str(settings.camunda_retry_on_status),
            "rules_file": str(rules_file_path()),
            "rules_file_exists": rules_file_path().exists(),
            "match_mode": resolve_match_mode(config),
            "config_style": "rules" if config.get("rules") else "flat",
            "process_definition_keys": self._target_keys(config),
            "variables": sorted((self._flat_rule(config) or {}).get("variables", {}) or {}),
            "rules_count": len(config.get("rules", [])),
            "rules_enabled": [
                rule.get("id") for rule in config.get("rules", []) if rule.get("enabled") is not False
            ],
            "default_type": defaults.get("type", "start_process"),
            "default_process_definition_key": defaults.get(
                "process_definition_key", settings.camunda_process_definition_key
            ),
            "default_message_name": defaults.get("message_name", settings.camunda_message_name),
            "log": {
                "enabled": bool(settings.camunda_log_enabled),
                "file": str(_log_file_path()),
            },
            "order_detail": {
                "enabled": bool(settings.webhook_order_detail_enabled),
                "path": str(settings.apisix_order_path),
                "translate": bool(settings.webhook_order_detail_translate),
                "timeout_seconds": settings.webhook_order_detail_timeout,
                "fail_open": True,
            },
            "order_status_gate": {
                "_comment": (
                    "Status order yang boleh men-start event. Gate global dari "
                    "WEBHOOK_ORDER_STATUS_ALLOWLIST (.env) + `order_status` per "
                    "aturan di camunda/config.json (aturan hanya mempersempit)."
                ),
                "global_allowlist": sorted(
                    parse_status_list(settings.webhook_order_status_allowlist) or []
                ),
                "rule_allowlist": (
                    flat_rule.get("order_status") or flat_rule.get("statuses")
                ),
                "rule_exclude": (
                    flat_rule.get("exclude_order_status")
                    or flat_rule.get("exclude_statuses")
                ),
                "skip_reason": "order_status_not_allowed",
            },
            "order_reserved": {
                "enabled": bool(settings.webhook_reserved_enabled),
                "max_products": settings.webhook_reserved_max_products,
                "query": (
                    f"{settings.apisix_order_path}?product_id=<produk di order> "
                    "(filter gateway, status order default /order)"
                ),
                "variables": ["total_reserved", "reserved_products"],
            },
            # Peta STATUS ORDER → nama SIGNAL Camunda (hot reload).
            "signal": describe_signal_config(config),
            # External service task worker (camunda/external_task.py).
            "external_tasks": self.describe_external_tasks(config),
        }

    @staticmethod
    def describe_external_tasks(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Ringkasan konfigurasi worker external task (tanpa memanggil Camunda)."""
        from infrastructure.camunda.external_task import (
            external_task_config,
            external_tasks_enabled,
            parse_topics,
        )
        from infrastructure.camunda.task_handlers import available_handlers

        section = external_task_config(config)
        topics = parse_topics(section)
        return {
            "enabled": external_tasks_enabled(section),
            "worker_id": section.get("worker_id"),
            "topics": [topic.to_dict() for topic in topics],
            "topics_count": len(topics),
            "max_tasks": section.get("max_tasks"),
            "poll_interval_seconds": section.get("poll_interval_seconds"),
            "builtin_handlers": available_handlers(),
            "endpoints": {
                "poll_once": "POST /api/v1/webhook/camunda/external-tasks/poll",
                "status": "GET /api/v1/webhook/camunda/external-tasks/status",
                "start": "POST /api/v1/webhook/camunda/external-tasks/start",
                "stop": "POST /api/v1/webhook/camunda/external-tasks/stop",
            },
        }

    # ------------------------------------------------------------------
    # Resolusi aturan → rencana request (plan)
    # ------------------------------------------------------------------

    @staticmethod
    def _flat_rule(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Ambil aturan dinamis dari kunci level atas (bentuk "flat").

        Return None bila config memakai bentuk `rules` (bentuk berfilter) atau
        tidak punya target proses sama sekali.
        """
        if config.get("rules"):
            return None

        rule = {key: config[key] for key in FLAT_RULE_KEYS if key in config}
        has_target = bool(
            rule.get("process_definition_key")
            or rule.get("process_definition_id")
            or rule.get("message_name")
            # type="signal" → targetnya nama sinyal (boleh dari peta `signal`
            # by_status/by_marketplace), jadi config yang HANYA mengirim signal
            # (tanpa start event) tetap dianggap punya target.
            or str(rule.get("type") or "").strip().lower() == "signal"
        )
        if not has_target:
            return None
        rule.setdefault("id", config.get("id") or "default")
        return rule

    @classmethod
    def _target_keys(cls, config: Dict[str, Any]) -> List[str]:
        """Daftar process definition key yang akan dijalankan (string/array)."""
        sources = []
        flat = cls._flat_rule(config)
        if flat:
            sources.append(flat)
        sources.extend(config.get("rules") or [])

        keys: List[str] = []
        for rule in sources:
            if rule.get("enabled") is False:
                continue
            value = rule.get("process_definition_key")
            if isinstance(value, (list, tuple)):
                keys.extend(str(item) for item in value if item)
            elif value:
                keys.append(str(value))
            elif rule.get("process_definition_id"):
                keys.append(str(rule["process_definition_id"]))
        return keys

    def _build_context(
        self,
        marketplace: str,
        signal_code: Any,
        body: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]],
        order_detail: Optional[Dict[str, Any]] = None,
        order_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Konteks untuk matching + template: meta, headers, payload mentah,
        dan DETAIL ORDER dari APISIX (namespace `order.*` di rules.py).

        `order_status` = status dari payload WEBHOOK (bukan detail order) — dipakai
        gate status order (`order_status` per aturan + WEBHOOK_ORDER_STATUS_ALLOWLIST).
        """
        return {
            "meta": {
                "marketplace": marketplace,
                "signal_code": signal_code,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "order_status": order_status,
            },
            "headers": headers or {},
            "body": body if isinstance(body, dict) else {},
            "order": order_detail if isinstance(order_detail, dict) else {},
            "variables": {},
        }

    # ------------------------------------------------------------------
    # DETAIL ORDER (produk apa saja yang di-order & berapa banyak)
    # ------------------------------------------------------------------

    async def _fetch_order_detail(
        self, marketplace: str, body: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Ambil detail order dari APISIX berdasarkan order id webhook.

        Dokumentasi APISIX §6.4: `GET /order?...&ids=<order id>`.
        Hasilnya dinormalisasi (camunda/order_detail.py) menjadi konteks
        `order` yang bisa dipetakan config.json lewat path `order.*`.

        Sifat FAIL-OPEN: kegagalan apa pun → log warning + kembalikan `{}`,
        sehingga start event Camunda TETAP dijalankan (variabel order dilewati).
        """
        if not settings.webhook_order_detail_enabled:
            return {}
        if not isinstance(body, dict) or not body:
            return {}

        order_id = extract_order_id(marketplace, body)
        if not order_id:
            logger.info(
                "Camunda: detail order dilewati — order id tidak ada di payload "
                "(marketplace=%s)",
                marketplace,
            )
            return {}

        shop_uuid = resolve_shop_uuid(marketplace, body)
        if not shop_uuid:
            logger.warning(
                "Camunda: detail order dilewati — shop_uuid tidak diketahui "
                "(marketplace=%s, order_id=%s) — set APISIX_<MP>_SHOP_UUID bila "
                "shop_id payload bukan shop_uuid",
                marketplace, order_id,
            )
            return normalize_order_detail(
                marketplace, None, order_id, shop_uuid, error="missing_shop_uuid"
            )

        client = self._apisix_client_factory()
        try:
            raw = None
            detail_error: Optional[str] = None
            try:
                raw = await client.fetch_order_detail(
                    marketplace=marketplace,
                    shop_uuid=shop_uuid,
                    order_id=order_id,
                    translate=bool(settings.webhook_order_detail_translate),
                    timeout=settings.webhook_order_detail_timeout,
                )
                if raw is None:
                    detail_error = "fetch_failed"
            except Exception as exc:  # noqa: BLE001 — webhook tidak boleh crash
                logger.exception("Camunda: gagal fetch detail order (order_id=%s): %s", order_id, exc)
                detail_error = f"{type(exc).__name__}: {exc}"

            detail = normalize_order_detail(
                marketplace, raw, order_id, shop_uuid, error=detail_error
            )

            # ── Total reserved stock per produk (APISIX /order?product_id=…) ──
            # Butuh product id dari detail order, jadi dijalankan setelah detail
            # sukses. GAGAL per produk TIDAK membatalkan start event.
            if detail.get("ok"):
                await self._attach_reserved_totals(detail, marketplace, shop_uuid, client)
                # ── PART ID dari sku_mapping (ikut start event) ────────────
                # Dijalankan SETELAH reserved stock (daftar produk sudah pasti
                # dari line item): product_id → sku_mapping ACTIVE → part_id.
                await self._attach_part_ids(detail)

            # PENTING: pakai %s (bukan %d) — saat gagal, item_count/total_quantity
            # memang TIDAK ada (None). Memakai %d dengan None membuat logging gagal
            # format dan mencetak traceback logging panjang (tapi request tetap jalan).
            if detail.get("ok"):
                logger.info(
                    "Camunda: detail order %s → %s item, %s unit, reserved=%s (%s)",
                    order_id,
                    detail.get("item_count"),
                    detail.get("total_quantity"),
                    detail.get("total_reserved", "-"),
                    detail.get("products_summary") or "-",
                )
            else:
                logger.warning(
                    "Camunda: detail order %s TIDAK terbaca (%s) — variabel order "
                    "dilewati, start event tetap dijalankan",
                    order_id,
                    detail.get("error") or "unknown",
                )
            return detail
        finally:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Camunda: gagal menutup HTTP client APISIX", exc_info=True)

    # ------------------------------------------------------------------
    # TOTAL RESERVED STOCK per produk (APISIX /order?product_id=…)
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_product_ids(detail: Dict[str, Any]) -> List[str]:
        """product_id unik (berurutan) dari line item sebuah detail order."""
        product_ids: List[str] = []
        for item in detail.get("items") or []:
            if not isinstance(item, dict):
                continue
            product_id = str(item.get("product_id") or "").strip()
            if product_id and product_id not in product_ids:
                product_ids.append(product_id)
        return product_ids

    @staticmethod
    def _matched_reserved_entries(
        raw: Any, product_id: str
    ) -> List[Dict[str, Any]]:
        """Entry ``reserved_stock`` milik SATU product (saringan sisi backend).

        Gateway sudah menyaring via ``?product_id=``, tapi hasilnya tetap
        disaring LAGI di sini sebagai pengaman: bila plugin gateway belum
        di-reload, param diabaikan dan response berisi seluruh produk toko —
        tanpa penyaringan kedua ini, angka reserved bisa salah (kebanyakan).

        Mengembalikan ``[]`` bila response bukan object / tidak punya
        ``reserved_stock`` berupa list.
        """
        if not isinstance(raw, dict):
            return []
        entries = raw.get("reserved_stock")
        if not isinstance(entries, list):
            return []

        dicts = [entry for entry in entries if isinstance(entry, dict)]
        wanted = str(product_id)
        matched = [
            entry for entry in dicts if str(entry.get("product_id") or "") == wanted
        ]
        if not raw.get("filtered_by") and len(matched) < len(dicts):
            # Penanda `filtered_by` hanya ditulis gateway yang SUDAH punya filter.
            logger.warning(
                "APISIX /order tidak menerapkan filter product_id (plugin "
                "request-transformer mungkin belum di-reload) — hasil disaring di "
                "sisi backend untuk product_id=%s",
                product_id,
            )
        return matched

    @staticmethod
    def _sum_product_reserved(raw: Any, product_id: str) -> Optional[int]:
        """Jumlahkan `total_reserved_stock` semua variant untuk SATU product.

        Gateway sudah menyaring via `?product_id=`, tapi hasilnya tetap disaring
        LAGI di sini sebagai pengaman (lihat ``_matched_reserved_entries``).

        Returns None bila response tidak terbaca sebagai object berisi
        ``reserved_stock`` (fetch gagal / body non-object) — caller melewatinya.
        """
        if not isinstance(raw, dict):
            return None
        entries = raw.get("reserved_stock")
        if not isinstance(entries, list):
            return None

        matched = CamundaWebhookTrigger._matched_reserved_entries(raw, product_id)
        total = 0
        for entry in matched:
            for variant in entry.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                try:
                    total += int(variant.get("total_reserved_stock") or 0)
                except (TypeError, ValueError):
                    continue
        return total

    @classmethod
    def _build_reserved_by_product(
        cls,
        snapshot: Dict[str, Any],
        variant_names: Optional[Dict[tuple, str]] = None,
    ) -> Dict[str, Any]:
        """Susun variabel ``reserved_products``: total produk + breakdown VARIANT.

        Bentuk (nested):

            {
              "<product_id>": {
                "total_reserved": 9,
                "variants": {
                  "<variant_id>": {
                    "variant_id": "1736672772166353964",
                    "seller_sku": "8F14-Pack-100",
                    "total_reserved_stock": 6,
                    "variant_name": "Pack of 100"
                  }
                }
              }
            }

        ``total_reserved`` per produk SAMA dengan bentuk lama (``{pid: total}``),
        jadi angka totalnya tetap konsisten; yang BARU adalah rincian per variant
        (variant_id + seller_sku + jumlah reserved + nama variant) — penting
        untuk produk multi-variant, yang sebelumnya hanya tampil sebagai satu
        angka total per produk.
        """
        names = variant_names or {}
        payloads = snapshot.get("payloads") or {}
        totals = snapshot.get("totals") or {}

        by_product: Dict[str, Any] = {}
        for product_id, total in totals.items():
            pid = str(product_id)
            variants: Dict[str, Any] = {}
            for entry in cls._matched_reserved_entries(
                payloads.get(product_id), pid
            ):
                for variant in entry.get("variants") or []:
                    if not isinstance(variant, dict):
                        continue
                    variant_id = str(variant.get("variant_id") or "").strip()
                    if not variant_id:
                        continue
                    try:
                        reserved_qty = int(
                            variant.get("total_reserved_stock") or 0
                        )
                    except (TypeError, ValueError):
                        reserved_qty = 0
                    variants[variant_id] = {
                        "variant_id": variant_id,
                        "seller_sku": variant.get("seller_sku"),
                        "total_reserved_stock": reserved_qty,
                        "variant_name": names.get((pid, variant_id)),
                    }
            by_product[pid] = {
                "total_reserved": int(total),
                "variants": variants,
            }
        return by_product

    @staticmethod
    def _reserved_order_ids(raw: Any) -> Set[str]:
        """Kumpulkan semua `order_id` di breakdown `orders[]` satu response /order.

        Dipakai untuk mendeteksi order PEMICU belum ikut di-agregasi gateway:
        webhook tiba hanya 1-4 detik setelah order dibuat, sementara agregasi
        `/order` membaca daftar order marketplace yang indeksnya bisa tertinggal
        beberapa detik (kasus nyata: total_reserved=4 padahal gateway — dicek
        beberapa saat kemudian — sudah 5).
        """
        if not isinstance(raw, dict):
            return set()
        entries = raw.get("reserved_stock")
        if not isinstance(entries, list):
            return set()

        order_ids: Set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            for variant in entry.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                for order in variant.get("orders") or []:
                    if not isinstance(order, dict):
                        continue
                    order_id = str(order.get("order_id") or "").strip()
                    if order_id:
                        order_ids.add(order_id)
        return order_ids

    @classmethod
    def _collect_reserved_order_ids(cls, payloads: Any) -> Set[str]:
        """Gabungan `order_id` dari beberapa response /order (satu per produk)."""
        order_ids: Set[str] = set()
        if not isinstance(payloads, dict):
            return order_ids
        for raw in payloads.values():
            order_ids |= cls._reserved_order_ids(raw)
        return order_ids

    async def _fetch_reserved_snapshot(
        self,
        client: Any,
        marketplace: str,
        shop_uuid: str,
        product_ids: List[str],
    ) -> Dict[str, Any]:
        """Ambil snapshot reserved stock: SATU panggilan /order per produk.

        Returns:
            ``{"totals": {product_id: total}, "payloads": {product_id: raw|None},
            "ok": bool}``. ``ok`` True bila minimal satu response terbaca sebagai
            object — response KOSONG tetap dihitung "terbaca", karena pada order
            yang baru dibuat `reserved_stock` memang bisa masih kosong.
        """
        totals: Dict[str, int] = {}
        payloads: Dict[str, Any] = {}
        any_parsed = False

        for product_id in product_ids:
            try:
                raw = await client.fetch_reserved_stock(
                    marketplace, shop_uuid, product_id=product_id
                )
            except Exception as exc:  # noqa: BLE001 — fail-open
                logger.warning(
                    "Camunda: gagal ambil reserved stock product_id=%s: %s: %s",
                    product_id, type(exc).__name__, str(exc) or repr(exc),
                )
                payloads[product_id] = None
                continue

            total = self._sum_product_reserved(raw, product_id)
            if total is None:
                logger.warning(
                    "Camunda: reserved stock product_id=%s tidak terbaca — "
                    "produk ini dilewati",
                    product_id,
                )
                payloads[product_id] = None
                continue

            totals[product_id] = total
            payloads[product_id] = raw
            any_parsed = True

        return {"totals": totals, "payloads": payloads, "ok": any_parsed}

    async def _attach_reserved_totals(
        self,
        detail: Dict[str, Any],
        marketplace: str,
        shop_uuid: str,
        client: Any,
    ) -> None:
        """Isi `order.total_reserved` + `order.reserved_by_product` di *detail*.

        Satu panggilan APISIX PER PRODUK yang ada di order:

            GET {base}/{order_path}?marketplace=…&shop_uuid=…&product_id=<pid>

        (filter gateway, docs §6.4 — status order tetap default `/order`).
        Hasilnya (NESTED: total produk + rincian per VARIANT):
            detail["reserved_by_product"] = {
                "<pid>": {
                    "total_reserved": 9,
                    "variants": {
                        "<variant_id>": {
                            "variant_id": "…",
                            "seller_sku": "8F14-Pack-100",
                            "total_reserved_stock": 6,
                            "variant_name": "Pack of 100",
                        }
                    },
                },
            }
            detail["total_reserved"]       = 6          # jumlah semua produk order

        ``variant_name`` diambil dari DB (``marketplace_variant.variant_name``)
        lewat :func:`resolve_variant_names_for_products` — FAIL-OPEN, nama saja
        yang hilang bila DB belum tersedia; id/SKU/jumlah tetap terkirim.

        FAIL-OPEN: kegagalan per produk dicatat warning dan dilewati; bila tidak
        ada satu pun yang berhasil, kedua kunci TIDAK ditulis sehingga variabel
        Camunda terkait otomatis dilewati (start event tetap berjalan).

        RETRY ORDER PEMICU: bila `order_id` order pemicu TIDAK muncul di snapshot
        (daftar order marketplace belum terindeks beberapa detik setelah order
        dibuat), tunggu `settings.webhook_reserved_retry_delay` detik lalu ambil
        ULANG sekali; hasil retry menimpa snapshot pertama (lebih baru). Bila
        order pemicu tetap tidak ada, dicatat warning eksplisit — angka yang
        terkirim kemungkinan kurang, bukan gagal diam-diam.
        """
        if not settings.webhook_reserved_enabled:
            return
        product_ids = self._collect_product_ids(detail)
        if not product_ids:
            return

        max_products = max(1, int(settings.webhook_reserved_max_products or 1))
        if len(product_ids) > max_products:
            logger.warning(
                "Camunda: reserved stock dibatasi %d produk (order punya %d) — "
                "sisa produk dilewati",
                max_products, len(product_ids),
            )
            product_ids = product_ids[:max_products]

        snapshot = await self._fetch_reserved_snapshot(
            client, marketplace, shop_uuid, product_ids
        )

        trigger_order_id = str(detail.get("order_id") or "").strip()
        retry_delay = float(settings.webhook_reserved_retry_delay or 0)
        if (
            snapshot["ok"]
            and trigger_order_id
            and retry_delay > 0
            and trigger_order_id
            not in self._collect_reserved_order_ids(snapshot["payloads"])
        ):
            logger.info(
                "Camunda: order pemicu %s belum ada di snapshot reserved stock "
                "(indeks daftar order marketplace tertinggal) — retry dalam %.1fs",
                trigger_order_id, retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry = await self._fetch_reserved_snapshot(
                client, marketplace, shop_uuid, product_ids
            )
            if retry["ok"]:
                # Snapshot kedua lebih baru: menang per produk. Produk yang
                # gagal di retry tetap memakai nilai snapshot pertama.
                snapshot["totals"].update(retry["totals"])
                for pid, payload in retry["payloads"].items():
                    if payload is not None:
                        snapshot["payloads"][pid] = payload

            if trigger_order_id in self._collect_reserved_order_ids(snapshot["payloads"]):
                logger.info(
                    "Camunda: order pemicu %s sudah terhitung setelah retry",
                    trigger_order_id,
                )
            else:
                logger.warning(
                    "Camunda: order pemicu %s TETAP tidak ada di snapshot reserved "
                    "stock setelah retry %.1fs — total_reserved kemungkinan kurang",
                    trigger_order_id, retry_delay,
                )

        totals: Dict[str, int] = snapshot["totals"]
        if not totals:
            return

        # Nama variant (opsional) dari DB — SATU query untuk semua produk order.
        # Fail-open: gagal → dict kosong, hanya nama yang hilang.
        variant_names = await resolve_variant_names_for_products(
            list(totals.keys()), shop_uuid
        )

        detail["reserved_by_product"] = self._build_reserved_by_product(
            snapshot, variant_names
        )
        detail["total_reserved"] = sum(totals.values())
        logger.info(
            "Camunda: reserved stock untuk order → %s (total=%s)",
            detail["reserved_by_product"], detail["total_reserved"],
        )

    async def _attach_part_ids(self, detail: Dict[str, Any]) -> None:
        """Isi ``order.part_id`` (ARRAY) dari tabel ``sku_mapping``.

        Dijalankan SETELAH reserved stock, memakai product_id line item:
        product_id → sku_mapping ACTIVE → inventree_part_id (lihat
        :func:`resolve_part_ids_for_products`). Hasilnya:

            detail["part_id"] = [7, 9]   # SEMUA part pada order ini (array)

        ``part_id`` sengaja berbentuk ARRAY (bukan skalar): satu product
        marketplace bisa memetakan ke BANYAK part_id (mis. satu listing
        TikTok berisi beberapa SKU InvenTree), dan SEMUA part itu yang harus
        dijalankan pipeline-nya — bukan hanya part pertama. Tidak ada
        variabel ``partIds`` terpisah: cukup ``partId`` (bertipe json).

        FAIL-OPEN: setting mati / DB gagal / tidak ada mapping ACTIVE → kunci
        tidak ditulis sama sekali (variabel partId otomatis dilewati oleh
        config.json, start event tetap jalan).
        """
        if not settings.webhook_part_id_enabled:
            return
        product_ids = self._collect_product_ids(detail)
        if not product_ids:
            return

        try:
            part_ids = await resolve_part_ids_for_products(product_ids)
        except Exception as exc:  # noqa: BLE001 — webhook tidak boleh gagal
            logger.warning(
                "Camunda: gagal resolve part_id dari sku_mapping (product=%s): %s: %s",
                product_ids, type(exc).__name__, str(exc) or repr(exc),
            )
            return

        if not part_ids:
            logger.info(
                "Camunda: product %s tidak punya sku_mapping ACTIVE — variabel "
                "partId dilewati (start event tetap jalan)",
                product_ids,
            )
            return

        detail["part_id"] = [int(pid) for pid in part_ids]
        logger.info(
            "Camunda: part_id dari sku_mapping → %s (array, product=%s)",
            detail["part_id"], product_ids,
        )

    def _plan_from_rule(
        self,
        rule: Dict[str, Any],
        defaults: Dict[str, Any],
        ctx: Dict[str, Any],
    ) -> Optional[CamundaPlan]:
        """Bangun rencana request Camunda dari satu aturan yang sudah cocok."""
        sources = [defaults, rule]

        def pick(key: str, env_value: Any = None) -> Any:
            """Nilai aturan → defaults → env (aturan paling tinggi)."""
            for source in reversed(sources):
                value = source.get(key)
                if value not in (None, ""):
                    return value
            return env_value

        raw_type = pick("type", "message" if settings.camunda_message_name else "start_process")
        request_type = normalize_request_type(raw_type)
        if request_type is None:
            logger.warning(
                "Camunda: type '%s' tidak dikenal (rule=%s) — dianggap start_process",
                raw_type, rule.get("id"),
            )
            request_type = "start_process"

        process_key = pick("process_definition_key", settings.camunda_process_definition_key)
        process_id = pick("process_definition_id", settings.camunda_process_definition_id)
        message_name = pick("message_name", settings.camunda_message_name)

        # type=message tanpa message_name → turunkan jadi start_process supaya
        # tidak mengirim request invalid ke Camunda.
        if request_type == "message" and not message_name:
            logger.warning(
                "Camunda: rule=%s bertipe message tanpa message_name — memakai start_process",
                rule.get("id"),
            )
            request_type = "start_process"

        # type=signal → nama sinyal wajib. Bila `signal_name` kosong, nama diambil
        # dari peta STATUS ORDER → SIGNAL di bagian `signal` config.json
        # (camunda/signals.py), sehingga satu aturan bisa menangani banyak status.
        signal_name: Optional[str] = None
        execution_id: Optional[str] = None
        if request_type == "signal":
            signal_name = str(pick("signal_name") or "").strip()
            execution_id = str(pick("execution_id") or "").strip() or None
            if not signal_name:
                section = signal_config()
                if not signal_is_enabled(section):
                    logger.warning(
                        "Camunda: rule=%s bertipe signal tapi signal.enabled=false — dilewati",
                        rule.get("id"),
                    )
                    return None
                signal_name = resolve_signal_name(
                    section,
                    (ctx.get("meta") or {}).get("marketplace"),
                    (ctx.get("meta") or {}).get("order_status"),
                )
                if not signal_name:
                    logger.warning(
                        "Camunda: rule=%s bertipe signal tapi nama sinyal untuk "
                        "status=%s marketplace=%s tidak dipetakan — dilewati",
                        rule.get("id"),
                        (ctx.get("meta") or {}).get("order_status"),
                        (ctx.get("meta") or {}).get("marketplace"),
                    )
                    return None

        if request_type == "start_process" and not process_key:
            if process_id:
                request_type = "start_process_by_id"
            else:
                logger.warning(
                    "Camunda: rule=%s tidak punya process_definition_key/message_name — dilewati",
                    rule.get("id"),
                )
                return None

        variables = resolve_variables(
            rule.get("variables") or {},
            defaults.get("variables") or {},
            ctx,
        )
        webhook_log_id = (ctx.get("meta") or {}).get("webhook_log_id")
        if webhook_log_id and "webhookLogId" not in variables:
            variables["webhookLogId"] = str(webhook_log_id)

        # Variabel hasil mapping diekspos ke konteks agar business key boleh

        # merujuknya (mis. \"{{var.resi}}:{{var.courier}}:{{var.retur_date}}\").
        plan_ctx = dict(ctx)
        plan_ctx["variables"] = variables

        # Nama signal boleh memakai placeholder dari variabel hasil mapping
        # (mis. "readyToShip${invoice}" / "readyToShip{{invoice}}") — dirender
        # dari `variables` plan di atas, lihat signals.render_signal_name.
        if request_type == "signal" and signal_name:
            rendered_signal = str(
                render_signal_name(signal_name, variables) or ""
            ).strip()
            if not rendered_signal:
                logger.warning(
                    "Camunda: rule=%s nama signal '%s' jadi KOSONG setelah "
                    "render variabel — dilewati",
                    rule.get("id"), signal_name,
                )
                return None
            signal_name = rendered_signal

        business_key_template = (
            rule.get("business_key")
            or defaults.get("business_key")
            or settings.camunda_business_key_template
        )
        business_key = None
        if business_key_template:
            rendered = render_template(str(business_key_template), plan_ctx)
            if rendered not in (None, ""):
                business_key = str(rendered)

        tenant_id = rule.get("tenant_id") or defaults.get("tenant_id") or settings.camunda_tenant_id

        return CamundaPlan(
            request_type=request_type,
            process_definition_key=str(process_key) if process_key else None,
            process_definition_id=str(process_id) if process_id else None,
            message_name=str(message_name) if message_name else None,
            variables=variables,
            variable_types=resolve_variable_types(defaults, rule),
            business_key=business_key,
            tenant_id=str(tenant_id) if tenant_id else None,
            extra_body=rule.get("extra_body") if isinstance(rule.get("extra_body"), dict) else {},
            rule_id=str(rule.get("id") or "unnamed"),
            signal_name=signal_name,
            execution_id=execution_id,
        )

    def _resolve_plans(self, ctx: Dict[str, Any]) -> List[CamundaPlan]:
        """Semua aturan yang cocok → daftar rencana request Camunda.

        - `mode: \"all\"` (default)  → semua rule cocok dijalankan.
        - `mode: \"first\"`          → hanya rule pertama yang cocok.
        - File aturan KOSONG + target dari .env diisi → mode env-only
          (satu plan \"env-default\").
        """
        config = load_rules()
        defaults = config.get("defaults") or {}
        rules = config.get("rules") or []

        # ── Bentuk FLAT (tanpa `rules`): seluruh config.json = satu aturan
        #    dinamis. SETIAP webhook dijalankan — tidak ada filter marketplace
        #    / signal code. `process_definition_key` boleh array.
        if not rules:
            flat = self._flat_rule(config)
            if flat is None:
                # Config kosong/tanpa target → pakai target dari .env bila ada.
                if not (
                    settings.camunda_process_definition_key
                    or settings.camunda_process_definition_id
                    or settings.camunda_message_name
                ):
                    return []
                return self._plans_from_rule({"id": "env-default", "variables": {}}, {}, ctx)
            if config.get("enabled") is False:
                logger.info(
                    "Camunda: config.json dimatikan (\"enabled\": false) — webhook dilewati"
                )
                return []
            # Gate status order bentuk FLAT. Bentuk ini dipakai apa adanya
            # (tanpa `rule_matches`), jadi filternya diperiksa di sini supaya
            # `order_status` / `exclude_order_status` juga berlaku.
            meta = ctx.get("meta") or {}
            if not order_status_matches(flat, meta.get("order_status")):
                logger.info(
                    "Camunda: webhook dilewati — status order %s tidak ada di "
                    "daftar \"order_status\" config.json (marketplace=%s)",
                    meta.get("order_status") or "(tidak ada di payload)",
                    meta.get("marketplace"),
                )
                return []
            return self._plans_from_rule(flat, defaults, ctx)

        matched = [rule for rule in rules if rule_matches(rule, ctx)]
        if resolve_match_mode(config) == "first":
            matched = matched[:1]

        # Tidak ada rule cocok:
        #   - file aturan PUNYA rules  → memang tidak ada yang cocok, dilewati
        #     (jangan diam-diam menembak proses default: bisa salah proses).
        #   - file aturan KOSONG/tidak ada → mode env-only: pakai target dari
        #     .env supaya konfigurasi tanpa config.json tetap berfungsi.
        # Tidak ada rule cocok → memang tidak ada yang harus dijalankan
        # (jangan diam-diam menembak proses default: bisa salah proses).
        if not matched:
            return []

        plans: List[CamundaPlan] = []
        for rule in matched:
            plans.extend(self._plans_from_rule(rule, defaults, ctx))
        return plans

    def _plans_from_rule(
        self,
        rule: Dict[str, Any],
        defaults: Dict[str, Any],
        ctx: Dict[str, Any],
    ) -> List[CamundaPlan]:
        """Rencana dari satu aturan.

        `process_definition_key` boleh ARRAY → satu process instance per key
        (mis. satu webhook harus men-start dua proses sekaligus).
        """
        if rule.get("enabled") is False:
            return []

        keys = rule.get("process_definition_key")
        if isinstance(keys, (list, tuple)):
            plans: List[CamundaPlan] = []
            for key in keys:
                variant = dict(rule)
                variant["process_definition_key"] = key
                plan = self._plan_from_rule(variant, defaults, ctx)
                if plan:
                    plans.append(plan)
            return plans

        plan = self._plan_from_rule(rule, defaults, ctx)
        return [plan] if plan else []

    # ------------------------------------------------------------------
    # Trigger
    # ------------------------------------------------------------------

    async def trigger_all(
        self,
        marketplace: str,
        signal_code: Any,
        body: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]] = None,
        raw_body: str = "",
        dry_run: bool = False,
        order_detail: Optional[Dict[str, Any]] = None,
        webhook_log_id: Optional[str] = None,
    ) -> List[CamundaTriggerResult]:
        """Teruskan satu webhook ke Camunda — SATU hasil per rule yang cocok.

        @param dry_run: bila True, aturan + variabel tetap dihitung dan dicatat
                        ke log, tapi TIDAK ada HTTP request ke Camunda.
        @param order_detail: detail order hasil fetch APISIX. Bila None,
                        backend mengambilnya sendiri (WEBHOOK_ORDER_DETAIL_ENABLED)
                        memakai order id dari payload webhook.
        @param webhook_log_id: UUID baris tabel webhook_log untuk pelacakan end-to-end.
        """
        # Status order dari payload WEBHOOK (bukan detail order) — dipakai gate
        # status: `order_status` per aturan + WEBHOOK_ORDER_STATUS_ALLOWLIST.
        order_status = extract_webhook_order_status(body)
        ctx = self._build_context(
            marketplace, signal_code, body, headers, order_status=order_status
        )
        if webhook_log_id:
            ctx.setdefault("meta", {})["webhook_log_id"] = str(webhook_log_id)

        if not self.is_enabled():
            result = CamundaTriggerResult(
                ok=True,
                request_type="skip",
                skipped_reason="camunda_disabled",
                marketplace=marketplace,
                signal_code=signal_code,
            )
            logger.info(
                "Camunda: trigger dilewati (CAMUNDA_ENABLED=false) — marketplace=%s signal=%s",
                marketplace, signal_code,
            )
            append_camunda_log(self._log_entry(result, raw_body, dry_run))
            return [result]

        # ── Gate STATUS ORDER (global, dari .env) ─────────────────────────
        # WEBHOOK_ORDER_STATUS_ALLOWLIST="UNPAID,ON_HOLD,AWAITING_SHIPMENT" →
        # hanya status itu yang boleh men-start event. Dijalankan SEBELUM fetch
        # detail order, jadi webhook yang tidak relevan tidak memakai panggilan
        # APISIX sama sekali. Kosong = tanpa gate (perilaku lama).
        allowed_statuses = parse_status_list(settings.webhook_order_status_allowlist)
        if allowed_statuses and (
            order_status is None or order_status.strip().lower() not in allowed_statuses
        ):
            result = CamundaTriggerResult(
                ok=True,
                request_type="skip",
                skipped_reason="order_status_not_allowed",
                marketplace=marketplace,
                signal_code=signal_code,
            )
            logger.info(
                "Camunda: trigger dilewati — status order %s tidak ada di "
                "WEBHOOK_ORDER_STATUS_ALLOWLIST=%s (marketplace=%s signal=%s)",
                order_status if order_status else "(tidak ada di payload)",
                sorted(allowed_statuses),
                marketplace, signal_code,
            )
            append_camunda_log(self._log_entry(result, raw_body, dry_run))
            return [result]

        # Detail order (produk apa saja & jumlahnya) — dipakai variabel
        # `order.*` di config.json. Gagal fetch tidak menghalangi start event.
        if order_detail is None:
            order_detail = await self._fetch_order_detail(marketplace, body)
        ctx["order"] = order_detail if isinstance(order_detail, dict) else {}

        # ── Update detail order & part_ids ke tabel webhook_log ───────────
        parsed_log_uuid = None
        log_status = None
        if webhook_log_id:
            try:
                import uuid as _uuid
                from infrastructure.db.database import async_session_factory
                from infrastructure.repositories.webhook_log_repository_impl import (
                    WebhookLogRepositoryImpl,
                )

                parsed_log_uuid = _uuid.UUID(str(webhook_log_id))
                part_ids_list = (
                    order_detail.get("part_id")
                    if isinstance(order_detail, dict)
                    else None
                )
                if (
                    str(order_status or "").upper() in ("ON_HOLD", "CANCELLED")
                    or not part_ids_list
                ):
                    log_status = "ON_HOLD"

                async with async_session_factory() as session:
                    repo = WebhookLogRepositoryImpl(session)
                    await repo.update_order_detail_and_parts(
                        log_id=parsed_log_uuid,
                        order_detail=order_detail if isinstance(order_detail, dict) else None,
                        part_ids=part_ids_list,
                        status=log_status,
                    )
            except Exception as exc:
                logger.warning(
                    "Camunda: gagal update webhook_log dengan order_detail/parts: %s",
                    exc,
                )

        # ── AUTO SIGNAL (opsional — bagian `signal` di config.json) ────────
        # "Status ini di-hit signal apa" diatur `signal.by_status` /
        # `signal.by_marketplace`; pengiriman otomatis saat webhook masuk hanya
        # terjadi bila `signal.send_on_webhook=true`. Dijalankan SEBELUM aturan
        # start event dievaluasi supaya status yang hanya perlu signal (tanpa
        # start proses) tetap terkirim. Gate status start event tetap berlaku
        # (status yang ditolak gate tidak sampai ke sini).
        signal_results: List[CamundaTriggerResult] = []
        if signal_send_on_webhook(signal_config()):
            signal_results.append(
                await self.send_signal(
                    marketplace=marketplace,
                    order_status=order_status,
                    body=body,
                    headers=headers,
                    signal_code=signal_code,
                    order_detail=order_detail,
                    raw_body=raw_body,
                    dry_run=dry_run,
                    source="webhook",
                )
            )

        plans = self._resolve_plans(ctx)
        if not plans:
            reason = self._skip_reason(ctx)
            result = CamundaTriggerResult(
                ok=True,
                request_type="skip",
                skipped_reason=reason,
                marketplace=marketplace,
                signal_code=signal_code,
            )
            logger.info(
                "Camunda: webhook marketplace=%s signal=%s dilewati (%s)",
                marketplace, signal_code, reason,
            )
            append_camunda_log(self._log_entry(result, raw_body, dry_run, order_detail))
            return signal_results + [result]

        results: List[CamundaTriggerResult] = signal_results
        for plan in plans:
            plan_res = await self._execute_plan(
                plan, marketplace, signal_code, raw_body, dry_run, order_detail
            )
            results.append(plan_res)
            # Catat instance ID Camunda ke webhook_log jika tersedia
            if parsed_log_uuid and plan_res.ok and isinstance(plan_res.response, dict):
                proc_id = plan_res.response.get("id")
                if proc_id:
                    try:
                        async with async_session_factory() as session:
                            repo = WebhookLogRepositoryImpl(session)
                            await repo.update_status(
                                log_id=parsed_log_uuid,
                                status="PENDING" if log_status != "ON_HOLD" else "ON_HOLD",
                                service_task_id=str(proc_id),
                            )
                    except Exception as exc:
                        logger.warning(
                            "Camunda: gagal simpan service_task_id ke webhook_log: %s",
                            exc,
                        )

        return results

    async def trigger(
        self,
        marketplace: str,
        signal_code: Any,
        body: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]] = None,
        raw_body: str = "",
        dry_run: bool = False,
        order_detail: Optional[Dict[str, Any]] = None,
        webhook_log_id: Optional[str] = None,
    ) -> CamundaTriggerResult:
        """Versi ringkas: hasil rule PERTAMA yang dijalankan (kompatibilitas).

        Dengan ``signal.send_on_webhook=true``, hasil auto-signal ikut masuk
        daftar (urutan depan) — supaya pemanggil lama tetap menerima HASIL
        ATURAN start/message, hasil non-skip diprioritaskan; b semua skip →
        skip pertama (mis. ``camunda_disabled`` / ``order_status_not_allowed``).
        """
        results = await self.trigger_all(
            marketplace=marketplace,
            signal_code=signal_code,
            body=body,
            headers=headers,
            raw_body=raw_body,
            dry_run=dry_run,
            order_detail=order_detail,
            webhook_log_id=webhook_log_id,
        )
        for result in results:
            if result.request_type != "skip":
                return result
        return results[0]


    async def _effective_signal_status(
        self,
        section: Dict[str, Any],
        marketplace: str,
        webhook_status: Optional[str],
        body: Optional[Dict[str, Any]],
        order_detail: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[str], str, Dict[str, Any]]:
        """Status EFEKTIF untuk memilih signal (bagian `signal.status_fallback`).

        Status dari payload WEBHOOK selalu dicoba lebih dulu. Bila status itu
        TIDAK punya mapping di `by_status`/`by_marketplace` — mis. `UNPAID` yang
        memang sengaja tidak dipetakan karena hanya memicu START — maka status
        dari DETAIL ORDER APISIX (`order.order_status` → variabel
        `orderDetailStatus`) dipakai sebagai gantinya.

        Alasan: webhook order-status (Shopee `code=3`) bisa datang saat order
        masih `UNPAID`, sementara pembacaan gateway (dilakukan beberapa saat
        kemudian di request yang sama) sudah melihat `READY_TO_SHIP`. Tanpa
        fallback, kasus itu berakhir `skipped_reason: signal_not_configured` —
        signal yang seharusnya dikirim hilang, padahal statusnya sudah diketahui.

        Aman secara default: bila status webhook SUDAH punya mapping, tidak ada
        panggilan APISIX tambahan dan hasilnya identik dengan perilaku lama.

        @return ``(status, sumber, order_detail)`` dengan ``sumber`` =
                "webhook" | "order_detail" | "" (tidak ada status).
                ``order_detail`` dikembalikan supaya pemanggil memakai hasil
                fetch yang barusan dilakukan (blok `order_detail` di log terisi).
        """
        detail = order_detail if isinstance(order_detail, dict) else {}
        webhook_source = "webhook" if webhook_status else ""

        # Status webhook sudah dipetakan (atau signal dimatikan) → perilaku lama.
        if webhook_status and resolve_signal_name(section, marketplace, webhook_status):
            return webhook_status, webhook_source, detail
        fallback = status_fallback_section(section)
        if fallback.get("enabled") is not True or not signal_is_enabled(section):
            return webhook_status, webhook_source, detail

        # Pemanggil belum menyuplai detail order (mis. endpoint manual
        # POST /camunda/signal atau topic `send-signal`) → ambil sendiri.
        # FAIL-OPEN: gagal fetch → tetap pakai status webhook apa adanya.
        if not detail:
            try:
                detail = await self._fetch_order_detail(marketplace, body)
            except Exception as exc:  # noqa: BLE001 — fallback tidak boleh crash
                logger.warning(
                    "Camunda: status_fallback gagal mengambil detail order: %s: %s",
                    type(exc).__name__, exc,
                )
                detail = {}

        if not isinstance(detail, dict):
            detail = {}
        detail_status = str(detail.get("order_status") or "").strip()
        if not detail_status:
            return webhook_status, webhook_source, detail
        if detail_status.upper() == str(webhook_status or "").strip().upper():
            return webhook_status, webhook_source, detail
        if detail_status.upper() in status_fallback_skip_statuses(section):
            logger.info(
                "Camunda: status DETAIL ORDER %s ada di signal.status_fallback."
                "skip_statuses — TIDAK dipakai sebagai status efektif "
                "(status webhook=%s)",
                detail_status, webhook_status or "(kosong)",
            )
            return webhook_status, webhook_source, detail
        if not resolve_signal_name(section, marketplace, detail_status):
            return webhook_status, webhook_source, detail

        logger.info(
            "Camunda: status webhook %s tidak punya signal → status DETAIL ORDER "
            "%s dipakai sebagai status efektif (marketplace=%s)",
            webhook_status or "(kosong)", detail_status, marketplace or "-",
        )
        return detail_status, "order_detail", detail

    async def send_signal(
        self,
        signal_name: Optional[str] = None,
        marketplace: str = "",
        order_status: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        variable_types: Optional[Dict[str, str]] = None,
        execution_id: Optional[str] = None,
        business_key: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        signal_code: Any = None,
        order_detail: Optional[Dict[str, Any]] = None,
        raw_body: str = "",
        dry_run: bool = False,
        config: Optional[Dict[str, Any]] = None,
        source: str = "manual",
    ) -> CamundaTriggerResult:
        """HIT SIGNAL ke Camunda berdasarkan STATUS ORDER.

        Nama sinyal diambil berurutan dari: argumen ``signal_name`` →
        ``signal.by_marketplace[<marketplace>][<STATUS>]`` →
        ``signal.by_status[<STATUS>]`` → ``signal.default_signal`` (semuanya di
        camunda/config.json, hot reload). ``order_status`` None → dibaca dari
        payload webhook (``data.status`` Shopee / ``data.order_status`` TikTok).

        Bila status itu TIDAK punya mapping dan ``signal.status_fallback.enabled``
        true, status dari DETAIL ORDER APISIX dipakai sebagai status efektif —
        lihat :meth:`_effective_signal_status`. Nama eksplisit (``signal_name``)
        tidak pernah ditimpa fallback ini.

        Variabel: mapping ``signal.variables`` (sintaks sama dengan aturan) lalu
        ditimpa argumen ``variables``. Variabel ``signalName`` selalu disertakan.

        Catatan Camunda 7: REST signal TIDAK punya field ``businessKey`` — nilai
        ``signal.business_key`` / argumen ``business_key`` dikirim sebagai
        VARIABEL ``businessKey`` supaya proses tetap bisa mengaitkannya.
        """
        section = signal_config(config)
        marketplace = str(marketplace or "").strip().lower()
        webhook_status = (
            str(order_status or extract_webhook_order_status(body) or "").strip()
            or None
        )

        # ── STATUS EFEKTIF (signal.status_fallback) ────────────────────────
        # Webhook menang; status DETAIL ORDER hanya dipakai bila status webhook
        # tidak punya mapping signal. `signal_name` eksplisit → fallback dilewati
        # (nama sudah ditentukan pemanggil, tidak perlu fetch apa pun).
        # `order_detail` bisa ikut terisi hasil fetch di sini (dipakai log).
        status = webhook_status
        status_source = "webhook" if webhook_status else ""
        if not str(signal_name or "").strip():
            status, status_source, order_detail = await self._effective_signal_status(
                section, marketplace, webhook_status, body, order_detail
            )

        ctx = self._build_context(
            marketplace, signal_code, body, headers, order_status=status
        )
        if isinstance(order_detail, dict) and order_detail:
            ctx["order"] = order_detail

        def _skip(reason: str) -> CamundaTriggerResult:
            """Hasil "tidak dikirim" — bukan error (ok=True) + tetap dicatat."""
            skipped = CamundaTriggerResult(
                ok=True,
                request_type="skip",
                skipped_reason=reason,
                marketplace=marketplace or None,
                signal_code=signal_code,
                signal_name=str(signal_name or "").strip() or None,
                execution_id=execution_id,
            )
            append_camunda_log(
                self._log_entry(
                    skipped, raw_body, dry_run, order_detail, event="signal"
                )
            )
            return skipped

        if not self.is_enabled():
            logger.info(
                "Camunda: signal dilewati (CAMUNDA_ENABLED=false) — marketplace=%s status=%s",
                marketplace or "-", status or "-",
            )
            return _skip("camunda_disabled")

        resolved_name = str(signal_name or "").strip()
        if not resolved_name:
            resolved_name, reason = resolve_signal_target(section, marketplace, status)
            if not resolved_name:
                logger.info(
                    "Camunda: signal TIDAK dikirim (%s) untuk status=%s marketplace=%s — "
                    "isi peta `signal.by_status` / `signal.by_marketplace` di camunda/config.json",
                    reason, status or "-", marketplace or "-",
                )
                return _skip(reason)

        resolved_variables = resolve_variables(section.get("variables") or {}, {}, ctx)
        resolved_variables.update(variables or {})
        if status:
            resolved_variables.setdefault("signalOrderStatus", status)
        if marketplace:
            resolved_variables.setdefault("signalMarketplace", marketplace)

        # Nama signal boleh memakai placeholder dari VARIABEL signal di atas
        # (mis. by_status "READY_TO_SHIP": "readyToShip${invoice}") — sintaks
        # ${invoice} ATAU {{invoice}}, lihat signals.render_signal_name.
        rendered_name = render_signal_name(resolved_name, resolved_variables)
        if not str(rendered_name or "").strip():
            logger.warning(
                "Camunda: nama signal '%s' jadi KOSONG setelah render variabel "
                "(status=%s marketplace=%s) — signal TIDAK dikirim",
                resolved_name, status or "-", marketplace or "-",
            )
            return _skip("signal_not_configured")
        resolved_name = str(rendered_name)
        resolved_variables.setdefault("signalName", resolved_name)
        # Asal status yang dipakai memilih signal ("webhook" | "order_detail") —
        # supaya di Cockpit kelihatan kenapa mis. signal `readyToShip…` dikirim
        # untuk webhook yang statusnya `UNPAID`.
        if status_source:
            resolved_variables.setdefault("signalStatusSource", status_source)

        resolved_types = resolve_variable_types(
            {}, {"variable_types": section.get("variable_types") or {}}
        )
        resolved_types.update(variable_types or {})

        # business key → variabel `businessKey` (lihat docstring).
        bk_template = business_key or section.get("business_key")
        business_key_value: Optional[str] = None
        if bk_template:
            bk_ctx = dict(ctx)
            bk_ctx["variables"] = resolved_variables
            rendered = render_template(str(bk_template), bk_ctx)
            if rendered not in (None, ""):
                business_key_value = str(rendered)
                resolved_variables.setdefault("businessKey", business_key_value)

        target_execution = str(
            execution_id or section.get("execution_id") or ""
        ).strip() or None

        # ── KORELASI signal → SATU execution (opsional, Camunda 7) ─────────
        # Signal Camunda itu BROADCAST: bila tidak ada instance yang sedang
        # menunggu, signal hilang tanpa error (HTTP 204). Bila
        # `signal.execution_lookup.enabled=true`, cari LEBIH DULU execution yang
        # benar-benar menunggu signal ini pada process instance dengan business
        # key cocok (LIKE) → kirim ke /execution/{id}/signal, bukan broadcast.
        if target_execution is None and not dry_run:
            lookup = execution_lookup_section(section)
            if lookup.get("enabled") is True:
                lookup_client = self._client_factory()
                try:
                    if lookup_client.version != "7":
                        logger.info(
                            "Camunda: execution_lookup dilewati — hanya didukung "
                            "Camunda 7 (signal per-eksekusi)"
                        )
                    else:
                        bk_like: Optional[str] = None
                        bk_template = str(
                            lookup.get("business_key_template") or ""
                        ).strip()
                        if bk_template:
                            bk_ctx = dict(ctx)
                            bk_ctx["variables"] = resolved_variables
                            rendered_bk = render_template(bk_template, bk_ctx)
                            bk_like = str(rendered_bk or "").strip() or None
                        match = await lookup_client.match_signal_subscription(
                            resolved_name, business_key_like=bk_like
                        )
                        target_execution = match.execution_id
                        if target_execution:
                            logger.info(
                                "Camunda: signal '%s' → execution %s "
                                "(business key LIKE %s, subscriptions=%d)",
                                resolved_name, target_execution, bk_like or "-",
                                match.subscriptions,
                            )
                        elif match.found:
                            # Subscriber ADA tapi tidak layak di-target per-execution
                            # (boundary event: subscription di scope execution tanpa
                            # activityId → Camunda menolak /execution/{id}/signal).
                            # Broadcast tetap aman selama nama signal per-order.
                            logger.info(
                                "Camunda: signal '%s' → %d subscriber "
                                "(business key LIKE %s) tidak bisa di-target "
                                "per-execution → kirim broadcast",
                                resolved_name, match.subscriptions, bk_like or "-",
                            )
                        else:
                            fallback = str(
                                lookup.get("fallback") or "broadcast"
                            ).strip().lower()
                            logger.warning(
                                "Camunda: TIDAK ada execution yang menunggu signal "
                                "'%s' (business key LIKE %s) — %s",
                                resolved_name, bk_like or "-",
                                "signal TIDAK dikirim (fallback=skip)"
                                if fallback == "skip"
                                else "dikirim sebagai BROADCAST (fallback=broadcast)",
                            )
                            if fallback == "skip":
                                return _skip("signal_no_subscriber")
                except Exception as exc:  # noqa: BLE001 — lookup tidak boleh menggagalkan signal
                    logger.warning(
                        "Camunda: lookup execution signal gagal: %s: %s",
                        type(exc).__name__, exc,
                    )
                    target_execution = None
                finally:
                    try:
                        await lookup_client.aclose()
                    except Exception:  # noqa: BLE001
                        logger.debug("Camunda: gagal menutup client lookup", exc_info=True)

        tenant_id = str(section.get("tenant_id") or settings.camunda_tenant_id or "").strip() or None
        extra_body = (
            section.get("extra_body")
            if isinstance(section.get("extra_body"), dict)
            else None
        )
        rule_id = str(section.get("id") or f"signal:{source}")

        if dry_run:
            logger.info(
                "Camunda: DRY-RUN signal '%s' (status=%s marketplace=%s) — variabel=%s",
                resolved_name, status or "-", marketplace or "-",
                sorted(resolved_variables.keys()),
            )
            result = CamundaTriggerResult(
                ok=True,
                request_type="signal",
                business_key=business_key_value,
                variables=resolved_variables,
                skipped_reason="dry_run",
                rule_id=rule_id,
                marketplace=marketplace or None,
                signal_code=signal_code,
                signal_name=resolved_name,
                execution_id=target_execution,
            )
            append_camunda_log(
                self._log_entry(result, raw_body, dry_run, order_detail, event="signal")
            )
            return result

        client = self._client_factory()
        try:
            result = await client.send_signal(
                signal_name=resolved_name,
                variables=resolved_variables,
                variable_types=resolved_types,
                tenant_id=tenant_id,
                execution_id=target_execution,
                extra_body=extra_body,
                rule_id=rule_id,
                marketplace=marketplace or None,
                signal_code=signal_code,
            )
        except Exception as exc:  # noqa: BLE001 — signal tidak boleh crash caller
            logger.exception("Camunda: signal gagal tak terduga: %s", exc)
            result = CamundaTriggerResult(
                ok=False,
                request_type="signal",
                variables=resolved_variables,
                error=f"{type(exc).__name__}: {exc}",
                rule_id=rule_id,
                marketplace=marketplace or None,
                signal_code=signal_code,
                signal_name=resolved_name,
                execution_id=target_execution,
            )
        finally:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Camunda: gagal menutup HTTP client", exc_info=True)

        if not result.business_key:
            result.business_key = business_key_value
        append_camunda_log(
            self._log_entry(result, raw_body, dry_run, order_detail, event="signal")
        )
        return result

    @staticmethod
    def _blocked_by_order_status(
        rules: List[Dict[str, Any]], ctx: Dict[str, Any]
    ) -> bool:
        """True bila ada aturan yang cocok SELAIN karena gate status order.

        Dipakai hanya untuk memberi alasan skip yang spesifik; pencocokan
        sebenarnya tetap dilakukan `rule_matches` di `_resolve_plans`.
        """
        order_status = (ctx.get("meta") or {}).get("order_status")
        for rule in rules:
            if not isinstance(rule, dict) or rule.get("enabled") is False:
                continue
            if order_status_matches(rule, order_status):
                continue
            without_status = {
                key: value
                for key, value in rule.items()
                if key not in ORDER_STATUS_RULE_KEYS
            }
            if rule_matches(without_status, ctx):
                return True
        return False

    @classmethod
    def _skip_reason(cls, ctx: Optional[Dict[str, Any]] = None) -> str:
        """Alasan webhook tidak diteruskan (untuk log & troubleshooting).

        `ctx` opsional dipakai untuk membedakan "tidak ada aturan cocok" dari
        "status order ditolak gate aturan" (`order_status` / `exclude_order_status`
        di camunda/config.json).
        """
        config = load_rules()
        if config.get("enabled") is False:
            return "config_disabled"

        rules = config.get("rules") or []
        if rules:
            if ctx is not None and cls._blocked_by_order_status(rules, ctx):
                return "order_status_not_allowed"
            return "no_matching_rule"

        flat = cls._flat_rule(config)
        if flat is None:
            return "no_target_configured"
        # Aturan bertipe signal: alasan spesifik supaya operator tahu bahwa
        # status order-nya belum dipetakan (bagian `signal` di config.json).
        if str(flat.get("type") or "").strip().lower() == "signal" and not str(
            flat.get("signal_name") or ""
        ).strip():
            section = signal_config(config)
            if not signal_is_enabled(section):
                return "signal_disabled"
            mapped = ctx is not None and resolve_signal_name(
                section,
                (ctx.get("meta") or {}).get("marketplace"),
                (ctx.get("meta") or {}).get("order_status"),
            )
            if not mapped:
                return "signal_not_configured"
        if ctx is not None and cls._blocked_by_order_status([flat], ctx):
            return "order_status_not_allowed"
        return "no_matching_rule"

    async def _execute_plan(
        self,
        plan: CamundaPlan,
        marketplace: str,
        signal_code: Any,
        raw_body: str,
        dry_run: bool,
        order_detail: Optional[Dict[str, Any]] = None,
    ) -> CamundaTriggerResult:
        """Jalankan satu rencana: dry-run, atau kirim ke Camunda + log."""
        if dry_run:
            logger.info(
                "Camunda: DRY-RUN (rule=%s) — variabel=%s",
                plan.rule_id, sorted(plan.variables.keys()),
            )
            result = CamundaTriggerResult(
                ok=True,
                request_type=plan.request_type,
                process_key=plan.process_definition_key,
                message_name=plan.message_name,
                business_key=plan.business_key,
                variables=plan.variables,
                skipped_reason="dry_run",
                rule_id=plan.rule_id,
                marketplace=marketplace,
                signal_code=signal_code,
                signal_name=plan.signal_name,
                execution_id=plan.execution_id,
            )
            append_camunda_log(self._log_entry(result, raw_body, dry_run, order_detail))
            return result

        client = self._client_factory()
        try:
            result = await client.trigger(
                request_type=plan.request_type,
                variables=plan.variables,
                variable_types=plan.variable_types,
                process_definition_key=plan.process_definition_key,
                process_definition_id=plan.process_definition_id,
                message_name=plan.message_name,
                business_key=plan.business_key,
                tenant_id=plan.tenant_id,
                extra_body=plan.extra_body,
                rule_id=plan.rule_id,
                marketplace=marketplace,
                signal_code=signal_code,
                signal_name=plan.signal_name,
                execution_id=plan.execution_id,
            )
        except Exception as exc:  # noqa: BLE001 — webhook tidak boleh crash
            logger.exception("Camunda: trigger gagal tak terduga: %s", exc)
            result = CamundaTriggerResult(
                ok=False,
                request_type=plan.request_type,
                process_key=plan.process_definition_key,
                message_name=plan.message_name,
                business_key=plan.business_key,
                variables=plan.variables,
                error=f"{type(exc).__name__}: {exc}",
                rule_id=plan.rule_id,
                marketplace=marketplace,
                signal_code=signal_code,
                signal_name=plan.signal_name,
                execution_id=plan.execution_id,
            )
        finally:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Camunda: gagal menutup HTTP client", exc_info=True)

        append_camunda_log(self._log_entry(result, raw_body, dry_run, order_detail))
        return result

    # ------------------------------------------------------------------
    # Log entry
    # ------------------------------------------------------------------

    @staticmethod
    def _order_log_entry(order_detail: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Ringkasan detail order untuk log (tanpa mengulang seluruh line item)."""
        if not isinstance(order_detail, dict) or not order_detail:
            return None
        entry: Dict[str, Any] = {
            "ok": order_detail.get("ok"),
            "source": order_detail.get("source"),
            "order_id": order_detail.get("order_id"),
            "order_status": order_detail.get("order_status"),
            "item_count": order_detail.get("item_count"),
            "total_quantity": order_detail.get("total_quantity"),
            "products_summary": order_detail.get("products_summary"),
            "error": order_detail.get("error"),
        }
        # Total reserved stock per produk (bila fetch /order?product_id= sukses).
        if "total_reserved" in order_detail:
            entry["total_reserved"] = order_detail.get("total_reserved")
            entry["reserved_by_product"] = order_detail.get("reserved_by_product")
        return entry

    @classmethod
    def _log_entry(
        cls,
        result: CamundaTriggerResult,
        raw_body: str,
        dry_run: bool,
        order_detail: Optional[Dict[str, Any]] = None,
        event: Optional[str] = None,
    ) -> Dict[str, Any]:
        entry: Dict[str, Any] = {
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "dry_run": dry_run,
        }
        # Jenis event: "start_process"/"message" (default) atau "signal" —
        # supaya baris log signal tidak tertukar dengan start event.
        if event:
            entry["event"] = event
        entry.update(result.to_dict())
        # Payload mentah disimpan juga supaya hasil trigger bisa diaudit
        # berdampingan dengan data yang memicunya.
        entry["raw_body"] = raw_body
        order_log = cls._order_log_entry(order_detail)
        if order_log is not None:
            entry["order_detail"] = order_log
        return entry


# Singleton — dipakai router webhook.
camunda_webhook_trigger = CamundaWebhookTrigger()
