"""
HANDLER bawaan untuk EXTERNAL SERVICE TASK (camunda/external_task.py).

Satu handler = satu pekerjaan yang bisa diminta BPMN lewat external service
task. Topic di camunda/config.json menunjuk handler dengan namanya:

    {"topic": "order-detail", "handler": "order_detail", ...}

Handler boleh juga menunjuk kode sendiri: ``"handler": "paket.modul:fungsi"``
(fungsi harus ``async def ftask(task)`` dan mengembalikan
``ExternalTaskResult`` / ``dict`` variabel / ``None``).

Handler bawaan
--------------
  order_detail        Ambil DETAIL ORDER dari APISIX (produk + jumlah) →
                      variabel orderItems/orderProducts/orderTotalQuantity/…
  send_signal         "Hit signal" ke Camunda berdasarkan STATUS ORDER
                      (pemetaan di config.json bagian `signal`).
  stock_sync_part     Jalankan stock sync HANYA untuk part_id yang diberikan
                      (`POST /api/v2/stock-sync/run-part` versi internal).
  stock_sync_global   Jalankan satu cycle pipeline stok GLOBAL.
  service_webhook_test HIT backend sendiri (POST /api/v2/stock-sync/run-part)
                      → jalankan pipeline dari awal untuk SETIAP partId;
                      COMPLETED hanya bila sukses (404/503/5xx/jaringan →
                      FAILURE + retry, bukan BPMN error — lihat catatan
                      di handler-nya).

Variabel INPUT yang dibaca tiap handler:

  order_detail       marketplace, orderId(/_id/ordersn), shopId/shop_uuid
  send_signal        marketplace, orderStatus/status, signalName (opsional),
                     executionId (opsional), signalVariables (opsional dict)
  stock_sync_part    partId/part_id (wajib; boleh ARRAY atau skalar)
  stock_sync_global  statusFilter (opsional, default ALL)
  service_webhook_test partId/part_id (wajib; ARRAY angka > 0 — satu product
                     marketplace bisa memetakan ke BANYAK part_id)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from infrastructure.camunda.external_task import ExternalTask, ExternalTaskResult

logger = logging.getLogger(__name__)

# Variabel yang HANYA dipakai sebagai kontrol handler (tidak diteruskan apa adanya
# sebagai variabel sinyal).
CONTROL_VARIABLES = {
    "signalName",
    "executionId",
    "signalVariables",
    "forwardVariables",
    "handler",
    "topic",
}


def _marketplace(task: ExternalTask) -> str:
    """Marketplace dari variabel task (case-insensitive, default kosong)."""
    return task.text("marketplace", "marketplaceCode", "marketplace_code").lower()


# ---------------------------------------------------------------------------
# 1. order_detail
# ---------------------------------------------------------------------------


async def handle_order_detail(task: ExternalTask) -> ExternalTaskResult:
    """Ambil detail order dari APISIX → variabel `order*` untuk proses Camunda."""
    from infrastructure.marketplace.apisix.client import ApisixClient
    from infrastructure.camunda.order_detail import (
        build_order_variables,
        normalize_order_detail,
        resolve_shop_uuid,
    )

    marketplace = _marketplace(task)
    order_id = task.text("orderId", "order_id", "ordersn", "orderSn")
    shop_uuid = task.text("shopId", "shop_id", "shop_uuid", "shopUuid")

    if not order_id:
        return ExternalTaskResult.bpmn_error_result(
            "ORDER_ID_MISSING",
            "variabel orderId wajib diisi untuk topic order-detail",
        )

    # resolve_shop_uuid: setting APISIX_<MP>_SHOP_UUID → payload (data.shop_id).
    body: Dict[str, Any] = {"data": {"order_id": order_id}}
    if shop_uuid:
        body["data"]["shop_id"] = shop_uuid
    shop_uuid = resolve_shop_uuid(marketplace, body)
    if not shop_uuid:
        return ExternalTaskResult.bpmn_error_result(
            "SHOP_UUID_MISSING",
            f"shop_uuid tidak diketahui untuk marketplace={marketplace or '-'} "
            "(isi variabel shopId atau APISIX_<MP>_SHOP_UUID)",
        )

    from core.config import settings

    client = ApisixClient()
    try:
        raw = await client.fetch_order_detail(
            marketplace=marketplace,
            shop_uuid=shop_uuid,
            order_id=order_id,
            translate=bool(settings.webhook_order_detail_translate),
            timeout=settings.webhook_order_detail_timeout,
        )
    finally:
        try:
            await client.aclose()
        except Exception:  # noqa: BLE001 — best-effort
            logger.debug("Camunda task: gagal menutup client APISIX", exc_info=True)

    detail = normalize_order_detail(
        marketplace,
        raw,
        order_id,
        shop_uuid,
        error=None if raw is not None else "fetch_failed",
    )
    if not detail.get("ok"):
        # Kegagalan teknis (gateway/marketplace) → FAILURE supaya engine retry,
        # bukan BPMN error (bukan kesalahan bisnis).
        return ExternalTaskResult.fail(
            f"gagal ambil detail order {order_id} (marketplace={marketplace or '-'})",
            error_details=str(detail.get("error")),
        )

    variables = build_order_variables(detail)
    variables["orderSource"] = detail.get("source")
    logger.info(
        "Camunda task order-detail: order=%s marketplace=%s item=%s unit=%s",
        order_id, marketplace or "-", detail.get("item_count"), detail.get("total_quantity"),
    )
    return ExternalTaskResult.ok(
        variables=variables,
        variable_types={"orderItems": "json", "orderProductNames": "json"},
    )


# ---------------------------------------------------------------------------
# 2. send_signal ("hit signal")
# ---------------------------------------------------------------------------


async def handle_send_signal(task: ExternalTask) -> ExternalTaskResult:
    """Kirim SIGNAL Camunda untuk status order task (peta di config.json)."""
    from infrastructure.camunda.service import camunda_webhook_trigger

    marketplace = _marketplace(task)
    status = task.text("orderStatus", "order_status", "status", "newStatus")
    order_id = task.text("orderId", "order_id", "ordersn")

    explicit_name = task.text("signalName", "signal_name")
    execution_id = task.text("executionId", "execution_id") or None

    extra: Dict[str, Any] = {}
    signal_variables = task.variable("signalVariables")
    # Variabel bertipe Json datang sebagai STRING (hasil unwrap fetchAndLock)
    # — parse dulu supaya isinya yang diteruskan, bukan string-nya.
    if isinstance(signal_variables, str):
        try:
            signal_variables = json.loads(signal_variables)
        except ValueError:
            logger.warning(
                "Camunda task send-signal: signalVariables bukan JSON valid — diabaikan"
            )
            signal_variables = None
    if isinstance(signal_variables, dict):
        extra.update(signal_variables)
    if task.variable("forwardVariables") is not False:
        # Default: teruskan variabel task (selain kontrol) — BPMN yang memutuskan
        # variabel apa yang dikirim saat membuat task.
        for name, value in (task.variables or {}).items():
            if name in CONTROL_VARIABLES:
                continue
            extra.setdefault(name, value)

    body: Dict[str, Any] = {"data": {}}
    if order_id:
        body["data"]["order_id"] = order_id
    if status:
        body["data"]["order_status"] = status

    result = await camunda_webhook_trigger.send_signal(
        signal_name=explicit_name or None,
        marketplace=marketplace,
        order_status=status or None,
        variables=extra,
        execution_id=execution_id,
        body=body,
        raw_body=str(task.raw.get("variables") or {}),
    )

    skipped = result.skipped_reason
    variables = {
        "signalName": result.signal_name or "",
        "signalSent": bool(result.ok and not skipped),
        "signalStatus": result.status_code,
        "signalSkippedReason": skipped or "",
        "signalError": result.error or "",
    }
    if skipped == "signal_not_configured":
        # Status tidak dipetakan → bukan error teknis; BPMN boleh lanjut tanpa
        # sinyal (boundary/decision), jadi dilaporkan sebagai variabel.
        logger.info(
            "Camunda task send-signal: status=%s marketplace=%s TIDAK dipetakan "
            "ke signal manapun (bagian `signal` di config.json)",
            status or "-", marketplace or "-",
        )
        return ExternalTaskResult.ok(variables=variables)
    if not result.ok:
        return ExternalTaskResult.fail(
            f"gagal kirim signal '{result.signal_name or '-'}'",
            error_details=str(result.error),
        )
    return ExternalTaskResult.ok(variables=variables)


# ---------------------------------------------------------------------------
# 3. stock_sync_part
# ---------------------------------------------------------------------------


async def handle_stock_sync_part(task: ExternalTask) -> ExternalTaskResult:
    """Jalankan stock sync HANYA untuk part_id yang diberikan (scope sku_mapping).

    ``partId`` boleh ARRAY (satu product marketplace bisa punya banyak part_id)
    maupun skalar — keduanya diterima lewat :func:`_extract_part_ids`.
    """
    raw_part = task.variable("partId", "part_id", "partID")
    part_ids = _extract_part_ids(raw_part)
    if not part_ids:
        return ExternalTaskResult.bpmn_error_result(
            "PART_ID_MISSING",
            f"variabel partId wajib berupa angka/array angka > 0 "
            f"(diterima: {raw_part!r})",
        )

    from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine

    pipeline = StockSyncPipelineEngine()
    try:
        result = await pipeline.run_once_for_parts(part_ids)
    except ValueError as exc:
        return ExternalTaskResult.bpmn_error_result("PART_ID_UNMAPPED", str(exc))
    finally:
        try:
            await pipeline.aclose()
        except Exception:  # noqa: BLE001 — best-effort
            logger.debug("Camunda task: gagal menutup pipeline", exc_info=True)

    logger.info(
        "Camunda task stock-sync-part: part_ids=%s report=%d queued=%d pushed=%d "
        "logged=%d failures=%d",
        part_ids, result.total_report_rows, result.total_queued,
        result.total_pushed, result.total_logged, len(result.failures),
    )
    return ExternalTaskResult.ok(
        variables={
            "partId": part_ids,
            "partIds": part_ids,
            "syncReportRows": result.total_report_rows,
            "syncQueued": result.total_queued,
            "syncPushed": result.total_pushed,
            "syncLogged": result.total_logged,
            "syncSkippedUnchanged": result.total_skipped_unchanged,
            "syncSkippedPending": result.total_skipped_pending,
            "syncFailures": list(result.failures),
            "syncPushEnabled": bool(result.push_enabled),
            "syncRunMode": "MANUAL_PART",
        },
        # partId WAJIB tetap dikirim balik sebagai TIPE STRING (teks JSON array).
        # Tanpa tipe eksplisit, list di-inferensi Camunda sebagai Json (Spin) →
        # variabel termutasi dan task berikutnya kena PART_ID_MISSING.
        variable_types={"partId": "String"},
    )


async def handle_webhook_part_batch(task: ExternalTask) -> ExternalTaskResult:
    """Drain webhook_log and process unique part IDs in one pipeline batch."""
    from infrastructure.db.database import async_session_factory
    from infrastructure.repositories.webhook_log_repository_impl import (
        WebhookLogRepositoryImpl,
    )

    batch_size = _as_int(
        task.variable("webhookBatchSize", "webhook_batch_size"), default=10
    )
    batch_size = max(1, min(batch_size, 100))
    async with async_session_factory() as session:
        repo = WebhookLogRepositoryImpl(session)
        rows = await repo.claim_pending_batch(batch_size, service_task_id=task.id)

    if not rows:
        return ExternalTaskResult.ok(
            variables={
                "webhookBatchStatus": "IDLE",
                "webhookBatchSize": 0,
                "webhookPartIds": [],
            }
        )

    part_ids: List[int] = []
    log_ids = []
    for row in rows:
        log_ids.append(row.id)
        for part_id in _extract_part_ids(row.part_ids):
            if part_id not in part_ids:
                part_ids.append(part_id)

    if not part_ids:
        async with async_session_factory() as session:
            await WebhookLogRepositoryImpl(session).update_batch_status(
                log_ids,
                "ON_HOLD",
                error_message="webhook tidak memiliki part_id",
                processed_at=datetime.now(timezone.utc),
                service_task_id=task.id,
            )
        return ExternalTaskResult.ok(
            variables={
                "webhookBatchStatus": "ON_HOLD",
                "webhookBatchSize": len(rows),
                "webhookPartIds": [],
            }
        )

    from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine

    pipeline = StockSyncPipelineEngine()
    try:
        result = await pipeline.run_once_for_parts(part_ids)
        failures = list(getattr(result, "failures", []) or [])
        status = "ON_HOLD" if failures else "SUCCESS"
    except Exception as exc:  # noqa: BLE001 - claimed rows must be auditable
        failures = [f"{type(exc).__name__}: {exc}"]
        status = "ON_HOLD"
    finally:
        try:
            await pipeline.aclose()
        except Exception:  # noqa: BLE001 - best effort cleanup
            logger.debug("Webhook batch: gagal menutup pipeline", exc_info=True)

    async with async_session_factory() as session:
        await WebhookLogRepositoryImpl(session).update_batch_status(
            log_ids,
            status,
            error_message="; ".join(str(item) for item in failures) if failures else None,
            processed_at=datetime.now(timezone.utc),
            service_task_id=task.id,
        )

    return ExternalTaskResult.ok(
        variables={
            "webhookBatchStatus": status,
            "webhookBatchSize": len(rows),
            "webhookPartIds": part_ids,
            "webhookBatchFailures": failures,
            "syncRunMode": "WEBHOOK_PART_BATCH",
        },
        variable_types={"webhookPartIds": "String"},
    )


# ---------------------------------------------------------------------------
# 4. stock_sync_global
# ---------------------------------------------------------------------------


async def handle_stock_sync_global(task: ExternalTask) -> ExternalTaskResult:
    """Jalankan SATU cycle pipeline stok GLOBAL (semua shop ACTIVE)."""
    status_filter = task.text("statusFilter", "status_filter") or "ALL"

    from application.usecases.stock_sync_pipeline import StockSyncPipelineEngine

    pipeline = StockSyncPipelineEngine()
    try:
        result = await pipeline.run_once(cycle_index=1, status_filter=status_filter)
    finally:
        try:
            await pipeline.aclose()
        except Exception:  # noqa: BLE001 — best-effort
            logger.debug("Camunda task: gagal menutup pipeline", exc_info=True)

    logger.info(
        "Camunda task stock-sync-global: filter=%s parts=%d pushed=%d logged=%d "
        "failures=%d",
        status_filter, result.total_parts_processed, result.total_pushed,
        result.total_logged, len(result.failures),
    )
    return ExternalTaskResult.ok(
        variables={
            "syncStatusFilter": status_filter,
            "syncPartsProcessed": result.total_parts_processed,
            "syncReportRows": result.total_report_rows,
            "syncQueued": result.total_queued,
            "syncPushed": result.total_pushed,
            "syncLogged": result.total_logged,
            "syncFailures": list(result.failures),
            "syncPushEnabled": bool(result.push_enabled),
            "syncRunMode": "POLLING_1H",
        }
    )


# ---------------------------------------------------------------------------
# 5. service_webhook_test (topic "serviceWebhookTest" di BPMN)
# ---------------------------------------------------------------------------


def _as_int(value: Any, default: int = 0) -> int:
    """Angka dari respons JSON (bisa int/str/None) → int."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


async def _mark_webhook_log(
    log_id: Any,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Persist terminal status for the webhook that created this task."""
    if not log_id:
        return
    try:
        import uuid

        from infrastructure.db.database import async_session_factory
        from infrastructure.repositories.webhook_log_repository_impl import (
            WebhookLogRepositoryImpl,
        )

        async with async_session_factory() as session:
            await WebhookLogRepositoryImpl(session).update_status(
                uuid.UUID(str(log_id)),
                status,
                error_message=error_message,
                processed_at=datetime.now(timezone.utc),
            )
    except Exception:  # noqa: BLE001 - status update must not fail the task
        logger.exception("Gagal menandai webhook_log id=%s sebagai %s", log_id, status)


async def _mark_webhook_logs(
    log_ids: Any,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    values = log_ids if isinstance(log_ids, (list, tuple, set)) else [log_ids]
    for log_id in values:
        await _mark_webhook_log(log_id, status, error_message)


def _extract_part_ids(value: Any) -> List[int]:
    """Normalisasi nilai variabel ``partId`` menjadi daftar int > 0.

    Satu product marketplace bisa memetakan ke BANYAK part_id, jadi ``partId``
    berbentuk ARRAY. Nilai yang diterima:
      • list/tuple/set int atau str (Camunda 8 / payload polos)
      • string JSON array, mis. ``"[3000100, 3000101]"`` (ini yang dikirim
        config.json — tipe String berisi teks JSON, lihat ``_comment_part``)
      • skalar int/str, mis. ``42`` / ``"42"`` (kompatibilitas BPMN lama)
      • dict bungkus ``{"value": …}`` — dipakai bila Camunda mengembalikan
        variabel typed yang tidak ter-unwrap otomatis.

    Nilai non-numerik, <= 0, dan duplikat dibuang (urut sesuai masukan).
    """
    if value is None:
        return []

    # Bungkus typed yang lolos unwrap: {"value": …} atau {"value": …, …}.
    # BUKAN dict deskriptor node JSON (nodeType/array/dataFormatName): itu
    # artefak tipe Json Camunda Spin yang isinya hilang — tidak bisa di-recover.
    if isinstance(value, dict):
        if "nodeType" in value or "dataFormatName" in value:
            logger.warning(
                "handle_service_webhook_test: partId datang sebagai deskriptor "
                "node JSON %r (tipe Json Camunda Spin tidak round-trip lewat "
                "fetchAndLock) — pastikan handler menulisnya kembali dgn "
                "variable_types={'partId': 'String'} dan topic memakai "
                "deserialize_values=false di camunda/config.json",
                value,
            )
            return []
        if "value" in value:
            return _extract_part_ids(value.get("value"))
        return []

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                value = json.loads(text)
            except (ValueError, TypeError):
                return []
        else:
            value = [text]
        if isinstance(value, dict):
            return _extract_part_ids(value)
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]

    part_ids: List[int] = []
    for item in items:
        try:
            parsed = int(str(item).strip())
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in part_ids:
            part_ids.append(parsed)
    return part_ids


async def _patch_reserved_stock_from_camunda(
    part_id: int,
    reserved_products_raw: Any,
) -> None:
    """Update ``total_reserved_stock`` di ``marketplace_variant`` sebelum pipeline.

    Pipeline MANUAL-PART melewati step 1 (GET products dari marketplace), sehingga
    kolom ``total_reserved_stock`` di DB bisa stale — formula membaca nilai lama
    dan menghasilkan ``target_stock == previous_stock`` → ``skipped_unchanged``
    → tidak ada push.

    Variabel Camunda ``reserved_products`` sekarang berbentuk NESTED
    (``{product_id: {"total_reserved": N, "variants": {...}}}``), tapi fungsi
    ini cukup memakai **KUNCI**-nya saja: ``product_id`` dipakai sebagai DAFTAR
    product yang perlu di-refresh. Nilai reserved PER-VARIANT ditarik dari
    APISIX ``/order?product_id=…`` (source of truth, sama seperti pipeline GLOBAL)
    dan ditulis ke variant yang ``external_variant_id``-nya cocok (variant yang
    tidak muncul & sweep tuntas → 0). Jadi bentuk lama ``{product_id: total}``
    maupun bentuk nested keduanya tetap kompatibel.

    Format ``reserved_products_raw``: JSON string atau dict — baik
    ``{product_id: total}`` (lama) maupun
    ``{product_id: {"total_reserved": …, "variants": …}}`` (baru).

    FAIL-OPEN: kegagalan apa pun hanya dicatat sebagai warning — pipeline tetap
    jalan dengan nilai DB yang ada (lebih baik push dengan angka kurang akurat
    daripada tidak push sama sekali).
    """
    import json as _json

    from sqlalchemy import select, update as sa_update

    from infrastructure.db.database import async_session_factory

    # ── Parse variabel Camunda (bisa datang sebagai string JSON atau dict) ──
    if isinstance(reserved_products_raw, str):
        try:
            reserved_map: dict = _json.loads(reserved_products_raw)
        except (ValueError, TypeError):
            logger.warning(
                "handle_service_webhook_test: reserved_products bukan JSON valid "
                "— skip patch reserved_stock (part_id=%d)",
                part_id,
            )
            return
    elif isinstance(reserved_products_raw, dict):
        reserved_map = reserved_products_raw
    else:
        return  # tipe tak terduga, lewati saja

    if not reserved_map:
        return

    from infrastructure.marketplace.apisix.client import ApisixClient
    from infrastructure.db.models import (
        MarketplaceModel,
        MarketplaceProductModel as _ProductModel,
        MarketplaceShopModel,
        MarketplaceVariantModel as _VariantModel,
    )
    from application.usecases.sync_engine import build_reserved_stock_map

    # product_id hanya dipakai sebagai daftar product yang di-refresh.
    product_ids = [str(k) for k in reserved_map.keys()]

    # ── 1. Petakan variant product tsb → shop + marketplace (native ids) ──
    try:
        async with async_session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        _VariantModel.marketplace_variant_id,
                        _VariantModel.external_variant_id,
                        _ProductModel.external_product_id,
                        _ProductModel.shop_id,
                        MarketplaceModel.code,
                    )
                    .join(
                        _ProductModel,
                        _VariantModel.marketplace_product_id
                        == _ProductModel.marketplace_product_id,
                    )
                    .join(
                        MarketplaceShopModel,
                        _ProductModel.shop_id
                        == MarketplaceShopModel.external_shop_id,
                    )
                    .join(
                        MarketplaceModel,
                        MarketplaceShopModel.marketplace_id == MarketplaceModel.id,
                    )
                    .where(_ProductModel.external_product_id.in_(product_ids))
                )
            ).all()
    except Exception as exc:  # noqa: BLE001 — jangan crash handler
        logger.warning(
            "handle_service_webhook_test: gagal membaca scope variant→product "
            "(part_id=%d): %s: %s",
            part_id,
            type(exc).__name__,
            exc,
        )
        return

    if not rows:
        logger.debug(
            "handle_service_webhook_test: tidak ada variant untuk "
            "product_ids=%s — patch dilewati",
            product_ids,
        )
        return

    # key: (marketplace_code_lower, external_shop_id, external_product_id)
    # value: list of (marketplace_variant_id, external_variant_id)
    groups: dict = {}
    for row in rows:
        internal_variant_id = str(
            getattr(row, "marketplace_variant_id", None) or ""
        ).strip()
        external_variant_id = str(
            getattr(row, "external_variant_id", None) or ""
        ).strip()
        external_product_id = str(
            getattr(row, "external_product_id", None) or ""
        ).strip()
        shop_external_id = str(getattr(row, "shop_id", None) or "").strip()
        marketplace_code = str(getattr(row, "code", None) or "").strip().lower()
        if not internal_variant_id or not external_product_id:
            continue
        if not shop_external_id or not marketplace_code:
            continue
        groups.setdefault(
            (marketplace_code, shop_external_id, external_product_id), []
        ).append((internal_variant_id, external_variant_id))

    # ── 2. Tarik reserved PER-VARIANT dari APISIX /order (filter product_id) ──
    updates: dict = {}
    apisix = ApisixClient()
    try:
        for (
            marketplace_code,
            shop_external_id,
            external_product_id,
        ), members in groups.items():
            try:
                raw = await apisix.fetch_reserved_stock(
                    marketplace_code,
                    shop_external_id,
                    product_id=external_product_id,
                )
            except Exception as exc:  # noqa: BLE001 — fail-open per product
                logger.warning(
                    "handle_service_webhook_test: /order gagal "
                    "(product=%s, part_id=%d): %s — pakai nilai DB lama",
                    external_product_id,
                    part_id,
                    exc,
                )
                continue
            if not isinstance(raw, dict):
                continue
            sweep_complete = raw.get("sweep_complete", True) is not False
            reserved_map_variant = build_reserved_stock_map(raw)
            for internal_variant_id, external_variant_id in members:
                if external_variant_id and external_variant_id in reserved_map_variant:
                    updates[internal_variant_id] = int(
                        reserved_map_variant[external_variant_id]
                    )
                elif external_variant_id and sweep_complete:
                    # Tidak muncul di /order product ini & sweep tuntas → 0.
                    updates[internal_variant_id] = 0
    finally:
        try:
            await apisix.aclose()
        except Exception:  # noqa: BLE001 — best-effort
            logger.debug(
                "handle_service_webhook_test: gagal menutup ApisixClient",
                exc_info=True,
            )

    if not updates:
        logger.info(
            "handle_service_webhook_test: reserved per-variant tidak ada yang "
            "berubah (part_id=%d product_ids=%s)",
            part_id,
            product_ids,
        )
        return

    # ── 3. Tulis nilai PER-VARIANT ke marketplace_variant ──
    try:
        async with async_session_factory() as session:
            for internal_variant_id, reserved in updates.items():
                await session.execute(
                    sa_update(_VariantModel)
                    .where(
                        _VariantModel.marketplace_variant_id == internal_variant_id
                    )
                    .values(total_reserved_stock=int(reserved))
                )
            await session.commit()
        logger.info(
            "handle_service_webhook_test: patch total_reserved_stock %d variant "
            "PER-VARIANT (part_id=%d product_ids=%s)",
            len(updates),
            part_id,
            product_ids,
        )
    except Exception as exc:  # noqa: BLE001 — jangan crash handler
        logger.warning(
            "handle_service_webhook_test: gagal patch total_reserved_stock "
            "(part_id=%d): %s: %s",
            part_id,
            type(exc).__name__,
            exc,
        )


async def handle_service_webhook_test(task: ExternalTask) -> ExternalTaskResult:
    """HIT backend ini → ``POST /api/v2/stock-sync/run-part`` (BODY JSON batch).

    Service Task External bertopic ``serviceWebhookTest`` (BPMN "TEST WEBHOOK
    DAPET APA KAGA"): baca variabel ``partId`` → jalankan pipeline INVENTORY
    TER-SCOPE (hanya part_id yang diterima; bukan scheduler global) lewat
    endpoint backend ini (self-call HTTP, base URL dari ``.env``:
    ``BACKEND_BASE_URL``, default ``http://localhost:8000``).

    ``partId`` berbentuk ARRAY: satu product marketplace bisa memetakan ke
    BANYAK part_id, dan SEMUA part itu yang harus dijalankan pipeline-nya
    (bukan hanya yang pertama). Skalar tetap diterima untuk kompatibilitas.

    Hasil:
      • 2xx + status "success" → **COMPLETE** + variabel ringkasan pipeline
        (pipelineStatus/pipelinePushed/pipelineFailures/…).
      • partId kosong / bukan angka → BPMN ERROR ``PART_ID_MISSING``.
      • 404 (tidak ada part yang punya variant di sku_mapping) → **FAILURE**.
      • 503 (stock_sync_enabled=false) → **FAILURE**.
      • timeout / error jaringan / 4xx lain / 5xx / 2xx tapi status="error"
        (pipeline melaporkan failures) → **FAILURE** → engine retry sesuai
        ``retries`` — task baru COMPLETED kalau run-part benar-benar sukses.

    KENAPA 404/503 = FAILURE (bukan BPMN error): di Camunda 7, BPMN error yang
    TIDAK ditangkap boundary error / event subprocess akan **MENGAKHIRI process
    instance**. Masalah teknis/konfigurasi (port ``BACKEND_BASE_URL`` salah,
    kill-switch ``stock_sync_enabled``, ``sku_mapping`` belum ada) jadi tampak
    seperti keputusan bisnis dan instance "hilang" tanpa pipeline pernah jalan.
    Dengan FAILURE: instance tetap hidup, incident muncul di Cockpit, dan task
    bisa di-retry setelah penyebabnya diperbaiki. BPMN error tetap dipakai untuk
    kondisi DATA yang memang kesalahan bisnis (mis. ``partId`` tidak ada).
    """
    import httpx

    from core.config import settings

    raw_part = task.variable("partId", "part_id", "partID")
    webhook_log_id = task.variable("webhookLogId", "webhook_log_id")
    part_ids = _extract_part_ids(raw_part)
    if not part_ids:
        return ExternalTaskResult.bpmn_error_result(
            "PART_ID_MISSING",
            f"variabel partId wajib berupa array angka > 0 (diterima: {raw_part!r})",
        )

    base = str(settings.backend_base_url or "").strip().rstrip("/")
    if not base:
        return ExternalTaskResult.fail(
            "BACKEND_BASE_URL kosong — self-call /run-part tidak bisa dijalankan"
        )

    url = f"{base}/api/v2/stock-sync/run-part"

    # BATCH via JSON body — banyak part_id dalam SATU request
    # (menggantikan query `?part_id=1&part_id=2`, yang dibatasi panjang URL).
    payload = {
        "part_id": [int(pid) for pid in part_ids],
        # Scheduler global adalah satu-satunya jalur full pipeline.
        # Service task webhook selalu hanya memproses part_id yang diterima.
        "full_pipeline": False,
    }

    try:
        async with httpx.AsyncClient(
            timeout=settings.backend_self_call_timeout
        ) as client:
            resp = await client.post(url, json=payload)
    except httpx.TimeoutException as exc:
        return ExternalTaskResult.fail(
            f"timeout hit backend {url} (part_ids={part_ids})",
            error_details=repr(exc),
        )
    except httpx.HTTPError as exc:
        return ExternalTaskResult.fail(
            f"gagal hit backend {url} (part_ids={part_ids})",
            error_details=repr(exc),
        )

    try:
        data = resp.json() if resp.content else {}
    except ValueError:
        data = {}
    if not isinstance(data, dict):
        data = {}

    message = str(data.get("message") or data.get("detail") or resp.text or "")

    if resp.status_code >= 400:
        # SEMUA 4xx/5xx (termasuk 404 & 503) → FAILURE, bukan BPMN error: biar
        # engine yang retry + incident (instance TIDAK diakhiri). Lihat catatan
        # "KENAPA 404/503 = FAILURE" pada docstring handler ini.
        hint = ""
        if resp.status_code == 404:
            hint = (
                " — PART_ID_UNMAPPED: tidak ada variant yang bisa diproses "
                "di sku_mapping"
            )
        elif resp.status_code == 503:
            hint = " — STOCK_SYNC_DISABLED: kill-switch stock_sync_enabled=false"
        await _mark_webhook_logs(
            webhook_log_id,
            "ON_HOLD",
            f"backend HTTP {resp.status_code}: {message}{hint}",
        )
        return ExternalTaskResult.fail(
            f"backend membalas {resp.status_code} untuk {url} "
            f"(part_ids={part_ids}){hint}",
            error_details=message,
        )

    status = str(data.get("status") or "").strip().lower()
    if status and status != "success":
        # 2xx TAPI pipeline melaporkan failures → belum "berhasil".
        await _mark_webhook_logs(
            webhook_log_id,
            "ON_HOLD",
            f"pipeline status={status}: {message}",
        )
        return ExternalTaskResult.fail(
            f"pipeline /run-part part_ids={part_ids} status={status}",
            error_details=message,
        )

    cycle = data.get("cycle") if isinstance(data.get("cycle"), dict) else {}
    scopes = data.get("scopes")
    if not isinstance(scopes, list) or not scopes:
        single = data.get("scope")
        scopes = [single] if isinstance(single, dict) else []
    variant_count = sum(
        len(s.get("variant_ids") or []) for s in scopes if isinstance(s, dict)
    )

    logger.info(
        "Camunda task service-webhook-test: part_ids=%s → %s (%s)",
        part_ids, url, status or "success",
    )
    await _mark_webhook_logs(webhook_log_id, "SUCCESS")
    return ExternalTaskResult.ok(
        variables={
            # partId WAJIB tetap tipe STRING (teks JSON array). Bila ditulis
            # sebagai list TANPA tipe, Camunda menyimpannya sebagai Json (Spin)
            # sehingga task BERIKUTNYA (mis. ``serviceWebhookCancel`` setelah
            # signal cancel) membaca deskriptor node dari fetchAndLock dan kena
            # BPMN error PART_ID_MISSING — padahal variabelnya jelas ada.
            "partId": part_ids,
            "partIds": part_ids,
            "pipelineStatus": str(data.get("status") or "success"),
            "pipelineRunMode": str(data.get("run_mode") or "MANUAL_PART"),
            "pipelineMessage": message,
            "pipelineVariantCount": variant_count,
            "pipelineReportRows": _as_int(cycle.get("total_report_rows")),
            "pipelineQueued": _as_int(cycle.get("total_queued")),
            "pipelinePushed": _as_int(cycle.get("total_pushed")),
            "pipelineLogged": _as_int(cycle.get("total_logged")),
            "pipelineSkippedUnchanged": _as_int(
                cycle.get("total_skipped_unchanged")
            ),
            "pipelineSkippedPending": _as_int(cycle.get("total_skipped_pending")),
            "pipelineFailures": list(cycle.get("failures") or []),
            "pipelinePushEnabled": bool(cycle.get("push_enabled", True)),
        },
        variable_types={"partId": "String"},
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Handler siap BATCH: boleh dipakai untuk topik dengan ``batch: true`` di
# config.json (banyak task di-merge jadi SATU aksi, mis. satu POST /run-part
# berisi semua part_id gabungan). Handler TANPA flag ini selalu per-task.
handle_service_webhook_test.batch_capable = True  # type: ignore[attr-defined]

HandlerFn = Callable[[ExternalTask], Awaitable[Any]]

HANDLERS: Dict[str, HandlerFn] = {
    "order_detail": handle_order_detail,
    "order-detail": handle_order_detail,
    "send_signal": handle_send_signal,
    "send-signal": handle_send_signal,
    "stock_sync_part": handle_stock_sync_part,
    "stock-sync-part": handle_stock_sync_part,
    "stock_sync_global": handle_stock_sync_global,
    "stock-sync-global": handle_stock_sync_global,
    "service_webhook_test": handle_service_webhook_test,
    "service-webhook-test": handle_service_webhook_test,
    "webhook_part_batch": handle_webhook_part_batch,
    "webhook-part-batch": handle_webhook_part_batch,
}


def register_handler(name: str, handler: HandlerFn) -> None:
    """Daftarkan handler kustom (dipanggil saat startup oleh aplikasi)."""
    key = str(name or "").strip()
    if key:
        HANDLERS[key] = handler


def load_handler(name: str) -> Optional[HandlerFn]:
    """Resolve nama handler → fungsi.

    Urutan: nama bawaan (``handlers``) → ``paket.modul:fungsi`` → ``paket.modul.fungsi``.
    @return fungsi handler, atau None bila tidak ditemukan.
    """
    key = str(name or "").strip()
    if not key:
        return None
    if key in HANDLERS:
        return HANDLERS[key]
    normalized = key.lower().replace("-", "_")
    if normalized in HANDLERS:
        return HANDLERS[normalized]
    if ":" in key or "." in key:
        return _import_handler(key)
    return None


def _import_handler(path: str) -> Optional[HandlerFn]:
    """Import handler dari string path (''module:func'' / ''module.func'')."""
    import importlib

    module_name, _, attribute = path.partition(":")
    if not attribute:
        module_name, _, attribute = path.rpartition(".")
    if not module_name or not attribute:
        return None

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001 — konfigurasi salah, bukan crash
        logger.error("Camunda task handler: gagal import '%s': %s", module_name, exc)
        return None

    handler = getattr(module, attribute, None)
    if handler is None or not callable(handler):
        logger.error(
            "Camunda task handler: '%s' tidak ditemukan / tidak callable di '%s'",
            attribute, module_name,
        )
        return None
    return handler


def available_handlers() -> List[str]:
    """Daftar nama handler bawaan (untuk endpoint status / dokumentasi)."""
    return sorted(HANDLERS.keys())
