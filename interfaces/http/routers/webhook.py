"""
Router PENERIMA WEBHOOK MENTAH (raw passthrough) dari APISIX.

KONFIGURASI SISI APISIX (apisix-conf/apisix/webhook-config.json)
----------------------------------------------------------------
DUA target mem-forward signal order-status ke endpoint ini — satu PER
marketplace supaya filter signal code lebih strict:

  - "order-status-backend-shopee" → marketplace=shopee, signal_codes=[3]
  - "order-status-backend-tiktok" → marketplace=tiktok, signal_codes=[1]

(dulu satu target: marketplace=all + signal_codes=[3,1] — kombinasi silang
seperti Shopee code=1 atau TikTok type=3 ikut ter-forward.)

    POST /api/v1/webhook/order/status

  - method       : POST
  - payload.mode : "raw"  → body = payload ASLI marketplace, TANPA dibungkus
                   { marketplace, event_type, payload }
  - signal_codes : [3] (Shopee code=3 = order_status_push) /
                   [1] (TikTok type=1 = order_status_change) — per target
  - query        : ?marketplace=shopee|tiktok
  - header       : Content-Type: application/json
                   X-Forwarded-By: apisix-webhook-forwarder
                   X-Webhook-Source: shopee|tiktok
                   X-Webhook-Target: order-status-backend-shopee |
                                     order-status-backend-tiktok

BENTUK BODY MENTAH YANG DITERIMA
--------------------------------
  Shopee : {"code": 3, "shop_id": ..., "timestamp": ...,
            "data": {"ordersn": "...", "status": "...", ...}}
  TikTok : {"type": 1, "shop_id": ..., "timestamp": ...,
            "data": {"order_id": "...", "order_status": "...", ...}}

CATATAN PENTING
---------------
- Body dibaca MENTAH lewat `await request.body()` dan TIDAK di-declare sebagai
  Pydantic body model, sehingga payload marketplace apa pun diterima utuh
  (field baru, signal baru, bahkan body non-JSON) tanpa divalidasi/diubah
  FastAPI. Marketplace HANYA dibaca untuk metadata log.
- Endpoint WAJIB membalas 2xx. APISIX menganggap forward gagal (dan melakukan
  retry bila `max_attempts` > 1) untuk response non-2xx — jadi pekerjaan
  berat/eksternal (mis. fetch detail order via APISIX) dikerjakan SETELAH
  request ini selesai, bukan di dalamnya.
- Backend IKUT memeriksa STATUS ORDER sebelum start event (gate `order_status`
  per aturan di camunda/config.json + `WEBHOOK_ORDER_STATUS_ALLOWLIST` di .env).
  Signal yang benar tapi status di luar daftar tidak men-start proses — lihat
  camunda/README.md bagian "Gate status order".
- Satu signal per request. Webhook mentah dicatat sebagai JSONL (1 baris per
  webhook) di `settings.webhook_log_dir` agar payload asli bisa diaudit/replay.
- Setelah payload dicatat, webhook diteruskan ke Camunda sebagai start event /
  message oleh paket `camunda/` (aturan: camunda/config.json). Satu webhook bisa
  men-start BEBERAPA proses (mode "all") — hasilnya satu baris log per proses.
  Trigger dijalankan sebagai BACKGROUND TASK sesudah response dikirim, jadi
  APISIX tetap menerima 2xx cepat walau Camunda lambat/mati. Lihat camunda/README.md.

HIT SIGNAL (camunda/signals.py)
--------------------------------

    POST /api/v1/webhook/camunda/signal?marketplace=tiktok&status=ON_HOLD

  Nama sinyal diambil dari peta STATUS ORDER → SIGNAL di camunda/config.json
  (bagian `signal`: `by_status` / `by_marketplace` / `default_signal`). Body JSON
  OPSIONAL: kalau dikirim, payload dipakai untuk variabel sinyal (path `data.*`,
  namespace `order.*`) dan status ikut dibaca dari body bila query `status` kosong.
  Bisa juga dipakai untuk menguji konfigurasi (`dry_run=true` = catat tanpa kirim).

EXTERNAL SERVICE TASK (camunda/external_task.py)
-----------------------------------------------

    GET  /api/v1/webhook/camunda/external-tasks/status   → konfigurasi + statistik worker
    POST /api/v1/webhook/camunda/external-tasks/poll     → jalankan N siklus poll sekarang
    POST /api/v1/webhook/camunda/external-tasks/start    → jalankan worker (loop)
    POST /api/v1/webhook/camunda/external-tasks/stop     → hentikan worker
"""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from infrastructure.camunda.external_task import (
    external_tasks_enabled,
    get_worker,
    start_worker,
    stop_worker,
)
from infrastructure.camunda.service import camunda_webhook_trigger
from core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/webhook", tags=["Webhook Receiver"])

# Root project (folder yang memuat main.py) — basis path log relatif.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Lokasi field kode signal per marketplace; sama dengan settings.signal_code_paths
# di webhook-config.json sisi APISIX (shopee="code", tiktok="type").
SIGNAL_CODE_PATHS: Dict[str, str] = {"shopee": "code", "tiktok": "type"}

# Nama field order id / status yang umum dipakai marketplace (fallback berurutan).
ORDER_ID_FIELDS = ("ordersn", "order_id", "order_sn", "order_no", "orderNo")
ORDER_STATUS_FIELDS = ("status", "order_status", "orderStatus", "new_status")

# Lock append log — endpoint async, banyak request bisa masuk bersamaan.
_LOG_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log_file_path() -> Path:
    """Path file log webhook mentah (relatif terhadap root project)."""
    base = Path(settings.webhook_log_dir)
    if not base.is_absolute():
        base = PROJECT_ROOT / base
    return base / settings.webhook_log_filename


def _read_body_as_json_dict(raw_text: str) -> Optional[dict]:
    """Parse body mentah sebagai JSON objek; None bila bukan JSON objek.

    Hasil parse hanya dipakai untuk metadata ringkasan log — body asli tetap
    yang ditulis ke file log.
    """
    if not raw_text.strip():
        return None
    try:
        parsed = json.loads(raw_text)
    except (json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _resolve_marketplace(
    request: Request,
    marketplace_param: Optional[str],
    body: Optional[dict],
) -> str:
    """Tentukan marketplace: query → header X-Webhook-Source → bentuk payload.

    Dipakai HANYA untuk metadata log; body tidak pernah diubah.
    """
    for candidate in (
        marketplace_param,
        request.headers.get("x-webhook-source"),
    ):
        if candidate and candidate.strip() and candidate.strip() != "{{marketplace}}":
            return candidate.strip().lower()

    # Fallback: Shopee memakai field "code", TikTok memakai field "type".
    if isinstance(body, dict):
        if "code" in body and "type" not in body:
            return "shopee"
        if "type" in body and "code" not in body:
            return "tiktok"
    return "unknown"


def _resolve_signal_code(marketplace: str, body: Optional[dict]) -> Any:
    """Ambil kode signal (shopee: code, tiktok: type) dari body mentah."""
    if not isinstance(body, dict):
        return None

    path = SIGNAL_CODE_PATHS.get(marketplace)
    if path is not None:
        return body.get(path)

    for key in ("code", "type"):
        if key in body:
            return body.get(key)
    return None


def _extract_order_ref(marketplace: str, body: Optional[dict]) -> Dict[str, Any]:
    """Ambil order_id & status dari body mentah — HANYA untuk ringkasan log.

    Body tidak diubah. Bila bentuk payload berbeda (signal / marketplace baru),
    nilainya None dan payload ASLI tetap tercatat utuh di log.
    """
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        return {"order_id": None, "order_status": None}

    order_id = None
    for key in ORDER_ID_FIELDS:
        value = data.get(key)
        if value not in (None, ""):
            order_id = str(value)
            break

    order_status = None
    for key in ORDER_STATUS_FIELDS:
        value = data.get(key)
        if value not in (None, ""):
            order_status = value
            break

    return {"order_id": order_id, "order_status": order_status}


def append_raw_webhook_log(entry: dict) -> None:
    """Append satu baris JSON (JSONL) berisi webhook MENTAH ke file log.

    Kegagalan tulis log TIDAK boleh menggagalkan penerimaan webhook (APISIX
    akan retry bila non-2xx), cukup dicatat sebagai warning.
    """
    if not settings.webhook_log_enabled:
        return

    try:
        path = _log_file_path()
        line = json.dumps(entry, ensure_ascii=False, default=str)
        with _LOG_LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
    except Exception as exc:  # noqa: BLE001 — log tidak boleh fatal
        logger.warning("Gagal menulis log webhook mentah: %s", exc)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/camunda/signal")
async def send_camunda_signal(
    request: Request,
    marketplace: Optional[str] = Query(
        None,
        description=(
            "'shopee' | 'tiktok'. Kosong → header X-Webhook-Source → bentuk body "
            "(shopee='code', tiktok='type')."
        ),
    ),
    status: Optional[str] = Query(
        None,
        description=(
            "STATUS ORDER yang menentukan nama sinyal (mis. ON_HOLD). Kosong → "
            "dibaca dari body JSON (data.status Shopee / data.order_status TikTok)."
        ),
    ),
    signal_name: Optional[str] = Query(
        None,
        description=(
            "Paksa nama sinyal tertentu (menimpa peta `signal.by_status` / "
            "`signal.by_marketplace` di camunda/config.json)."
        ),
    ),
    order_id: Optional[str] = Query(
        None, description="Order id (mengisi konteks data.order_id untuk variabel sinyal)."
    ),
    execution_id: Optional[str] = Query(
        None,
        description=(
            "Camunda 7: kirim sinyal ke SATU eksekusi saja "
            "(POST /execution/{id}/signal). Kosong → broadcast ke semua handler."
        ),
    ),
    dry_run: bool = Query(
        False, description="Hitung + catat hasilnya ke log, TAPI tidak mengirim ke Camunda."
    ),
) -> dict:
    """HIT SIGNAL Camunda berdasarkan STATUS ORDER (bagian `signal` config.json).

    Berguna untuk: menguji pemetaan status→signal, mengirim sinyal manual dari
    operator/dashboard, atau replay sinyal yang gagal. Body JSON opsional — bila
    dikirim, payload dipakai sebagai konteks variabel sinyal (path `data.*` dan
    namespace `order.*`), sama seperti webhook biasa.

    Balasan selalu 200 dengan detail hasil termasuk ``skipped_reason`` bila
    sinyal tidak dikirim (mis. ``signal_not_configured`` untuk status yang belum
    dipetakan, atau ``camunda_disabled``).
    """
    raw_body = await request.body()
    raw_text = raw_body.decode("utf-8", errors="replace")
    body = _read_body_as_json_dict(raw_text)

    resolved_marketplace = _resolve_marketplace(request, marketplace, body)

    # Query status / order_id dipakai sebagai konteks tambahan bila body kosong
    # (mis. pemanggilan manual: ?status=ON_HOLD&order_id=...).
    if status or order_id:
        data = body.get("data") if isinstance(body, dict) and isinstance(body.get("data"), dict) else {}
        if order_id:
            data.setdefault("order_id", order_id)
        if status:
            data.setdefault("order_status", status)
        body = dict(body or {})
        body["data"] = data

    result = await camunda_webhook_trigger.send_signal(
        signal_name=signal_name,
        marketplace=resolved_marketplace if resolved_marketplace != "unknown" else "",
        order_status=status,
        execution_id=execution_id,
        body=body,
        headers=dict(request.headers),
        raw_body=raw_text,
        dry_run=dry_run,
        source="api",
    )

    logger.info(
        "Camunda signal (manual): marketplace=%s status=%s signal=%s ok=%s skipped=%s",
        resolved_marketplace,
        status,
        result.signal_name,
        result.ok,
        result.skipped_reason,
    )
    return result.to_dict()


@router.get("/camunda/external-tasks/status")
async def external_tasks_status() -> dict:
    """Konfigurasi efektif + statistik worker external service task.

    Tidak memanggil Camunda. Bila worker sedang berjalan, konfigurasi yang
    dilaporkan adalah yang sedang dipakai (reload file hanya berlaku saat
    worker berhenti, atau lewat POST /external-tasks/stop lalu /start).
    """
    worker = get_worker(reload_config=True)
    return {
        "enabled": external_tasks_enabled(),
        "base_url": settings.camunda_base_url,
        "version": str(settings.camunda_version),
        "rest_path": str(settings.camunda_rest_path) or "(default per versi)",
        "worker": worker.status(),
    }


@router.post("/camunda/external-tasks/poll")
async def external_tasks_poll(
    limit: int = Query(
        1,
        ge=1,
        le=20,
        description="Jumlah siklus fetchAndLock→complete yang dijalankan sekarang.",
    ),
) -> dict:
    """Jalankan siklus poll SEKARANG (tanpa menyalakan worker loop).

    Dipakai untuk uji coba: pastikan topic/handler benar dan Camunda bisa
    dijangkau. Task yang terambil BENAR-BENAR dikerjakan dan di-complete.
    """
    worker = get_worker(reload_config=True)
    if not worker.enabled_topics:
        return {
            "status": "error",
            "message": (
                "tidak ada topic dengan handler — periksa `external_tasks.topics` "
                "dan nama `handler` di camunda/config.json"
            ),
            "worker": worker.status(),
        }

    outcomes = []
    for _ in range(int(limit)):
        outcomes.extend(await worker.poll_once())

    return {
        "status": "ok",
        "iterations": int(limit),
        "fetched": len(outcomes),
        "outcomes": [outcome.to_dict() for outcome in outcomes],
        "worker": worker.status(),
    }


@router.post("/camunda/external-tasks/start")
async def external_tasks_start(
    force: bool = Query(
        False,
        description="Start walau `external_tasks.enabled=false` (untuk uji coba).",
    ),
) -> dict:
    """Nyalakan worker external service task (loop polling)."""
    worker = get_worker(reload_config=True)

    if not force and not external_tasks_enabled():
        logger.info(
            "Camunda external task: start DITOLAK — external_tasks.enabled=false "
            "(pakai ?force=true untuk memaksa)"
        )
        return {
            "status": "disabled",
            "message": (
                "external_tasks.enabled=false di camunda/config.json — set true "
                "(lalu restart / panggil endpoint ini) atau pakai ?force=true"
            ),
            "worker": worker.status(),
        }

    if not worker.enabled_topics:
        raise HTTPException(
            status_code=400,
            detail=(
                "tidak ada topic dengan handler terdaftar — periksa "
                "`external_tasks.topics` + nama `handler` di camunda/config.json "
                f"(topics={[t.name for t in worker.topics]}, "
                f"handlers={sorted(worker.handlers)}) "
            ),
        )

    started = await worker.start()
    return {
        "status": "started" if started else "already_running",
        "worker": worker.status(),
    }


@router.post("/camunda/external-tasks/stop")
async def external_tasks_stop() -> dict:
    """Hentikan worker external service task (task yang sedang berjalan diselesaikan dulu)."""
    worker = get_worker()
    was_running = worker.is_running()
    await stop_worker()
    logger.info(
        "Camunda external task: worker dihentikan (sebelumnya_berjalan=%s)", was_running
    )
    return {
        "status": "stopped" if was_running else "not_running",
        "previous_worker_id": worker.worker_id,
        "totals": worker.status().get("totals"),
    }


@router.get("/camunda/config")
async def camunda_config_status() -> dict:
    """Ringkasan konfigurasi Camunda efektif (tanpa mengirim apa pun).

    Berguna untuk memastikan URL/versi/rule sudah benar sebelum webhook nyata
    masuk — tidak ada request ke Camunda dan tidak ada proses yang di-start.
    """
    return camunda_webhook_trigger.describe_config()


@router.post("/order/status")
async def receive_order_status_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    marketplace: Optional[str] = Query(
        None,
        description=(
            "Diisi otomatis oleh APISIX ({{marketplace}}): 'shopee' atau 'tiktok'. "
            "Bila kosong, backend mendeteksinya dari header X-Webhook-Source / "
            "bentuk payload (shopee='code', tiktok='type')."
        ),
    ),
) -> JSONResponse:
    """Terima webhook order-status MENTAH dari APISIX (payload_mode="raw").

    Alur: baca body mentah → catat JSONL (termasuk payload asli) → jadwalkan
    start event Camunda (background) → balas 200.

    Trigger Camunda sengaja dijadwalkan sebagai background task: APISIX hanya
    menunggu response ini, jadi retry/timeout Camunda tidak memperlambat
    (atau menggagalkan) forward dari sisi gateway.
    """
    raw_body = await request.body()
    raw_text = raw_body.decode("utf-8", errors="replace")
    body = _read_body_as_json_dict(raw_text)

    resolved_marketplace = _resolve_marketplace(request, marketplace, body)
    signal_code = _resolve_signal_code(resolved_marketplace, body)
    order_ref = _extract_order_ref(resolved_marketplace, body)

    limit = settings.webhook_log_max_body_chars
    raw_for_log = raw_text
    body_truncated = False
    if limit and len(raw_for_log) > limit:
        raw_for_log = raw_for_log[:limit]
        body_truncated = True

    received_at = datetime.now(timezone.utc).isoformat()
    append_raw_webhook_log(
        {
            "received_at": received_at,
            "marketplace": resolved_marketplace,
            "signal_code": signal_code,
            "order_id": order_ref["order_id"],
            "order_status": order_ref["order_status"],
            "forwarded_by": request.headers.get("x-forwarded-by"),
            "target_id": request.headers.get("x-webhook-target"),
            "content_type": request.headers.get("content-type"),
            "client": request.client.host if request.client else None,
            "body_bytes": len(raw_body),
            "body_truncated": body_truncated,
            "raw_body": raw_for_log,
        }
    )

    # ── Simpan log awal ke tabel webhook_log ─────────────────────────────
    import uuid
    from infrastructure.db.database import async_session_factory
    from infrastructure.repositories.webhook_log_repository_impl import (
        WebhookLogRepositoryImpl,
    )

    webhook_log_id = uuid.uuid4()
    initial_status = (
        "ON_HOLD"
        if str(order_ref.get("order_status") or "").upper() in ("ON_HOLD", "CANCELLED")
        else "PENDING"
    )
    try:
        async with async_session_factory() as session:
            repo = WebhookLogRepositoryImpl(session)
            await repo.create(
                marketplace=resolved_marketplace,
                signal_code=signal_code,
                order_id=order_ref.get("order_id"),
                order_status=order_ref.get("order_status"),
                raw_payload=body,
                status=initial_status,
                log_id=webhook_log_id,
            )
            await session.commit()
    except Exception as exc:
        logger.warning("Gagal menyimpan webhook log awal: %s", exc)

    logger.info(
        "Webhook order-status diterima: id=%s marketplace=%s signal_code=%s "
        "order_id=%s order_status=%s bytes=%d",
        webhook_log_id,
        resolved_marketplace,
        signal_code,
        order_ref["order_id"],
        order_ref["order_status"],
        len(raw_body),
    )

    # ── Trigger Camunda (start event / message) ────────────────────────────
    # Dijalankan SETELAH response terkirim ke APISIX. Aturan webhook → proses
    # Camunda ada di camunda/config.json (bisa diedit tanpa restart).
    camunda_queued = False
    if camunda_webhook_trigger.is_enabled():
        background_tasks.add_task(
            camunda_webhook_trigger.trigger_all,
            marketplace=resolved_marketplace,
            signal_code=signal_code,
            body=body,
            headers=dict(request.headers),
            raw_body=raw_text,
            webhook_log_id=str(webhook_log_id),
        )
        camunda_queued = True

    return JSONResponse(
        status_code=200,
        content={
            "status": "accepted",
            "webhook_log_id": str(webhook_log_id),
            "marketplace": resolved_marketplace,
            "signal_code": signal_code,
            "order_id": order_ref["order_id"],
            "received_at": received_at,
            "camunda": "queued" if camunda_queued else "disabled",
        },
    )

