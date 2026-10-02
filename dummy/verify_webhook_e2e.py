"""verify_webhook_e2e.py — verifikasi alur Webhook → Camunda → part_id → Service Task
TANPA APISIX asli dan TANPA Camunda asli.

Cara pakai (dari root project):

    python dummy/verify_webhook_e2e.py

Yang diperiksa:

  FASE 0  Guardrail statis (tidak butuh service apa pun):
          A. method repository `find_active_part_ids_by_external_product_ids`
             benar-benar ada di impl & interface (temuan: HILANG → part_id selalu kosong)
          B. `app/main.py` memulai worker external task saat startup (temuan: TIDAK)
          C. `ApisixClient.fetch_reserved_stock` menerima `product_id`
             (temuan: TIDAK → reserved stock webhook selalu gagal)

  FASE 1  Dummy APISIX + Dummy Camunda + stub /run-part dinyalakan lokal.

  FASE 2  Kode ASLI `camunda_webhook_trigger.trigger_all(...)` dijalankan terhadap
          dummy. Dicek: start event terkirim, variabel `partId`, `orderItems`,
          `total_reserved` ada di payload Camunda.

  FASE 3  Worker external task ASLI (`get_worker().poll_once()`) mengambil task
          dari Dummy Camunda, menjalankan handler `service_webhook_test`, lalu
          self-call ke stub `/run-part`. Dicek: task COMPLETE + stub menerima
          daftar part_id.

Catatan: lapisan DB (resolve part_id dari `sku_mapping` & nama varian) di-stub di
FASE 2 karena harness ini sengaja bebas-PostgreSQL. Kebenaran jalur DB dicek di
FASE 0 (keberadaan method repository).

Exit code 0 = semua PASS, 1 = ada FAIL.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dummy.dummy_apisix import DummyApisix  # noqa: E402
from dummy.dummy_camunda import DummyCamunda  # noqa: E402

# Nilai uji — bebas, tidak perlu cocok dengan data produksi.
MARKETPLACE = "tiktok"
SHOP_ID = "7494709429666874412"
ORDER_ID = "ORDER-DUMMY-1"
PRODUCT_ID = "PROD-DUMMY-1"
SELLER_SKU = "1A1"
PART_ID = 321
PROCESS_KEY = "serviceWebhook"


# ---------------------------------------------------------------------------
# Pelaporan
# ---------------------------------------------------------------------------

class Report:
    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []

    def add(self, phase: str, name: str, ok: Optional[bool], detail: str = "") -> None:
        self.rows.append({"phase": phase, "name": name, "ok": ok, "detail": detail})
        marker = "[FAIL]" if ok is False else ("[SKIP]" if ok is None else "[ OK ]")
        print(f"  {marker} {name}" + (f" - {detail}" if detail else ""))

    def failed(self) -> bool:
        return any(row["ok"] is False for row in self.rows)

    def passed(self) -> int:
        return sum(1 for row in self.rows if row["ok"] is True)


# ---------------------------------------------------------------------------
# FASE 0 — guardrail statis
# ---------------------------------------------------------------------------

def phase_static(report: Report) -> None:
    print("\nFASE 0 - Guardrail statis (kode sumber, tanpa service)")

    from infrastructure.repositories.sku_mapping_repository_impl import SKUMappingRepositoryImpl
    from domain.repositories.sku_mapping_repository import SKUMappingRepository

    method_name = "find_active_part_ids_by_external_product_ids"

    has_impl = hasattr(SKUMappingRepositoryImpl, method_name)
    report.add(
        "FASE0",
        "A. Impl repository punya method part_id",
        has_impl,
        "OK" if has_impl else "MISSING -> resolve_part_ids_for_products selalu AttributeError -> part_id kosong",
    )

    abstract = getattr(SKUMappingRepository, "__abstractmethods__", frozenset())
    has_iface = method_name in abstract
    report.add(
        "FASE0",
        "A2. Interface mendeklarasikan method part_id",
        has_iface,
        "OK" if has_iface else "MISSING di interface (SKUMappingRepository)",
    )

    import app.main as app_main

    main_src = inspect.getsource(app_main)
    worker_wired = ("start_worker" in main_src) or ("external_tasks" in main_src)
    report.add(
        "FASE0",
        "B. app/main.py memulai worker external task saat startup",
        worker_wired,
        "OK" if worker_wired else "TIDAK ada start_worker()/external_tasks di lifespan -> worker hanya jalan via endpoint manual",
    )

    from infrastructure.marketplace.apisix.client import ApisixClient

    params = inspect.signature(ApisixClient.fetch_reserved_stock).parameters
    supports_product = "product_id" in params
    report.add(
        "FASE0",
        "C. ApisixClient.fetch_reserved_stock menerima product_id",
        supports_product,
        "OK" if supports_product else "TIDAK -> pemanggil webhook (product_id=...) kena TypeError -> reserved selalu gagal",
    )

    from interfaces.http.routers import stock_sync as stock_sync_router

    route_paths = {
        getattr(route, "path", "") for route in stock_sync_router.router.routes
    }
    has_run_part = "/api/v2/stock-sync/run-part" in route_paths
    report.add(
        "FASE0",
        "D. Endpoint /api/v2/stock-sync/run-part terdaftar (hit client external)",
        has_run_part,
        "OK" if has_run_part else "TIDAK -> external task serviceWebhookTest self-call 404 -> pipeline tak jalan",
    )

    from infrastructure.camunda.external_task import external_task_config, parse_topics

    topics = parse_topics(external_task_config())
    svc = next((t for t in topics if t.name == "serviceWebhookTest"), None)
    batch_on = svc is not None and svc.batch is True
    report.add(
        "FASE0",
        "E. Topic serviceWebhookTest dikonfigurasi BATCH (config.json, bisa diubah)",
        batch_on,
        f"batch={getattr(svc, 'batch', None)} -> true=Satu hit /run-part utk semua task, false=per task",
    )


# ---------------------------------------------------------------------------
# FASE 1 — stub /run-part (backend self-call)
# ---------------------------------------------------------------------------

class RunPartStub:
    """Stub endpoint backend `POST /api/v2/stock-sync/run-part`."""

    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> str:
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # noqa: D401
                return

            def do_POST(self) -> None:  # noqa: N802
                import json as _json
                from urllib.parse import urlparse

                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = _json.loads(raw.decode("utf-8")) if raw else {}
                except (ValueError, UnicodeDecodeError):
                    body = {}
                outer.calls.append(
                    {
                        "path": parsed.path,
                        "part_id": [str(x) for x in (body.get("part_id") or [])],
                        "full_pipeline": body.get("full_pipeline"),
                    }
                )
                payload = {
                    "status": "success",
                    "run_mode": "MANUAL_PART",
                    "message": "stub ok",
                    "cycle": {
                        "total_report_rows": 1,
                        "total_queued": 1,
                        "total_pushed": 0,
                        "total_logged": 1,
                        "total_skipped_unchanged": 0,
                        "total_skipped_pending": 0,
                        "failures": [],
                        "push_enabled": False,
                    },
                    "scopes": [{"variant_ids": [PART_ID]}],
                }
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


# ---------------------------------------------------------------------------
# FASE 2 — webhook → Camunda start
# ---------------------------------------------------------------------------

async def phase_trigger(report: Report, apisix: DummyApisix, camunda: DummyCamunda) -> None:
    print("\nFASE 2 - Webhook -> Camunda start (kode ASLI, APISIX/Camunda dummy)")

    import infrastructure.camunda.service as service_module
    from infrastructure.camunda.service import camunda_webhook_trigger

    # DB di-stub: harness bebas-PostgreSQL. Kebenaran jalur DB = FASE 0.
    async def fake_resolve_part_ids(product_ids: List[str]) -> List[int]:
        return [PART_ID]

    async def fake_resolve_variant_names(product_ids: List[str], shop_uuid: str = "") -> Dict[tuple, str]:
        return {}

    service_module.resolve_part_ids_for_products = fake_resolve_part_ids
    service_module.resolve_variant_names_for_products = fake_resolve_variant_names

    payload = {
        "type": 1,
        "shop_id": SHOP_ID,
        "timestamp": 1790059971,
        "data": {"order_id": ORDER_ID, "order_status": "UNPAID"},
    }

    results = await camunda_webhook_trigger.trigger_all(
        marketplace=MARKETPLACE,
        signal_code=1,
        body=payload,
        headers={},
        raw_body=json.dumps(payload),
        webhook_log_id=None,  # sengaja None → lewati penulisan DB
    )

    ok_start = any(r.ok for r in results)
    report.add(
        "FASE2",
        "Start event terkirim ke Camunda (dummy)",
        bool(camunda.starts),
        f"starts={len(camunda.starts)} results_ok={sum(1 for r in results if r.ok)}",
    )

    if not camunda.starts:
        report.add("FASE2", "Variabel start tersedia", False, "tidak ada start event -> tidak bisa dicek")
        return

    start = camunda.starts[-1]
    variables = start.get("variables") or {}

    report.add(
        "FASE2",
        f"Process key = {PROCESS_KEY}",
        start.get("key") == PROCESS_KEY,
        f"key={start.get('key')}",
    )

    # orderItems (detail order dari APISIX dummy)
    report.add(
        "FASE2",
        "Variabel orderItems ada & memuat product_id",
        "orderItems" in variables and PRODUCT_ID in json.dumps(variables.get("orderItems", {})),
        f"value={variables.get('orderItems', {}).get('value', '') if isinstance(variables.get('orderItems'), dict) else variables.get('orderItems')}",
    )

    # partId (inti)
    part_raw = variables.get("partId")
    part_value = part_raw.get("value") if isinstance(part_raw, dict) else part_raw
    try:
        parsed = json.loads(part_value) if isinstance(part_value, str) else part_value
    except (ValueError, TypeError):
        parsed = None
    report.add(
        "FASE2",
        f"Variabel partId = [{PART_ID}]",
        parsed == [PART_ID],
        f"value={part_value!r}",
    )

    # total_reserved (menunjukkan bug C: fetch_reserved_stock(product_id=…))
    report.add(
        "FASE2",
        "Variabel total_reserved ada (reserved stock per produk)",
        "total_reserved" in variables,
        f"value={variables.get('total_reserved', {}).get('value') if 'total_reserved' in variables else 'MISSING (bug C)'}",
    )


# ---------------------------------------------------------------------------
# FASE 3 — external task worker
# ---------------------------------------------------------------------------

async def phase_worker(report: Report, camunda: DummyCamunda, run_part: RunPartStub) -> None:
    print("\nFASE 3 - External Task Worker (kode ASLI, Camunda dummy)")

    from infrastructure.camunda.external_task import get_worker
    from infrastructure.camunda.task_handlers import load_handler

    worker = get_worker(reload_config=True)
    if "serviceWebhookTest" not in worker.handlers:
        handler = load_handler("service_webhook_test")
        if handler is not None:
            worker.register("serviceWebhookTest", handler)

    report.add(
        "FASE3",
        "Worker punya handler topic serviceWebhookTest",
        "serviceWebhookTest" in worker.handlers,
        f"handlers={sorted(worker.handlers)}",
    )

    outcomes = await worker.poll_once()
    actions = [o.action for o in outcomes]
    report.add(
        "FASE3",
        "Worker menjalankan task (fetchAndLock -> complete/failure)",
        bool(outcomes),
        f"outcomes={actions}",
    )

    ok_complete = bool(camunda.completions)
    report.add(
        "FASE3",
        "Camunda (dummy) menerima task COMPLETE",
        ok_complete,
        f"completions={len(camunda.completions)}",
    )

    if run_part.calls:
        call = run_part.calls[-1]
        report.add(
            "FASE3",
            "Backend /run-part dipanggil dengan daftar part_id (BODY array)",
            call["part_id"] == [str(PART_ID)] and call["full_pipeline"] is True,
            f"part_id={call['part_id']} full_pipeline={call['full_pipeline']}",
        )
    else:
        report.add(
            "FASE3",
            "Backend /run-part dipanggil dengan daftar part_id",
            False,
            "tidak ada panggilan /run-part (kemungkinan partId tidak sampai ke worker)",
        )

    # Jika complete terjadi, pastikan handler melaporkan sukses pipeline.
    if camunda.completions:
        body = camunda.completions[-1]["body"]
        vars_ = (body.get("variables") or {}) if isinstance(body, dict) else {}
        pstatus = vars_.get("pipelineStatus")
        pstatus_val = pstatus.get("value") if isinstance(pstatus, dict) else pstatus
        report.add(
            "FASE3",
            "Variabel pipelineStatus = success",
            str(pstatus_val).lower() == "success",
            f"pipelineStatus={pstatus_val!r}",
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run() -> int:
    report = Report()

    print("=" * 72)
    print("  VERIFIKASI E2E WEBHOOK -> CAMUNDA -> part_id -> SERVICE TASK")
    print("  (APISIX & Camunda ASLI diganti dummy - bebas PostgreSQL)")
    print("=" * 72)

    # Simpan setting asli untuk dipulihkan.
    from core.config import settings

    saved = {k: getattr(settings, k) for k in (
        "apisix_base_url", "apisix_order_base_url", "apisix_order_path",
        "apisix_reserved_stock_path", "apisix_tiktok_shop_uuid",
        "camunda_base_url", "camunda_version", "camunda_rest_path",
        "camunda_max_attempts", "camunda_retry_delay_seconds", "camunda_enabled",
        "backend_base_url", "backend_self_call_timeout",
        "webhook_order_detail_enabled", "webhook_order_detail_translate",
        "webhook_reserved_enabled", "webhook_reserved_retry_delay",
        "webhook_part_id_enabled", "webhook_order_status_allowlist",
    )}

    apisix = DummyApisix(
        marketplace=MARKETPLACE, order_id=ORDER_ID, product_id=PRODUCT_ID,
        seller_sku=SELLER_SKU, reserved=3,
    )
    camunda = DummyCamunda(topics=["serviceWebhookTest"])
    run_part = RunPartStub()

    try:
        phase_static(report)

        apisix_url = apisix.start()
        camunda_url = camunda.start()
        run_part_url = run_part.start()

        settings.apisix_base_url = apisix_url
        settings.apisix_order_base_url = ""
        settings.apisix_order_path = "/order"
        settings.apisix_reserved_stock_path = "/order"
        settings.apisix_tiktok_shop_uuid = SHOP_ID
        settings.camunda_base_url = camunda_url
        settings.camunda_version = "7"
        settings.camunda_rest_path = ""
        settings.camunda_max_attempts = 1
        settings.camunda_retry_delay_seconds = 0.0
        settings.camunda_enabled = True
        settings.backend_base_url = run_part_url
        settings.backend_self_call_timeout = 5.0
        settings.webhook_order_detail_enabled = True
        settings.webhook_order_detail_translate = False
        settings.webhook_reserved_enabled = True
        settings.webhook_reserved_retry_delay = 0.0
        settings.webhook_part_id_enabled = True
        settings.webhook_order_status_allowlist = ""

        await phase_trigger(report, apisix, camunda)
        await phase_worker(report, camunda, run_part)

    finally:
        apisix.stop()
        camunda.stop()
        run_part.stop()
        for key, value in saved.items():
            setattr(settings, key, value)

    print("\n" + "=" * 72)
    total = len(report.rows)
    if report.failed():
        print(f"  HASIL: ADA FAIL - {report.passed()}/{total} PASS")
        print("  Artinya alur belum benar. Perbaiki item [FAIL] di atas, lalu ulangi.")
    else:
        print(f"  HASIL: SEMUA PASS - {report.passed()}/{total}")
    print("=" * 72)
    return 1 if report.failed() else 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
