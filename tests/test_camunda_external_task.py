"""
Test EXTERNAL SERVICE TASK Camunda (camunda/external_task.py + task_handlers.py).

Cakupan:
  1. Konfigurasi topic (parse_topics / ExternalTaskTopic.from_dict / fetch_body).
  2. fetch_and_lock: Camunda 7 (/external-task/fetchAndLock) & Camunda 8
     (/v2/jobs/activation) — URL, body, parsing task.
  3. execute_task / poll_once: complete (dengan variabel bertipe), failure
     (retries diturunkan), bpmnError, dan topic tanpa handler.
  4. status() & loop run() (stop setelah N iterasi, tanpa sleep).
  5. task_handlers: registry, path custom "module:func", dan handler bawaan
     yang tidak butuh DB (send_signal pakai service yang di-monkeypatch).
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from infrastructure.camunda.external_task import (
    ACTION_BPMN_ERROR,
    ACTION_COMPLETED,
    ACTION_FAILED,
    ACTION_NO_HANDLER,
    ExternalTask,
    ExternalTaskResult,
    ExternalTaskTopic,
    ExternalTaskWorker,
    external_task_config,
    external_tasks_enabled,
    get_worker,
    parse_topics,
    set_worker,
)
from infrastructure.camunda.models import CamundaTriggerResult
from infrastructure.camunda.task_handlers import (
    available_handlers,
    handle_send_signal,
    handle_service_webhook_test,
    handle_stock_sync_part,
    load_handler,
)
from infrastructure.camunda.client import CamundaClient
from core.config import settings
from infrastructure.marketplace.apisix.client import ApisixClient

FETCH_PATH = "/external-task/fetchAndLock"
COMPLETE_PATH = "/external-task/task-1/complete"


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def camunda_env(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "camunda_enabled", True)
    monkeypatch.setattr(settings, "camunda_base_url", "http://camunda.test:8080")
    monkeypatch.setattr(settings, "camunda_version", "7")
    monkeypatch.setattr(settings, "camunda_rest_path", "")
    monkeypatch.setattr(settings, "camunda_auth_type", "none")
    monkeypatch.setattr(settings, "camunda_max_attempts", 1)
    monkeypatch.setattr(settings, "camunda_retry_delay_seconds", 0)
    monkeypatch.setattr(settings, "camunda_typed_variables", True)
    monkeypatch.setattr(settings, "camunda_tenant_id", "")
    monkeypatch.setattr(settings, "camunda_log_enabled", False)
    monkeypatch.setattr(settings, "camunda_log_dir", str(tmp_path))
    return tmp_path


def _make_router_transport(routes):
    """MockTransport yang merutekan berdasarkan akhiran path URL.

    ``routes`` = {"<akhiran path>": (status, payload), ...}
    @return (transport, calls)
    """
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        for needle, (status, payload) in routes.items():
            if path.endswith(needle):
                return httpx.Response(status, json=payload)
        return httpx.Response(404, json={"error": f"unrouted: {path}"})

    return httpx.MockTransport(handler), calls


def _body(call: httpx.Request) -> dict:
    return json.loads(call.content.decode("utf-8"))


def _raw_task(**overrides) -> dict:
    task = {
        "id": "task-1",
        "topicName": "order-detail",
        "workerId": "w1",
        "lockExpirationTime": "2026-09-22T10:00:00.000+0000",
        "processInstanceId": "proc-1",
        "executionId": "exec-1",
        "activityId": "Activity_1",
        "processDefinitionId": "serviceWebhook:1:abc",
        "processDefinitionKey": "serviceWebhook",
        "businessKey": "BK-1",
        "retries": 3,
        "variables": {"orderId": "586198966233958181", "marketplace": "tiktok"},
        "tenantId": None,
        "priority": 50,
    }
    task.update(overrides)
    return task


def _worker_with_handler(handler, transport, **kwargs):
    worker = ExternalTaskWorker(
        worker_id="test-worker",
        topics=[ExternalTaskTopic(name="order-detail", handler="order-detail")],
        client=CamundaClient(transport=transport),
        **kwargs,
    )
    worker.register("order-detail", handler)
    return worker


# ---------------------------------------------------------------------------
# 1. Konfigurasi topic
# ---------------------------------------------------------------------------


def test_parse_topics_skips_disabled_and_invalid():
    section = {
        "topics": [
            {"topic": "order-detail", "handler": "order_detail", "variables": ["orderId"]},
            {"topic": "disabled-topic", "enabled": False},
            {"handler": "tanpa-nama-topic"},
            "bukan-dict",
            {"name": "alias-name", "lock_duration_ms": 1000, "variables": ["a", "", "b"]},
        ]
    }
    topics = parse_topics(section)
    assert [t.name for t in topics] == ["order-detail", "alias-name"]
    assert topics[0].variables == ["orderId"]
    assert topics[1].variables == ["a", "b"]  # entri kosong dibuang
    assert topics[1].lock_duration_ms == 1000


def test_topic_fetch_body_contains_lock_and_variables():
    topic = ExternalTaskTopic.from_dict(
        {
            "topic": "order-detail",
            "handler": "order_detail",
            "lock_duration_ms": 30000,
            "variables": ["orderId", "shopId"],
            "local_variables": True,
        }
    )
    body = topic.fetch_body("worker-1")
    assert body["topicName"] == "order-detail"
    assert body["lockDuration"] == 30000
    assert body["variables"] == ["orderId", "shopId"]
    assert body["localVariables"] is True
    assert topic.to_dict()["handler"] == "order_detail"


def test_external_task_config_from_file(camunda_env, tmp_path, monkeypatch):
    rules = tmp_path / "rules.json"
    rules.write_text(
        json.dumps(
            {
                "id": "x",
                "external_tasks": {
                    "enabled": True,
                    "worker_id": "w-test",
                    "topics": [{"topic": "order-detail", "handler": "order_detail"}],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))

    section = external_task_config()
    assert external_tasks_enabled(section) is True
    assert section["worker_id"] == "w-test"
    worker = ExternalTaskWorker.from_config()
    assert worker.worker_id == "w-test"
    assert [t.name for t in worker.topics] == ["order-detail"]
    assert "order-detail" in worker.handlers  # handler bawaan ter-resolve


# ---------------------------------------------------------------------------
# 2. fetch_and_lock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_and_lock_v7_body_and_parsing(camunda_env):
    transport, calls = _make_router_transport(
        {FETCH_PATH: (200, [_raw_task()])}
    )
    worker = _worker_with_handler(lambda task: None, transport, max_tasks=3)
    worker.topics[0].variables = ["orderId", "marketplace"]

    tasks, error = await worker.fetch_and_lock()

    assert error is None
    assert len(calls) == 1
    assert str(calls[0].url) == "http://camunda.test:8080/engine-rest/external-task/fetchAndLock"
    body = _body(calls[0])
    assert body["workerId"] == "test-worker"
    assert body["maxTasks"] == 3
    assert body["topics"][0]["topicName"] == "order-detail"
    assert body["topics"][0]["variables"] == ["orderId", "marketplace"]

    task = tasks[0]
    assert isinstance(task, ExternalTask)
    assert task.id == "task-1"
    assert task.topic_name == "order-detail"
    assert task.variables["orderId"] == "586198966233958181"
    assert task.business_key == "BK-1"
    assert task.retries == 3
    assert task.variable("orderId", "order_id") == "586198966233958181"
    assert task.text("order_id", "orderId") == "586198966233958181"


@pytest.mark.asyncio
async def test_fetch_and_lock_v8_uses_jobs_activation(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_version", "8")
    transport, calls = _make_router_transport(
        {
            "/jobs/activation": (
                200,
                {"jobs": [{"jobKey": "job-9", "type": "order-detail", "variables": {"a": 1}}]},
            )
        }
    )
    worker = _worker_with_handler(lambda task: None, transport, max_tasks=2)
    worker.topics[0].variables = ["a"]

    tasks, error = await worker.fetch_and_lock()

    assert error is None
    assert str(calls[0].url) == "http://camunda.test:8080/v2/jobs/activation"
    body = _body(calls[0])
    assert body["type"] == "order-detail"
    assert body["maxJobsToActivate"] == 2
    assert body["fetchVariable"] == ["a"]
    assert tasks[0].id == "job-9"


# ---------------------------------------------------------------------------
# 3. execute_task / poll_once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_task_completes_with_variables(camunda_env):
    transport, calls = _make_router_transport(
        {f"/external-task/task-1/complete": (204, {})}
    )

    async def handler(task: ExternalTask) -> dict:
        return {"orderItemCount": 2, "orderProducts": "A x1, B x1"}

    worker = _worker_with_handler(handler, transport)
    outcome = await worker.execute_task(ExternalTask.from_raw(_raw_task()))

    assert outcome.ok is True
    assert outcome.action == ACTION_COMPLETED
    assert outcome.handler == "handler"
    assert sorted(outcome.variables) == ["orderItemCount", "orderProducts"]

    body = _body(calls[0])
    assert body["workerId"] == "test-worker"
    assert body["variables"]["orderItemCount"] == {"value": 2, "type": "Long"}
    assert body["variables"]["orderProducts"]["value"] == "A x1, B x1"
    assert worker.total_completed == 1


@pytest.mark.asyncio
async def test_execute_task_handler_exception_reports_failure_with_retries(camunda_env):
    transport, calls = _make_router_transport(
        {f"/external-task/task-1/failure": (204, {})}
    )

    async def handler(task: ExternalTask) -> None:
        raise RuntimeError("APISIX mati")

    worker = _worker_with_handler(handler, transport)
    outcome = await worker.execute_task(ExternalTask.from_raw(_raw_task(retries=3)))

    assert outcome.ok is True  # laporan failure berhasil dikirim
    assert outcome.action == ACTION_FAILED
    assert "APISIX mati" in (outcome.error or "")
    assert worker.total_failed == 1

    body = _body(calls[0])
    assert body["workerId"] == "test-worker"
    assert body["retries"] == 2  # 3 - 1 (decrement_retries=true)
    assert "APISIX mati" in body["errorMessage"]
    assert "RuntimeError" in body["errorDetails"]


@pytest.mark.asyncio
async def test_execute_task_bpmn_error_uses_bpmn_endpoint(camunda_env):
    transport, calls = _make_router_transport(
        {f"/external-task/task-1/bpmnError": (204, {})}
    )

    async def handler(task: ExternalTask) -> ExternalTaskResult:
        return ExternalTaskResult.bpmn_error_result("ORDER_NOT_FOUND", "order hilang")

    worker = _worker_with_handler(handler, transport)
    outcome = await worker.execute_task(ExternalTask.from_raw(_raw_task()))

    assert outcome.action == ACTION_BPMN_ERROR
    body = _body(calls[0])
    assert body["errorCode"] == "ORDER_NOT_FOUND"
    # kode error ikut tercatat di outcome → tercetak di log worker
    assert "errorCode=ORDER_NOT_FOUND" in (outcome.error or "")
    assert worker.total_bpmn_errors == 1


@pytest.mark.asyncio
async def test_execute_task_without_handler_reports_failure(camunda_env):
    transport, calls = _make_router_transport(
        {f"/external-task/task-1/failure": (204, {})}
    )
    worker = _worker_with_handler(lambda task: None, transport)
    worker.handlers.clear()  # topic tanpa handler

    outcome = await worker.execute_task(ExternalTask.from_raw(_raw_task()))

    assert outcome.action == ACTION_NO_HANDLER
    assert "no handler for topic" in (outcome.error or "")
    assert "no handler registered" in _body(calls[0])["errorMessage"]


@pytest.mark.asyncio
async def test_poll_once_fetches_executes_and_completes(camunda_env):
    transport, calls = _make_router_transport(
        {
            FETCH_PATH: (200, [_raw_task()]),
            "/external-task/task-1/complete": (204, {}),
        }
    )

    async def handler(task: ExternalTask) -> ExternalTaskResult:
        return ExternalTaskResult.ok(
            variables={"orderDetailOk": True},
            variable_types={"orderDetailOk": "boolean"},
        )

    worker = _worker_with_handler(handler, transport)
    outcomes = await worker.poll_once()

    assert [o.action for o in outcomes] == [ACTION_COMPLETED]
    assert worker.total_fetched == 1
    assert worker.iterations == 1
    assert worker.last_error is None
    assert worker.last_poll_at is not None
    assert len(calls) == 2
    complete_body = _body(calls[1])
    assert complete_body["variables"]["orderDetailOk"] == {"value": True, "type": "Boolean"}


@pytest.mark.asyncio
async def test_poll_once_without_topics_reports_error(camunda_env):
    transport, calls = _make_router_transport({})
    worker = ExternalTaskWorker(topics=[])
    worker._client = None

    outcomes = await worker.poll_once()

    assert outcomes == []
    assert worker.last_error is not None
    assert calls == []


@pytest.mark.asyncio
async def test_worker_run_loop_stops_after_iterations(camunda_env):
    transport, calls = _make_router_transport({FETCH_PATH: (200, [])})
    worker = _worker_with_handler(
        lambda task: None,
        transport,
        poll_interval_seconds=0,
        idle_backoff_seconds=0,
    )

    await worker.run(max_iterations=3)

    assert worker.iterations == 3
    assert len(calls) == 3
    assert worker.is_running() is False


# ---------------------------------------------------------------------------
# 4. Status & singleton
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_reports_config_and_totals(camunda_env, tmp_path, monkeypatch):
    rules = tmp_path / "rules.json"
    rules.write_text(
        json.dumps(
            {
                "id": "x",
                "external_tasks": {
                    "enabled": False,
                    "worker_id": "w-status",
                    "max_tasks": 4,
                    "topics": [
                        {"topic": "order-detail", "handler": "order_detail"},
                        {"topic": "topik-tanpa-handler", "handler": "tidak-ada"},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))

    worker = get_worker(reload_config=True)
    try:
        status = worker.status()
        assert status["worker_id"] == "w-status"
        assert status["running"] is False
        assert status["config"]["max_tasks"] == 4
        assert "order-detail" in status["handlers"]
        assert status["unhandled_topics"] == ["topik-tanpa-handler"]
        assert status["totals"] == {
            "fetched": 0,
            "completed": 0,
            "failed": 0,
            "bpmn_errors": 0,
            "errors": 0,
        }
        assert external_tasks_enabled({"enabled": False}) is False
    finally:
        set_worker(None)


# ---------------------------------------------------------------------------
# 5. task_handlers
# ---------------------------------------------------------------------------


def test_load_handler_resolves_builtin_aliases_and_custom_path():
    assert load_handler("order_detail") is not None
    assert load_handler("order-detail") is not None
    assert load_handler("send-signal") is not None
    assert load_handler("stock_sync_part") is not None
    assert load_handler("service_webhook_test") is not None
    assert load_handler("service-webhook-test") is not None
    assert load_handler("tidak-ada") is None
    assert load_handler("") is None
    # path custom: modul valid → fungsi ikut ter-resolve
    assert load_handler("infrastructure.camunda.signals:resolve_signal_name") is not None
    assert load_handler("infrastructure.camunda.signals.resolve_signal_name") is not None
    assert load_handler("modul.tidak.ada:fungsi") is None
    assert "order_detail" in available_handlers()
    assert "service_webhook_test" in available_handlers()


@pytest.mark.asyncio
async def test_handle_send_signal_delegates_to_service(camunda_env, monkeypatch):
    captured = {}

    async def fake_send_signal(**kwargs):
        captured.update(kwargs)
        return CamundaTriggerResult(
            ok=True,
            request_type="signal",
            signal_name="retur-order-on-hold",
            status_code=204,
        )

    from infrastructure.camunda.service import camunda_webhook_trigger

    monkeypatch.setattr(camunda_webhook_trigger, "send_signal", fake_send_signal)

    task = ExternalTask.from_raw(
        _raw_task(
            topicName="send-signal",
            variables={
                "marketplace": "tiktok",
                "orderStatus": "ON_HOLD",
                "orderId": "586198966233958181",
                "signalName": "retur-order-on-hold",
                "extra": "diteruskan",
            },
        )
    )
    result = await handle_send_signal(task)

    assert captured["marketplace"] == "tiktok"
    assert captured["order_status"] == "ON_HOLD"
    assert captured["signal_name"] == "retur-order-on-hold"
    assert captured["body"]["data"]["order_id"] == "586198966233958181"
    # variabel task diteruskan, kecuali variabel kontrol
    assert captured["variables"]["extra"] == "diteruskan"
    assert "signalName" not in captured["variables"]

    assert result.complete is True
    assert result.variables["signalName"] == "retur-order-on-hold"
    assert result.variables["signalSent"] is True
    assert result.variables["signalStatus"] == 204


@pytest.mark.asyncio
async def test_handle_send_signal_unmapped_status_reports_variable(camunda_env, monkeypatch):
    async def fake_send_signal(**kwargs):
        return CamundaTriggerResult(
            ok=True,
            request_type="skip",
            skipped_reason="signal_not_configured",
        )

    from infrastructure.camunda.service import camunda_webhook_trigger

    monkeypatch.setattr(camunda_webhook_trigger, "send_signal", fake_send_signal)

    task = ExternalTask.from_raw(
        _raw_task(topicName="send-signal", variables={"orderStatus": "IN_TRANSIT"})
    )
    result = await handle_send_signal(task)

    assert result.complete is True  # bukan error teknis
    assert result.variables["signalSent"] is False
    assert result.variables["signalSkippedReason"] == "signal_not_configured"


@pytest.mark.asyncio
async def test_handle_stock_sync_part_requires_numeric_part_id(camunda_env):
    missing = await handle_stock_sync_part(ExternalTask.from_raw(_raw_task(variables={})))
    assert missing.bpmn_error is True
    assert missing.error_code == "PART_ID_MISSING"

    invalid = await handle_stock_sync_part(
        ExternalTask.from_raw(_raw_task(variables={"partId": "abc"}))
    )
    assert invalid.bpmn_error is True
    assert invalid.error_code == "PART_ID_MISSING"


# ---------------------------------------------------------------------------
# 6. service_webhook_test — self-call POST /run-part (pipeline MANUAL part)
# ---------------------------------------------------------------------------


def _run_part_payload(part_id: int = 42, status: str = "success") -> dict:
    """Respons POST /api/v2/stock-sync/run-part (StockSyncManualPartResponse)."""
    return {
        "status": status,
        "run_mode": "MANUAL_PART",
        "trigger_source": "MANUAL_PART",
        "log_source": "MANUAL_PART",
        "part_id": part_id,
        "scope": {
            "part_id": part_id,
            "mappings": 1,
            "variant_ids": ["var-1", "var-2"],
            "product_ids": ["prod-1"],
            "inactive_mapping_ids": [],
            "missing_variant_ids": [],
            "deleted_variant_ids": [],
        },
        "cycle": {
            "cycle_index": 1,
            "total_report_rows": 2,
            "total_queued": 1,
            "total_pushed": 1,
            "total_logged": 1,
            "total_skipped_unchanged": 1,
            "total_skipped_pending": 0,
            "push_enabled": True,
            "failures": [],
        },
        "message": f"Run manual part_id={part_id} selesai",
    }


def _run_part_request_body(request) -> dict:
    """BODY JSON request POST /run-part (batch part_id, bukan query param)."""
    import json as _json

    return _json.loads(request.content.decode("utf-8"))


# Kelas httpx ASLI — disimpan sekali saat import supaya _patch_self_call boleh
# dipanggil berulang dalam satu test (patch ke-2 tetap memakai kelas asli,
# bukan factory dari patch sebelumnya).
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _patch_self_call(monkeypatch, transport):
    """Ganti httpx.AsyncClient yang dipakai handler → MockTransport (tanpa jaringan)."""

    def factory(**kwargs):
        kwargs["transport"] = transport
        return _REAL_ASYNC_CLIENT(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


async def _run_service_task(variables: dict):
    return await handle_service_webhook_test(
        ExternalTask.from_raw(
            _raw_task(topicName="serviceWebhookTest", variables=variables)
        )
    )


@pytest.mark.asyncio
async def test_service_webhook_test_requires_numeric_part_id(camunda_env):
    for variables in ({}, {"partId": "abc"}, {"partId": "0"}):
        result = await _run_service_task(variables)
        assert result.bpmn_error is True
        assert result.error_code == "PART_ID_MISSING"


def test_from_raw_unwraps_typed_variables():
    """fetchAndLock Camunda 7 mengirim variabel TYPED {type, value, valueInfo}
    — wajib di-unwrap supaya handler dapat nilai aslinya (regresi produksi:
    partId datang sebagai dict → PART_ID_MISSING 0ms padahal variabel ADA)."""
    task = ExternalTask.from_raw(
        _raw_task(
            variables={
                "partId": {"type": "Long", "value": 42, "valueInfo": {}},
                "marketplace": {"type": "String", "value": "tiktok", "valueInfo": {}},
                "nullVar": {"type": "Null", "value": None, "valueInfo": {}},
                # payload polos (uji coba / Camunda 8) tetap dipertahankan
                "polos": "langsung",
                # objek biasa yang kebetulan punya kunci type/value → JANGAN di-unwrap
                "nested": {"type": "inner", "value": "v", "extra": 1},
            }
        )
    )

    assert task.variable("partId") == 42
    assert task.text("marketplace") == "tiktok"
    assert task.variable("nullVar") is None  # None → dianggap tidak ada
    assert task.variable("polos") == "langsung"
    assert task.variable("nested") == {"type": "inner", "value": "v", "extra": 1}


@pytest.mark.asyncio
async def test_service_webhook_test_reads_typed_part_id(camunda_env, monkeypatch):
    """Regresi end-to-end: partId TYPED {type: Long, value: 42} → /run-part
    dipanggil dengan 42 dan task di-COMPLETE."""
    transport, calls = _make_router_transport(
        {"/api/v2/stock-sync/run-part": (200, _run_part_payload(part_id=42))}
    )
    _patch_self_call(monkeypatch, transport)
    monkeypatch.setattr(settings, "backend_base_url", "http://backend.test:8000")

    task = ExternalTask.from_raw(
        _raw_task(
            topicName="serviceWebhookTest",
            variables={"partId": {"type": "Long", "value": 42, "valueInfo": {}}},
        )
    )
    result = await handle_service_webhook_test(task)

    assert result.complete is True and result.bpmn_error is False
    assert _run_part_request_body(calls[0])["part_id"] == [42]
    # ``partId`` dilaporkan sebagai ARRAY (satu product → banyak part_id).
    assert result.variables["partId"] == [42]
    assert result.variables["partIds"] == [42]
    # Webhook service task selalu memakai pipeline scoped; scheduler global
    # adalah satu-satunya jalur full pipeline.
    assert _run_part_request_body(calls[0])["full_pipeline"] is False


@pytest.mark.asyncio
async def test_service_webhook_test_hits_run_part_and_completes(
    camunda_env, monkeypatch
):
    transport, calls = _make_router_transport(
        {"/api/v2/stock-sync/run-part": (200, _run_part_payload())}
    )
    _patch_self_call(monkeypatch, transport)
    monkeypatch.setattr(settings, "backend_base_url", "http://backend.test:8000")

    result = await _run_service_task({"partId": "42"})

    # sukses → COMPLETE + variabel ringkasan pipeline
    assert result.complete is True and result.failure is False
    assert len(calls) == 1
    assert calls[0].method == "POST"
    assert calls[0].url.path == "/api/v2/stock-sync/run-part"
    assert calls[0].url.scheme == "http"
    assert calls[0].url.host == "backend.test"
    assert calls[0].url.port == 8000
    # part_id TIDAK lewat query — dikirim sebagai BODY array (batch).
    assert calls[0].url.query == b""
    assert _run_part_request_body(calls[0])["part_id"] == [42]

    assert result.variables["partId"] == [42]
    assert result.variables["pipelineStatus"] == "success"
    assert result.variables["pipelineRunMode"] == "MANUAL_PART"
    assert result.variables["pipelineVariantCount"] == 2
    assert result.variables["pipelineReportRows"] == 2
    assert result.variables["pipelinePushed"] == 1
    assert result.variables["pipelineFailures"] == []
    assert result.variables["pipelinePushEnabled"] is True


@pytest.mark.asyncio
async def test_service_webhook_test_part_id_returned_as_string_type(
    camunda_env, monkeypatch
):
    """partId yang ditulis ulang saat COMPLETE harus bertipe String, bukan Json.

    Regresi produksi: completion menulis ``partId`` sebagai list TANPA tipe →
    Camunda menyimpannya sebagai Json (Spin) → task BERIKUTNYA
    (``serviceWebhookCancel`` setelah signal cancel) membaca deskriptor node
    dari fetchAndLock dan kena BPMN error PART_ID_MISSING padahal variabel ada.
    """
    import json as _json

    from infrastructure.camunda.client import CamundaClient

    transport, calls = _make_router_transport(
        {"/api/v2/stock-sync/run-part": (200, _run_part_payload())}
    )
    _patch_self_call(monkeypatch, transport)
    monkeypatch.setattr(settings, "backend_base_url", "http://backend.test:8000")

    result = await _run_service_task({"partId": "[3000100, 3000101]"})

    assert result.complete is True
    assert result.variable_types.get("partId") == "String", "partId wajib String"

    # Nilai yang benar-benar dikirim ke Camunda = TEKS JSON array bertipe String
    # (bukan list yang akan di-inferensi Camunda sebagai Json).
    body = CamundaClient().build_variables(result.variables, result.variable_types)
    assert body["partId"]["type"] == "String"
    assert _json.loads(body["partId"]["value"]) == [3000100, 3000101]


@pytest.mark.asyncio
async def test_service_webhook_test_404_and_503_are_retryable_failures(
    camunda_env, monkeypatch
):
    """404 (mapping kosong) & 503 (kill-switch) → FAILURE, BUKAN BPMN error.

    Di Camunda 7, BPMN error yang tidak ditangkap boundary error / event
    subprocess akan MENGAKHIRI process instance — masalah teknis jadi tampak
    seperti keputusan bisnis dan instance "hilang" tanpa pipeline pernah jalan.
    Dengan FAILURE: task di-retry sesuai `retries` dan instance tetap hidup.
    """
    transport, calls = _make_router_transport(
        {"/run-part": (404, {"detail": "part_id=9 tidak punya variant"})}
    )
    _patch_self_call(monkeypatch, transport)

    missing = await _run_service_task({"partId": "9"})

    assert missing.failure is True and missing.complete is False
    assert missing.bpmn_error is False
    assert "404" in missing.error_message
    assert "PART_ID_UNMAPPED" in missing.error_message
    assert "tidak punya variant" in missing.error_details
    assert len(calls) == 1

    transport, _ = _make_router_transport(
        {"/run-part": (503, {"detail": "Stock sync disabled via stock_sync_enabled=false"})}
    )
    _patch_self_call(monkeypatch, transport)

    disabled = await _run_service_task({"partId": "9"})
    assert disabled.failure is True
    assert disabled.bpmn_error is False
    assert "503" in disabled.error_message
    assert "STOCK_SYNC_DISABLED" in disabled.error_message


@pytest.mark.asyncio
async def test_service_webhook_test_server_error_reports_failure(
    camunda_env, monkeypatch
):
    transport, _calls = _make_router_transport({"/run-part": (500, {"detail": "boom"})})
    _patch_self_call(monkeypatch, transport)

    result = await _run_service_task({"partId": "42"})

    assert result.failure is True and result.complete is False
    assert "500" in result.error_message  # → engine retry


@pytest.mark.asyncio
async def test_service_webhook_test_pipeline_error_status_fails(
    camunda_env, monkeypatch
):
    """2xx TAPI response status=error (ada failures) → belum berhasil → retry."""
    payload = _run_part_payload(status="error")
    payload["message"] = "failures=1"
    transport, _calls = _make_router_transport({"/run-part": (200, payload)})
    _patch_self_call(monkeypatch, transport)

    result = await _run_service_task({"partId": "42"})

    assert result.failure is True
    assert "status=error" in result.error_message


@pytest.mark.asyncio
async def test_service_webhook_test_network_error_reports_failure(
    camunda_env, monkeypatch
):
    async def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    _patch_self_call(monkeypatch, httpx.MockTransport(boom))

    result = await _run_service_task({"partId": "42"})

    assert result.failure is True
    assert "gagal hit backend" in result.error_message


# ---------------------------------------------------------------------------
# 7. _patch_reserved_stock_from_camunda — per-VARIANT, bukan total product
# ---------------------------------------------------------------------------


def _sql_update_params(session):
    """Kumpulkan {marketplace_variant_id: total_reserved_stock} dari UPDATE."""
    updates = {}
    for call in session.execute.call_args_list:
        stmt = call.args[0] if call.args else None
        try:
            params = stmt.compile().params
        except Exception:
            continue
        if "total_reserved_stock" in params and "marketplace_variant_id_1" in params:
            updates[params["marketplace_variant_id_1"]] = params["total_reserved_stock"]
    return updates


@pytest.mark.asyncio
async def test_patch_reserved_stock_from_camunda_writes_per_variant(
    camunda_env, monkeypatch
):
    """Regresi: ``reserved_products`` = total per PRODUCT, jadi patch WAJIB
    menulis nilai PER-VARIANT dari APISIX ``/order`` — bukan menulis total
    produk ke SETIAP variant (bug lama untuk product multi-variant)."""
    from infrastructure.camunda.task_handlers import _patch_reserved_stock_from_camunda

    rows = [
        SimpleNamespace(
            marketplace_variant_id="INT-V1", external_variant_id="EXT-V1",
            external_product_id="EXT-P1", shop_id="SHOP-A", code="shopee",
        ),
        SimpleNamespace(
            marketplace_variant_id="INT-V2", external_variant_id="EXT-V2",
            external_product_id="EXT-P1", shop_id="SHOP-A", code="shopee",
        ),
    ]
    read_result = MagicMock()
    read_result.all.return_value = rows

    session = MagicMock()
    session.commit = AsyncMock()

    def _execute(*args, **kwargs):
        stmt = str(args[0]) if args else ""
        if "marketplace_product" in stmt and "marketplace_variant" in stmt:
            return read_result
        return MagicMock()

    session.execute = AsyncMock(side_effect=_execute)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(
        "infrastructure.db.database.async_session_factory",
        MagicMock(return_value=cm),
    )

    async def _fake_fetch(
        self, marketplace, shop_uuid, product_id=None, variant_id=None
    ):
        return {
            "marketplace": marketplace,
            "sweep_complete": True,
            "reserved_stock": [
                {
                    "product_id": product_id,
                    "variants": [
                        {"variant_id": "EXT-V1", "total_reserved_stock": 2},
                    ],
                }
            ],
        }

    monkeypatch.setattr(
        ApisixClient, "fetch_reserved_stock", _fake_fetch, raising=True
    )

    # Variabel Camunda hanya memuat TOTAL per product (5) — TIDAK boleh ditulis
    # apa adanya ke tiap variant.
    await _patch_reserved_stock_from_camunda(42, {"EXT-P1": 5})

    updates = _sql_update_params(session)
    assert updates == {"INT-V1": 2, "INT-V2": 0}
    session.commit.assert_awaited()


# ---------------------------------------------------------------------------
# 8. partId ARRAY (satu product marketplace → banyak part_id)
# ---------------------------------------------------------------------------


def test_extract_part_ids_accepts_array_json_and_scalar():
    """``partId`` boleh array/list, string JSON array, atau skalar (lama)."""
    from infrastructure.camunda.task_handlers import _extract_part_ids

    assert _extract_part_ids([3000100, 3000101]) == [3000100, 3000101]
    assert _extract_part_ids("[3000100, 3000101]") == [3000100, 3000101]
    assert _extract_part_ids("  [ 7, 9 ]  ") == [7, 9]
    assert _extract_part_ids(42) == [42]
    assert _extract_part_ids("42") == [42]
    # Bungkus typed yang belum ter-unwrap → ambil ``value``-nya.
    assert _extract_part_ids({"value": "[7, 9]"}) == [7, 9]
    assert _extract_part_ids({"value": 42}) == [42]
    # Non-numerik / <= 0 / duplikat dibuang.
    assert _extract_part_ids([0, -3, "abc", 7, 7, "9"]) == [7, 9]
    assert _extract_part_ids(None) == []
    assert _extract_part_ids("") == []
    assert _extract_part_ids("{bukan json}") == []


def test_extract_part_ids_rejects_json_node_descriptor():
    """Deskriptor node JSON (artefak tipe Json Camunda Spin) → [] + warning.

    Regresi produksi: ``partId`` bertipe ``Json`` di-fetchAndLock-kan kembali
    sebagai objek deskriptor (``nodeType``/``dataFormatName``/``array``…)
    sehingga isi array HILANG → service task gagal ``PART_ID_MISSING``.
    Dengan ``variable_types.partId=string`` ini tidak lagi terjadi, tapi
    parser tetap menolak nilai tak-terbaca itu dengan jelas.
    """
    from infrastructure.camunda.task_handlers import _extract_part_ids

    descriptor = {
        "value": False,
        "array": True,
        "null": False,
        "dataFormatName": "application/json",
        "object": False,
        "boolean": False,
        "number": False,
        "string": False,
        "nodeType": "ARRAY",
    }
    assert _extract_part_ids(descriptor) == []
    # Dict tanpa kunci `value` → juga kosong (bukan angka).
    assert _extract_part_ids({"foo": 1}) == []


@pytest.mark.asyncio
async def test_service_webhook_test_forwards_all_part_ids(camunda_env, monkeypatch):
    """partId (teks JSON array) → SATU self-call dengan SEMUA part_id + full_pipeline."""
    transport, calls = _make_router_transport(
        {
            "/api/v2/stock-sync/run-part": (
                200,
                _run_part_payload(part_id=3000100),
            )
        }
    )
    _patch_self_call(monkeypatch, transport)
    monkeypatch.setattr(settings, "backend_base_url", "http://backend.test:8000")

    # Bentuk produksi: variabel String berisi teks JSON array (config.json
    # variable_types.partId = "string"), tetapi bentuk typed Camunda tetap
    # diterima juga untuk kompatibilitas.
    result = await _run_service_task(
        {
            "partId": {
                "type": "String",
                "value": "[3000100, 3000101, 3000102]",
                "valueInfo": {},
            }
        }
    )

    assert result.complete is True and result.failure is False
    assert len(calls) == 1
    body = _run_part_request_body(calls[0])
    assert body["part_id"] == [3000100, 3000101, 3000102]
    assert body["full_pipeline"] is False
    assert result.variables["partId"] == [3000100, 3000101, 3000102]
    assert result.variables["partIds"] == [3000100, 3000101, 3000102]


@pytest.mark.asyncio
async def test_service_webhook_test_empty_part_id_array_is_bpmn_error(
    camunda_env, monkeypatch
):
    """Array kosong / tanpa angka valid → BPMN ERROR PART_ID_MISSING."""
    transport, calls = _make_router_transport({})
    _patch_self_call(monkeypatch, transport)

    for raw in ({}, {"partId": "[]"}, {"partId": [0, -1]}, {"partId": "abc"}):
        result = await _run_service_task(raw)
        assert result.bpmn_error is True
        assert result.error_code == "PART_ID_MISSING"
    assert calls == []
