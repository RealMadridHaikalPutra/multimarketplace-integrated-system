"""
Test integrasi WEBHOOK → CAMUNDA (paket camunda/).

Cakupan:
  1. rules.py — pemilihan rule (marketplace / signal_codes / conditions),
     mapping variabel (path / template / literal / first / default),
     placeholder header, dan hot-reload file aturan.
  2. client.py — URL & body untuk Camunda 7 (engine-rest) dan Camunda 8
     (Zeebe REST), variabel bertipe, business key, retry 5xx, tanpa retry 4xx.
  3. service.py — orkestrasi (ok / disabled / tanpa rule cocok) + log JSONL.
  4. api/routers/webhook.py — webhook mentah menjadwalkan trigger Camunda
     sebagai background task dan tetap membalas 200.
"""

import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interfaces.http.routers import webhook as webhook_module
from infrastructure.camunda.client import CamundaClient, CamundaConfigError, infer_variable_type
from infrastructure.camunda.order_detail import extract_webhook_order_status
from infrastructure.camunda.rules import load_rules, resolve_variables, select_rule
from infrastructure.camunda.service import CamundaWebhookTrigger, camunda_webhook_trigger
from core.config import settings

ENDPOINT = "/api/v1/webhook/order/status"


def _configured_process_key() -> str:
    """Process definition key AKTIF dibaca dari camunda/config.json (bentuk
    flat) supaya test tidak usang saat target proses diganti."""
    config_path = Path(__file__).resolve().parent.parent / "infrastructure" / "camunda" / "config.json"
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "serviceWebhook"
    keys = data.get("process_definition_key")
    if isinstance(keys, list):
        return str(keys[0]) if keys else "serviceWebhook"
    return str(keys) if keys else "serviceWebhook"


PROCESS_KEY = _configured_process_key()
CAMUNDA_START_URL = (
    f"http://camunda.test:8080/engine-rest/process-definition/key/{PROCESS_KEY}/start"
)

# Payload MENTAH ASLI hasil capture dari logs/webhook/order_webhook.log
# (Shopee code=3 dan TikTok type=1). Perhatikan: TIDAK ada field resi/courier —
# mapping memakai fallback 'first' (lihat camunda/config.json).
SHOPEE_BODY = {
    "code": 3,
    "data": {
        "items": [],
        "update_time": 1736323997,
        "completed_scenario": "",
        "ordersn": "2501080NKAMXA8",
        "status": "UNPAID",
    },
    "msg_id": "85bb37f009e143af84852e17d50b572d",
    "shop_id": 341431138,
    "timestamp": 1736323998,
}
TIKTOK_BODY = {
    "type": 1,
    "tts_notification_id": "7688249035767170823",
    "data": {
        "order_id": "586196597913388837",
        "order_status": "UNPAID",
        "is_on_hold_order": False,
        "update_time": 1790059970,
    },
    "shop_id": "7494709429666874412",
    "timestamp": 1790059971,
}

HEADERS = {
    "X-Forwarded-By": "apisix-webhook-forwarder",
    "X-Webhook-Source": "shopee",
    "X-Webhook-Target": "order-status-backend",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transport(responses):
    """MockTransport async: respons diambil berurutan (terakhir diulang)."""
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        index = min(len(calls) - 1, len(responses) - 1)
        status, payload = responses[index]
        return httpx.Response(status, json=payload)

    return httpx.MockTransport(handler), calls


def _context(marketplace, signal_code, body, headers=None):
    """Konteks seperti yang dibangun `trigger_all` (termasuk status order dari
    payload — dipakai gate status order)."""
    from infrastructure.camunda.service import CamundaWebhookTrigger as _T

    return _T()._build_context(
        marketplace,
        signal_code,
        body,
        headers or {},
        order_status=extract_webhook_order_status(body),
    )


@pytest.fixture
def camunda_env(tmp_path, monkeypatch):
    """Setting Camunda default untuk test (base_url dummy, log ke tmp)."""
    monkeypatch.setattr(settings, "camunda_enabled", True)
    monkeypatch.setattr(settings, "camunda_base_url", "http://camunda.test:8080")
    monkeypatch.setattr(settings, "camunda_version", "7")
    monkeypatch.setattr(settings, "camunda_rest_path", "")
    monkeypatch.setattr(settings, "camunda_auth_type", "none")
    monkeypatch.setattr(settings, "camunda_max_attempts", 1)
    monkeypatch.setattr(settings, "camunda_retry_delay_seconds", 0)
    monkeypatch.setattr(settings, "camunda_typed_variables", True)
    monkeypatch.setattr(settings, "camunda_tenant_id", "")
    monkeypatch.setattr(settings, "camunda_process_definition_key", "")
    monkeypatch.setattr(settings, "camunda_process_definition_id", "")
    monkeypatch.setattr(settings, "camunda_message_name", "")
    monkeypatch.setattr(settings, "camunda_business_key_template", "")
    monkeypatch.setattr(settings, "camunda_rules_file", "infrastructure/camunda/config.json")
    monkeypatch.setattr(settings, "camunda_log_enabled", True)
    monkeypatch.setattr(settings, "camunda_log_dir", str(tmp_path))
    monkeypatch.setattr(settings, "camunda_log_filename", "camunda_trigger.log")
    # Test di file ini fokus webhook → Camunda; fetch DETAIL ORDER dari APISIX
    # diuji terpisah di tests/test_camunda_order_detail.py (tanpa network).
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    return tmp_path


def _log_lines(tmp_path):
    path = Path(tmp_path) / settings.camunda_log_filename
    assert path.exists(), f"log trigger Camunda tidak dibuat: {path}"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ===========================================================================
# 1. rules.py
# ===========================================================================


def test_flat_config_has_no_marketplace_or_signal_filter(camunda_env):
    """config.json bentuk dinamis: TANPA filter marketplace/signal code —
    tapi GATE STATUS ORDER (`order_status` di config.json) tetap berlaku."""
    service = CamundaWebhookTrigger()
    cases = [
        ("shopee", 3, SHOPEE_BODY),
        ("tiktok", 1, TIKTOK_BODY),
        # marketplace & signal code BARU tetap jalan asalkan statusnya lolos gate
        ("lazada", 99, {"data": {"tracking_number": "TRK-1", "status": "UNPAID"}}),
    ]
    for marketplace, signal, payload in cases:
        plans = service._resolve_plans(_context(marketplace, signal, payload))
        assert len(plans) == 1, f"{marketplace}/{signal} harus menghasilkan 1 plan"
        assert plans[0].process_definition_key == PROCESS_KEY
        assert plans[0].rule_id == "retur-from-order-status"

    # ...tapi status di luar daftar / status yang tidak ada → DILEWATI.
    for blocked in (
        {"data": {"tracking_number": "TRK-2", "status": "CANCELLED"}},
        {"data": {"tracking_number": "TRK-3"}},  # tanpa status sama sekali
    ):
        assert service._resolve_plans(_context("lazada", 99, blocked)) == []

    # Bentuk flat → tidak ada aturan berfilter yang bisa dipilih
    assert select_rule(load_rules(), _context("shopee", 3, SHOPEE_BODY)) is None


def test_variables_mapped_from_real_raw_payload(camunda_env):
    """Mapping variabel mengikuti payload MENTAH asli (Shopee & TikTok)."""
    config = load_rules()
    flat_variables = CamundaWebhookTrigger._flat_rule(config)["variables"]
    default_variables = config["defaults"].get("variables") or {}

    shopee_ctx = _context("shopee", 3, SHOPEE_BODY, HEADERS)
    shopee_vars = resolve_variables(flat_variables, default_variables, shopee_ctx)

    # invoice jatuh ke ordersn (Shopee tidak mengirim tracking_number di push ini)
    assert shopee_vars["invoice"] == "2501080NKAMXA8"
    assert shopee_vars["courier"] == ""  # tidak ada di payload → default ""
    assert shopee_vars["retur_date"] == "1736323997" or shopee_vars["retur_date"] == 1736323997
    assert shopee_vars["orderId"] == "2501080NKAMXA8"
    assert shopee_vars["orderStatus"] == "UNPAID"
    assert shopee_vars["shopId"] == 341431138
    assert shopee_vars["marketplace"] == "shopee"
    assert shopee_vars["signalCode"] == 3

    tiktok_ctx = _context("tiktok", 1, TIKTOK_BODY)
    tiktok_vars = resolve_variables(flat_variables, default_variables, tiktok_ctx)
    assert tiktok_vars["invoice"] == "586196597913388837"
    assert tiktok_vars["courier"] == ""
    assert tiktok_vars["orderStatus"] == "UNPAID"
    assert tiktok_vars["shopId"] == "7494709429666874412"
    assert tiktok_vars["marketplace"] == "tiktok"

    # Marketplace lain dengan nama field berbeda tetap aman (array = fallback)
    other_ctx = _context(
        "lazada",
        5,
        {"data": {"tracking_number": "TRK1", "courier": "JNE", "update_time": 1700000000}},
    )
    lazada_vars = resolve_variables(flat_variables, {}, other_ctx)
    assert lazada_vars["invoice"] == "TRK1"
    assert lazada_vars["courier"] == "JNE"
    # shopId = path level atas ("shop_id") — bukan string literal
    assert shopee_vars["shopId"] == 341431138


def test_variable_specs_path_template_literal_first_default(camunda_env):
    """Semua bentuk spesifikasi nilai variabel bekerja (lihat camunda/README.md)."""
    from infrastructure.camunda.rules import render_template  # noqa: F401  (dokumentasi API)

    ctx = _context("shopee", 3, SHOPEE_BODY, HEADERS)
    variables = resolve_variables(
        {
            "byPath": {"path": "data.ordersn"},
            "byFirst": {"first": [{"path": "data.order_id"}, {"path": "data.ordersn"}]},
            "byLiteral": {"literal": 12},
            "byTemplate": {"template": "{{marketplace}}/{{data.ordersn}}"},
            "withDefault": {"path": "data.missing_field", "default": "UNKNOWN"},
            "skipped": {"path": "data.tidak_ada"},
            "fromHeader": "{{headers.X-Webhook-Target}}",
            "singlePlaceholderNumeric": "{{shop_id}}",
        },
        {},
        ctx,
    )

    assert variables["byPath"] == "2501080NKAMXA8"
    assert variables["byFirst"] == "2501080NKAMXA8"  # fallback ke field yang ada
    assert variables["byLiteral"] == 12
    assert variables["byTemplate"] == "shopee/2501080NKAMXA8"
    assert variables["withDefault"] == "UNKNOWN"
    assert variables["fromHeader"] == "order-status-backend"
    # placeholder tunggal mempertahankan tipe asli (angka tetap angka)
    assert variables["singlePlaceholderNumeric"] == 341431138
    # path kosong tanpa default → variabel dilewati
    assert "skipped" not in variables


def test_conditions_filter_and_hot_reload(camunda_env, tmp_path, monkeypatch):
    """conditions menyaring rule + file aturan dibaca ulang tanpa restart."""
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {
                        "id": "only-ready-to-ship",
                        "marketplace": "shopee",
                        "signal_codes": [3],
                        "conditions": [{"path": "data.status", "op": "eq", "value": "UNPAID"}],
                        "process_definition_key": "ready-process",
                        "variables": {"orderId": {"path": "data.ordersn"}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))

    assert select_rule(load_rules(), _context("shopee", 3, SHOPEE_BODY))["id"] == "only-ready-to-ship"

    changed = dict(SHOPEE_BODY)
    changed["data"] = {"ordersn": "X", "status": "CANCELLED"}
    assert select_rule(load_rules(), _context("shopee", 3, changed)) is None

    # Edit file → langsung berlaku (mtime berubah)
    rules_file.write_text(
        json.dumps(
            {
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {
                        "id": "cancelled-only",
                        "marketplace": "shopee",
                        "signal_codes": [3],
                        "process_definition_key": "cancel-process",
                        "variables": {},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    assert select_rule(load_rules(), _context("shopee", 3, SHOPEE_BODY))["id"] == "cancelled-only"


# ===========================================================================
# 2. client.py
# ===========================================================================


@pytest.mark.asyncio
async def test_client_v7_start_process_url_and_typed_variables(camunda_env):
    """URL/body Camunda 7 untuk start_process_instance (process key eksplisit)."""
    transport, calls = _make_transport([(200, {"id": "proc-1", "definitionId": "order:1"})])
    client = CamundaClient(transport=transport)

    result = await client.start_process_instance(
        "order-status-sync",
        {"orderId": "231111ABCDEF", "qty": 2, "flag": True},
        variable_types={"orderId": "String"},
        business_key="shopee-231111ABCDEF",
        rule_id="shopee-order-status",
    )

    assert result.ok is True
    assert result.status_code == 200
    assert result.url == (
        "http://camunda.test:8080/engine-rest/process-definition/key/order-status-sync/start"
    )
    assert len(calls) == 1

    body = json.loads(calls[0].content)
    assert body["businessKey"] == "shopee-231111ABCDEF"
    assert body["variables"]["orderId"] == {"value": "231111ABCDEF", "type": "String"}
    assert body["variables"]["qty"] == {"value": 2, "type": "Long"}
    assert body["variables"]["flag"] == {"value": True, "type": "Boolean"}


@pytest.mark.asyncio
async def test_client_coerces_value_to_declared_type(camunda_env):
    """Nilai diselaraskan dengan variable_types (anti HTTP 400 dari Camunda 7)."""
    transport, calls = _make_transport([(200, {"id": "proc-1"})])
    client = CamundaClient(transport=transport)

    result = await client.start_process_instance(
        "p",
        {"shopId": 2778101, "qty": "3", "flag": "true", "blob": {"a": 1}},
        variable_types={"shopId": "String", "qty": "Long", "flag": "Boolean", "blob": "Json"},
    )

    assert result.ok is True
    variables = json.loads(calls[0].content)["variables"]
    assert variables["shopId"] == {"value": "2778101", "type": "String"}
    assert variables["qty"] == {"value": 3, "type": "Long"}
    assert variables["flag"] == {"value": True, "type": "Boolean"}
    # Tipe Json WAJIB berupa teks JSON (string) — Camunda 7 menolak array/objek:
    #   Must provide 'null' or String value for value of SerializableValue type 'Json'.
    assert variables["blob"]["type"] == "Json"
    assert variables["blob"]["value"] == '{"a": 1}'
    assert json.loads(variables["blob"]["value"]) == {"a": 1}

    # Nilai yang TIDAK bisa dikonversi → tipe diminta diabaikan (pakai inferensi)
    transport2, calls2 = _make_transport([(200, {"id": "proc-2"})])
    await CamundaClient(transport=transport2).start_process_instance(
        "p", {"qty": "bukan-angka"}, variable_types={"qty": "Long"}
    )
    fallback = json.loads(calls2[0].content)["variables"]["qty"]
    assert fallback == {"value": "bukan-angka", "type": "String"}


@pytest.mark.asyncio
async def test_client_v8_uses_zeebe_rest_and_plain_variables(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_version", "8")
    transport, calls = _make_transport([(200, {"processInstanceKey": "2251799813685249"})])
    client = CamundaClient(transport=transport)

    result = await client.start_process_instance("order-status-sync", {"orderId": "A1"})

    assert result.ok is True
    assert result.url == "http://camunda.test:8080/v2/process-instances"
    body = json.loads(calls[0].content)
    assert body["processDefinitionId"] == "order-status-sync"
    assert body["variables"] == {"orderId": "A1"}  # v8: nilai polos, tanpa {value,type}
    assert infer_variable_type({"a": 1}) == "Json"


@pytest.mark.asyncio
async def test_client_message_endpoint_v7(camunda_env):
    transport, calls = _make_transport([(200, {"messageName": "ORDER_STATUS_RECEIVED"})])
    client = CamundaClient(transport=transport)

    result = await client.send_message(
        "ORDER_STATUS_RECEIVED", {"orderId": "A1"}, business_key="shopee-A1"
    )

    assert result.ok is True
    assert result.url == "http://camunda.test:8080/engine-rest/message"
    body = json.loads(calls[0].content)
    assert body["messageName"] == "ORDER_STATUS_RECEIVED"
    assert body["processVariables"]["orderId"]["value"] == "A1"
    assert body["businessKey"] == "shopee-A1"


@pytest.mark.asyncio
async def test_client_retries_on_5xx_but_not_on_4xx(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_max_attempts", 3)

    transport, calls = _make_transport(
        [(503, {"message": "engine down"}), (200, {"id": "proc-2"})]
    )
    result = await CamundaClient(transport=transport).start_process_instance("p", {"a": 1})

    assert result.ok is True
    assert result.attempts == 2
    assert len(calls) == 2

    transport_400, calls_400 = _make_transport([(400, {"message": "bad variables"})])
    failed = await CamundaClient(transport=transport_400).start_process_instance("p", {"a": 1})

    assert failed.ok is False
    assert failed.status_code == 400
    assert len(calls_400) == 1  # 400 tidak diretry
    assert "HTTP 400" in failed.error


@pytest.mark.asyncio
async def test_client_does_not_double_rest_path(camunda_env, monkeypatch):
    """base_url yang sudah memuat /engine-rest tidak boleh jadi dobel.

    Library external task client memakai default
    "http://localhost:8080/engine-rest", jadi pola ini sangat mungkin dipakai.
    """
    monkeypatch.setattr(settings, "camunda_base_url", "http://camunda.test:8080/engine-rest")
    transport, calls = _make_transport([(200, {"id": "proc-1"})])
    client = CamundaClient(transport=transport)

    result = await client.start_process_instance("Mirorim_Operasional.Trigger_Retur", {})

    assert result.ok is True
    assert result.url == (
        "http://camunda.test:8080/engine-rest/process-definition/key/"
        "Mirorim_Operasional.Trigger_Retur/start"
    )
    assert "engine-rest/engine-rest" not in str(calls[0].url)


@pytest.mark.asyncio
async def test_client_without_base_url_is_config_error(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_base_url", "")
    result = await CamundaClient(transport=_make_transport([(200, {})])[0]).start_process_instance("p", {})

    assert result.ok is False
    assert "CAMUNDA_BASE_URL" in result.error
    with pytest.raises(CamundaConfigError):
        CamundaClient().build_url("start_process", "p")


# ===========================================================================
# 3. service.py
# ===========================================================================


@pytest.mark.asyncio
async def test_service_triggers_retur_process_and_writes_log(camunda_env):
    """Webhook nyata → start Mirorim_Operasional.Trigger_Retur (payload referensi)."""
    transport, calls = _make_transport([(200, {"id": "proc-9"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(
        marketplace="shopee", signal_code=3, body=SHOPEE_BODY, headers=HEADERS, raw_body=json.dumps(SHOPEE_BODY)
    )

    assert result.ok is True
    assert result.rule_id == "retur-from-order-status"
    assert result.url == CAMUNDA_START_URL
    # businessKey = invoice:courier:retur_date (persis pola referensi)
    assert result.business_key == "2501080NKAMXA8::1736323997"

    body = json.loads(calls[0].content)
    assert body["businessKey"] == "2501080NKAMXA8::1736323997"
    # invoice & courier bertipe string ("type": "string" di config.json → String)
    assert body["variables"]["invoice"] == {"value": "2501080NKAMXA8", "type": "String"}
    assert body["variables"]["courier"] == {"value": "", "type": "String"}
    # shop_id angka Shopee tetap diselaraskan ke string oleh variable_types
    assert body["variables"]["shopId"] == {"value": "341431138", "type": "String"}

    # Catatan: baris log PERTAMA kini adalah auto-signal (send_on_webhook=true)
    # yang untuk UNPAID dilewati (tidak dipetakan) — ambil baris START EVENT-nya.
    entry = next(
        e for e in _log_lines(camunda_env)
        if e.get("rule_id") == "retur-from-order-status"
    )
    assert entry["ok"] is True and entry["rule_id"] == "retur-from-order-status"
    assert entry["variables"]["invoice"] == "2501080NKAMXA8"
    assert json.loads(entry["raw_body"]) == SHOPEE_BODY
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_service_runs_all_matching_rules_in_mode_all(camunda_env, tmp_path, monkeypatch):
    """mode 'all': satu webhook bisa men-start BEBERAPA proses sekaligus."""
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "mode": "all",
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {
                        "id": "proc-a",
                        "marketplace": "all",
                        "signal_codes": [3],
                        "process_definition_key": "proc-a",
                        "variables": {"orderId": {"path": "data.ordersn"}},
                    },
                    {
                        "id": "proc-b",
                        "marketplace": "shopee",
                        "signal_codes": [3],
                        "process_definition_key": "proc-b",
                        "type": "start",  # alias gaya helper JS
                        "variables": {"resi": {"path": "data.ordersn"}},
                        "variable_types": {"resi": "string"},  # lowercase
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))

    transport, calls = _make_transport([(200, {"id": "1"}), (200, {"id": "2"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    results = await service.trigger_all(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    assert [r.rule_id for r in results] == ["proc-a", "proc-b"]
    assert all(r.ok for r in results)
    assert [c.url.path for c in calls] == [
        "/engine-rest/process-definition/key/proc-a/start",
        "/engine-rest/process-definition/key/proc-b/start",
    ]
    # type "string" lowercase → tetap String
    assert json.loads(calls[1].content)["variables"]["resi"] == {
        "value": "2501080NKAMXA8", "type": "String"
    }
    # satu baris log per rule
    assert [e["rule_id"] for e in _log_lines(camunda_env)] == ["proc-a", "proc-b"]


@pytest.mark.asyncio
async def test_service_mode_first_runs_only_one_rule(camunda_env, tmp_path, monkeypatch):
    """mode 'first': hanya rule pertama yang jalan (perilaku lama)."""
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "mode": "first",
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {"id": "proc-a", "signal_codes": [3], "process_definition_key": "proc-a", "variables": {}},
                    {"id": "proc-b", "signal_codes": [3], "process_definition_key": "proc-b", "variables": {}},
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))

    transport, calls = _make_transport([(200, {"id": "1"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    results = await service.trigger_all(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    assert [r.rule_id for r in results] == ["proc-a"]
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_service_skips_when_disabled(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_enabled", False)
    transport, calls = _make_transport([(200, {})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    assert result.ok is True
    assert result.skipped_reason == "camunda_disabled"
    assert calls == []  # tidak ada HTTP call sama sekali
    assert _log_lines(camunda_env)[0]["skipped_reason"] == "camunda_disabled"


@pytest.mark.asyncio
async def test_service_runs_for_unknown_signal_and_marketplace(camunda_env):
    """Bentuk flat tidak memfilter signal/marketplace: kombinasi baru tetap jalan
    (asalkan status order-nya lolos gate `order_status` config.json)."""
    transport, calls = _make_transport([(200, {"id": "proc-x"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(
        marketplace="lazada",
        signal_code=42,
        body={"data": {"tracking_number": "TRK-9", "status": "UNPAID"}},
    )

    assert result.ok is True
    assert result.skipped_reason is None
    # retur_date tidak ada di payload → variabel dilewati (bukan literal "timestamp"),
    # courier punya default "" → segmen business key tetap ada
    assert result.business_key == "TRK-9::"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_service_skips_when_config_disabled(camunda_env, tmp_path, monkeypatch):
    disabled = tmp_path / "disabled.json"
    disabled.write_text(
        json.dumps(
            {
                "enabled": False,
                "process_definition_key": PROCESS_KEY,
                "variables": {"resi": ["data.ordersn"]},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(disabled))
    transport, calls = _make_transport([(200, {})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    assert result.ok is True
    assert result.skipped_reason == "config_disabled"
    assert calls == []


@pytest.mark.asyncio
async def test_service_process_definition_key_array(camunda_env, tmp_path, monkeypatch):
    """process_definition_key boleh ARRAY → satu instance per key."""
    rules_file = tmp_path / "array-keys.json"
    rules_file.write_text(
        json.dumps(
            {
                "id": "multi-process",
                "process_definition_key": ["proc-1", "proc-2"],
                "business_key": "{{var.resi}}",
                "variables": {"resi": ["data.ordersn", "data.order_id"]},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))

    transport, calls = _make_transport([(200, {"id": "1"}), (200, {"id": "2"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    results = await service.trigger_all(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)

    assert [r.process_key for r in results] == ["proc-1", "proc-2"]
    assert [c.url.path for c in calls] == [
        "/engine-rest/process-definition/key/proc-1/start",
        "/engine-rest/process-definition/key/proc-2/start",
    ]
    assert json.loads(calls[1].content)["businessKey"] == "2501080NKAMXA8"


def test_variable_array_chain_and_array_literal(camunda_env):
    """Array = rantai fallback; {"literal": [...]} = nilai array dikirim apa adanya."""
    ctx = _context("shopee", 3, {"data": {"ordersn": "A1", "items": ["x", "y"]}, "shop_id": 9})
    variables = resolve_variables(
        {
            "chain": ["data.tracking_number", "data.ordersn", {"literal": "-"}],
            "chainDefault": ["data.tidak_ada", {"literal": ""}],
            "arrayLiteral": {"literal": ["x", "y"]},
            "topLevelPath": ["shop_id"],
            "skippedAll": ["data.tidak_ada", "data.juga_tidak", "field_hilang"],
        },
        {},
        ctx,
    )

    assert variables["chain"] == "A1"
    assert variables["chainDefault"] == ""
    assert variables["arrayLiteral"] == ["x", "y"]
    assert variables["topLevelPath"] == 9
    assert "skippedAll" not in variables  # semua kandidat kosong → dilewati


@pytest.mark.asyncio
async def test_service_dry_run_does_not_call_camunda(camunda_env):
    transport, calls = _make_transport([(200, {})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY, dry_run=True)

    assert result.skipped_reason == "dry_run"
    assert result.variables["invoice"] == "2501080NKAMXA8"
    assert result.business_key == "2501080NKAMXA8::1736323997"
    assert calls == []
    assert _log_lines(camunda_env)[0]["dry_run"] is True


def test_service_describe_config(camunda_env):
    config = camunda_webhook_trigger.describe_config()
    assert config["base_url"] == "http://camunda.test:8080"
    assert config["version"] == "7"
    assert config["rules_file_exists"] is True
    assert config["config_style"] == "flat"
    assert config["process_definition_keys"] == [PROCESS_KEY]
    assert "invoice" in config["variables"] and "courier" in config["variables"]


@pytest.mark.asyncio
async def test_service_env_only_fallback_rule(camunda_env, monkeypatch):
    """Tanpa config.json + env diisi → tetap start process (rule env-default)."""
    monkeypatch.setattr(settings, "camunda_rules_file", str(camunda_env / "tidak-ada.json"))
    monkeypatch.setattr(settings, "camunda_process_definition_key", "env-process")
    monkeypatch.setattr(settings, "camunda_business_key_template", "{{marketplace}}-{{signal_code}}")
    transport, calls = _make_transport([(200, {"id": "proc-env"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(marketplace="shopee", signal_code=7, body=SHOPEE_BODY)

    assert result.ok is True
    assert result.rule_id == "env-default"
    assert result.business_key == "shopee-7"
    assert calls[0].url.path == "/engine-rest/process-definition/key/env-process/start"


# ===========================================================================
# 4. Router: webhook → background trigger
# ===========================================================================


@pytest.fixture
def webhook_client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "webhook_log_enabled", True)
    monkeypatch.setattr(settings, "webhook_log_dir", str(tmp_path))
    monkeypatch.setattr(settings, "camunda_enabled", True)

    app = FastAPI()
    app.include_router(webhook_module.router)
    with TestClient(app) as client:
        yield client


def test_endpoint_schedules_camunda_trigger(webhook_client, monkeypatch):
    """Webhook mentah wajib menjadwalkan trigger Camunda & tetap balas 200."""
    fake_trigger = AsyncMock()
    monkeypatch.setattr(camunda_webhook_trigger, "trigger_all", fake_trigger)

    response = webhook_client.post(ENDPOINT, params={"marketplace": "shopee"}, json=SHOPEE_BODY, headers=HEADERS)

    assert response.status_code == 200
    assert response.json()["camunda"] == "queued"

    # Background task sudah dijalankan oleh TestClient
    fake_trigger.assert_awaited_once()
    kwargs = fake_trigger.await_args.kwargs
    assert kwargs["marketplace"] == "shopee"
    assert kwargs["signal_code"] == 3
    assert kwargs["body"] == SHOPEE_BODY
    assert kwargs["headers"]["x-webhook-target"] == "order-status-backend"
    assert json.loads(kwargs["raw_body"]) == SHOPEE_BODY


def test_endpoint_reports_camunda_disabled(webhook_client, monkeypatch):
    monkeypatch.setattr(settings, "camunda_enabled", False)
    fake_trigger = AsyncMock()
    monkeypatch.setattr(camunda_webhook_trigger, "trigger_all", fake_trigger)

    response = webhook_client.post(ENDPOINT, json=TIKTOK_BODY)

    assert response.status_code == 200
    assert response.json()["camunda"] == "disabled"
    fake_trigger.assert_not_awaited()


def test_endpoint_camunda_config_status(webhook_client):
    response = webhook_client.get("/api/v1/webhook/camunda/config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["config_style"] == "flat"
    assert payload["process_definition_keys"] == [PROCESS_KEY]
    assert "camunda_trigger.log" in payload["log"]["file"]
    # Gate status order ikut terlihat (mudah dicek tanpa mengirim webhook).
    gate = payload["order_status_gate"]
    # Start event HANYA untuk UNPAID — status lain cukup mengirim signal.
    assert gate["rule_allowlist"] == ["UNPAID"]
    assert gate["global_allowlist"] == []
    assert gate["skip_reason"] == "order_status_not_allowed"


# ===========================================================================
# 5. Gate status order (start event hanya untuk status tertentu)
# ===========================================================================


@pytest.mark.asyncio
async def test_env_allowlist_blocks_other_status_before_calling_camunda(
    camunda_env, monkeypatch, caplog
):
    """WEBHOOK_ORDER_STATUS_ALLOWLIST: status di luar daftar → TIDAK ada panggilan
    Camunda, tercatat sebagai skip dengan alasan spesifik."""
    caplog.set_level(logging.INFO, logger="infrastructure.camunda.service")
    monkeypatch.setattr(settings, "webhook_order_status_allowlist", "READY_TO_SHIP")
    transport, calls = _make_transport([(200, {"id": "proc-gated"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(
        marketplace="shopee",
        signal_code=3,
        body=SHOPEE_BODY,  # status UNPAID → tidak ada di allowlist
        raw_body=json.dumps(SHOPEE_BODY),
    )

    assert result.ok is True
    assert result.skipped_reason == "order_status_not_allowed"
    assert calls == []  # Camunda tidak dipanggil sama sekali
    assert "tidak ada di WEBHOOK_ORDER_STATUS_ALLOWLIST" in caplog.text

    entry = _log_lines(camunda_env)[0]
    assert entry["skipped_reason"] == "order_status_not_allowed"
    assert json.loads(entry["raw_body"]) == SHOPEE_BODY


@pytest.mark.asyncio
async def test_env_allowlist_accepts_comma_separated_and_any_case(camunda_env, monkeypatch):
    """Format .env: dipisah koma + huruf kecil tetap cocok (case-insensitive)."""
    monkeypatch.setattr(settings, "webhook_order_status_allowlist", "unpaid, awaiting_shipment")
    transport, calls = _make_transport([(200, {"id": "proc-ok"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(marketplace="tiktok", signal_code=1, body=TIKTOK_BODY)

    assert result.ok is True
    assert result.skipped_reason is None
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_env_allowlist_blocks_payload_without_status(camunda_env, monkeypatch):
    """Status tidak ada di payload = tidak lolos gate (bukan dianggap boleh)."""
    monkeypatch.setattr(settings, "webhook_order_status_allowlist", "UNPAID")
    transport, calls = _make_transport([(200, {"id": "proc-none"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    result = await service.trigger(
        marketplace="tiktok", signal_code=1, body={"data": {"order_id": "X"}}
    )

    assert result.skipped_reason == "order_status_not_allowed"
    assert calls == []


@pytest.mark.asyncio
async def test_rule_order_status_filter_blocks_with_specific_reason(
    camunda_env, tmp_path, monkeypatch
):
    """`order_status` per aturan (bentuk rules): status di luar daftar dilewati,
    dan alasannya spesifik (`order_status_not_allowed`)."""
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {
                        "id": "unpaid-only",
                        "order_status": ["UNPAID"],
                        "process_definition_key": "unpaid-process",
                        "variables": {"orderId": {"path": "data.ordersn"}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))
    transport, calls = _make_transport([(200, {"id": "proc-rule"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    allowed = await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)
    assert allowed.skipped_reason is None
    assert len(calls) == 1

    blocked = await service.trigger(
        marketplace="shopee",
        signal_code=3,
        body={"code": 3, "data": {"ordersn": "X", "status": "CANCELLED"}},
    )
    assert blocked.skipped_reason == "order_status_not_allowed"
    assert len(calls) == 1  # tidak ada panggilan baru


@pytest.mark.asyncio
async def test_rule_exclude_order_status(camunda_env, tmp_path, monkeypatch):
    """`exclude_order_status` memblokir status tertentu walau status lain lolos."""
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "defaults": {"type": "start_process", "variables": {}},
                "rules": [
                    {
                        "id": "all-but-cancelled",
                        "exclude_order_status": ["CANCELLED"],
                        "process_definition_key": "proc",
                        "variables": {},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))
    transport, calls = _make_transport([(200, {"id": "proc-ex"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    ok = await service.trigger(marketplace="shopee", signal_code=3, body=SHOPEE_BODY)
    assert ok.skipped_reason is None

    # huruf kecil di payload tetap kena (perbandingan case-insensitive)
    blocked = await service.trigger(
        marketplace="shopee",
        signal_code=3,
        body={"code": 3, "data": {"ordersn": "X", "status": "cancelled"}},
    )
    assert blocked.skipped_reason == "order_status_not_allowed"


# ===========================================================================
# 6. Status EFEKTIF dari detail order (signal.status_fallback)
# ===========================================================================


@pytest.mark.asyncio
async def test_auto_signal_uses_order_detail_status_when_webhook_unpaid(
    camunda_env, tmp_path, monkeypatch
):
    """Kasus produksi: webhook `UNPAID` (lolos gate START) sementara order di
    gateway sudah `READY_TO_SHIP` → auto-signal memakai status DETAIL ORDER,
    bukan berhenti di `signal_not_configured`.

    Aturan produksi hanya memetakan status lanjutan (ON_HOLD/READY_TO_SHIP/…),
    jadi webhook yang statusnya sudah "ketinggalan" dulu kehilangan signalnya.
    """
    monkeypatch.setattr(settings, "webhook_order_status_allowlist", "")
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "defaults": {"type": "start_process"},
                "rules": [
                    {
                        "id": "start-unpaid",
                        "marketplace": "shopee",
                        "signal_codes": [3],
                        "order_status": ["UNPAID"],
                        "process_definition_key": PROCESS_KEY,
                        "variables": {
                            "orderId": ["data.ordersn"],
                            "orderStatus": ["data.status"],
                        },
                    }
                ],
                "signal": {
                    "enabled": True,
                    "send_on_webhook": True,
                    "by_status": {"READY_TO_SHIP": "readyToShip${invoice}"},
                    "by_marketplace": {},
                    "default_signal": "",
                    "status_fallback": {"enabled": True, "skip_statuses": []},
                    "variables": {"invoice": ["data.ordersn"]},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules_file))

    # Auto-signal dikirim SEBELUM start event (lihat trigger_all).
    transport, calls = _make_transport([(204, {}), (200, {"id": "proc-1"})])
    service = CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))

    async def fake_fetch_order_detail(marketplace, body):
        return {"ok": True, "source": "order_detail", "order_status": "READY_TO_SHIP"}

    monkeypatch.setattr(service, "_fetch_order_detail", fake_fetch_order_detail)

    results = await service.trigger_all(
        marketplace="shopee",
        signal_code=3,
        body=SHOPEE_BODY,
        raw_body=json.dumps(SHOPEE_BODY),
    )

    signal_result = results[0]
    assert signal_result.request_type == "signal"
    assert signal_result.signal_name == "readyToShip2501080NKAMXA8"
    assert signal_result.execution_id is None

    signal_body = json.loads(calls[0].content.decode())
    assert calls[0].url.path == "/engine-rest/signal"
    assert signal_body["name"] == "readyToShip2501080NKAMXA8"
    assert signal_body["variables"]["signalOrderStatus"]["value"] == "READY_TO_SHIP"
    assert signal_body["variables"]["signalStatusSource"]["value"] == "order_detail"

    # Start event tetap dijalankan dengan status WEBHOOK apa adanya.
    start_result = results[1]
    assert start_result.request_type == "start_process"
    assert start_result.variables["orderStatus"] == "UNPAID"
    assert "start" in str(calls[1].url)
