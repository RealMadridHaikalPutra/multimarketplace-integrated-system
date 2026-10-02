"""
Test HIT SIGNAL Camunda (paket camunda/, bagian `signal` di config.json).

Cakupan:
  1. signals.py — peta STATUS ORDER → NAMA SIGNAL (by_marketplace → by_status →
     default_signal), case-insensitive, dan alasan skip yang spesifik.
  2. client.py — URL/body signal: Camunda 7 broadcast (/signal, field "name"),
     Camunda 7 per-eksekusi (/execution/{id}/signal, field "signalName"),
     Camunda 8 (/v2/signals/broadcast, field "signalName").
  3. service.send_signal — variabel (mapping config + argumen eksplisit),
     business_key sebagai VARIABEL, dry-run, gate enabled, log JSONL (event=signal).
  4. Aturan config.json bertipe "signal" (type=signal) → plan signal.
  5. Auto-signal saat webhook masuk (`signal.send_on_webhook=true`).
  6. Korelasi signal → execution spesifik (`signal.execution_lookup`).
  7. Status EFEKTIF dari detail order (`signal.status_fallback`): status webhook
     yang tidak punya mapping (UNPAID) memakai status DETAIL ORDER APISIX
     (`orderDetailStatus`, mis. READY_TO_SHIP).
"""

import json
from pathlib import Path

import httpx
import pytest

from infrastructure.camunda.client import CamundaClient
from infrastructure.camunda.service import CamundaWebhookTrigger
from infrastructure.camunda.signals import (
    describe_signal_config,
    render_signal_name,
    resolve_signal_name,
    resolve_signal_target,
    signal_config,
    signal_send_on_webhook,
)
from core.config import settings

SIGNAL_SECTION = {
    "enabled": True,
    "send_on_webhook": False,
    "by_status": {
        "UNPAID": "retur-order-unpaid",
        "ON_HOLD": "retur-order-on-hold",
    },
    "by_marketplace": {
        "_comment": "contoh override",
        "tiktok": {"ON_HOLD": "retur-on-hold-tiktok"},
    },
    "default_signal": "",
    "variables": {
        "orderId": ["data.ordersn", "data.order_id"],
        "orderStatus": ["data.status", "data.order_status"],
        "marketplace": "{{marketplace}}",
    },
    "variable_types": {"orderId": "string"},
}

TIKTOK_BODY = {
    "type": 1,
    "data": {
        "order_id": "586198966233958181",
        "order_status": "ON_HOLD",
        "update_time": 1790070581,
    },
    "shop_id": "7494709429666874412",
}


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def camunda_env(tmp_path, monkeypatch):
    """Setting Camunda default test (base_url dummy, log ke tmp)."""
    monkeypatch.setattr(settings, "camunda_enabled", True)
    monkeypatch.setattr(settings, "camunda_base_url", "http://camunda.test:8080")
    monkeypatch.setattr(settings, "camunda_version", "7")
    monkeypatch.setattr(settings, "camunda_rest_path", "")
    monkeypatch.setattr(settings, "camunda_auth_type", "none")
    monkeypatch.setattr(settings, "camunda_max_attempts", 1)
    monkeypatch.setattr(settings, "camunda_retry_delay_seconds", 0)
    monkeypatch.setattr(settings, "camunda_typed_variables", True)
    monkeypatch.setattr(settings, "camunda_tenant_id", "")
    # Target proses dari .env dinolkan supaya hasil test tidak bergantung .env
    # (aturan di file sementara hanya mendefinisikan signal).
    monkeypatch.setattr(settings, "camunda_process_definition_key", "")
    monkeypatch.setattr(settings, "camunda_process_definition_id", "")
    monkeypatch.setattr(settings, "camunda_message_name", "")
    monkeypatch.setattr(settings, "camunda_business_key_template", "")
    monkeypatch.setattr(settings, "camunda_log_enabled", True)
    monkeypatch.setattr(settings, "camunda_log_dir", str(tmp_path))
    monkeypatch.setattr(settings, "camunda_log_filename", "camunda_trigger.log")
    return tmp_path


def _write_rules(tmp_path: Path, payload: dict) -> Path:
    """Tulis file aturan sementara (test tidak menyentuh camunda/config.json)."""
    target = Path(tmp_path)
    target.mkdir(parents=True, exist_ok=True)
    path = target / "rules.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _make_transport(responses):
    """MockTransport async: respons diambil berurutan (terakhir diulang)."""
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        index = min(len(calls) - 1, len(responses) - 1)
        status, payload = responses[index]
        return httpx.Response(status, json=payload)

    return httpx.MockTransport(handler), calls


def _log_lines(tmp_path: Path):
    path = tmp_path / settings.camunda_log_filename
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _service(transport):
    return CamundaWebhookTrigger(client_factory=lambda: CamundaClient(transport=transport))


# ---------------------------------------------------------------------------
# 1. signals.py — pemetaan status → signal
# ---------------------------------------------------------------------------


def test_resolve_signal_name_precedence():
    """by_marketplace menimpa by_status; status tak dipetakan → default/None."""
    assert resolve_signal_name(SIGNAL_SECTION, "tiktok", "ON_HOLD") == "retur-on-hold-tiktok"
    assert resolve_signal_name(SIGNAL_SECTION, "shopee", "ON_HOLD") == "retur-order-on-hold"
    assert resolve_signal_name(SIGNAL_SECTION, "tiktok", "UNPAID") == "retur-order-unpaid"
    assert resolve_signal_name(SIGNAL_SECTION, "tiktok", "CANCELLED") is None

    with_default = dict(SIGNAL_SECTION, default_signal="retur-default")
    assert resolve_signal_name(with_default, "shopee", "CANCELLED") == "retur-default"


def test_resolve_signal_name_status_case_and_whitespace_insensitive():
    assert resolve_signal_name(SIGNAL_SECTION, "shopee", "  on_hold ") == "retur-order-on-hold"
    assert resolve_signal_name(SIGNAL_SECTION, "", "unpaid") == "retur-order-unpaid"


def test_resolve_signal_target_reasons():
    name, reason = resolve_signal_target(SIGNAL_SECTION, "shopee", "ON_HOLD")
    assert (name, reason) == ("retur-order-on-hold", "")

    name, reason = resolve_signal_target(SIGNAL_SECTION, "shopee", "IN_TRANSIT")
    assert name is None and reason == "signal_not_configured"

    disabled = dict(SIGNAL_SECTION, enabled=False)
    name, reason = resolve_signal_target(disabled, "shopee", "ON_HOLD")
    assert name is None and reason == "signal_disabled"


def test_describe_signal_config_summarises_map():
    info = describe_signal_config({"signal": SIGNAL_SECTION})
    assert info["enabled"] is True
    assert info["statuses"] == ["ON_HOLD", "UNPAID"]
    assert info["by_marketplace"]["tiktok"] == {"ON_HOLD": "retur-on-hold-tiktok"}
    assert info["effective_map"]["tiktok:ON_HOLD"] == "retur-on-hold-tiktok"
    assert info["effective_map"]["ON_HOLD"] == "retur-order-on-hold"
    assert info["skip_reason"] == "signal_not_configured"


# ---------------------------------------------------------------------------
# 1b. signals.py — render placeholder NAMA signal dari variabel signal
# ---------------------------------------------------------------------------


def test_render_signal_name_dollar_and_curly_syntax():
    """Placeholder `${invoice}` (JS template literal) = `{{invoice}}`."""
    variables = {"invoice": "SH-1", "qty": 2, "flag": True}

    assert render_signal_name("readyToShip${invoice}", variables) == "readyToShipSH-1"
    assert render_signal_name("readyToShip{{invoice}}", variables) == "readyToShipSH-1"
    # awalan var. / variables. boleh ditulis atau tidak
    assert render_signal_name("readyToShip${var.invoice}", variables) == "readyToShipSH-1"
    assert render_signal_name("readyToShip${variables.invoice}", variables) == "readyToShipSH-1"
    # beberapa placeholder sekaligus + nilai bukan string
    assert render_signal_name("${qty}-${invoice}-${qty}", variables) == "2-SH-1-2"
    assert render_signal_name("flag-${flag}", variables) == "flag-true"


def test_render_signal_name_without_placeholder_is_untouched():
    assert render_signal_name("retur-order-on-hold", {"invoice": "SH-1"}) == "retur-order-on-hold"
    assert render_signal_name("retur-order-on-hold", None) == "retur-order-on-hold"
    assert render_signal_name(None, {}) is None


def test_render_signal_name_missing_variable_keeps_placeholder():
    """Variabel tidak ada → placeholder DIBIARKAN utuh (warning), bukan dipotong."""
    assert render_signal_name("readyToShip${invoice}", {}) == "readyToShip${invoice}"
    assert render_signal_name("${invoice}", {"other": "x"}) == "${invoice}"
    assert render_signal_name("${invoice}", {"invoice": None}) == "${invoice}"
    # hasil kosong (nilai "") → pemanggil wajib men-skip kirim signal
    assert render_signal_name("${invoice}", {"invoice": ""}) == ""
    assert render_signal_name("${invoice}", {"invoice": 0}) == "0"


def test_signal_section_loaded_from_config_file(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))

    section = signal_config()
    assert section["by_status"]["ON_HOLD"] == "retur-order-on-hold"
    assert signal_send_on_webhook(section) is False


# ---------------------------------------------------------------------------
# 2. client.py — URL & body signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_v7_broadcast_signal_url_and_body(camunda_env):
    transport, calls = _make_transport([(204, {})])
    client = CamundaClient(transport=transport)

    result = await client.send_signal(
        "retur-order-on-hold",
        {"orderId": "586198966233958181", "qty": 2},
        variable_types={"orderId": "String"},
    )

    assert result.ok is True
    assert result.request_type == "signal"
    assert result.signal_name == "retur-order-on-hold"
    assert result.url == "http://camunda.test:8080/engine-rest/signal"
    body = json.loads(calls[0].content.decode())
    assert body["name"] == "retur-order-on-hold"
    assert body["variables"]["orderId"] == {"value": "586198966233958181", "type": "String"}
    assert body["variables"]["qty"] == {"value": 2, "type": "Long"}
    assert "businessKey" not in body  # C7 signal tidak punya field ini


@pytest.mark.asyncio
async def test_client_v7_execution_signal_uses_signalname_field(camunda_env):
    transport, calls = _make_transport([(204, {})])
    client = CamundaClient(transport=transport)

    result = await client.send_signal("retur-x", {"a": 1}, execution_id="exec-123")

    assert result.url == "http://camunda.test:8080/engine-rest/execution/exec-123/signal"
    assert result.execution_id == "exec-123"
    body = json.loads(calls[0].content.decode())
    assert body["signalName"] == "retur-x"
    assert "name" not in body


@pytest.mark.asyncio
async def test_client_v8_broadcast_signal_url(camunda_env, monkeypatch):
    monkeypatch.setattr(settings, "camunda_version", "8")
    transport, calls = _make_transport([(204, {})])
    client = CamundaClient(transport=transport)

    result = await client.send_signal("retur-x", {"a": 1})

    assert result.url == "http://camunda.test:8080/v2/signals/broadcast"
    body = json.loads(calls[0].content.decode())
    assert body["signalName"] == "retur-x"
    assert "name" not in body


@pytest.mark.asyncio
async def test_client_v8_execution_signal_rejected(camunda_env, monkeypatch):
    """Camunda 8 tidak punya signal per-eksekusi → error jelas, tanpa HTTP call."""
    monkeypatch.setattr(settings, "camunda_version", "8")
    transport, calls = _make_transport([(204, {})])
    client = CamundaClient(transport=transport)

    result = await client.send_signal("retur-x", {}, execution_id="exec-1")

    assert result.ok is False
    assert "Camunda 8 tidak mendukung" in (result.error or "")
    assert calls == []


@pytest.mark.asyncio
async def test_client_signal_requires_name(camunda_env):
    transport, calls = _make_transport([(204, {})])
    client = CamundaClient(transport=transport)

    result = await client.send_signal("   ", {})

    assert result.ok is False
    assert "signal_name wajib diisi" in (result.error or "")
    assert calls == []


# ---------------------------------------------------------------------------
# 3. service.send_signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_signal_maps_status_to_signal_and_logs(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok",
        order_status="ON_HOLD",
        body=TIKTOK_BODY,
        raw_body=json.dumps(TIKTOK_BODY),
    )

    assert result.ok is True
    assert result.signal_name == "retur-on-hold-tiktok"  # override per marketplace
    assert result.status_code == 204
    assert len(calls) == 1

    body = json.loads(calls[0].content.decode())
    assert body["name"] == "retur-on-hold-tiktok"
    variables = {name: spec["value"] for name, spec in body["variables"].items()}
    # variabel dari mapping config (namespace data.*) + variabel otomatis
    assert variables["orderId"] == "586198966233958181"
    assert variables["orderStatus"] == "ON_HOLD"
    assert variables["marketplace"] == "tiktok"
    assert variables["signalName"] == "retur-on-hold-tiktok"
    assert variables["signalOrderStatus"] == "ON_HOLD"
    assert variables["signalMarketplace"] == "tiktok"

    lines = _log_lines(tmp_path)
    assert len(lines) == 1
    entry = lines[0]
    assert entry["event"] == "signal"
    assert entry["signal_name"] == "retur-on-hold-tiktok"
    assert entry["ok"] is True
    assert entry["request_type"] == "signal"


@pytest.mark.asyncio
async def test_send_signal_status_from_body_when_not_given(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(marketplace="shopee", body={"code": 3, "data": {"status": "UNPAID"}})

    assert result.signal_name == "retur-order-unpaid"
    assert json.loads(calls[0].content.decode())["name"] == "retur-order-unpaid"


@pytest.mark.asyncio
async def test_send_signal_explicit_name_overrides_map(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        signal_name="signal-manual",
        marketplace="shopee",
        order_status="CANCELLED",  # tidak ada di peta
    )

    assert result.signal_name == "signal-manual"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_send_signal_unmapped_status_skips_without_http(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee", order_status="IN_TRANSIT", raw_body=""
    )

    assert result.ok is True  # bukan error — hanya tidak dikirim
    assert result.skipped_reason == "signal_not_configured"
    assert result.signal_name is None
    assert calls == []

    lines = _log_lines(tmp_path)
    assert len(lines) == 1
    assert lines[0]["skipped_reason"] == "signal_not_configured"
    assert lines[0]["event"] == "signal"


@pytest.mark.asyncio
async def test_send_signal_disabled_and_dry_run(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path, {"id": "x", "signal": dict(SIGNAL_SECTION, enabled=False)}
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    disabled = await service.send_signal(marketplace="shopee", order_status="ON_HOLD")
    assert disabled.skipped_reason == "signal_disabled"
    assert calls == []

    # dry-run: tetap dihitung & dicatat, tanpa HTTP
    rules2 = _write_rules(tmp_path / "two", {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules2))
    dry = await service.send_signal(marketplace="shopee", order_status="ON_HOLD", dry_run=True)
    assert dry.skipped_reason == "dry_run"
    assert dry.signal_name == "retur-order-on-hold"
    assert calls == []


@pytest.mark.asyncio
async def test_send_signal_camunda_disabled_skips(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    monkeypatch.setattr(settings, "camunda_enabled", False)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(marketplace="shopee", order_status="ON_HOLD")

    assert result.skipped_reason == "camunda_disabled"
    assert calls == []


@pytest.mark.asyncio
async def test_send_signal_business_key_sent_as_variable(camunda_env, tmp_path, monkeypatch):
    """Camunda 7: business_key dikirim sebagai VARIABEL `businessKey`."""
    section = dict(SIGNAL_SECTION, business_key="{{var.signalName}}:{{var.orderId}}")
    rules = _write_rules(tmp_path, {"id": "x", "signal": section})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee", order_status="ON_HOLD", body={"code": 3, "data": {"ordersn": "SH-1", "status": "ON_HOLD"}}
    )

    assert result.business_key == "retur-order-on-hold:SH-1"
    variables = json.loads(calls[0].content.decode())["variables"]
    assert variables["businessKey"]["value"] == "retur-order-on-hold:SH-1"


@pytest.mark.asyncio
async def test_send_signal_explicit_variables_override_config(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    await service.send_signal(
        marketplace="shopee",
        order_status="ON_HOLD",
        variables={"orderId": "OVERRIDE", "extra": "yes"},
    )

    variables = json.loads(calls[0].content.decode())["variables"]
    assert variables["orderId"]["value"] == "OVERRIDE"
    assert variables["extra"]["value"] == "yes"


# ---------------------------------------------------------------------------
# 3b. service.send_signal — nama signal DINAMIS (${invoice} / {{invoice}})
# ---------------------------------------------------------------------------


def _dynamic_section():
    """Peta dengan nama signal dinamis + variabel `invoice` dari payload."""
    return dict(
        SIGNAL_SECTION,
        by_status={
            **SIGNAL_SECTION["by_status"],
            "ON_HOLD": "readyToShip${invoice}",
        },
        variables={
            **SIGNAL_SECTION["variables"],
            "invoice": ["data.ordersn", "data.order_id"],
        },
    )


@pytest.mark.asyncio
async def test_send_signal_renders_dynamic_name_from_signal_variables(
    camunda_env, tmp_path, monkeypatch
):
    """`readyToShip${invoice}` dihitung dari signal.variables sebelum di-hit."""
    rules = _write_rules(tmp_path, {"id": "x", "signal": _dynamic_section()})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        order_status="ON_HOLD",
        body={"code": 3, "data": {"ordersn": "SH-1", "status": "ON_HOLD"}},
    )

    assert result.ok is True
    assert result.signal_name == "readyToShipSH-1"
    body = json.loads(calls[0].content.decode())
    assert body["name"] == "readyToShipSH-1"
    # signalName yang dikirim ikut konsisten (dipakai business_key {{var.signalName}})
    assert body["variables"]["signalName"]["value"] == "readyToShipSH-1"
    assert body["variables"]["invoice"]["value"] == "SH-1"

    entry = _log_lines(tmp_path)[0]
    assert entry["signal_name"] == "readyToShipSH-1"


@pytest.mark.asyncio
async def test_send_signal_dynamic_name_with_curly_syntax(camunda_env, tmp_path, monkeypatch):
    """Sintaks {{invoice}} + awalan var. juga di-resolve, bukan hanya ${...}."""
    section = _dynamic_section()
    section["by_status"] = {
        **section["by_status"],
        "ON_HOLD": "readyToShip{{var.invoice}}",
    }
    rules = _write_rules(tmp_path, {"id": "x", "signal": section})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        order_status="ON_HOLD",
        body={"code": 3, "data": {"ordersn": "SH-2", "status": "ON_HOLD"}},
    )

    assert result.signal_name == "readyToShipSH-2"
    assert json.loads(calls[0].content.decode())["name"] == "readyToShipSH-2"


@pytest.mark.asyncio
async def test_send_signal_unresolved_placeholder_sent_literal(camunda_env, tmp_path, monkeypatch):
    """Variabel tidak ada → placeholder tetap apa adanya + warning (tercatat di log)."""
    section = _dynamic_section()
    section["variables"] = {
        k: v for k, v in section["variables"].items() if k != "invoice"
    }
    rules = _write_rules(tmp_path, {"id": "x", "signal": section})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        order_status="ON_HOLD",
        body={"code": 3, "data": {"status": "ON_HOLD"}},
    )

    assert result.ok is True
    assert result.signal_name == "readyToShip${invoice}"  # tidak dipotong
    assert json.loads(calls[0].content.decode())["name"] == "readyToShip${invoice}"


@pytest.mark.asyncio
async def test_send_signal_empty_rendered_name_skips(camunda_env, tmp_path, monkeypatch):
    """Render jadi KOSONG (nilai variabel kosong) → signal tidak dikirim."""
    rules = _write_rules(tmp_path, {"id": "x", "signal": SIGNAL_SECTION})
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        signal_name="${invoice}",
        marketplace="shopee",
        order_status="ON_HOLD",
        variables={"invoice": ""},
    )

    assert result.ok is True
    assert result.skipped_reason == "signal_not_configured"
    assert calls == []


# ---------------------------------------------------------------------------
# 4. Aturan config.json bertipe signal
# ---------------------------------------------------------------------------


def test_rule_type_signal_resolves_name_from_status_map(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path,
        {
            "id": "signal-only",
            "enabled": True,
            "type": "signal",
            "signal": SIGNAL_SECTION,
            "variables": {"orderId": ["data.ordersn", "data.order_id"]},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    service = CamundaWebhookTrigger()

    ctx = service._build_context(
        "tiktok", 1, TIKTOK_BODY, {}, order_status="ON_HOLD"
    )
    plans = service._resolve_plans(ctx)

    assert len(plans) == 1
    assert plans[0].request_type == "signal"
    assert plans[0].signal_name == "retur-on-hold-tiktok"
    assert plans[0].process_definition_key is None


def test_rule_type_signal_without_mapping_is_skipped(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path,
        {
            "id": "signal-only",
            "enabled": True,
            "type": "signal",
            "signal": SIGNAL_SECTION,
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    service = CamundaWebhookTrigger()

    ctx = service._build_context(
        "shopee", 3, {"code": 3, "data": {"status": "CANCELLED"}}, {}, order_status="CANCELLED"
    )
    assert service._resolve_plans(ctx) == []
    assert service._skip_reason(ctx) == "signal_not_configured"


def test_rule_type_signal_renders_dynamic_name(camunda_env, tmp_path, monkeypatch):
    """Plan type=signal juga me-render placeholder nama dari variabel plan."""
    rules = _write_rules(
        tmp_path,
        {
            "id": "signal-only",
            "enabled": True,
            "type": "signal",
            "signal": _dynamic_section(),
            "variables": {"invoice": ["data.ordersn"]},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    service = CamundaWebhookTrigger()

    ctx = service._build_context(
        "shopee",
        3,
        {"code": 3, "data": {"ordersn": "SH-9", "status": "ON_HOLD"}},
        {},
        order_status="ON_HOLD",
    )
    plans = service._resolve_plans(ctx)

    assert len(plans) == 1
    assert plans[0].request_type == "signal"
    assert plans[0].signal_name == "readyToShipSH-9"


@pytest.mark.asyncio
async def test_rule_type_signal_executed_via_trigger(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path,
        {
            "id": "signal-only",
            "enabled": True,
            "type": "signal",
            "signal": SIGNAL_SECTION,
            "variables": {"orderId": ["data.order_id"]},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    results = await service.trigger_all(
        marketplace="tiktok", signal_code=1, body=TIKTOK_BODY
    )

    assert len(results) == 1
    assert results[0].request_type == "signal"
    assert results[0].signal_name == "retur-on-hold-tiktok"
    assert str(calls[0].url).endswith("/engine-rest/signal")


# ---------------------------------------------------------------------------
# 5. Auto-signal saat webhook masuk
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_on_webhook_sends_signal_then_starts_process(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path,
        {
            "id": "flat",
            "enabled": True,
            "type": "start_process",
            "process_definition_key": "serviceWebhook",
            "signal": dict(SIGNAL_SECTION, send_on_webhook=True),
            "variables": {"orderId": ["data.order_id"]},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    transport, calls = _make_transport([(204, {}), (200, {"id": "proc-1"})])
    service = _service(transport)

    results = await service.trigger_all(
        marketplace="tiktok", signal_code=1, body=TIKTOK_BODY
    )

    assert [r.request_type for r in results] == ["signal", "start_process"]
    assert results[0].signal_name == "retur-on-hold-tiktok"
    assert str(calls[0].url).endswith("/engine-rest/signal")
    assert str(calls[1].url).endswith(
        "/engine-rest/process-definition/key/serviceWebhook/start"
    )


@pytest.mark.asyncio
async def test_send_on_webhook_disabled_by_default(camunda_env, tmp_path, monkeypatch):
    rules = _write_rules(
        tmp_path,
        {
            "id": "flat",
            "enabled": True,
            "process_definition_key": "serviceWebhook",
            "signal": SIGNAL_SECTION,  # send_on_webhook=False
            "variables": {},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    transport, calls = _make_transport([(200, {"id": "proc-1"})])
    service = _service(transport)

    results = await service.trigger_all(
        marketplace="tiktok", signal_code=1, body=TIKTOK_BODY
    )

    assert [r.request_type for r in results] == ["start_process"]
    assert len(calls) == 1
    assert str(calls[0].url).endswith("/start")


# ---------------------------------------------------------------------------
# 6. Korelasi signal → execution spesifik (signal.execution_lookup)
# ---------------------------------------------------------------------------


LOOKUP_SIGNAL_SECTION = {
    "enabled": True,
    "send_on_webhook": False,
    "by_status": {"CANCEL": "cancel", "CANCELLED": "cancel"},
    "by_marketplace": {},
    "default_signal": "",
    "execution_lookup": {
        "enabled": True,
        "business_key_template": "{{var.invoice}}:%",
        "fallback": "broadcast",
    },
    "variables": {
        "invoice": ["data.order_id"],
        "orderStatus": ["data.status", "data.order_status"],
    },
    "variable_types": {"orderId": "string"},
}


def _lookup_rules(tmp_path, monkeypatch, section):
    """Tulis aturan sementara bertipe signal dengan konfigurasi `signal` tertentu."""
    rules = _write_rules(
        tmp_path,
        {
            "id": "signal-only",
            "enabled": True,
            "type": "signal",
            "signal": section,
            "variables": {},
        },
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    monkeypatch.setattr(settings, "webhook_order_detail_enabled", False)
    return rules


@pytest.mark.asyncio
async def test_send_signal_targets_specific_execution(camunda_env, tmp_path, monkeypatch):
    """execution_lookup → POST /execution/{id}/signal, bukan /signal broadcast."""
    _lookup_rules(tmp_path, monkeypatch, LOOKUP_SIGNAL_SECTION)
    transport, calls = _make_transport(
        [
            (200, [{"id": "pi-1"}]),                                # process-instance LIKE
            (
                200,
                [
                    {
                        "id": "exec-1",
                        "processInstanceId": "pi-1",
                        "activityId": "Event_0xcz77i",  # layak di-signal
                    }
                ],
            ),
            (204, {}),                                              # signal POST
        ]
    )
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok", order_status="CANCEL", body=TIKTOK_BODY
    )

    assert result.ok is True
    assert result.request_type == "signal"
    assert result.signal_name == "cancel"
    assert result.execution_id == "exec-1"
    assert calls[0].url.path == "/engine-rest/process-instance"
    assert calls[0].url.params["businessKeyLike"] == "586198966233958181:%"
    assert calls[1].url.path == "/engine-rest/execution"
    assert calls[1].url.params["signalEventSubscriptionName"] == "cancel"
    assert calls[2].url.path == "/engine-rest/execution/exec-1/signal"
    assert json.loads(calls[2].content)["signalName"] == "cancel"


@pytest.mark.asyncio
async def test_send_signal_broadcasts_when_subscription_not_signalable(
    camunda_env, tmp_path, monkeypatch
):
    """Subscriber ketemu tapi tanpa activityId (boundary event) → broadcast.

    Regresi produksi: POST /execution/{id}/signal pada boundary event ditolak
    engine dengan \"it has no current activity\" — karena itu jangan dipaksa,
    cukup broadcast (nama signal per-order sudah mengisolasi order).
    """
    _lookup_rules(tmp_path, monkeypatch, LOOKUP_SIGNAL_SECTION)
    transport, calls = _make_transport(
        [
            (200, [{"id": "pi-1"}]),
            (200, [{"id": "exec-scope", "processInstanceId": "pi-1"}]),  # tanpa activityId
            (204, {}),
        ]
    )
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok", order_status="CANCEL", body=TIKTOK_BODY
    )

    assert result.ok is True
    assert result.request_type == "signal"
    assert result.execution_id is None, "tidak di-target, tetap broadcast"
    assert calls[2].url.path == "/engine-rest/signal"
    assert json.loads(calls[2].content)["name"] == "cancel"


@pytest.mark.asyncio
async def test_send_signal_falls_back_to_broadcast_without_subscriber(
    camunda_env, tmp_path, monkeypatch
):
    """Tidak ada instance yang menunggu → fallback broadcast (default)."""
    _lookup_rules(tmp_path, monkeypatch, LOOKUP_SIGNAL_SECTION)
    transport, calls = _make_transport([(200, []), (204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok", order_status="CANCEL", body=TIKTOK_BODY
    )

    assert result.ok is True
    assert result.execution_id is None
    assert str(calls[-1].url).endswith("/engine-rest/signal")


@pytest.mark.asyncio
async def test_send_signal_skips_when_lookup_fallback_skip(camunda_env, tmp_path, monkeypatch):
    """fallback=skip → signal TIDAK dikirim bila tidak ada subscriber."""
    section = dict(LOOKUP_SIGNAL_SECTION)
    section["execution_lookup"] = {
        "enabled": True,
        "business_key_template": "{{var.invoice}}:%",
        "fallback": "skip",
    }
    _lookup_rules(tmp_path, monkeypatch, section)
    transport, calls = _make_transport([(200, [])])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok", order_status="CANCEL", body=TIKTOK_BODY
    )

    assert result.request_type == "skip"
    assert result.skipped_reason == "signal_no_subscriber"
    assert len(calls) == 1, "tidak ada POST signal"


@pytest.mark.asyncio
async def test_send_signal_broadcast_when_lookup_disabled(camunda_env, tmp_path, monkeypatch):
    """execution_lookup tidak aktif → broadcast seperti semula (tanpa GET)."""
    _lookup_rules(tmp_path, monkeypatch, SIGNAL_SECTION)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="tiktok", order_status="ON_HOLD", body=TIKTOK_BODY
    )

    assert result.request_type == "signal"
    assert len(calls) == 1
    assert str(calls[0].url).endswith("/engine-rest/signal")


# ---------------------------------------------------------------------------
# 7. STATUS EFEKTIF dari DETAIL ORDER (signal.status_fallback)
# ---------------------------------------------------------------------------


FALLBACK_SIGNAL_SECTION = {
    "enabled": True,
    "send_on_webhook": False,
    "by_status": {"READY_TO_SHIP": "readyToShip${invoice}"},
    "by_marketplace": {},
    "default_signal": "",
    "status_fallback": {"enabled": True, "skip_statuses": []},
    "variables": {"invoice": ["data.ordersn", "data.order_id"]},
}

# Kasus produksi: webhook Shopee code=3 status UNPAID (hanya memicu START),
# sementara pembacaan gateway sudah melihat READY_TO_SHIP.
FALLBACK_BODY = {
    "code": 3,
    "data": {"ordersn": "260929BB0CBNDC", "status": "UNPAID"},
}


def _ready_to_ship_detail(status: str = "READY_TO_SHIP") -> dict:
    """Detail order normal (camunda/order_detail.py) dengan status di gateway."""
    return {"ok": True, "source": "order_detail", "order_status": status}


def _fallback_rules(tmp_path: Path, monkeypatch, section: dict = None) -> Path:
    """File aturan sementara dengan bagian `signal` (termasuk status_fallback)."""
    rules = _write_rules(
        tmp_path, {"id": "x", "signal": section or FALLBACK_SIGNAL_SECTION}
    )
    monkeypatch.setattr(settings, "camunda_rules_file", str(rules))
    return rules


def _signal_variables(call) -> dict:
    """Variabel yang benar-benar dikirim ke Camunda dari satu request mock."""
    body = json.loads(call.content.decode())
    return {name: spec["value"] for name, spec in body["variables"].items()}


@pytest.mark.asyncio
async def test_send_signal_uses_detail_status_when_webhook_status_unmapped(
    camunda_env, tmp_path, monkeypatch
):
    """UNPAID di webhook + READY_TO_SHIP di detail order → signal tetap dikirim."""
    _fallback_rules(tmp_path, monkeypatch)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        body=FALLBACK_BODY,
        order_detail=_ready_to_ship_detail(),
    )

    assert result.ok is True
    assert result.request_type == "signal"
    assert result.signal_name == "readyToShip260929BB0CBNDC"
    assert json.loads(calls[0].content.decode())["name"] == "readyToShip260929BB0CBNDC"

    variables = _signal_variables(calls[0])
    # signalOrderStatus = status EFEKTIF (yang benar-benar memilih signal).
    assert variables["signalOrderStatus"] == "READY_TO_SHIP"
    assert variables["signalStatusSource"] == "order_detail"

    lines = _log_lines(tmp_path)
    assert lines[0]["signal_name"] == "readyToShip260929BB0CBNDC"
    assert lines[0]["order_detail"]["order_status"] == "READY_TO_SHIP"


@pytest.mark.asyncio
async def test_send_signal_webhook_status_wins_over_detail_status(
    camunda_env, tmp_path, monkeypatch
):
    """Status webhook yang SUDAH punya mapping tidak pernah ditimpa detail order."""
    section = dict(
        FALLBACK_SIGNAL_SECTION,
        by_status={
            "READY_TO_SHIP": "readyToShip${invoice}",
            "CANCELLED": "cancelInvoice${invoice}",
        },
    )
    _fallback_rules(tmp_path, monkeypatch, section)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        body={"code": 3, "data": {"ordersn": "X-1", "status": "READY_TO_SHIP"}},
        order_detail=_ready_to_ship_detail("CANCELLED"),
    )

    assert result.signal_name == "readyToShipX-1"
    variables = _signal_variables(calls[0])
    assert variables["signalOrderStatus"] == "READY_TO_SHIP"
    assert variables["signalStatusSource"] == "webhook"


@pytest.mark.asyncio
async def test_send_signal_status_fallback_disabled_skips(
    camunda_env, tmp_path, monkeypatch
):
    """status_fallback.enabled=false → perilaku lama (skip signal_not_configured)."""
    section = dict(
        FALLBACK_SIGNAL_SECTION, status_fallback={"enabled": False}
    )
    _fallback_rules(tmp_path, monkeypatch, section)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        body=FALLBACK_BODY,
        order_detail=_ready_to_ship_detail(),
    )

    assert result.request_type == "skip"
    assert result.skipped_reason == "signal_not_configured"
    assert calls == []


@pytest.mark.asyncio
async def test_send_signal_status_fallback_respects_skip_statuses(
    camunda_env, tmp_path, monkeypatch
):
    """Status detail di `skip_statuses` TIDAK dipakai sebagai status efektif."""
    section = dict(
        FALLBACK_SIGNAL_SECTION,
        by_status={
            "READY_TO_SHIP": "readyToShip${invoice}",
            "CANCELLED": "cancelInvoice${invoice}",
        },
        status_fallback={"enabled": True, "skip_statuses": ["cancelled"]},
    )
    _fallback_rules(tmp_path, monkeypatch, section)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        body=FALLBACK_BODY,
        order_detail=_ready_to_ship_detail("CANCELLED"),
    )

    assert result.request_type == "skip"
    assert result.skipped_reason == "signal_not_configured"
    assert calls == []


@pytest.mark.asyncio
async def test_send_signal_status_fallback_ignored_when_detail_status_unmapped(
    camunda_env, tmp_path, monkeypatch
):
    """Status detail juga tidak dipetakan → tetap skip (fallback bukan paksaan)."""
    _fallback_rules(tmp_path, monkeypatch)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)

    result = await service.send_signal(
        marketplace="shopee",
        body=FALLBACK_BODY,
        order_detail=_ready_to_ship_detail("AWAITING_COLLECTION"),
    )

    assert result.request_type == "skip"
    assert result.skipped_reason == "signal_not_configured"
    assert calls == []


@pytest.mark.asyncio
async def test_send_signal_explicit_name_skips_status_fallback(
    camunda_env, tmp_path, monkeypatch
):
    """Nama signal eksplisit (mis. topic send-signal) → tanpa fallback/fetch."""
    _fallback_rules(tmp_path, monkeypatch)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)
    fetched = []

    async def fake_fetch(marketplace, body):
        fetched.append(marketplace)
        return _ready_to_ship_detail()

    monkeypatch.setattr(service, "_fetch_order_detail", fake_fetch)

    result = await service.send_signal(
        signal_name="signal-manual", marketplace="shopee", body=FALLBACK_BODY
    )

    assert result.signal_name == "signal-manual"
    assert fetched == [], "nama eksplisit → detail order tidak perlu diambil"


@pytest.mark.asyncio
async def test_send_signal_status_fallback_fetches_detail_when_missing(
    camunda_env, tmp_path, monkeypatch
):
    """Pemanggil tidak menyuplai detail (mis. endpoint manual) → backend fetch."""
    _fallback_rules(tmp_path, monkeypatch)
    transport, calls = _make_transport([(204, {})])
    service = _service(transport)
    fetched = []

    async def fake_fetch(marketplace, body):
        fetched.append((marketplace, body))
        return _ready_to_ship_detail()

    monkeypatch.setattr(service, "_fetch_order_detail", fake_fetch)

    result = await service.send_signal(marketplace="shopee", body=FALLBACK_BODY)

    assert [item[0] for item in fetched] == ["shopee"]
    assert result.signal_name == "readyToShip260929BB0CBNDC"
    assert _signal_variables(calls[0])["signalStatusSource"] == "order_detail"


def test_describe_signal_config_includes_status_fallback():
    info = describe_signal_config({"signal": FALLBACK_SIGNAL_SECTION})
    assert info["status_fallback"] == {"enabled": True, "skip_statuses": []}

    without = describe_signal_config({"signal": SIGNAL_SECTION})
    assert without["status_fallback"] == {"enabled": False, "skip_statuses": []}
