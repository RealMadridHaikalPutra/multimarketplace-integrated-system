"""
Unit test: penerima webhook MENTAH order-status (api/routers/webhook.py).

Kontrak yang diuji (sesuai apisix/apisix/webhook-config.json, target
"order-status-backend"):
  1. POST /api/v1/webhook/order/status menerima body marketplace apa adanya
     (payload_mode="raw") dan membalas 2xx — APISIX retry bila non-2xx.
  2. Marketplace & signal code terdeteksi dari query/header/bentuk payload
     (Shopee: code=3, TikTok: type=1).
  3. Payload ASLI tercatat utuh di file log webhook (JSONL).
  4. Body non-JSON tetap diterima (mentah) dan tetap tercatat.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from interfaces.http.routers import webhook as webhook_module
from infrastructure.camunda.service import camunda_webhook_trigger
from core.config import settings

ENDPOINT = "/api/v1/webhook/order/status"

SHOPEE_BODY = {
    "code": 3,
    "shop_id": 2778101,
    "timestamp": 1700000000,
    "data": {"ordersn": "231111ABCDEF", "status": "READY_TO_SHIP"},
}

TIKTOK_BODY = {
    "type": 1,
    "shop_id": "749447000000000000",
    "timestamp": 1700000001,
    "data": {"order_id": "5770000000000000001", "order_status": "AWAITING_SHIPMENT"},
}


@pytest.fixture
def fake_camunda_trigger(monkeypatch):
    """Trigger Camunda diganti AsyncMock.

    Test di file ini fokus pada penerimaan webhook MENTAH, bukan pemanggilan
    Camunda yang sebenarnya (lihat tests/test_camunda_trigger.py).
    """
    fake_trigger = AsyncMock()
    monkeypatch.setattr(camunda_webhook_trigger, "trigger_all", fake_trigger)
    return fake_trigger


@pytest.fixture
def client(tmp_path, monkeypatch, fake_camunda_trigger):
    """TestClient dengan router webhook saja (tanpa DB/lifespan main.app)."""
    monkeypatch.setattr(settings, "webhook_log_enabled", True)
    monkeypatch.setattr(settings, "webhook_log_dir", str(tmp_path))

    app = FastAPI()
    app.include_router(webhook_module.router)
    with TestClient(app) as test_client:
        yield test_client


def _read_log_lines(tmp_path) -> list:
    log_path = Path(tmp_path) / webhook_module.settings.webhook_log_filename
    assert log_path.exists(), f"file log webhook tidak dibuat: {log_path}"
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_shopee_raw_webhook_accepted_and_logged(client, tmp_path):
    response = client.post(
        ENDPOINT,
        params={"marketplace": "shopee"},
        json=SHOPEE_BODY,
        headers={
            "X-Forwarded-By": "apisix-webhook-forwarder",
            "X-Webhook-Source": "shopee",
            "X-Webhook-Target": "order-status-backend",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["marketplace"] == "shopee"
    assert payload["signal_code"] == 3
    assert payload["order_id"] == "231111ABCDEF"

    entries = _read_log_lines(tmp_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry["marketplace"] == "shopee"
    assert entry["signal_code"] == 3
    assert entry["order_status"] == "READY_TO_SHIP"
    assert entry["target_id"] == "order-status-backend"
    assert entry["body_truncated"] is False
    # Payload mentah tersimpan APA ADANYA (bisa di-replay)
    assert json.loads(entry["raw_body"]) == SHOPEE_BODY


def test_tiktok_raw_webhook_detected_from_body(client, tmp_path):
    """Tanpa query marketplace: deteksi dari field 'type' pada body mentah."""
    response = client.post(ENDPOINT, json=TIKTOK_BODY)

    assert response.status_code == 200
    payload = response.json()
    assert payload["marketplace"] == "tiktok"
    assert payload["signal_code"] == 1
    assert payload["order_id"] == "5770000000000000001"

    entry = _read_log_lines(tmp_path)[0]
    assert entry["marketplace"] == "tiktok"
    assert entry["order_status"] == "AWAITING_SHIPMENT"


def test_unknown_shape_body_still_accepted(client, tmp_path):
    """Signal/bentuk payload baru tidak boleh ditolak (raw passthrough)."""
    response = client.post(ENDPOINT, params={"marketplace": "shopee"}, json={"code": 99, "foo": "bar"})
    assert response.json()["camunda"] == "queued"  # tetap diteruskan ke Camunda

    assert response.status_code == 200
    payload = response.json()
    assert payload["signal_code"] == 99
    assert payload["order_id"] is None

    entry = _read_log_lines(tmp_path)[0]
    assert json.loads(entry["raw_body"]) == {"code": 99, "foo": "bar"}


def test_non_json_body_still_accepted(client, tmp_path, fake_camunda_trigger):
    """Body non-JSON tetap 2xx supaya APISIX tidak menganggap forward gagal."""
    response = client.post(
        ENDPOINT,
        content=b"code=3&ordersn=231111ABCDEF",
        headers={"Content-Type": "text/plain", "X-Webhook-Source": "shopee"},
    )

    assert response.status_code == 200
    assert response.json()["signal_code"] is None

    entry = _read_log_lines(tmp_path)[0]
    assert entry["raw_body"] == "code=3&ordersn=231111ABCDEF"
    assert entry["marketplace"] == "shopee"
    # Pemetaan Camunda tetap dijalankan walau body bukan JSON
    assert fake_camunda_trigger.await_args.kwargs["body"] is None
