"""Regression: POST /api/v2/stock-sync/run-part.

Endpoint ini berfungsi ganda:
  - HIT CLIENT external task Camunda (`serviceWebhookTest`) — self-call
    ``?part_id=…&full_pipeline=true`` untuk menjalankan pipeline dari awal.
  - Run manual per part oleh operator.

Kontrak yang diuji:
  1. 503 saat kill-switch ``stock_sync_enabled=false``.
  2. 404 saat part_id tidak punya variant ACTIVE di ``sku_mapping``.
  3. 200 + menjalankan ``run_once_for_parts`` untuk SEMUA part_id sekaligus,
     meneruskan ``full_pipeline``, dan mengembalikan scope + ringkasan cycle.
  4. 422 saat part_id bukan angka > 0.
  5. part_id dikirim sebagai ARRAY di request body (batch) — bukan query
     param, supaya jumlahnya tidak terbatas panjang URL.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from application.usecases.stock_sync_pipeline import PartScope, PipelineCycleResult
from core.config import settings
from interfaces.http.routers import stock_sync as router_module

ENDPOINT = "/api/v2/stock-sync/run-part"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router_module.router)
    with TestClient(app) as test_client:
        yield test_client


class _FakeEngine:
    """Pengganti StockSyncPipelineEngine (tanpa DB / jaringan)."""

    def __init__(self, scopes=None, result=None, fail_run=None):
        self._scopes = scopes or {}
        self._result = result
        self._fail_run = fail_run
        self.calls = []
        self.closed = False

    async def resolve_part_scope(self, part_id):
        return self._scopes.get(int(part_id), PartScope(part_id=int(part_id)))

    async def run_once_for_parts(self, part_ids, full_pipeline=False):
        self.calls.append({"part_ids": list(part_ids), "full_pipeline": full_pipeline})
        if self._fail_run is not None:
            raise self._fail_run
        return self._result

    async def aclose(self):
        self.closed = True


def _install(monkeypatch, engine) -> _FakeEngine:
    monkeypatch.setattr(
        router_module, "StockSyncPipelineEngine", lambda *a, **k: engine
    )
    return engine


def test_run_part_503_when_stock_sync_disabled(client, monkeypatch):
    monkeypatch.setattr(settings, "stock_sync_enabled", False)
    _install(monkeypatch, _FakeEngine())

    resp = client.post(ENDPOINT, json={"part_id": [42]})

    assert resp.status_code == 503


def test_run_part_404_when_no_scoped_variant(client, monkeypatch):
    monkeypatch.setattr(settings, "stock_sync_enabled", True)
    _install(monkeypatch, _FakeEngine(scopes={42: PartScope(part_id=42)}))

    resp = client.post(ENDPOINT, json={"part_id": [42]})

    assert resp.status_code == 404
    assert "sku_mapping" in resp.json()["detail"]


def test_run_part_runs_all_parts_with_full_pipeline(client, monkeypatch):
    monkeypatch.setattr(settings, "stock_sync_enabled", True)
    scopes = {
        11: PartScope(
            part_id=11, mappings=1, variant_ids=["v11"], product_ids=["p11"]
        ),
        22: PartScope(
            part_id=22,
            mappings=2,
            variant_ids=["v22a", "v22b"],
            product_ids=["p22"],
        ),
    }
    result = PipelineCycleResult(
        cycle_index=1,
        total_report_rows=3,
        total_queued=3,
        total_pushed=2,
        total_logged=3,
    )
    engine = _install(monkeypatch, _FakeEngine(scopes=scopes, result=result))

    resp = client.post(
        ENDPOINT, json={"part_id": [11, 22], "full_pipeline": True}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["run_mode"] == "MANUAL_PART"
    assert body["part_id"] == 11
    assert body["scope"]["variant_ids"] == ["v11"]
    assert {s["part_id"] for s in body["scopes"]} == {11, 22}
    assert body["cycle"]["total_pushed"] == 2
    assert engine.calls == [{"part_ids": [11, 22], "full_pipeline": True}]
    assert engine.closed is True


def test_run_part_404_when_pipeline_reports_no_valid_part(client, monkeypatch):
    monkeypatch.setattr(settings, "stock_sync_enabled", True)
    scope = {11: PartScope(part_id=11, variant_ids=["v11"], product_ids=["p11"])}
    _install(
        monkeypatch,
        _FakeEngine(scopes=scope, fail_run=ValueError("tidak ada part_id yang valid")),
    )

    resp = client.post(ENDPOINT, json={"part_id": [11]})

    assert resp.status_code == 404


def test_run_part_rejects_non_positive_part_id(client, monkeypatch):
    monkeypatch.setattr(settings, "stock_sync_enabled", True)
    _install(monkeypatch, _FakeEngine())

    resp = client.post(ENDPOINT, json={"part_id": [0]})

    assert resp.status_code == 422
