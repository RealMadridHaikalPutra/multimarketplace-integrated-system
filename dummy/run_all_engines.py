#!/usr/bin/env python
"""run_all_engines.py — TEST SEMUA MESIN dengan data dummy (offline).

Menjalankan seluruh mesin aplikasi TANPA APISIX asli, TANPA Camunda asli,
dan TANPA PostgreSQL (semua external disimulasikan dummy/mocked):

  1. E2E WEBHOOK -> CAMUNDA -> part_id -> SERVICE TASK
     (dummy APISIX + dummy Camunda + stub /run-part, memakai kode ASLI)
  2. Seluruh ENGINE lewat pytest dengan data dummy, dikelompokkan per mesin:
       A. Webhook + Camunda
       B. SyncEngine + Parser/APISIX client
       C. MappingEngine + Validasi SKU
       D. StockSyncPipeline (report/queue/push/log, scheduler, run-part, batch)

Usage:
    python dummy/run_all_engines.py
    python dummy/run_all_engines.py --group D          # hanya satu kelompok
    python dummy/run_all_engines.py --skip-e2e         # lewati fase E2E

Exit code 0 = SEMUA LANCAR, 1 = ada kegagalan.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Kelompok mesin -> file test (pytest.ini tetap meng-ignore file WIP).
GROUPS: Dict[str, List[str]] = {
    "A. Webhook + Camunda": [
        "tests/test_webhook_receiver.py",
        "tests/test_camunda_trigger.py",
        "tests/test_camunda_order_detail.py",
        "tests/test_camunda_external_task.py",
        "tests/test_camunda_signal.py",
    ],
    "B. SyncEngine + Parser APISIX": [
        "tests/test_apisix_client.py",
        "tests/test_shopee_parser.py",
        "tests/test_sync_engine_reserved.py",
        "tests/test_sync_pack_sx_gate.py",
    ],
    "C. MappingEngine + Validasi SKU": [
        "tests/test_mapping_service_factor.py",
        "tests/test_mapping_validation.py",
    ],
    "D. StockSyncPipeline": [
        "tests/test_pipeline_inventory_global.py",
        "tests/test_pipeline_activation.py",
        "tests/test_stock_sync_schedule.py",
        "tests/test_stock_sync_retry.py",
        "tests/test_sync_log_queue_error_capture.py",
        "tests/test_stock_sync_run_part_endpoint.py",
        "tests/test_external_task_batch.py",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count(text: str) -> Tuple[int, int, int]:
    """(passed, failed, errors) dari ringkasan output pytest."""
    passed = sum(int(n) for n in re.findall(r"(\d+) passed", text))
    failed = sum(int(n) for n in re.findall(r"(\d+) failed", text))
    errors = sum(int(n) for n in re.findall(r"(\d+) error", text))
    return passed, failed, errors


def _run_pytest(files: List[str]) -> Tuple[int, int, int, str]:
    existing = [f for f in files if (PROJECT_ROOT / f).exists()]
    missing = [f for f in files if not (PROJECT_ROOT / f).exists()]
    if not existing:
        return 0, 0, 1, f"tidak ada file test yang cocok: {files}"

    cmd = [sys.executable, "-m", "pytest", *existing, "-q", "--no-header"]
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    passed, failed, errors = _count(out)
    if missing:
        out += f"\n(note: file tidak ditemukan, dilewati: {', '.join(missing)})"
    tail = "\n".join(out.strip().splitlines()[-3:])
    return passed, failed, errors, tail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Test semua mesin dengan data dummy")
    parser.add_argument("--group", help="Jalankan hanya kelompok bernomor, mis. B")
    parser.add_argument("--skip-e2e", action="store_true", help="Lewati fase E2E webhook")
    args = parser.parse_args()

    results: List[Tuple[str, int, int, int, str]] = []

    print("=" * 76)
    print("  TEST SEMUA MESIN DENGAN DATA DUMMY (offline)")
    print("  Tanpa APISIX / Camunda / PostgreSQL asli")
    print("=" * 76)

    # ── Fase 1: E2E webhook -> Camunda -> part_id -> service task ───────
    if not args.skip_e2e:
        print("\n" + "#" * 76)
        print("#  FASE 1: E2E WEBHOOK -> CAMUNDA -> SERVICE TASK (dummy)")
        print("#" * 76)
        from dummy.verify_webhook_e2e import run as e2e_run

        try:
            e2e_rc = asyncio.run(e2e_run())
        except Exception as exc:  # noqa: BLE001
            print(f"  [FAIL] E2E crash: {type(exc).__name__}: {exc}")
            e2e_rc = 1
        results.append(("Fase 1: E2E Webhook -> Camunda", 1 if e2e_rc == 0 else 0, 1 if e2e_rc else 0, 0,
                        "SEMUA PASS" if e2e_rc == 0 else "ADA FAIL (lihat output di atas)"))

    # ── Fase 2: engine per kelompok ─────────────────────────────────────
    print("\n" + "#" * 76)
    print("#  FASE 2: MESIN PER KELOMPOK (pytest, data dummy/mocked)")
    print("#" * 76)

    for name, files in GROUPS.items():
        if args.group and not name.startswith(f"{args.group}."):
            continue
        print(f"\n--- {name} ---")
        passed, failed, errors, tail = _run_pytest(files)
        status = "OK" if failed == 0 and errors == 0 else "GAGAL"
        print(f"  [{status}] {name}: {passed} passed, {failed} failed, {errors} errors")
        if tail:
            print("  " + tail.replace("\n", "\n  "))
        results.append((name, passed, failed, errors, tail))

    # ── Ringkasan ───────────────────────────────────────────────────────
    total_pass = sum(r[1] for r in results)
    total_fail = sum(r[2] for r in results)
    total_err = sum(r[3] for r in results)

    print("\n" + "=" * 76)
    print("  RINGKASAN")
    print("=" * 76)
    for name, passed, failed, errors, _ in results:
        mark = "[FAIL]" if (failed or errors) else "[ OK ]"
        print(f"  {mark} {name:<44} pass={passed:<5} fail={failed:<4} err={errors}")
    print("-" * 76)
    print(f"  TOTAL: {total_pass} lulus, {total_fail} gagal, {total_err} error")
    if total_fail or total_err:
        print("  HASIL: ADA GAGAL — perbaiki dulu sebelum lanjut.")
    else:
        print("  HASIL: SEMUA MESIN BERJALAN LANCAR.")
    print("=" * 76)
    return 1 if (total_fail or total_err) else 0


if __name__ == "__main__":
    sys.exit(main())
