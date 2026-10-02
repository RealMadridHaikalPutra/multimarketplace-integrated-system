#!/usr/bin/env python
"""
reset_db.py — Drop database & push model terbaru.

USAGE:
    python reset_db.py              # drop, recreate, migrate, seed
    python reset_db.py --no-seed    # skip seeding
    python reset_db.py --dry-run    # tampilkan langkah tanpa eksekusi

What it does:
    1. Terminate semua koneksi ke database
    2. Drop database
    3. Create database kosong
    4. Hapus semua file migration lama
    5. Generate migration baru dari models.py
    6. Apply migration (alembic upgrade head)
    7. Seed data marketplace
"""

import argparse
import configparser
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
ALEMBIC_VERSIONS = PROJECT_ROOT / "alembic" / "versions"


# ---------------------------------------------------------------------------
# Database config helpers
# ---------------------------------------------------------------------------

def _read_alembic_url() -> str:
    """Baca sqlalchemy.url dari alembic.ini."""
    if not ALEMBIC_INI.exists():
        sys.exit(f"[ERROR] alembic.ini not found at {ALEMBIC_INI}")
    cfg = configparser.ConfigParser()
    cfg.read(ALEMBIC_INI)
    return cfg.get("alembic", "sqlalchemy.url")


def _parse_db_url(url: str) -> dict:
    """Extract host, port, user, password, dbname dari URL (sync atau async)."""
    url_clean = url.replace("+asyncpg", "")
    parsed = urlparse(url_clean)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
        "dbname": parsed.path.lstrip("/") or "sync_service",
    }


# ---------------------------------------------------------------------------
# Low-level ops
# ---------------------------------------------------------------------------

def _check_psql() -> bool:
    """Cek apakah psql tersedia di PATH."""
    return shutil.which("psql") is not None


def _run_sql(sql: str, target_db: str = "postgres", db_info: dict | None = None) -> tuple[bool, str]:
    """Jalankan SQL via psql. Return (success, output)."""
    if not _check_psql():
        return False, "psql tidak ditemukan di PATH. Install PostgreSQL client tools."

    if db_info is None:
        db_info = _parse_db_url(_read_alembic_url())

    env = os.environ.copy()
    env["PGPASSWORD"] = db_info["password"]

    try:
        result = subprocess.run(
            [
                "psql",
                "-h", db_info["host"],
                "-p", str(db_info["port"]),
                "-U", db_info["user"],
                "-d", target_db,
                "-c", sql,
            ],
            capture_output=True, text=True, env=env, timeout=15,
        )
        output = (result.stdout + result.stderr).strip()
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "psql command timed out"


def _run_alembic(*args: str) -> bool:
    """Jalankan perintah alembic. Return True jika sukses."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=str(PROJECT_ROOT),
        capture_output=True, text=True, timeout=60,
    )
    print(result.stdout.strip())
    if result.returncode != 0:
        print(f"[ERROR] {result.stderr.strip()}")
        return False
    return True


def _run_seed() -> bool:
    """Jalankan seeding langsung via import Python."""
    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        import asyncio
        from infrastructure.db.database import async_session_factory
        from infrastructure.db.seed import run_all_seeds

        async def _seed():
            async with async_session_factory() as session:
                await run_all_seeds(session)
                await session.commit()
            print("[OK] Seed selesai!")

        asyncio.run(_seed())
        return True
    except Exception as e:
        print(f"[WARN] Seed error: {e}")
        return False


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def _step(msg: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {msg}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def reset_database(*, seed: bool = True, dry_run: bool = False) -> None:
    """Drop DB & push model terbaru."""
    db_url = _read_alembic_url()
    db_info = _parse_db_url(db_url)

    print(f"[INFO] Database target: {db_info['host']}:{db_info['port']}/{db_info['dbname']}")

    if dry_run:
        print("[DRY-RUN] Mode — tidak ada perubahan yang dilakukan.\n")
        _print_dry_run(db_info)
        return

    # --- 1. Terminate connections ---
    _step(f"1/5  Terminate semua koneksi ke '{db_info['dbname']}'...")
    ok, out = _run_sql(
        f"SELECT pg_terminate_backend(pg_stat_activity.pid) "
        f"FROM pg_stat_activity "
        f"WHERE pg_stat_activity.datname = '{db_info['dbname']}' "
        f"AND pid <> pg_backend_pid();"
    )
    if not ok:
        print(f"[WARN] Terminate connections: {out}")
    else:
        print(f"  Terminated.")

    # --- 2. Drop database ---
    _step(f"2/5  Drop database '{db_info['dbname']}'...")
    ok, out = _run_sql(f"DROP DATABASE IF EXISTS {db_info['dbname']};")
    if not ok:
        print(f"[FAIL] Gagal drop database: {out}")
        print("  Pastikan tidak ada aplikasi yang terkoneksi.")
        sys.exit(1)
    print(f"  {out or 'OK'}")

    # --- 3. Create database ---
    _step(f"3/5  Create database '{db_info['dbname']}'...")
    ok, out = _run_sql(f"CREATE DATABASE {db_info['dbname']};")
    if not ok:
        print(f"[FAIL] Gagal create database: {out}")
        sys.exit(1)
    print(f"  {out or 'OK'}")

    # --- 4. Regenerate migration ---
    _step("4/5  Generate migration baru dari models.py...")
    for f in sorted(ALEMBIC_VERSIONS.glob("*.py")):
        if f.name == ".gitkeep":
            continue
        try:
            f.unlink()
            print(f"  Deleted: {f.name}")
        except OSError as e:
            print(f"[WARN] Gagal hapus {f.name}: {e}")

    if not _run_alembic("revision", "--autogenerate", "-m", "initial_schema"):
        print("[FAIL] Gagal generate migration.")
        sys.exit(1)

    # --- 5. Apply migration ---
    _step("5/5  Apply migration (alembic upgrade head)...")
    if not _run_alembic("upgrade", "head"):
        print("[FAIL] Gagal apply migration.")
        sys.exit(1)

    # --- 6. Seed ---
    if seed:
        _step("Seed marketplace data...")
        _run_seed()

    print(f"\n{'=' * 60}")
    print(f"  ✅ SELESAI! Database '{db_info['dbname']}' siap pakai.")
    print(f"{'=' * 60}\n")


def _print_dry_run(db_info: dict) -> None:
    """Tampilkan rencana langkah tanpa eksekusi."""
    steps = [
        f"Terminate koneksi ke '{db_info['dbname']}'",
        f"DROP DATABASE IF EXISTS {db_info['dbname']}",
        f"CREATE DATABASE {db_info['dbname']}",
        "Hapus semua file di alembic/versions/*.py",
        "alembic revision --autogenerate -m initial_schema",
        "alembic upgrade head",
        "Seed marketplace data (shopee, tiktok)",
    ]
    for i, s in enumerate(steps, 1):
        print(f"  {i}. {s}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Drop database & push model terbaru dari models.py"
    )
    parser.add_argument("--no-seed", action="store_true", help="Skip seeding data")
    parser.add_argument("--dry-run", action="store_true", help="Tampilkan langkah tanpa eksekusi")
    args = parser.parse_args()

    reset_database(seed=not args.no_seed, dry_run=args.dry_run)
