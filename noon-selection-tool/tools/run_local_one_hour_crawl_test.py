from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an isolated 1-hour local crawl test.")
    parser.add_argument("--category-minutes", type=int, default=30)
    parser.add_argument("--keyword-minutes", type=int, default=30)
    parser.add_argument("--categories", default="sports,pets")
    parser.add_argument("--category-product-count", type=int, default=80)
    parser.add_argument(
        "--keyword-monitor-config",
        default=str(ROOT / "config" / "keyword_monitor_local_1h.json"),
    )
    parser.add_argument("--keyword-noon-count", type=int, default=24)
    parser.add_argument("--keyword-amazon-count", type=int, default=24)
    parser.add_argument(
        "--run-root",
        default="",
        help="Optional explicit output root. Defaults to data/local_crawl_tests/<timestamp>",
    )
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now().isoformat()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_env(*, stage_db: Path, warehouse_db: Path, ops_db: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["NOON_PRODUCT_STORE_DB"] = str(stage_db)
    env["NOON_WAREHOUSE_DB"] = str(warehouse_db)
    env["NOON_OPS_DB"] = str(ops_db)
    return env


def run_subprocess(
    *,
    name: str,
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: int,
    log_path: Path,
) -> dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = now_iso()
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        timed_out = False
        return_code: int | None = None
        try:
            return_code = proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            proc.terminate()
            try:
                return_code = proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                return_code = proc.wait(timeout=10)
    return {
        "name": name,
        "command": cmd,
        "log_path": str(log_path),
        "started_at": started_at,
        "finished_at": now_iso(),
        "timeout_seconds": timeout_seconds,
        "timed_out": timed_out,
        "return_code": return_code,
    }


def sqlite_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        counts: dict[str, int] = {}
        for table in (
            "products",
            "crawl_observations",
            "product_identity",
            "observation_events",
            "keyword_runs",
            "keywords",
        ):
            if table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = int(cur.fetchone()[0])
        return counts
    finally:
        conn.close()


def sync_phase(
    *,
    reason: str,
    stage_db: Path,
    warehouse_db: Path,
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
) -> dict[str, Any]:
    return run_subprocess(
        name=f"sync:{reason}",
        cmd=[
            sys.executable,
            "run_shared_warehouse_sync.py",
            "--actor",
            "local_1h_test",
            "--reason",
            reason,
            "--trigger-db",
            str(stage_db),
            "--warehouse-db",
            str(warehouse_db),
        ],
        cwd=cwd,
        env=env,
        timeout_seconds=900,
        log_path=log_path,
    )


def main() -> int:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_root = Path(args.run_root) if args.run_root else ROOT / "data" / "local_crawl_tests" / timestamp
    stage_db = run_root / "stage" / "product_store.db"
    warehouse_db = run_root / "analytics" / "warehouse.db"
    ops_db = run_root / "ops" / "ops.db"
    keyword_data_root = run_root / "runtime_data" / "keyword"
    category_output_dir = run_root / "batch_scans" / "category"
    logs_dir = run_root / "logs"
    status_path = run_root / "status.json"
    env = build_env(stage_db=stage_db, warehouse_db=warehouse_db, ops_db=ops_db)

    status: dict[str, Any] = {
        "started_at": now_iso(),
        "run_root": str(run_root),
        "stage_db": str(stage_db),
        "warehouse_db": str(warehouse_db),
        "ops_db": str(ops_db),
        "phases": [],
        "current_phase": "category",
        "category_minutes": args.category_minutes,
        "keyword_minutes": args.keyword_minutes,
    }
    write_json(status_path, status)

    category_result = run_subprocess(
        name="category",
        cmd=[
            sys.executable,
            "run_ready_category_scan.py",
            "--report",
            str(ROOT / "data" / "reports" / "scan_readiness_2026-03-27_v8.json"),
            "--categories",
            args.categories,
            "--product-count",
            str(args.category_product_count),
            "--output-dir",
            str(category_output_dir),
            "--persist",
        ],
        cwd=ROOT,
        env=env,
        timeout_seconds=max(60, int(args.category_minutes) * 60),
        log_path=logs_dir / "category.log",
    )
    status["phases"].append(category_result)
    status["stage_counts_after_category"] = sqlite_counts(stage_db)
    write_json(status_path, status)

    category_sync = sync_phase(
        reason="local_1h_category",
        stage_db=stage_db,
        warehouse_db=warehouse_db,
        cwd=ROOT,
        env=env,
        log_path=logs_dir / "category_sync.log",
    )
    status["phases"].append(category_sync)
    status["warehouse_counts_after_category_sync"] = sqlite_counts(warehouse_db)
    status["current_phase"] = "keyword"
    write_json(status_path, status)

    keyword_result = run_subprocess(
        name="keyword",
        cmd=[
            sys.executable,
            "run_keyword_monitor.py",
            "--monitor-config",
            str(args.keyword_monitor_config),
            "--data-root",
            str(keyword_data_root),
            "--db-path",
            str(stage_db),
            "--persist",
            "--noon-count",
            str(args.keyword_noon_count),
            "--amazon-count",
            str(args.keyword_amazon_count),
        ],
        cwd=ROOT,
        env=env,
        timeout_seconds=max(60, int(args.keyword_minutes) * 60),
        log_path=logs_dir / "keyword.log",
    )
    status["phases"].append(keyword_result)
    status["stage_counts_after_keyword"] = sqlite_counts(stage_db)
    write_json(status_path, status)

    keyword_sync = sync_phase(
        reason="local_1h_keyword",
        stage_db=stage_db,
        warehouse_db=warehouse_db,
        cwd=ROOT,
        env=env,
        log_path=logs_dir / "keyword_sync.log",
    )
    status["phases"].append(keyword_sync)
    status["warehouse_counts_after_keyword_sync"] = sqlite_counts(warehouse_db)
    status["finished_at"] = now_iso()
    status["current_phase"] = "completed"
    write_json(status_path, status)

    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
