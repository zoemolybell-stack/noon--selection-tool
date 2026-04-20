from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.postgres import ensure_postgres_database_exists
from ops.task_store import OpsStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test ops control plane against Postgres.")
    parser.add_argument("--postgres-dsn", required=True, help="Target Postgres DSN")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    previous = os.environ.get("NOON_OPS_DATABASE_URL")
    ensure_postgres_database_exists(args.postgres_dsn)
    os.environ["NOON_OPS_DATABASE_URL"] = args.postgres_dsn
    try:
        store = OpsStore()
        try:
            task = store.create_task(
                task_type="warehouse_sync",
                payload={"reason": "postgres_smoke"},
                created_by="postgres_smoke",
            )
            lease = store.lease_next_task(worker_type="sync", lease_owner="postgres-smoke-worker")
            if not lease.task or not lease.run_id:
                raise RuntimeError("failed to lease smoke task")
            store.mark_task_running(task_id=int(task["id"]), run_id=int(lease.run_id), command=["python", "noop"])
            finished = store.finish_task_run(
                task_id=int(task["id"]),
                run_id=int(lease.run_id),
                final_status="completed",
                result={"returncode": 0},
            )
            plan = store.create_crawl_plan(
                plan_type="category_single",
                name="postgres smoke plan",
                created_by="postgres_smoke",
                payload={"category": "pets", "product_count": 10},
                schedule_kind="interval",
                schedule_json={"seconds": 3600},
            )
            print("backend:", store.backend)
            print("task_id:", finished["id"])
            print("task_status:", finished["status"])
            print("plan_id:", plan["id"])
            print("plan_schedule:", plan["schedule_kind"])
            return 0
        finally:
            store.close()
    finally:
        if previous is None:
            os.environ.pop("NOON_OPS_DATABASE_URL", None)
        else:
            os.environ["NOON_OPS_DATABASE_URL"] = previous


if __name__ == "__main__":
    raise SystemExit(main())
