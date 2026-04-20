from __future__ import annotations

import argparse
import json
from pathlib import Path

from ops.task_store import OpsStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Task center CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="create a task")
    create_parser.add_argument("--task-type", required=True, choices=["keyword_monitor", "keyword_once", "category_single", "category_ready_scan", "warehouse_sync"])
    create_parser.add_argument("--created-by", default="cli")
    create_parser.add_argument("--priority", type=int, default=100)
    create_parser.add_argument("--schedule-type", default="manual", choices=["manual", "interval"])
    create_parser.add_argument("--schedule-seconds", type=int, default=0)
    create_parser.add_argument("--next-run-at", default="")
    create_parser.add_argument("--payload-json", default="")

    create_parser.add_argument("--monitor-config", default="")
    create_parser.add_argument("--keyword", default="")
    create_parser.add_argument("--category", default="")
    create_parser.add_argument("--categories", default="")
    create_parser.add_argument("--platforms", nargs="*", default=None)
    create_parser.add_argument("--noon-count", type=int, default=30)
    create_parser.add_argument("--amazon-count", type=int, default=30)
    create_parser.add_argument("--product-count", type=int, default=50)
    create_parser.add_argument("--persist", action="store_true")
    create_parser.add_argument("--no-persist", action="store_true")
    create_parser.add_argument("--export-excel", action="store_true")
    create_parser.add_argument("--reason", default="")
    create_parser.add_argument("--trigger-db", default="")

    list_parser = subparsers.add_parser("list", help="list tasks")
    list_parser.add_argument("--status", default="")
    list_parser.add_argument("--worker-type", default="")
    list_parser.add_argument("--limit", type=int, default=100)

    cancel_parser = subparsers.add_parser("cancel", help="cancel task")
    cancel_parser.add_argument("task_id", type=int)

    retry_parser = subparsers.add_parser("retry", help="retry task")
    retry_parser.add_argument("task_id", type=int)

    runs_parser = subparsers.add_parser("runs", help="list task runs")
    runs_parser.add_argument("--task-id", type=int, default=None)
    runs_parser.add_argument("--limit", type=int, default=100)

    subparsers.add_parser("workers", help="list workers")
    return parser


def _payload_from_args(args: argparse.Namespace) -> dict:
    persist_value = False if getattr(args, "no_persist", False) else bool(args.persist)
    if args.task_type in {"keyword_monitor", "keyword_once", "category_single", "category_ready_scan"} and not args.persist and not getattr(args, "no_persist", False):
        persist_value = True

    if args.payload_json:
        return json.loads(args.payload_json)

    if args.task_type == "keyword_monitor":
        return {
            "monitor_config": args.monitor_config,
            "noon_count": args.noon_count,
            "amazon_count": args.amazon_count,
            "persist": persist_value,
        }
    if args.task_type == "keyword_once":
        return {
            "keyword": args.keyword,
            "platforms": args.platforms or ["noon", "amazon"],
            "noon_count": args.noon_count,
            "amazon_count": args.amazon_count,
            "persist": persist_value,
            "export_excel": bool(args.export_excel),
        }
    if args.task_type == "category_single":
        return {
            "category": args.category,
            "persist": persist_value,
            "export_excel": bool(args.export_excel),
            "product_count": args.product_count,
        }
    if args.task_type == "category_ready_scan":
        categories = [item.strip() for item in args.categories.split(",") if item.strip()] if args.categories else []
        return {
            "categories": categories,
            "product_count": args.product_count,
            "persist": persist_value,
            "export_excel": bool(args.export_excel),
        }
    if args.task_type == "warehouse_sync":
        return {
            "reason": args.reason or "manual_sync",
            "trigger_db": args.trigger_db,
        }
    raise ValueError(f"unsupported task_type: {args.task_type}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = OpsStore()
    try:
        if args.command == "create":
            schedule_expr = str(args.schedule_seconds) if args.schedule_type == "interval" and args.schedule_seconds > 0 else ""
            task = store.create_task(
                task_type=args.task_type,
                payload=_payload_from_args(args),
                created_by=args.created_by,
                priority=args.priority,
                schedule_type=args.schedule_type,
                schedule_expr=schedule_expr,
                next_run_at=args.next_run_at or None,
            )
            print(json.dumps(task, ensure_ascii=False, indent=2))
            return 0
        if args.command == "list":
            print(json.dumps(store.list_tasks(status=args.status, worker_type=args.worker_type, limit=args.limit), ensure_ascii=False, indent=2))
            return 0
        if args.command == "cancel":
            print(json.dumps(store.cancel_task(args.task_id), ensure_ascii=False, indent=2))
            return 0
        if args.command == "retry":
            print(json.dumps(store.retry_task(args.task_id), ensure_ascii=False, indent=2))
            return 0
        if args.command == "runs":
            print(json.dumps(store.list_task_runs(task_id=args.task_id, limit=args.limit), ensure_ascii=False, indent=2))
            return 0
        if args.command == "workers":
            print(json.dumps(store.list_workers(), ensure_ascii=False, indent=2))
            return 0
        raise SystemExit(f"unsupported command: {args.command}")
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
