from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"
DEFAULT_POLL_SECONDS = 15
DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.task_store import OpsStore


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp_token() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Drain current local 7x24 crawlers at a safe category boundary, sync warehouse, then restart fresh workers/tasks.",
    )
    parser.add_argument("--category-task-id", type=int, required=True)
    parser.add_argument("--keyword-task-id", type=int, required=True)
    parser.add_argument("--category-worker-pid", type=int, required=True)
    parser.add_argument("--keyword-worker-pid", type=int, required=True)
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser


class Orchestrator:
    def __init__(self, *, report_dir: Path) -> None:
        self.report_dir = report_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.report_dir / "drain_restart.log"
        self.summary_path = self.report_dir / "drain_restart_summary.json"
        self.summary: dict[str, Any] = {
            "started_at": utcnow_iso(),
            "events": [],
        }

    def log(self, message: str, **payload: Any) -> None:
        event = {
            "timestamp": utcnow_iso(),
            "message": message,
        }
        if payload:
            event["payload"] = payload
        self.summary["events"].append(event)
        line = f"[{event['timestamp']}] {message}"
        if payload:
            line += f" | {json.dumps(payload, ensure_ascii=False, sort_keys=True)}"
        print(line, flush=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        self.flush()

    def flush(self) -> None:
        self.summary_path.write_text(json.dumps(self.summary, ensure_ascii=False, indent=2), encoding="utf-8")


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    proc = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return str(pid) in proc.stdout


def terminate_process_tree(pid: int, *, orchestrator: Orchestrator, label: str) -> None:
    if not is_pid_running(pid):
        orchestrator.log("skip dead process", label=label, pid=pid)
        return
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    orchestrator.log("terminated process tree", label=label, pid=pid)


def run_sync(*, orchestrator: Orchestrator) -> dict[str, Any]:
    command = [
        sys.executable,
        str(ROOT / "run_shared_warehouse_sync.py"),
        "--actor",
        "codex_drain_restart",
        "--reason",
        "local_7x24_drain_restart",
    ]
    proc = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    result = {
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout.splitlines()[-20:],
        "stderr_tail": proc.stderr.splitlines()[-20:],
    }
    orchestrator.log("shared sync executed", result=result)
    return result


def create_clone_task(
    *,
    store: OpsStore,
    source_task: dict[str, Any],
    created_by: str,
    display_name: str,
) -> dict[str, Any]:
    schedule_expr = str(source_task.get("schedule_expr") or "")
    schedule_type = str(source_task.get("schedule_type") or "manual")
    payload = deepcopy(source_task.get("payload") or {})
    task = store.create_task(
        task_type=str(source_task["task_type"]),
        payload=payload,
        created_by=created_by,
        priority=int(source_task.get("priority") or 100),
        schedule_type=schedule_type,
        schedule_expr=schedule_expr,
        next_run_at=utcnow_iso(),
        display_name=display_name,
    )
    return task


def start_worker(
    *,
    worker_type: str,
    worker_name: str,
    poll_seconds: int,
    stdout_path: Path,
    stderr_path: Path,
) -> int:
    stdout_handle = stdout_path.open("a", encoding="utf-8")
    stderr_handle = stderr_path.open("a", encoding="utf-8")
    creationflags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "run_task_scheduler.py"),
            "--mode",
            "worker",
            "--worker-type",
            worker_type,
            "--worker-name",
            worker_name,
            "--poll-seconds",
            str(poll_seconds),
        ],
        cwd=str(ROOT),
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creationflags,
    )
    stdout_handle.close()
    stderr_handle.close()
    return int(process.pid)


def wait_for_category_boundary(
    *,
    category_task_id: int,
    baseline_completed: int,
    poll_seconds: int,
    timeout_seconds: int,
    orchestrator: Orchestrator,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        store = OpsStore()
        try:
            task = store.get_task(category_task_id)
        finally:
            store.close()
        if task is None:
            raise RuntimeError(f"category task disappeared: {category_task_id}")
        progress = dict(task.get("progress") or {})
        metrics = dict(progress.get("metrics") or {})
        categories_completed = int(metrics.get("categories_completed") or 0)
        status = str(task.get("status") or "").strip().lower()
        current_category = str((progress.get("details") or {}).get("current_category") or "")
        message = str(progress.get("message") or "")
        orchestrator.log(
            "category drain poll",
            task_status=status,
            current_category=current_category,
            categories_completed=categories_completed,
            baseline_completed=baseline_completed,
            progress_message=message,
        )
        if status != "running":
            return {
                "reason": "task_finished",
                "task_status": status,
                "current_category": current_category,
                "categories_completed": categories_completed,
                "message": message,
            }
        if categories_completed > baseline_completed:
            return {
                "reason": "category_boundary_reached",
                "task_status": status,
                "current_category": current_category,
                "categories_completed": categories_completed,
                "message": message,
            }
        time.sleep(max(5, poll_seconds))
    raise TimeoutError("timed out waiting for category boundary")


def main() -> int:
    args = build_parser().parse_args()
    report_dir = REPORTS_DIR / f"local_7x24_drain_restart_{timestamp_token()}"
    orchestrator = Orchestrator(report_dir=report_dir)
    orchestrator.summary["input"] = vars(args)

    store = OpsStore()
    try:
        category_task = store.get_task(args.category_task_id)
        keyword_task = store.get_task(args.keyword_task_id)
    finally:
        store.close()

    if category_task is None:
        raise SystemExit(f"missing category task: {args.category_task_id}")
    if keyword_task is None:
        raise SystemExit(f"missing keyword task: {args.keyword_task_id}")

    baseline_completed = int((((category_task.get("progress") or {}).get("metrics") or {}).get("categories_completed")) or 0)
    orchestrator.summary["baseline"] = {
        "category_task": category_task,
        "keyword_task": keyword_task,
        "baseline_completed": baseline_completed,
    }
    orchestrator.flush()

    store = OpsStore()
    try:
        keyword_state = str((store.get_task(args.keyword_task_id) or {}).get("status") or "").strip().lower()
        if keyword_state in {"pending", "leased", "running"}:
            store.cancel_task(args.keyword_task_id)
            orchestrator.log("keyword task cancelled before drain", task_id=args.keyword_task_id, previous_status=keyword_state)
        else:
            orchestrator.log("keyword task already inactive", task_id=args.keyword_task_id, status=keyword_state)
    finally:
        store.close()

    boundary = wait_for_category_boundary(
        category_task_id=args.category_task_id,
        baseline_completed=baseline_completed,
        poll_seconds=args.poll_seconds,
        timeout_seconds=args.timeout_seconds,
        orchestrator=orchestrator,
    )
    orchestrator.summary["boundary"] = boundary
    orchestrator.flush()

    store = OpsStore()
    try:
        category_state = str((store.get_task(args.category_task_id) or {}).get("status") or "").strip().lower()
        if category_state in {"pending", "leased", "running"}:
            store.cancel_task(args.category_task_id)
            orchestrator.log("category task cancelled after safe boundary", task_id=args.category_task_id, previous_status=category_state)
        else:
            orchestrator.log("category task already inactive", task_id=args.category_task_id, status=category_state)
    finally:
        store.close()

    time.sleep(max(10, args.poll_seconds))
    terminate_process_tree(args.category_worker_pid, orchestrator=orchestrator, label="old-category-worker")
    terminate_process_tree(args.keyword_worker_pid, orchestrator=orchestrator, label="old-keyword-worker")

    sync_result = run_sync(orchestrator=orchestrator)
    orchestrator.summary["sync_result"] = sync_result
    orchestrator.flush()

    created_by = f"codex_drain_restart_{timestamp_token()}"
    store = OpsStore()
    try:
        new_category_task = create_clone_task(
            store=store,
            source_task=category_task,
            created_by=created_by,
            display_name="local category 7x24 restarted",
        )
        new_keyword_task = create_clone_task(
            store=store,
            source_task=keyword_task,
            created_by=created_by,
            display_name="local keyword 7x24 restarted",
        )
    finally:
        store.close()

    worker_token = timestamp_token()
    new_category_worker_name = f"local-category-7x24-restart-{worker_token}"
    new_keyword_worker_name = f"local-keyword-7x24-restart-{worker_token}"
    new_category_worker_pid = start_worker(
        worker_type="category",
        worker_name=new_category_worker_name,
        poll_seconds=10,
        stdout_path=report_dir / "category_worker.stdout.log",
        stderr_path=report_dir / "category_worker.stderr.log",
    )
    new_keyword_worker_pid = start_worker(
        worker_type="keyword",
        worker_name=new_keyword_worker_name,
        poll_seconds=10,
        stdout_path=report_dir / "keyword_worker.stdout.log",
        stderr_path=report_dir / "keyword_worker.stderr.log",
    )

    orchestrator.summary["restart"] = {
        "new_category_task": new_category_task,
        "new_keyword_task": new_keyword_task,
        "new_category_worker": {
            "name": new_category_worker_name,
            "pid": new_category_worker_pid,
        },
        "new_keyword_worker": {
            "name": new_keyword_worker_name,
            "pid": new_keyword_worker_pid,
        },
    }
    orchestrator.summary["finished_at"] = utcnow_iso()
    orchestrator.log(
        "drain restart completed",
        new_category_task_id=int(new_category_task["id"]),
        new_keyword_task_id=int(new_keyword_task["id"]),
        new_category_worker_pid=new_category_worker_pid,
        new_keyword_worker_pid=new_keyword_worker_pid,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
