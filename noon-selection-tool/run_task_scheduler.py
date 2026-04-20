from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from ops.crawler_runtime_contract import (
    ACTIVE_CATEGORY_CRAWL_SKIP_REASON,
    ACTIVE_MONITOR_SKIP_REASON,
    detect_duplicate_lock_skip_reason,
)
from ops.crawler_control import dispatch_due_plans
from ops.task_store import LeaseResult, OpsStore


ROOT = Path(__file__).resolve().parent
DEFAULT_POLL_SECONDS = 10
DEFAULT_LEASE_TIMEOUT_SECONDS = 3600

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("task-scheduler")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NAS task scheduler / worker entrypoint")
    parser.add_argument("--mode", choices=["scheduler", "worker"], default="worker")
    parser.add_argument("--worker-type", choices=["keyword", "category", "sync"], default=os.getenv("NOON_WORKER_TYPE") or "")
    parser.add_argument("--worker-name", type=str, default="", help="explicit worker identity")
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("WORKER_POLL_SECONDS") or DEFAULT_POLL_SECONDS))
    parser.add_argument("--lease-timeout-seconds", type=int, default=int(os.getenv("TASK_LEASE_TIMEOUT_SECONDS") or DEFAULT_LEASE_TIMEOUT_SECONDS))
    parser.add_argument("--max-tasks", type=int, default=0, help="stop after N tasks, 0 means loop forever")
    return parser.parse_args()


def _python() -> str:
    return sys.executable


def _worker_name(mode: str, worker_type: str, explicit: str = "") -> str:
    if explicit.strip():
        return explicit.strip()
    host = _worker_node_host()
    pid = os.getpid()
    if _worker_node_role(worker_type) == "remote_category" and worker_type == "category":
        return f"remote-category-{host}-{pid}"
    suffix = worker_type or mode
    return f"{suffix}-{host}-{pid}"


def _worker_node_role(worker_type: str = "") -> str:
    explicit = str(os.getenv("NOON_WORKER_NODE_ROLE") or "").strip().lower()
    if explicit:
        return explicit
    if worker_type == "category" and str(os.getenv("NOON_REMOTE_CATEGORY_NODE_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return "remote_category"
    return ""


def _worker_node_host() -> str:
    explicit = str(
        os.getenv("NOON_WORKER_NODE_HOST")
        or os.getenv("NOON_NODE_HOST")
        or os.getenv("HOSTNAME")
        or os.getenv("COMPUTERNAME")
        or ""
    ).strip()
    return explicit or "local"


def _worker_details(worker_type: str, **extra: Any) -> dict[str, Any]:
    details: dict[str, Any] = {**extra}
    node_role = _worker_node_role(worker_type)
    if node_role:
        details["node_role"] = node_role
        details["node_host"] = _worker_node_host()
    return details


def _normalize_platforms(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [item.strip() for item in raw.replace(",", " ").split() if item.strip()]
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _payload_bool(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    raw = payload.get(key, default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_keyword_monitor_active_message(lines: list[str] | tuple[str, ...]) -> bool:
    return detect_duplicate_lock_skip_reason("\n".join(line for line in lines if line)) == ACTIVE_MONITOR_SKIP_REASON


def _is_category_crawl_active_message(lines: list[str] | tuple[str, ...]) -> bool:
    return detect_duplicate_lock_skip_reason("\n".join(line for line in lines if line)) == ACTIVE_CATEGORY_CRAWL_SKIP_REASON


def _write_keyword_batch_file(task: dict[str, Any], keywords: list[str]) -> Path:
    task_inputs_dir = ROOT / "data" / "ops" / "task_inputs"
    task_inputs_dir.mkdir(parents=True, exist_ok=True)
    file_path = task_inputs_dir / f"keyword_batch_task_{int(task['id'])}.txt"
    file_path.write_text("\n".join(keywords) + "\n", encoding="utf-8")
    return file_path


def build_task_command(task: dict[str, Any]) -> list[str]:
    payload = task.get("payload") or {}
    task_type = str(task.get("task_type") or "").strip().lower()
    command: list[str]

    if task_type == "keyword_monitor":
        command = [
            _python(),
            str(ROOT / "run_keyword_monitor.py"),
            "--monitor-config",
            str(payload["monitor_config"]),
            "--runtime-scope",
            "keyword",
            "--noon-count",
            str(int(payload.get("noon_count") or 30)),
            "--amazon-count",
            str(int(payload.get("amazon_count") or 30)),
        ]
        if str(payload.get("snapshot") or "").strip():
            command.extend(["--snapshot", str(payload["snapshot"])])
        if _payload_bool(payload, "resume", False):
            command.append("--resume")
        if _payload_bool(payload, "persist", True):
            command.append("--persist")
        if _payload_bool(payload, "monitor_report", False):
            command.append("--monitor-report")
        monitor_seed_keyword = str(payload.get("monitor_seed_keyword") or "").strip()
        if monitor_seed_keyword:
            command.extend(["--monitor-seed-keyword", monitor_seed_keyword])
        return command

    if task_type == "keyword_once":
        command = [
            _python(),
            str(ROOT / "keyword_main.py"),
            "--step",
            "scrape",
            "--keyword",
            str(payload["keyword"]),
            "--runtime-scope",
            "keyword",
            "--noon-count",
            str(int(payload.get("noon_count") or 30)),
            "--amazon-count",
            str(int(payload.get("amazon_count") or 30)),
        ]
        if str(payload.get("snapshot") or "").strip():
            command.extend(["--snapshot", str(payload["snapshot"])])
        if _payload_bool(payload, "resume", False):
            command.append("--resume")
        platforms = _normalize_platforms(payload.get("platforms"))
        if platforms:
            command.extend(["--platforms", *platforms])
        if _payload_bool(payload, "persist", True):
            command.append("--persist")
        if _payload_bool(payload, "export_excel", False):
            command.append("--export-excel")
        return command

    if task_type == "keyword_batch":
        keywords = payload.get("keywords") or []
        if isinstance(keywords, str):
            keywords = [item.strip() for item in keywords.splitlines() if item.strip()]
        keywords = [str(item).strip() for item in keywords if str(item).strip()]
        if not keywords:
            raise ValueError("keyword_batch requires payload.keywords")
        keywords_file = _write_keyword_batch_file(task, keywords)
        command = [
            _python(),
            str(ROOT / "keyword_main.py"),
            "--step",
            "scrape",
            "--keywords-file",
            str(keywords_file),
            "--runtime-scope",
            "keyword",
            "--noon-count",
            str(int(payload.get("noon_count") or 30)),
            "--amazon-count",
            str(int(payload.get("amazon_count") or 30)),
        ]
        if str(payload.get("snapshot") or "").strip():
            command.extend(["--snapshot", str(payload["snapshot"])])
        if _payload_bool(payload, "resume", False):
            command.append("--resume")
        platforms = _normalize_platforms(payload.get("platforms"))
        if platforms:
            command.extend(["--platforms", *platforms])
        if _payload_bool(payload, "persist", True):
            command.append("--persist")
        if _payload_bool(payload, "export_excel", False):
            command.append("--export-excel")
        return command

    if task_type == "category_single":
        command = [
            _python(),
            str(ROOT / "main.py"),
            "--step",
            "category",
            "--category",
            str(payload["category"]),
            "--noon-count",
            str(int(payload.get("product_count") or 50)),
        ]
        if str(payload.get("snapshot") or "").strip():
            command.extend(["--snapshot", str(payload["snapshot"])])
        if _payload_bool(payload, "resume", False):
            command.append("--resume")
        target_subcategory = str(payload.get("target_subcategory") or "").strip()
        if target_subcategory:
            command.extend(["--target-subcategory", target_subcategory])
        if _payload_bool(payload, "persist", True):
            command.append("--persist")
        if _payload_bool(payload, "export_excel", False):
            command.append("--export-excel")
        return command

    if task_type == "category_ready_scan":
        default_count = int(
            payload.get("default_product_count_per_leaf")
            or payload.get("product_count")
            or 50
        )
        command = [
            _python(),
            str(ROOT / "run_ready_category_scan.py"),
            "--product-count",
            str(default_count),
        ]
        output_dir = str(payload.get("output_dir") or "").strip()
        if output_dir:
            command.extend(["--output-dir", output_dir])
        db_path = str(payload.get("db_path") or "").strip()
        if db_path:
            command.extend(["--db-path", db_path])
        warehouse_db = str(payload.get("warehouse_db") or "").strip()
        if warehouse_db:
            command.extend(["--warehouse-db", warehouse_db])
        categories = _normalize_platforms(payload.get("categories"))
        if categories:
            command.extend(["--categories", ",".join(categories)])
        category_overrides = payload.get("category_overrides") or {}
        if isinstance(category_overrides, dict) and category_overrides:
            command.extend(["--category-overrides-json", json.dumps(category_overrides, ensure_ascii=False)])
        subcategory_overrides = payload.get("subcategory_overrides") or {}
        if isinstance(subcategory_overrides, dict) and subcategory_overrides:
            command.extend(["--subcategory-overrides-json", json.dumps(subcategory_overrides, ensure_ascii=False)])
        if _payload_bool(payload, "persist", True):
            command.append("--persist")
        if not _payload_bool(payload, "export_excel", False):
            command.append("--no-export-excel")
        return command

    if task_type == "warehouse_sync":
        command = [
            _python(),
            str(ROOT / "run_shared_warehouse_sync.py"),
            "--actor",
            str(payload.get("actor") or "ops_scheduler"),
            "--reason",
            str(payload.get("reason") or "scheduled_sync"),
        ]
        trigger_db = str(payload.get("trigger_db") or "").strip()
        if trigger_db:
            command.extend(["--trigger-db", trigger_db])
        warehouse_db = str(payload.get("warehouse_db") or "").strip()
        if warehouse_db:
            command.extend(["--warehouse-db", warehouse_db])
        for item in payload.get("builder_args") or []:
            command.extend(["--builder-arg", str(item)])
        return command

    raise ValueError(f"unsupported task_type: {task_type}")


def maybe_run_followup_sync(task: dict[str, Any]) -> dict[str, Any] | None:
    payload = task.get("payload") or {}
    task_type = str(task.get("task_type") or "").strip().lower()
    if task_type != "category_ready_scan" or not _payload_bool(payload, "persist", True):
        return None

    # Category batch scans already perform incremental syncs during subcategory
    # persistence and a forced final sync before the crawler process exits. Doing
    # another full warehouse sync here keeps the task in `running` long after the
    # ERP has already been updated, and it blocks the alternating runner from
    # advancing to the next phase.
    return None


def _tail_lines(path: Path, limit: int = 20) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    return [line for line in lines if line.strip()][-limit:]


def _keepalive_task_run(
    *,
    task_id: int,
    lease_owner: str,
    worker_type: str,
    worker_name: str,
    task_type: str,
    poll_seconds: int,
    lease_timeout_seconds: int,
 ) -> tuple[bool, str]:
    store = OpsStore()
    try:
        task = store.get_task(task_id)
        if task is None:
            return False, "missing"
        task_status = str(task.get("status") or "").strip().lower()
        if task_status == "cancelled":
            store.heartbeat_worker(
                worker_name=worker_name,
                worker_type=worker_type,
                status="cancelling",
                current_task_id=task_id,
                details=_worker_details(worker_type, task_id=task_id, task_type=task_type, reason="task_cancelled"),
            )
            return False, "cancelled"

        refreshed = store.refresh_task_lease(
            task_id=task_id,
            lease_owner=lease_owner,
            lease_timeout_seconds=lease_timeout_seconds,
        )
        if not refreshed:
            return False, task_status or "lease_lost"
        store.heartbeat_worker(
            worker_name=worker_name,
            worker_type=worker_type,
            status="running",
            current_task_id=task_id,
            details=_worker_details(worker_type, task_id=task_id, task_type=task_type, poll_seconds=poll_seconds),
        )
        store.prune_stale_workers(max_age_seconds=max(lease_timeout_seconds * 2, 900))
        return True, "running"
    finally:
        store.close()


def _terminate_process_tree(process: subprocess.Popen[str], *, reason: str) -> None:
    if process.poll() is not None:
        return
    logger.warning("stopping task process pid=%s reason=%s", process.pid, reason)
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )
        else:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except Exception:
        try:
            process.terminate()
        except Exception:
            return

    deadline = time.time() + 10
    while process.poll() is None and time.time() < deadline:
        time.sleep(0.2)

    if process.poll() is None:
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            else:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass


def execute_task(
    task: dict[str, Any],
    *,
    worker_name: str,
    poll_seconds: int,
    lease_timeout_seconds: int,
) -> tuple[str, dict[str, Any], str]:
    command = build_task_command(task)
    task_id = int(task["id"])
    worker_type = str(task.get("worker_type") or "").strip().lower()
    task_type = str(task.get("task_type") or "").strip().lower()
    keepalive_interval = max(1, min(int(poll_seconds or DEFAULT_POLL_SECONDS), 30))
    cancelled_by_operator = False
    lease_lost = ""

    stdout_file = tempfile.NamedTemporaryFile(prefix=f"task_{task_id}_", suffix=".stdout.log", delete=False)
    stderr_file = tempfile.NamedTemporaryFile(prefix=f"task_{task_id}_", suffix=".stderr.log", delete=False)
    stdout_file.close()
    stderr_file.close()
    stdout_path = Path(stdout_file.name)
    stderr_path = Path(stderr_file.name)
    try:
        with stdout_path.open("w", encoding="utf-8", errors="replace") as stdout_handle, stderr_path.open(
            "w", encoding="utf-8", errors="replace"
        ) as stderr_handle:
            env = os.environ.copy()
            env["NOON_OPS_PROGRESS_ENABLED"] = "1"
            env["NOON_OPS_TASK_ID"] = str(task_id)
            env["NOON_OPS_TASK_TYPE"] = task_type
            process = subprocess.Popen(
                command,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=str(ROOT),
                env=env,
                start_new_session=True,
            )
            while process.poll() is None:
                time.sleep(keepalive_interval)
                keepalive_ok, task_state = _keepalive_task_run(
                    task_id=task_id,
                    lease_owner=worker_name,
                    worker_type=worker_type,
                    worker_name=worker_name,
                    task_type=task_type,
                    poll_seconds=keepalive_interval,
                    lease_timeout_seconds=lease_timeout_seconds,
                )
                if not keepalive_ok:
                    if task_state == "cancelled":
                        cancelled_by_operator = True
                        _terminate_process_tree(process, reason="task_cancelled")
                    else:
                        lease_lost = task_state or "lease_lost"
                        _terminate_process_tree(process, reason=lease_lost)
                    break
            returncode = int(process.wait())

        stdout_lines = _tail_lines(stdout_path)
        stderr_lines = _tail_lines(stderr_path)
    finally:
        stdout_path.unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)

    result = {
        "command": command,
        "returncode": returncode,
        "stdout_tail": stdout_lines[-20:],
        "stderr_tail": stderr_lines[-20:],
    }
    if cancelled_by_operator:
        result["cancel_reason"] = "task_cancelled_by_operator"
        return "cancelled", result, "task_cancelled_by_operator"
    if lease_lost:
        result["lease_state"] = lease_lost
        return "failed", result, f"task_lease_lost:{lease_lost}"
    if returncode != 0:
        error_text = stderr_lines[-1] if stderr_lines else (stdout_lines[-1] if stdout_lines else f"returncode={returncode}")
        duplicate_skip_reason = detect_duplicate_lock_skip_reason(
            "\n".join(list(stderr_lines or []) + list(stdout_lines or []) + [error_text])
        )
        if duplicate_skip_reason == ACTIVE_MONITOR_SKIP_REASON:
            result["skip_reason"] = ACTIVE_MONITOR_SKIP_REASON
            result["skip_detail"] = error_text
            return "skipped", result, ACTIVE_MONITOR_SKIP_REASON
        if duplicate_skip_reason == ACTIVE_CATEGORY_CRAWL_SKIP_REASON:
            result["skip_reason"] = ACTIVE_CATEGORY_CRAWL_SKIP_REASON
            result["skip_detail"] = error_text
            return "skipped", result, ACTIVE_CATEGORY_CRAWL_SKIP_REASON
        return "failed", result, error_text

    followup_sync = maybe_run_followup_sync(task)
    if followup_sync is not None:
        result["followup_sync"] = followup_sync
        if followup_sync["returncode"] != 0:
            return "failed", result, "category_ready_scan followup warehouse sync failed"

    return "completed", result, ""


def run_scheduler_loop(args: argparse.Namespace) -> int:
    worker_name = _worker_name("scheduler", "scheduler", args.worker_name)
    logger.info("scheduler loop started as %s", worker_name)
    handled = 0
    while True:
        store = OpsStore()
        try:
            released = store.release_expired_leases(lease_timeout_seconds=args.lease_timeout_seconds)
            dispatched = dispatch_due_plans(store)
            dispatched_tasks = [item for item in dispatched if item.get("id") is not None]
            skipped_plans = [item for item in dispatched if str(item.get("status") or "").strip().lower() == "skipped"]
            store.prune_stale_workers(max_age_seconds=max(args.lease_timeout_seconds * 2, 86400))
            store.heartbeat_worker(
                worker_name=worker_name,
                worker_type="scheduler",
                status="idle",
                details=_worker_details(
                    "scheduler",
                    released_expired_leases=released,
                    dispatched_plans=len(dispatched_tasks),
                    dispatched_task_ids=[int(item["id"]) for item in dispatched_tasks],
                    skipped_plans=len(skipped_plans),
                    skipped_plan_ids=[int(item["plan_id"]) for item in skipped_plans if item.get("plan_id") is not None],
                    skip_reasons=sorted({str(item.get("skip_reason") or "") for item in skipped_plans if item.get("skip_reason")}),
                ),
            )
        finally:
            store.close()
        if args.max_tasks and (released or dispatched_tasks):
            handled += int(released or 0) + len(dispatched_tasks)
            if handled >= args.max_tasks:
                return 0
        time.sleep(max(1, args.poll_seconds))


def run_worker_loop(args: argparse.Namespace) -> int:
    if not args.worker_type:
        raise SystemExit("--worker-type is required in worker mode")

    worker_name = _worker_name("worker", args.worker_type, args.worker_name)
    logger.info("worker loop started: %s (%s)", worker_name, args.worker_type)
    completed_count = 0

    while True:
        store = OpsStore()
        try:
            store.release_expired_leases(lease_timeout_seconds=args.lease_timeout_seconds)
            store.prune_stale_workers(max_age_seconds=max(args.lease_timeout_seconds * 2, 86400))
            store.heartbeat_worker(
                worker_name=worker_name,
                worker_type=args.worker_type,
                status="idle",
                details=_worker_details(args.worker_type, poll_seconds=args.poll_seconds),
            )
            lease = store.lease_next_task(
                worker_type=args.worker_type,
                lease_owner=worker_name,
                lease_timeout_seconds=args.lease_timeout_seconds,
            )
            if not lease.task or not lease.run_id:
                task = None
            else:
                task = lease.task
                store.mark_task_running(task_id=int(task["id"]), run_id=int(lease.run_id), command=build_task_command(task))
                store.heartbeat_worker(
                    worker_name=worker_name,
                    worker_type=args.worker_type,
                    status="running",
                    current_task_id=int(task["id"]),
                    details=_worker_details(args.worker_type, task_type=task["task_type"], task_id=int(task["id"])),
                )
        finally:
            store.close()

        if not task:
            time.sleep(max(1, args.poll_seconds))
            continue

        final_status, result, error_text = execute_task(
            task,
            worker_name=worker_name,
            poll_seconds=args.poll_seconds,
            lease_timeout_seconds=args.lease_timeout_seconds,
        )

        store = OpsStore()
        try:
            store.finish_task_run(
                task_id=int(task["id"]),
                run_id=int(lease.run_id),
                final_status=final_status,
                result=result,
                error_text=error_text,
            )
            store.heartbeat_worker(
                worker_name=worker_name,
                worker_type=args.worker_type,
                status="idle",
                details=_worker_details(args.worker_type, last_task_id=int(task["id"]), last_status=final_status),
            )
        finally:
            store.close()

        completed_count += 1
        if args.max_tasks and completed_count >= args.max_tasks:
            return 0


def main() -> int:
    args = parse_args()
    if args.mode == "scheduler":
        return run_scheduler_loop(args)
    return run_worker_loop(args)


if __name__ == "__main__":
    raise SystemExit(main())
