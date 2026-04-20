from __future__ import annotations

import os
import socket
import time
from typing import Any

from ops.task_store import OpsStore

ACTIVE_WAREHOUSE_SYNC_STATUSES = ("pending", "leased", "running")
TERMINAL_TASK_STATUSES = {"completed", "failed", "cancelled", "skipped"}
DEFAULT_SYNC_POLL_SECONDS = int(os.getenv("NOON_CATEGORY_SYNC_POLL_SECONDS") or "5")
DEFAULT_SYNC_WAIT_SECONDS = int(os.getenv("NOON_CATEGORY_SYNC_WAIT_SECONDS") or "1800")


def _canonical_sync_db_refs(trigger_db: str, warehouse_db: str) -> tuple[str, str]:
    canonical_trigger_db = str(os.getenv("NOON_SHARED_SYNC_TRIGGER_DB_REF") or "").strip() or str(trigger_db).strip()
    canonical_warehouse_db = str(os.getenv("NOON_SHARED_SYNC_WAREHOUSE_DB_REF") or "").strip() or str(warehouse_db).strip()
    return canonical_trigger_db, canonical_warehouse_db


def category_sync_mode() -> str:
    return str(os.getenv("NOON_CATEGORY_WAREHOUSE_SYNC_MODE") or "").strip().lower() or "local_runner"


def enqueue_shared_sync_enabled() -> bool:
    return category_sync_mode() == "enqueue_shared"


def detect_source_node() -> str:
    explicit = (
        str(os.getenv("NOON_SOURCE_NODE") or "").strip()
        or str(os.getenv("NOON_WORKER_NODE_HOST") or "").strip()
        or str(os.getenv("NOON_NODE_HOST") or "").strip()
    )
    if explicit:
        return explicit
    env_host = str(os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "").strip()
    if env_host:
        return env_host
    try:
        return socket.gethostname().strip() or "local"
    except Exception:
        return "local"


def _active_sync_tasks(store: OpsStore) -> list[dict[str, Any]]:
    seen_ids: set[int] = set()
    items: list[dict[str, Any]] = []
    for status in ACTIVE_WAREHOUSE_SYNC_STATUSES:
        for task in store.list_tasks(status=status, worker_type="sync", limit=100):
            if str(task.get("task_type") or "").strip().lower() != "warehouse_sync":
                continue
            task_id = int(task.get("id") or 0)
            if task_id <= 0 or task_id in seen_ids:
                continue
            seen_ids.add(task_id)
            items.append(task)
    items.sort(key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)))
    return items


def _latest_task_run(store: OpsStore, task_id: int) -> dict[str, Any] | None:
    runs = store.list_task_runs(task_id=int(task_id), limit=1)
    if not runs:
        return None
    return runs[0]


def _build_sync_result_payload(
    *,
    task: dict[str, Any] | None,
    run: dict[str, Any] | None,
    reason: str,
    actor: str,
    trigger_db: str,
    warehouse_db: str,
    source_node: str,
    snapshot_id: str,
    created: bool,
    reused_existing: bool,
    timeout: bool = False,
) -> dict[str, Any]:
    task = task or {}
    run = run or {}
    task_status = str(task.get("status") or "").strip().lower()
    result = dict(run.get("result") or {})
    sync_state = result.get("sync_state")
    if not isinstance(sync_state, dict):
        sync_state = {"status": task_status or ("failed" if timeout else "unknown")}
    payload = {
        "status": "failed" if timeout else (task_status or str(result.get("status") or "unknown")),
        "reason": str(result.get("reason") or reason),
        "actor": actor,
        "trigger_db": trigger_db,
        "warehouse_db": warehouse_db,
        "source_node": source_node,
        "snapshot_id": snapshot_id,
        "task_id": int(task.get("id") or 0) or None,
        "task_status": task_status,
        "created": bool(created),
        "reused_existing": bool(reused_existing),
        "skip_reason": str(result.get("skip_reason") or ""),
        "sync_state": sync_state,
        "log_tail": list(result.get("stderr_tail") or result.get("stdout_tail") or []),
        "error": str(run.get("error_text") or result.get("error") or ("warehouse_sync_wait_timeout" if timeout else "")),
    }
    return payload


def enqueue_or_reuse_warehouse_sync_task(
    *,
    actor: str,
    reason: str,
    trigger_db: str,
    warehouse_db: str,
    source_node: str,
    snapshot_id: str,
    created_by: str,
    priority: int = 90,
) -> tuple[OpsStore, dict[str, Any], bool]:
    trigger_db, warehouse_db = _canonical_sync_db_refs(trigger_db, warehouse_db)
    store = OpsStore()
    active_tasks = _active_sync_tasks(store)
    if active_tasks:
        return store, active_tasks[0], True
    task = store.create_task(
        task_type="warehouse_sync",
        payload={
            "actor": actor,
            "reason": reason,
            "trigger_db": trigger_db,
            "warehouse_db": warehouse_db,
            "source_node": source_node,
            "snapshot_id": snapshot_id,
        },
        created_by=created_by,
        priority=int(priority),
        schedule_type="manual",
        worker_type="sync",
        display_name=f"warehouse sync / {reason}",
    )
    return store, task, False


def enqueue_and_wait_for_warehouse_sync(
    *,
    actor: str,
    reason: str,
    trigger_db: str,
    warehouse_db: str,
    snapshot_id: str,
    source_node: str | None = None,
    created_by: str = "category_sync_request",
    poll_seconds: int | None = None,
    wait_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    resolved_source_node = str(source_node or detect_source_node()).strip() or "local"
    resolved_poll_seconds = max(1, int(poll_seconds or DEFAULT_SYNC_POLL_SECONDS))
    resolved_wait_seconds = max(resolved_poll_seconds, int(wait_timeout_seconds or DEFAULT_SYNC_WAIT_SECONDS))
    trigger_db, warehouse_db = _canonical_sync_db_refs(trigger_db, warehouse_db)
    store, task, reused_existing = enqueue_or_reuse_warehouse_sync_task(
        actor=actor,
        reason=reason,
        trigger_db=trigger_db,
        warehouse_db=warehouse_db,
        source_node=resolved_source_node,
        snapshot_id=snapshot_id,
        created_by=created_by,
    )
    created = not reused_existing
    try:
        deadline = time.time() + resolved_wait_seconds
        while time.time() < deadline:
            current_task = store.get_task(int(task["id"]))
            latest_run = _latest_task_run(store, int(task["id"]))
            current_status = str((current_task or {}).get("status") or "").strip().lower()
            if current_status in TERMINAL_TASK_STATUSES:
                return _build_sync_result_payload(
                    task=current_task,
                    run=latest_run,
                    reason=reason,
                    actor=actor,
                    trigger_db=trigger_db,
                    warehouse_db=warehouse_db,
                    source_node=resolved_source_node,
                    snapshot_id=snapshot_id,
                    created=created,
                    reused_existing=reused_existing,
                )
            time.sleep(resolved_poll_seconds)
        current_task = store.get_task(int(task["id"]))
        latest_run = _latest_task_run(store, int(task["id"]))
        return _build_sync_result_payload(
            task=current_task,
            run=latest_run,
            reason=reason,
            actor=actor,
            trigger_db=trigger_db,
            warehouse_db=warehouse_db,
            source_node=resolved_source_node,
            snapshot_id=snapshot_id,
            created=created,
            reused_existing=reused_existing,
            timeout=True,
        )
    finally:
        store.close()


__all__ = [
    "category_sync_mode",
    "enqueue_shared_sync_enabled",
    "detect_source_node",
    "enqueue_or_reuse_warehouse_sync_task",
    "enqueue_and_wait_for_warehouse_sync",
]
