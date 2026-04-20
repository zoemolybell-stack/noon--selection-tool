from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.config import (
    DatabaseConfig,
    get_ops_database_config,
    get_product_store_database_config,
    get_warehouse_database_config,
)
from ops.crawler_runtime_contract import DUPLICATE_LOCK_PATTERNS, detect_duplicate_lock_skip_reason


TERMINAL_STATUSES = {"completed", "failed", "cancelled", "skipped"}
ACTIVE_RUNTIME_STATUSES = {"queued", "pending", "running", "leased", "retrying"}
DEFAULT_SMOKE_SUBSTRINGS = ("smoke", "predeploy", "main_window_sleep_plan")
@dataclass
class CleanupPlan:
    stale_worker_names: list[str]
    stranded_running_task_ids: list[int]
    duplicate_lock_failed_task_ids: list[int]
    duplicate_lock_skip_reason_by_task_id: dict[int, str]
    duplicate_lock_last_error_by_task_id: dict[int, str]
    live_keyword_snapshot_ids: list[str]
    orphan_task_ids: list[int]
    smoke_task_ids: list[int]
    plan_linked_terminal_task_ids: list[int]
    history_task_ids: list[int]
    reasons_by_task_id: dict[int, list[str]]
    summary: dict[str, Any]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_required_dt(raw_value: Any, *, field_name: str) -> datetime:
    parsed = parse_dt(raw_value)
    if parsed is None:
        raise ValueError(f"Invalid {field_name}: {raw_value!r}")
    return parsed


def parse_dt(raw_value: Any) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def sqlite_table_columns(conn, table_name: str) -> set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info({table_name})")
    except Exception:
        return set()
    rows = cur.fetchall()
    names: set[str] = set()
    for row in rows:
        if isinstance(row, sqlite3.Row):
            names.add(str(row["name"]))
        elif isinstance(row, (tuple, list)) and len(row) > 1:
            names.add(str(row[1]))
    return names


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def apply_env_values(values: dict[str, str]) -> dict[str, str | None]:
    import os

    previous: dict[str, str | None] = {}
    for key, value in values.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    return previous


def restore_env(previous: dict[str, str | None]) -> None:
    import os

    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def json_load(raw_value: Any, default: Any):
    if isinstance(raw_value, (dict, list)):
        return raw_value
    text = str(raw_value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _duplicate_lock_skip_reason_for_failed_task(task: dict[str, Any]) -> str:
    status = str(task.get("status") or "").strip().lower()
    if status != "failed":
        return ""
    task_type = str(task.get("task_type") or "").strip().lower()
    if task_type not in {"keyword_monitor", "category_single", "category_ready_scan"}:
        return ""
    lease_owner = str(task.get("lease_owner") or "").strip()
    lease_expires_at = str(task.get("lease_expires_at") or "").strip()
    if lease_owner or lease_expires_at:
        return ""
    return detect_duplicate_lock_skip_reason(task.get("last_error"))


def _duplicate_lock_reclass_result(skip_reason: str, raw_error: str) -> str:
    return json.dumps(
        {
            "status": "skipped",
            "reason": "cleanup_ops_history_duplicate_lock_reclassify",
            "skip_reason": skip_reason,
            "skip_detail": raw_error or skip_reason,
            "reconciled": True,
        },
        ensure_ascii=False,
    )


def connect_ops_database(config: DatabaseConfig):
    if config.is_postgres:
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(config.dsn, row_factory=dict_row, autocommit=True)

    sqlite_path = config.sqlite_path_or_raise("ops database")
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    return conn


def connect_generic_database(config: DatabaseConfig):
    if config.is_postgres:
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(config.dsn, row_factory=dict_row, autocommit=True)

    sqlite_path = config.sqlite_path_or_raise("database")
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    return conn


def fetchall_dicts(conn, query: str) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            results.append(dict(row))
        elif hasattr(row, "keys"):
            results.append({key: row[key] for key in row.keys()})
        else:
            raise TypeError(f"Unsupported row type: {type(row)!r}")
    return results


def build_cleanup_plan(
    *,
    conn,
    stale_worker_seconds: int,
    orphan_terminal_days: int,
    smoke_terminal_hours: int,
    plan_linked_terminal_days: int,
    include_plan_linked_terminal: bool,
    smoke_substrings: tuple[str, ...],
    deploy_started_at: datetime | None = None,
    post_deploy_reconcile: bool = False,
) -> CleanupPlan:
    now = utcnow()
    workers = fetchall_dicts(conn, "SELECT * FROM workers ORDER BY heartbeat_at ASC, worker_name ASC")
    tasks = fetchall_dicts(
        conn,
        """
        SELECT id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json
        FROM tasks
        ORDER BY created_at ASC, id ASC
        """,
    )

    stale_worker_cutoff = now - timedelta(seconds=int(stale_worker_seconds))
    orphan_cutoff = now - timedelta(days=int(orphan_terminal_days))
    smoke_cutoff = now - timedelta(hours=int(smoke_terminal_hours))
    plan_linked_cutoff = now - timedelta(days=int(plan_linked_terminal_days))

    worker_rows: dict[str, dict[str, Any]] = {}
    live_worker_names: set[str] = set()
    newer_worker_types_after_deploy: set[str] = set()
    stale_workers_by_time: list[str] = []
    for worker in workers:
        worker_name = str(worker.get("worker_name") or "").strip()
        if not worker_name:
            continue
        worker_rows[worker_name] = worker
        live_worker_names.add(worker_name)
        heartbeat_at = parse_dt(worker.get("heartbeat_at"))
        if heartbeat_at and heartbeat_at < stale_worker_cutoff:
            stale_workers_by_time.append(worker_name)
        if deploy_started_at and heartbeat_at and heartbeat_at >= deploy_started_at:
            worker_type = str(worker.get("worker_type") or "").strip().lower()
            if worker_type:
                newer_worker_types_after_deploy.add(worker_type)

    stale_worker_set = set(stale_workers_by_time)
    stranded_running_task_ids: list[int] = []
    active_task_ids_by_worker_name: dict[str, list[int]] = defaultdict(list)
    for task in tasks:
        status = str(task.get("status") or "").strip().lower()
        lease_owner = str(task.get("lease_owner") or "").strip()
        if status not in {"running", "leased", "retrying", "pending", "queued"}:
            continue
        task_id = int(task["id"])
        if lease_owner:
            active_task_ids_by_worker_name[lease_owner].append(task_id)
        lease_expires_at = parse_dt(task.get("lease_expires_at"))
        current_worker = worker_rows.get(lease_owner) if lease_owner else None
        current_task_id = current_worker.get("current_task_id") if current_worker else None
        heartbeat_at = parse_dt(current_worker.get("heartbeat_at")) if current_worker else None
        current_worker_type = str(current_worker.get("worker_type") or "").strip().lower() if current_worker else ""

        is_stranded = False
        if not lease_owner:
            is_stranded = True
        elif lease_owner in stale_worker_set:
            is_stranded = True
        elif lease_owner not in live_worker_names:
            is_stranded = True
        elif lease_expires_at and lease_expires_at < now:
            is_stranded = True
        elif current_task_id is not None and int(current_task_id or 0) != task_id:
            is_stranded = True
        elif (
            post_deploy_reconcile
            and deploy_started_at
            and heartbeat_at
            and heartbeat_at < deploy_started_at
            and current_worker_type in newer_worker_types_after_deploy
        ):
            is_stranded = True

        if is_stranded:
            stranded_running_task_ids.append(task_id)

    if post_deploy_reconcile:
        stale_workers = []
        stranded_running_task_set = set(stranded_running_task_ids)
        for worker_name, worker in worker_rows.items():
            heartbeat_at = parse_dt(worker.get("heartbeat_at"))
            worker_type = str(worker.get("worker_type") or "").strip().lower()
            if not heartbeat_at or not deploy_started_at or heartbeat_at >= deploy_started_at:
                continue
            if worker_type not in newer_worker_types_after_deploy:
                continue
            active_task_ids = active_task_ids_by_worker_name.get(worker_name) or []
            if any(task_id not in stranded_running_task_set for task_id in active_task_ids):
                continue
            stale_workers.append(worker_name)
    else:
        stale_workers = stale_workers_by_time

    live_keyword_snapshot_ids: list[str] = []
    stranded_running_task_set = set(stranded_running_task_ids)
    for task in tasks:
        status = str(task.get("status") or "").strip().lower()
        if status not in ACTIVE_RUNTIME_STATUSES:
            continue
        if int(task["id"]) in stranded_running_task_set:
            continue
        if str(task.get("task_type") or "").strip().lower() != "keyword_monitor":
            continue
        payload = json_load(task.get("payload_json"), {})
        snapshot_id = str(payload.get("snapshot") or "").strip()
        if snapshot_id:
            live_keyword_snapshot_ids.append(snapshot_id)

    reasons_by_task_id: dict[int, list[str]] = defaultdict(list)
    orphan_task_ids: list[int] = []
    smoke_task_ids: list[int] = []
    plan_linked_terminal_task_ids: list[int] = []
    duplicate_lock_failed_task_ids: list[int] = []
    duplicate_lock_skip_reason_by_task_id: dict[int, str] = {}
    duplicate_lock_last_error_by_task_id: dict[int, str] = {}

    for task in tasks:
        task_id = int(task["id"])
        duplicate_skip_reason = _duplicate_lock_skip_reason_for_failed_task(task)
        if duplicate_skip_reason:
            duplicate_lock_failed_task_ids.append(task_id)
            duplicate_lock_skip_reason_by_task_id[task_id] = duplicate_skip_reason
            duplicate_lock_last_error_by_task_id[task_id] = str(task.get("last_error") or "").strip()

    if not post_deploy_reconcile:
        for task in tasks:
            status = str(task.get("status") or "").strip().lower()
            if status not in TERMINAL_STATUSES:
                continue
            task_id = int(task["id"])
            created_at = parse_dt(task.get("created_at"))
            created_by = str(task.get("created_by") or "").strip().lower()
            if created_at is None:
                continue

            is_orphan = (
                task.get("plan_id") is None
                and task.get("round_id") is None
                and task.get("round_item_id") is None
            )
            if is_orphan and created_at < orphan_cutoff:
                orphan_task_ids.append(task_id)
                reasons_by_task_id[task_id].append("orphan_terminal")

            if created_at < smoke_cutoff and any(marker in created_by for marker in smoke_substrings):
                smoke_task_ids.append(task_id)
                reasons_by_task_id[task_id].append("smoke_terminal")

            is_plan_linked = not is_orphan
            if (
                include_plan_linked_terminal
                and is_plan_linked
                and status in {"failed", "cancelled", "skipped"}
                and created_at < plan_linked_cutoff
            ):
                plan_linked_terminal_task_ids.append(task_id)
                reasons_by_task_id[task_id].append("plan_linked_terminal")

    history_task_ids = sorted(reasons_by_task_id.keys())
    summary = {
        "workers_total": len(workers),
        "stale_workers": len(stale_workers),
        "live_workers": len(live_worker_names),
        "stranded_running_tasks": len(set(stranded_running_task_ids)),
        "duplicate_lock_failed_candidates": len(set(duplicate_lock_failed_task_ids)),
        "tasks_total": len(tasks),
        "orphan_terminal_candidates": len(set(orphan_task_ids)),
        "smoke_terminal_candidates": len(set(smoke_task_ids)),
        "plan_linked_terminal_candidates": len(set(plan_linked_terminal_task_ids)),
        "history_candidate_total": len(history_task_ids),
        "task_candidates_total": len(history_task_ids),
        "stale_worker_cutoff": stale_worker_cutoff.isoformat(),
        "orphan_cutoff": orphan_cutoff.isoformat(),
        "smoke_cutoff": smoke_cutoff.isoformat(),
        "plan_linked_cutoff": plan_linked_cutoff.isoformat(),
        "include_plan_linked_terminal": bool(include_plan_linked_terminal),
        "post_deploy_reconcile": bool(post_deploy_reconcile),
        "deploy_started_at": deploy_started_at.isoformat() if deploy_started_at else "",
        "newer_worker_types_after_deploy": sorted(newer_worker_types_after_deploy),
    }
    return CleanupPlan(
        stale_worker_names=sorted(set(stale_workers)),
        stranded_running_task_ids=sorted(set(stranded_running_task_ids)),
        duplicate_lock_failed_task_ids=sorted(set(duplicate_lock_failed_task_ids)),
        duplicate_lock_skip_reason_by_task_id=dict(duplicate_lock_skip_reason_by_task_id),
        duplicate_lock_last_error_by_task_id=dict(duplicate_lock_last_error_by_task_id),
        live_keyword_snapshot_ids=sorted(set(live_keyword_snapshot_ids)),
        orphan_task_ids=sorted(set(orphan_task_ids)),
        smoke_task_ids=sorted(set(smoke_task_ids)),
        plan_linked_terminal_task_ids=sorted(set(plan_linked_terminal_task_ids)),
        history_task_ids=history_task_ids,
        reasons_by_task_id={key: sorted(set(value)) for key, value in reasons_by_task_id.items()},
        summary=summary,
    )


def build_result_payload(
    plan: CleanupPlan,
    *,
    mode: str,
    actions: dict[str, bool],
    dry_run: bool,
    deleted_workers: int = 0,
    deleted_task_runs: int = 0,
    deleted_tasks: int = 0,
    cancelled_running_tasks: int = 0,
    reconciled_task_runs: int = 0,
    reclassified_duplicate_tasks: int = 0,
    reclassified_duplicate_task_runs: int = 0,
    reconciled_terminal_task_runs: int = 0,
    reconciled_keyword_source_runs: int = 0,
    reconciled_keyword_warehouse_runs: int = 0,
) -> dict[str, Any]:
    sample_task_reasons = [
        {"task_id": task_id, "reasons": plan.reasons_by_task_id[task_id]}
        for task_id in plan.history_task_ids[:20]
    ]
    return {
        "status": "completed",
        "mode": "dry_run" if dry_run else "apply",
        "cleanup_mode": mode,
        "actions": actions,
        "summary": plan.summary,
        "stale_worker_names": plan.stale_worker_names,
        "stranded_running_task_ids": plan.stranded_running_task_ids,
        "duplicate_lock_failed_task_ids": plan.duplicate_lock_failed_task_ids,
        "orphan_task_ids": plan.orphan_task_ids,
        "smoke_task_ids": plan.smoke_task_ids,
        "plan_linked_terminal_task_ids": plan.plan_linked_terminal_task_ids,
        "history_task_ids": plan.history_task_ids,
        "task_ids": plan.history_task_ids,
        "sample_task_reasons": sample_task_reasons,
        "deleted_workers": deleted_workers,
        "deleted_task_runs": deleted_task_runs,
        "deleted_tasks": deleted_tasks,
        "cancelled_running_tasks": cancelled_running_tasks,
        "reconciled_task_runs": reconciled_task_runs,
        "reclassified_duplicate_tasks": reclassified_duplicate_tasks,
        "reclassified_duplicate_task_runs": reclassified_duplicate_task_runs,
        "reconciled_terminal_task_runs": reconciled_terminal_task_runs,
        "reconciled_keyword_source_runs": reconciled_keyword_source_runs,
        "reconciled_keyword_warehouse_runs": reconciled_keyword_warehouse_runs,
    }


def reconcile_terminal_task_runs(conn, config: DatabaseConfig, *, finished_at_text: str) -> int:
    active_statuses = tuple(sorted(ACTIVE_RUNTIME_STATUSES))
    terminal_statuses = tuple(sorted(TERMINAL_STATUSES))
    reconciled = 0
    if config.is_postgres:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tr.id, COALESCE(t.status, '') AS task_status
                    FROM task_runs tr
                    LEFT JOIN tasks t ON t.id = tr.task_id
                    WHERE tr.status = ANY(%s)
                      AND (
                        t.id IS NULL
                        OR COALESCE(t.status, '') = ANY(%s)
                      )
                    """,
                    (list(active_statuses), list(terminal_statuses)),
                )
                rows = cur.fetchall()
                for row in rows:
                    row_id = int(row["id"])
                    task_status = str(row["task_status"] or "").strip().lower()
                    final_status = task_status if task_status in TERMINAL_STATUSES else "cancelled"
                    cur.execute(
                        """
                        UPDATE task_runs
                        SET status = %s,
                            finished_at = CASE WHEN COALESCE(finished_at, '') = '' THEN %s ELSE finished_at END,
                            error_text = CASE
                                WHEN COALESCE(error_text, '') = '' AND %s = 'cancelled' THEN 'reconciled after deploy cleanup (task already terminal)'
                                ELSE error_text
                            END
                        WHERE id = %s
                        """,
                        (final_status, finished_at_text, final_status, row_id),
                    )
                    reconciled += int(cur.rowcount or 0)
        return reconciled

    cur = conn.cursor()
    task_run_columns = sqlite_table_columns(conn, "task_runs")
    placeholders_active = ",".join("?" for _ in active_statuses)
    placeholders_terminal = ",".join("?" for _ in terminal_statuses)
    rows = cur.execute(
        f"""
        SELECT tr.id, COALESCE(t.status, '') AS task_status
        FROM task_runs tr
        LEFT JOIN tasks t ON t.id = tr.task_id
        WHERE tr.status IN ({placeholders_active})
          AND (
            t.id IS NULL
            OR COALESCE(t.status, '') IN ({placeholders_terminal})
          )
        """,
        (*active_statuses, *terminal_statuses),
    ).fetchall()
    for row in rows:
        row_id = int(row["id"])
        task_status = str(row["task_status"] or "").strip().lower()
        final_status = task_status if task_status in TERMINAL_STATUSES else "cancelled"
        set_clauses = ["status = ?"]
        params: list[Any] = [final_status]
        if "finished_at" in task_run_columns:
            set_clauses.append("finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)")
        if "error_text" in task_run_columns:
            set_clauses.append(
                "error_text = CASE WHEN COALESCE(error_text, '') = '' AND ? = 'cancelled' THEN 'reconciled after deploy cleanup (task already terminal)' ELSE error_text END"
            )
            params.append(final_status)
        cur.execute(
            f"""
            UPDATE task_runs
            SET {", ".join(set_clauses)}
            WHERE id = ?
            """,
            (*params, row_id),
        )
        reconciled += int(cur.rowcount or 0)
    conn.commit()
    return reconciled


def reconcile_keyword_runtime_runs(
    config: DatabaseConfig,
    *,
    table_name: str,
    deploy_started_at: datetime,
    live_snapshot_ids: list[str],
    finished_at_text: str,
    source_config: DatabaseConfig | None = None,
) -> int:
    if config.is_sqlite and (not config.sqlite_path or not config.sqlite_path.exists()):
        return 0
    conn = connect_generic_database(config)
    deploy_started_text = deploy_started_at.isoformat()
    reconciled = 0
    try:
        if config.is_postgres:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
                row = cur.fetchone()
                exists = bool(row[0] if isinstance(row, (tuple, list)) else row["exists"])
            if not exists:
                return 0
        else:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            if not row or int(row[0]) == 0:
                return 0
        if table_name == "keyword_runs_log" and source_config is not None:
            source_conn = connect_generic_database(source_config)
            try:
                if source_config.is_postgres:
                    with source_conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT id, COALESCE(status, '') AS status, COALESCE(finished_at, '') AS finished_at
                            FROM keyword_runs
                            """
                        )
                        source_rows = {
                            int(row["id"]): {
                                "status": str(row["status"] or "").strip().lower(),
                                "finished_at": str(row["finished_at"] or "").strip(),
                            }
                            for row in cur.fetchall()
                        }
                else:
                    cur = source_conn.cursor()
                    source_rows = {
                        int(row["id"]): {
                            "status": str(row["status"] or "").strip().lower(),
                            "finished_at": str(row["finished_at"] or "").strip(),
                        }
                        for row in cur.execute(
                            """
                            SELECT id, COALESCE(status, '') AS status, COALESCE(finished_at, '') AS finished_at
                            FROM keyword_runs
                            """
                        ).fetchall()
                    }
            finally:
                source_conn.close()

            if config.is_postgres:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT id, source_run_id, COALESCE(snapshot_id, '') AS snapshot_id, COALESCE(started_at, '') AS started_at
                            FROM {table_name}
                            WHERE status = ANY(%s)
                            """,
                            (list(sorted(ACTIVE_RUNTIME_STATUSES)),),
                        )
                        rows = cur.fetchall()
                        for row in rows:
                            row_id = int(row["id"])
                            source_run_id = int(row["source_run_id"] or 0)
                            snapshot_id = str(row["snapshot_id"] or "").strip()
                            started_at = str(row["started_at"] or "").strip()
                            source_row = source_rows.get(source_run_id)
                            if source_row and source_row["status"] and source_row["status"] not in ACTIVE_RUNTIME_STATUSES:
                                cur.execute(
                                    f"""
                                    UPDATE {table_name}
                                    SET status = %s,
                                        finished_at = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        source_row["status"],
                                        source_row["finished_at"] or finished_at_text,
                                        row_id,
                                    ),
                                )
                                reconciled += int(cur.rowcount or 0)
                                continue
                            if snapshot_id in live_snapshot_ids:
                                continue
                            if started_at and started_at < deploy_started_text:
                                cur.execute(
                                    f"""
                                    UPDATE {table_name}
                                    SET status = 'cancelled',
                                        finished_at = COALESCE(NULLIF(finished_at, ''), %s)
                                    WHERE id = %s
                                    """,
                                    (finished_at_text, row_id),
                                )
                                reconciled += int(cur.rowcount or 0)
                return reconciled

            cur = conn.cursor()
            placeholders_active = ",".join("?" for _ in ACTIVE_RUNTIME_STATUSES)
            rows = cur.execute(
                f"""
                SELECT id, source_run_id, COALESCE(snapshot_id, '') AS snapshot_id, COALESCE(started_at, '') AS started_at
                FROM {table_name}
                WHERE status IN ({placeholders_active})
                """,
                tuple(sorted(ACTIVE_RUNTIME_STATUSES)),
            ).fetchall()
            for row in rows:
                row_id = int(row["id"])
                source_run_id = int(row["source_run_id"] or 0)
                snapshot_id = str(row["snapshot_id"] or "").strip()
                started_at = str(row["started_at"] or "").strip()
                source_row = source_rows.get(source_run_id)
                if source_row and source_row["status"] and source_row["status"] not in ACTIVE_RUNTIME_STATUSES:
                    cur.execute(
                        f"""
                        UPDATE {table_name}
                        SET status = ?, finished_at = ?
                        WHERE id = ?
                        """,
                        (source_row["status"], source_row["finished_at"] or finished_at_text, row_id),
                    )
                    reconciled += int(cur.rowcount or 0)
                    continue
                if snapshot_id in live_snapshot_ids:
                    continue
                if started_at and started_at < deploy_started_text:
                    cur.execute(
                        f"""
                        UPDATE {table_name}
                        SET status = 'cancelled',
                            finished_at = COALESCE(NULLIF(finished_at, ''), ?)
                        WHERE id = ?
                        """,
                        (finished_at_text, row_id),
                    )
                    reconciled += int(cur.rowcount or 0)
            conn.commit()
            return reconciled
        if config.is_postgres:
            with conn.transaction():
                with conn.cursor() as cur:
                    sql = f"""
                        UPDATE {table_name}
                        SET status = 'cancelled',
                            finished_at = CASE
                                WHEN COALESCE(finished_at, '') = '' THEN %s
                                ELSE finished_at
                            END
                        WHERE status = ANY(%s)
                          AND COALESCE(started_at, '') <> ''
                          AND started_at < %s
                    """
                    params: list[Any] = [finished_at_text, list(sorted(ACTIVE_RUNTIME_STATUSES)), deploy_started_text]
                    if live_snapshot_ids:
                        sql += " AND COALESCE(snapshot_id, '') <> ALL(%s)"
                        params.append(list(live_snapshot_ids))
                    cur.execute(sql, tuple(params))
                    reconciled = int(cur.rowcount or 0)
            return reconciled

        cur = conn.cursor()
        placeholders_active = ",".join("?" for _ in ACTIVE_RUNTIME_STATUSES)
        sql = f"""
            UPDATE {table_name}
            SET status = 'cancelled',
                finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)
            WHERE status IN ({placeholders_active})
              AND COALESCE(started_at, '') <> ''
              AND started_at < ?
        """
        params: list[Any] = [*sorted(ACTIVE_RUNTIME_STATUSES), deploy_started_text]
        if live_snapshot_ids:
            placeholders_live = ",".join("?" for _ in live_snapshot_ids)
            sql += f" AND COALESCE(snapshot_id, '') NOT IN ({placeholders_live})"
            params.extend(live_snapshot_ids)
        cur.execute(sql, tuple(params))
        reconciled = int(cur.rowcount or 0)
        conn.commit()
        return reconciled
    finally:
        conn.close()


def execute_cleanup(
    conn,
    config: DatabaseConfig,
    plan: CleanupPlan,
    *,
    delete_workers: bool,
    cancel_stranded_running: bool,
    delete_history: bool,
    reclassify_duplicate_failures: bool,
) -> tuple[int, int, int, int, int, int, int, int, int, int]:
    deleted_workers = 0
    deleted_task_runs = 0
    deleted_tasks = 0
    cancelled_running_tasks = 0
    reconciled_task_runs = 0
    reclassified_duplicate_tasks = 0
    reclassified_duplicate_task_runs = 0
    reconciled_terminal_task_runs = 0
    reconciled_keyword_source_runs = 0
    reconciled_keyword_warehouse_runs = 0
    finished_at_text = utcnow().isoformat()
    duplicate_ids_by_reason: dict[str, list[int]] = defaultdict(list)
    for task_id, skip_reason in plan.duplicate_lock_skip_reason_by_task_id.items():
        duplicate_ids_by_reason[skip_reason].append(int(task_id))

    if config.is_postgres:
        with conn.transaction():
            with conn.cursor() as cur:
                if cancel_stranded_running and plan.stranded_running_task_ids:
                    cur.execute(
                        """
                        UPDATE tasks
                        SET status = 'cancelled',
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            updated_at = NOW(),
                            last_error = CASE
                                WHEN COALESCE(last_error, '') = '' THEN 'cancelled by cleanup_ops_history (stale worker lease)'
                                ELSE last_error
                            END
                        WHERE id = ANY(%s)
                        """,
                        (plan.stranded_running_task_ids,),
                    )
                    cancelled_running_tasks = int(cur.rowcount or 0)
                    cur.execute(
                        """
                        UPDATE task_runs
                        SET status = 'cancelled',
                            finished_at = %s,
                            error_text = CASE
                                WHEN COALESCE(error_text, '') = '' THEN 'reconciled after deploy cleanup (stale running task)'
                                ELSE error_text
                            END,
                            result_json = CASE
                                WHEN COALESCE(result_json, '') IN ('', '{}') THEN %s
                                ELSE result_json
                            END
                        WHERE task_id = ANY(%s)
                          AND status IN ('leased', 'running')
                        """,
                        (
                            finished_at_text,
                            json.dumps(
                                {
                                    "status": "cancelled",
                                    "reason": "post_deploy_reconcile",
                                    "skip_reason": "",
                                    "reconciled": True,
                                },
                                ensure_ascii=False,
                            ),
                            plan.stranded_running_task_ids,
                        ),
                    )
                    reconciled_task_runs = int(cur.rowcount or 0)
                if reclassify_duplicate_failures and plan.duplicate_lock_failed_task_ids:
                    for skip_reason, task_ids in duplicate_ids_by_reason.items():
                        if not task_ids:
                            continue
                        skip_detail = plan.duplicate_lock_last_error_by_task_id.get(task_ids[0], skip_reason)
                        cur.execute(
                            """
                            UPDATE tasks
                            SET status = 'skipped',
                                lease_owner = NULL,
                                lease_expires_at = NULL,
                                updated_at = NOW(),
                                last_error = %s
                            WHERE id = ANY(%s)
                              AND status = 'failed'
                            """,
                            (skip_reason, task_ids),
                        )
                        reclassified_duplicate_tasks += int(cur.rowcount or 0)
                        cur.execute(
                            """
                            UPDATE task_runs
                            SET status = 'skipped',
                                finished_at = CASE
                                    WHEN COALESCE(finished_at, '') = '' THEN %s
                                    ELSE finished_at
                                END,
                                error_text = %s,
                                result_json = %s
                            WHERE task_id = ANY(%s)
                              AND status = 'failed'
                            """,
                            (
                                finished_at_text,
                                skip_reason,
                                _duplicate_lock_reclass_result(skip_reason, skip_detail),
                                task_ids,
                            ),
                        )
                        reclassified_duplicate_task_runs += int(cur.rowcount or 0)
                if delete_workers and plan.stale_worker_names:
                    cur.execute("DELETE FROM workers WHERE worker_name = ANY(%s)", (plan.stale_worker_names,))
                    deleted_workers = int(cur.rowcount or 0)
                if delete_history and plan.history_task_ids:
                    cur.execute("DELETE FROM task_runs WHERE task_id = ANY(%s)", (plan.history_task_ids,))
                    deleted_task_runs = int(cur.rowcount or 0)
                    cur.execute("DELETE FROM tasks WHERE id = ANY(%s)", (plan.history_task_ids,))
                    deleted_tasks = int(cur.rowcount or 0)
        if cancel_stranded_running:
            reconciled_terminal_task_runs = reconcile_terminal_task_runs(conn, config, finished_at_text=finished_at_text)
        if plan.summary.get("post_deploy_reconcile"):
            product_store_config = get_product_store_database_config(Path("data/product_store.db"))
            warehouse_config = get_warehouse_database_config(Path("data/analytics/warehouse.db"))
            reconciled_keyword_source_runs = reconcile_keyword_runtime_runs(
                product_store_config,
                table_name="keyword_runs",
                deploy_started_at=parse_required_dt(plan.summary.get("deploy_started_at"), field_name="deploy_started_at"),
                live_snapshot_ids=plan.live_keyword_snapshot_ids,
                finished_at_text=finished_at_text,
            )
            reconciled_keyword_warehouse_runs = reconcile_keyword_runtime_runs(
                warehouse_config,
                table_name="keyword_runs_log",
                deploy_started_at=parse_required_dt(plan.summary.get("deploy_started_at"), field_name="deploy_started_at"),
                live_snapshot_ids=plan.live_keyword_snapshot_ids,
                finished_at_text=finished_at_text,
                source_config=product_store_config,
            )
        return (
            deleted_workers,
            deleted_task_runs,
            deleted_tasks,
            cancelled_running_tasks,
            reconciled_task_runs,
            reclassified_duplicate_tasks,
            reclassified_duplicate_task_runs,
            reconciled_terminal_task_runs,
            reconciled_keyword_source_runs,
            reconciled_keyword_warehouse_runs,
        )

    cur = conn.cursor()
    task_run_columns = sqlite_table_columns(conn, "task_runs")
    if cancel_stranded_running and plan.stranded_running_task_ids:
        placeholders = ",".join("?" for _ in plan.stranded_running_task_ids)
        cur.execute(
            f"""
            UPDATE tasks
            SET status = 'cancelled',
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = CURRENT_TIMESTAMP,
                last_error = CASE
                    WHEN COALESCE(last_error, '') = '' THEN 'cancelled by cleanup_ops_history (stale worker lease)'
                    ELSE last_error
                END
            WHERE id IN ({placeholders})
            """,
            tuple(plan.stranded_running_task_ids),
        )
        cancelled_running_tasks = int(cur.rowcount or 0)
        set_clauses = ["status = 'cancelled'"]
        params: list[Any] = []
        if "finished_at" in task_run_columns:
            set_clauses.append("finished_at = CURRENT_TIMESTAMP")
        if "error_text" in task_run_columns:
            set_clauses.append(
                "error_text = CASE WHEN COALESCE(error_text, '') = '' THEN 'reconciled after deploy cleanup (stale running task)' ELSE error_text END"
            )
        if "result_json" in task_run_columns:
            set_clauses.append(
                "result_json = CASE WHEN COALESCE(result_json, '') IN ('', '{}') THEN ? ELSE result_json END"
            )
            params.append(
                json.dumps(
                    {
                        "status": "cancelled",
                        "reason": "post_deploy_reconcile",
                        "skip_reason": "",
                        "reconciled": True,
                    },
                    ensure_ascii=False,
                )
            )
        cur.execute(
            f"""
            UPDATE task_runs
            SET {", ".join(set_clauses)}
            WHERE task_id IN ({placeholders})
              AND status IN ('leased', 'running')
            """,
            (*params, *tuple(plan.stranded_running_task_ids)),
        )
        reconciled_task_runs = int(cur.rowcount or 0)
    if reclassify_duplicate_failures and plan.duplicate_lock_failed_task_ids:
        for skip_reason, task_ids in duplicate_ids_by_reason.items():
            if not task_ids:
                continue
            skip_detail = plan.duplicate_lock_last_error_by_task_id.get(task_ids[0], skip_reason)
            placeholders = ",".join("?" for _ in task_ids)
            cur.execute(
                f"""
                UPDATE tasks
                SET status = 'skipped',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = CURRENT_TIMESTAMP,
                    last_error = ?
                WHERE id IN ({placeholders})
                  AND status = 'failed'
                """,
                (skip_reason, *task_ids),
            )
            reclassified_duplicate_tasks += int(cur.rowcount or 0)
            set_clauses = ["status = 'skipped'"]
            params: list[Any] = []
            if "finished_at" in task_run_columns:
                set_clauses.append("finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)")
            set_clauses.append("error_text = ?")
            params.append(skip_reason)
            if "result_json" in task_run_columns:
                set_clauses.append("result_json = ?")
                params.append(_duplicate_lock_reclass_result(skip_reason, skip_detail))
            cur.execute(
                f"""
                UPDATE task_runs
                SET {", ".join(set_clauses)}
                WHERE task_id IN ({placeholders})
                  AND status = 'failed'
                """,
                (*params, *task_ids),
            )
            reclassified_duplicate_task_runs += int(cur.rowcount or 0)
    if delete_workers and plan.stale_worker_names:
        placeholders = ",".join("?" for _ in plan.stale_worker_names)
        cur.execute(f"DELETE FROM workers WHERE worker_name IN ({placeholders})", tuple(plan.stale_worker_names))
        deleted_workers = int(cur.rowcount or 0)
    if delete_history and plan.history_task_ids:
        placeholders = ",".join("?" for _ in plan.history_task_ids)
        cur.execute(f"DELETE FROM task_runs WHERE task_id IN ({placeholders})", tuple(plan.history_task_ids))
        deleted_task_runs = int(cur.rowcount or 0)
        cur.execute(f"DELETE FROM tasks WHERE id IN ({placeholders})", tuple(plan.history_task_ids))
        deleted_tasks = int(cur.rowcount or 0)
    conn.commit()
    if cancel_stranded_running:
        reconciled_terminal_task_runs = reconcile_terminal_task_runs(conn, config, finished_at_text=finished_at_text)
    if plan.summary.get("post_deploy_reconcile"):
        product_store_config = get_product_store_database_config(Path("data/product_store.db"))
        warehouse_config = get_warehouse_database_config(Path("data/analytics/warehouse.db"))
        deploy_started_at = parse_required_dt(plan.summary.get("deploy_started_at"), field_name="deploy_started_at")
        reconciled_keyword_source_runs = reconcile_keyword_runtime_runs(
            product_store_config,
            table_name="keyword_runs",
            deploy_started_at=deploy_started_at,
            live_snapshot_ids=plan.live_keyword_snapshot_ids,
            finished_at_text=finished_at_text,
        )
        reconciled_keyword_warehouse_runs = reconcile_keyword_runtime_runs(
            warehouse_config,
            table_name="keyword_runs_log",
            deploy_started_at=deploy_started_at,
            live_snapshot_ids=plan.live_keyword_snapshot_ids,
            finished_at_text=finished_at_text,
            source_config=product_store_config,
        )
    return (
        deleted_workers,
        deleted_task_runs,
        deleted_tasks,
        cancelled_running_tasks,
        reconciled_task_runs,
        reclassified_duplicate_tasks,
        reclassified_duplicate_task_runs,
        reconciled_terminal_task_runs,
        reconciled_keyword_source_runs,
        reconciled_keyword_warehouse_runs,
    )


def resolve_mode(*, workers_only: bool, running_only: bool, history_only: bool) -> str:
    chosen = [name for name, enabled in (("workers", workers_only), ("running", running_only), ("history", history_only)) if enabled]
    if len(chosen) > 1:
        raise ValueError("Only one of --workers-only, --running-only, --history-only may be set at a time.")
    return chosen[0] if chosen else "full"


def resolve_actions(mode: str, *, post_deploy_reconcile: bool = False) -> dict[str, bool]:
    if post_deploy_reconcile:
        return {
            "delete_workers": True,
            "cancel_stranded_running": True,
            "delete_history": False,
            "reclassify_duplicate_failures": True,
        }
    if mode == "workers":
        return {
            "delete_workers": True,
            "cancel_stranded_running": False,
            "delete_history": False,
            "reclassify_duplicate_failures": False,
        }
    if mode == "running":
        return {
            "delete_workers": False,
            "cancel_stranded_running": True,
            "delete_history": False,
            "reclassify_duplicate_failures": False,
        }
    if mode == "history":
        return {
            "delete_workers": False,
            "cancel_stranded_running": False,
            "delete_history": True,
            "reclassify_duplicate_failures": False,
        }
    return {
        "delete_workers": True,
        "cancel_stranded_running": True,
        "delete_history": True,
        "reclassify_duplicate_failures": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean stale worker rows and old terminal ops task history.")
    parser.add_argument("--env-file", default="", help="Optional .env style file. When provided, NOON_* values are loaded from it.")
    parser.add_argument("--apply", action="store_true", help="Execute deletion. Defaults to dry-run.")
    parser.add_argument("--output-json", default="", help="Optional output path for the result payload.")
    parser.add_argument("--stale-worker-seconds", type=int, default=21600, help="Delete worker rows whose heartbeat is older than this many seconds. Default: 21600 (6h).")
    parser.add_argument("--orphan-terminal-days", type=int, default=7, help="Delete terminal tasks older than this many days when they are not linked to a plan/round. Default: 7.")
    parser.add_argument("--smoke-terminal-hours", type=int, default=6, help="Delete smoke/dev terminal tasks older than this many hours. Default: 6.")
    parser.add_argument("--include-plan-linked-terminal", action="store_true", help="Also delete failed/cancelled/skipped terminal tasks that are linked to a plan/round when older than the plan-linked cutoff.")
    parser.add_argument("--plan-linked-terminal-days", type=int, default=5, help="Age threshold in days for plan-linked failed/cancelled/skipped terminal tasks. Default: 5.")
    parser.add_argument("--post-deploy-reconcile", action="store_true", help="Only reconcile records that are provably stranded because they predate the current deploy and have newer replacement worker rows.")
    parser.add_argument("--deploy-started-at", default="", help="Required with --post-deploy-reconcile. UTC ISO timestamp marking the start of the deploy window.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--workers-only", action="store_true", help="Only delete stale worker rows.")
    mode_group.add_argument("--running-only", action="store_true", help="Only cancel stale running tasks that belong to stale workers.")
    mode_group.add_argument("--history-only", action="store_true", help="Only prune terminal history by retention policy.")
    args = parser.parse_args()

    env_values: dict[str, str] = {}
    if args.env_file:
        env_path = Path(args.env_file).resolve()
        env_values = load_env_file(env_path)
    previous_env = apply_env_values(env_values) if env_values else {}

    try:
        if args.post_deploy_reconcile and not args.deploy_started_at:
            raise ValueError("--deploy-started-at is required with --post-deploy-reconcile.")
        deploy_started_at = (
            parse_required_dt(args.deploy_started_at, field_name="deploy_started_at")
            if args.deploy_started_at
            else None
        )
        mode = resolve_mode(
            workers_only=args.workers_only,
            running_only=args.running_only,
            history_only=args.history_only,
        )
        actions = resolve_actions(mode, post_deploy_reconcile=args.post_deploy_reconcile)
        ops_config = get_ops_database_config(Path("data/ops/ops.db"))
        conn = connect_ops_database(ops_config)
        try:
            plan = build_cleanup_plan(
                conn=conn,
                stale_worker_seconds=args.stale_worker_seconds,
                orphan_terminal_days=args.orphan_terminal_days,
                smoke_terminal_hours=args.smoke_terminal_hours,
                plan_linked_terminal_days=args.plan_linked_terminal_days,
                include_plan_linked_terminal=args.include_plan_linked_terminal,
                smoke_substrings=DEFAULT_SMOKE_SUBSTRINGS,
                deploy_started_at=deploy_started_at,
                post_deploy_reconcile=args.post_deploy_reconcile,
            )
            if args.apply:
                (
                    deleted_workers,
                    deleted_task_runs,
                    deleted_tasks,
                    cancelled_running_tasks,
                    reconciled_task_runs,
                    reclassified_duplicate_tasks,
                    reclassified_duplicate_task_runs,
                    reconciled_terminal_task_runs,
                    reconciled_keyword_source_runs,
                    reconciled_keyword_warehouse_runs,
                ) = execute_cleanup(
                    conn,
                    ops_config,
                    plan,
                    delete_workers=actions["delete_workers"],
                    cancel_stranded_running=actions["cancel_stranded_running"],
                    delete_history=actions["delete_history"],
                    reclassify_duplicate_failures=actions["reclassify_duplicate_failures"],
                )
                result = build_result_payload(
                    plan,
                    mode=mode,
                    actions=actions,
                    dry_run=False,
                    deleted_workers=deleted_workers,
                    deleted_task_runs=deleted_task_runs,
                    deleted_tasks=deleted_tasks,
                    cancelled_running_tasks=cancelled_running_tasks,
                    reconciled_task_runs=reconciled_task_runs,
                    reclassified_duplicate_tasks=reclassified_duplicate_tasks,
                    reclassified_duplicate_task_runs=reclassified_duplicate_task_runs,
                    reconciled_terminal_task_runs=reconciled_terminal_task_runs,
                    reconciled_keyword_source_runs=reconciled_keyword_source_runs,
                    reconciled_keyword_warehouse_runs=reconciled_keyword_warehouse_runs,
                )
            else:
                result = build_result_payload(plan, mode=mode, actions=actions, dry_run=True)
        finally:
            conn.close()
    finally:
        if previous_env:
            restore_env(previous_env)

    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output_json:
        output_path = Path(args.output_json).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
