from __future__ import annotations

import json
import sqlite3
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db.config import DatabaseConfig, get_ops_database_config

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OPS_DB = ROOT / "data" / "ops" / "ops.db"

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency in local sqlite mode
    psycopg = None
    dict_row = None

TASK_TYPE_TO_WORKER = {
    "keyword_monitor": "keyword",
    "keyword_once": "keyword",
    "keyword_batch": "keyword",
    "category_single": "category",
    "category_ready_scan": "category",
    "warehouse_sync": "sync",
}

TERMINAL_STATUSES = {"completed", "failed", "cancelled", "skipped"}
RUNNABLE_STATUSES = {"pending"}
SUPPORTED_TASK_SCHEDULE_TYPES = {"manual", "once", "interval", "weekly"}
SUPPORTED_PLAN_SCHEDULE_KINDS = {"manual", "once", "interval", "weekly"}
SUPPORTED_TASK_STATUSES = {"pending", "leased", "running", "completed", "failed", "cancelled", "skipped"}
SUPPORTED_PLAN_TYPES = {"category_single", "category_ready_scan", "keyword_batch", "keyword_monitor"}
SUPPORTED_ROUND_STATUSES = {"active", "completed", "failed", "cancelled", "skipped"}
SUPPORTED_ROUND_ITEM_STATUSES = {"pending", "running", "completed", "skipped"}
WEEKDAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
WEEKDAY_TO_INDEX = {day: idx for idx, day in enumerate(WEEKDAY_ORDER)}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().isoformat()


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _json_load(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _route_hash(payload: Any) -> str:
    return hashlib.sha1(_json_dump(payload).encode("utf-8")).hexdigest()


def _normalize_schedule_type(value: str | None) -> str:
    schedule_type = (value or "manual").strip().lower() or "manual"
    if schedule_type not in SUPPORTED_TASK_SCHEDULE_TYPES:
        raise ValueError(f"unsupported schedule_type: {schedule_type}")
    return schedule_type


def _normalize_schedule_kind(value: str | None) -> str:
    schedule_kind = (value or "manual").strip().lower() or "manual"
    if schedule_kind not in SUPPORTED_PLAN_SCHEDULE_KINDS:
        raise ValueError(f"unsupported schedule_kind: {schedule_kind}")
    return schedule_kind


def _normalize_status(value: str | None) -> str:
    status = (value or "pending").strip().lower() or "pending"
    if status not in SUPPORTED_TASK_STATUSES:
        raise ValueError(f"unsupported status: {status}")
    return status


def _normalize_worker_type(task_type: str, worker_type: str | None = None) -> str:
    if worker_type:
        return worker_type.strip().lower()
    resolved = TASK_TYPE_TO_WORKER.get(task_type)
    if not resolved:
        raise ValueError(f"unsupported task_type: {task_type}")
    return resolved


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_weekly_days(days: list[Any] | None) -> list[str]:
    alias_map = {
        "mo": "mon",
        "tu": "tue",
        "we": "wed",
        "th": "thu",
        "fr": "fri",
        "sa": "sat",
        "su": "sun",
    }
    normalized = []
    for item in days or []:
        token = str(item or "").strip().lower()[:3]
        token = alias_map.get(token, token)
        if token in WEEKDAY_TO_INDEX and token not in normalized:
            normalized.append(token)
    return normalized


def _validate_schedule_json(schedule_kind: str, schedule_json: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(schedule_json or {})
    if schedule_kind == "manual":
        return {}
    if schedule_kind == "once":
        run_at = str(payload.get("run_at") or "").strip()
        if not run_at:
            raise ValueError("schedule_json.run_at is required for once")
        if _parse_dt(run_at) is None:
            raise ValueError("schedule_json.run_at must be iso datetime")
        return {"run_at": _parse_dt(run_at).isoformat()}
    if schedule_kind == "interval":
        seconds = _safe_int(payload.get("seconds"), 0)
        if seconds <= 0:
            raise ValueError("schedule_json.seconds must be > 0 for interval")
        return {"seconds": seconds}
    if schedule_kind == "weekly":
        days = _normalize_weekly_days(payload.get("days"))
        time_value = str(payload.get("time") or "").strip()
        if not days:
            raise ValueError("schedule_json.days is required for weekly")
        if len(time_value) != 5 or ":" not in time_value:
            raise ValueError("schedule_json.time must use HH:MM for weekly")
        hours, minutes = time_value.split(":", 1)
        hour = _safe_int(hours, -1)
        minute = _safe_int(minutes, -1)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError("schedule_json.time must use HH:MM for weekly")
        return {"days": days, "time": f"{hour:02d}:{minute:02d}"}
    raise ValueError(f"unsupported schedule_kind: {schedule_kind}")


def compute_plan_next_run_at(
    schedule_kind: str,
    schedule_json: dict[str, Any] | None,
    *,
    reference_time: datetime | None = None,
) -> str | None:
    normalized_kind = _normalize_schedule_kind(schedule_kind)
    payload = _validate_schedule_json(normalized_kind, schedule_json)
    now = _ensure_utc(reference_time or utcnow())

    if normalized_kind == "manual":
        return None
    if normalized_kind == "once":
        return str(payload["run_at"])
    if normalized_kind == "interval":
        return (now + timedelta(seconds=int(payload["seconds"]))).isoformat()

    days = _normalize_weekly_days(payload.get("days"))
    hour, minute = [int(part) for part in str(payload.get("time") or "00:00").split(":", 1)]
    current = now
    for offset in range(0, 8):
        candidate = current + timedelta(days=offset)
        if WEEKDAY_ORDER[candidate.weekday()] not in days:
            continue
        candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate > now:
            return candidate.isoformat()
    candidate = current + timedelta(days=7)
    candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
    while WEEKDAY_ORDER[candidate.weekday()] not in days:
        candidate += timedelta(days=1)
    return candidate.isoformat()


def get_ops_db_path() -> Path:
    return get_ops_db_config().sqlite_path_or_raise("ops task store")


def get_ops_db_config() -> DatabaseConfig:
    return get_ops_database_config(DEFAULT_OPS_DB)


@dataclass
class LeaseResult:
    task: dict[str, Any] | None
    run_id: int | None


class _TxCursorProxy:
    def __init__(self, cursor, *, backend: str):
        self._cursor = cursor
        self._backend = backend
        self.lastrowid = getattr(cursor, "lastrowid", None)

    def execute(self, sql: str, params: tuple[Any, ...] = ()):
        if self._backend == "sqlite":
            self._cursor.execute(sql, params)
            self.lastrowid = getattr(self._cursor, "lastrowid", None)
        else:
            self._cursor.execute(sql.replace("?", "%s"), params)
            self.lastrowid = getattr(self._cursor, "lastrowid", None)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class OpsStore:
    def __init__(self, db_path: Path | None = None):
        self.db_config = (
            DatabaseConfig(backend="sqlite", source_env="explicit", sqlite_path=Path(db_path))
            if db_path is not None
            else get_ops_db_config()
        )
        self.db_path = self.db_config.sqlite_path
        self.backend = self.db_config.backend
        self.param_token = "?" if self.backend == "sqlite" else "%s"
        self.conn = self._connect()
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _connect(self):
        if self.backend == "sqlite":
            db_path = self.db_config.sqlite_path_or_raise("ops task store")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            return conn

        if psycopg is None or dict_row is None:  # pragma: no cover - depends on optional package
            raise RuntimeError(
                "Postgres backend requested for ops task store, but psycopg is not installed. "
                "Install `psycopg[binary]` before enabling NOON_OPS_DATABASE_URL."
            )
        return psycopg.connect(
            str(self.db_config.dsn),
            row_factory=dict_row,
            autocommit=True,
        )

    def _sql(self, sql: str) -> str:
        if self.backend == "sqlite":
            return sql
        return sql.replace("?", "%s")

    def _table_exists(self, table_name: str) -> bool:
        if self.backend == "sqlite":
            row = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            return row is not None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = %s
                LIMIT 1
                """,
                (table_name,),
            )
            return cur.fetchone() is not None

    def _table_columns(self, table_name: str) -> set[str]:
        if self.backend == "sqlite":
            return {
                row["name"]
                for row in self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                """,
                (table_name,),
            )
            return {str(row["column_name"]) for row in cur.fetchall()}

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        if self.backend == "sqlite":
            self.conn.execute(sql, params)
            return
        with self.conn.cursor() as cur:
            cur.execute(self._sql(sql), params)

    def _fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | sqlite3.Row | None:
        if self.backend == "sqlite":
            return self.conn.execute(sql, params).fetchone()
        with self.conn.cursor() as cur:
            cur.execute(self._sql(sql), params)
            return cur.fetchone()

    def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any] | sqlite3.Row]:
        if self.backend == "sqlite":
            return list(self.conn.execute(sql, params).fetchall())
        with self.conn.cursor() as cur:
            cur.execute(self._sql(sql), params)
            return list(cur.fetchall())

    def _execute_script(self, sql_script: str) -> None:
        if self.backend == "sqlite":
            self.conn.executescript(sql_script)
            return
        with self.conn.cursor() as cur:
            statements = [statement.strip() for statement in sql_script.split(";") if statement.strip()]
            for statement in statements:
                cur.execute(statement)

    @contextmanager
    def _tx(self):
        if self.backend == "sqlite":
            cursor = self.conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                yield _TxCursorProxy(cursor, backend="sqlite")
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            return

        with self.conn.transaction():
            with self.conn.cursor() as cursor:
                yield _TxCursorProxy(cursor, backend="postgres")

    def _insert_and_get_id(self, cur, sql: str, params: tuple[Any, ...]) -> int:
        if self.backend == "sqlite":
            cur.execute(sql, params)
            return int(cur.lastrowid)
        cur.execute(self._sql(sql + " RETURNING id"), params)
        row = cur.fetchone()
        return int(row["id"])

    def _init_schema(self) -> None:
        if self.backend == "sqlite":
            self._execute_script(
                """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER,
                round_id INTEGER,
                round_item_id INTEGER,
                task_type TEXT NOT NULL,
                worker_type TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                created_by TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                progress_json TEXT NOT NULL DEFAULT '{}',
                schedule_type TEXT NOT NULL DEFAULT 'manual',
                schedule_expr TEXT NOT NULL DEFAULT '',
                last_run_at TEXT,
                next_run_at TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                worker_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                lease_owner TEXT NOT NULL,
                attempt_number INTEGER NOT NULL DEFAULT 1,
                command_json TEXT NOT NULL DEFAULT '[]',
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                error_text TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            );
            CREATE INDEX IF NOT EXISTS idx_task_runs_task
            ON task_runs(task_id, started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_task_runs_status
            ON task_runs(status, started_at DESC);

            CREATE TABLE IF NOT EXISTS workers (
                worker_name TEXT PRIMARY KEY,
                worker_type TEXT NOT NULL,
                status TEXT NOT NULL,
                current_task_id INTEGER,
                heartbeat_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS crawl_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_type TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                schedule_kind TEXT NOT NULL DEFAULT 'manual',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}',
                last_dispatched_at TEXT,
                next_run_at TEXT,
                last_run_status TEXT NOT NULL DEFAULT '',
                last_run_task_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS crawl_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                plan_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                payload_json TEXT NOT NULL DEFAULT '{}',
                context_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                current_task_id INTEGER,
                last_task_status TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS crawl_round_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER NOT NULL,
                item_key TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                item_order INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                current_task_id INTEGER,
                last_task_id INTEGER,
                last_task_status TEXT NOT NULL DEFAULT '',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(round_id, item_key)
            );
            """
            )
            self._migrate_schema()
            self._execute_script(
                """
            CREATE INDEX IF NOT EXISTS idx_tasks_worker_status_due
            ON tasks(worker_type, status, next_run_at, priority, created_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
            ON tasks(status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_plan
            ON tasks(plan_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_round
            ON tasks(round_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_round_item
            ON tasks(round_item_id, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_crawl_plans_enabled_due
            ON crawl_plans(enabled, next_run_at, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_crawl_plans_type
            ON crawl_plans(plan_type, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_crawl_rounds_plan_status
            ON crawl_rounds(plan_id, status, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_crawl_round_items_round_status
            ON crawl_round_items(round_id, status, item_order, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_ui_filter_presets_user_view
            ON ui_filter_presets(user_key, view_name, is_default, last_used_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ui_filter_history_user_view
            ON ui_filter_history(user_key, view_name, last_used_at DESC);
            """
            )
            self.conn.commit()
            return

        self._execute_script(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id BIGSERIAL PRIMARY KEY,
                plan_id BIGINT,
                round_id BIGINT,
                round_item_id BIGINT,
                task_type TEXT NOT NULL,
                worker_type TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                created_by TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                progress_json TEXT NOT NULL DEFAULT '{}',
                schedule_type TEXT NOT NULL DEFAULT 'manual',
                schedule_expr TEXT NOT NULL DEFAULT '',
                last_run_at TEXT,
                next_run_at TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_runs (
                id BIGSERIAL PRIMARY KEY,
                task_id BIGINT NOT NULL,
                task_type TEXT NOT NULL,
                worker_type TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                lease_owner TEXT NOT NULL,
                attempt_number INTEGER NOT NULL DEFAULT 1,
                command_json TEXT NOT NULL DEFAULT '[]',
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT NOT NULL DEFAULT '{}',
                error_text TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_task_runs_task
            ON task_runs(task_id, started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_task_runs_status
            ON task_runs(status, started_at DESC);

            CREATE TABLE IF NOT EXISTS workers (
                worker_name TEXT PRIMARY KEY,
                worker_type TEXT NOT NULL,
                status TEXT NOT NULL,
                current_task_id BIGINT,
                heartbeat_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS crawl_plans (
                id BIGSERIAL PRIMARY KEY,
                plan_type TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_by TEXT NOT NULL,
                schedule_kind TEXT NOT NULL DEFAULT 'manual',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}',
                last_dispatched_at TEXT,
                next_run_at TEXT,
                last_run_status TEXT NOT NULL DEFAULT '',
                last_run_task_id BIGINT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS crawl_rounds (
                id BIGSERIAL PRIMARY KEY,
                plan_id BIGINT NOT NULL,
                plan_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                payload_json TEXT NOT NULL DEFAULT '{}',
                context_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                current_task_id BIGINT,
                last_task_status TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS crawl_round_items (
                id BIGSERIAL PRIMARY KEY,
                round_id BIGINT NOT NULL,
                item_key TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                item_order INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                current_task_id BIGINT,
                last_task_id BIGINT,
                last_task_status TEXT NOT NULL DEFAULT '',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(round_id, item_key)
            );
            """
        )
        self._migrate_schema()
        self._execute_script(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_worker_status_due
            ON tasks(worker_type, status, next_run_at, priority, created_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
            ON tasks(status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_plan
            ON tasks(plan_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_round
            ON tasks(round_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_round_item
            ON tasks(round_item_id, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_crawl_plans_enabled_due
            ON crawl_plans(enabled, next_run_at, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_crawl_plans_type
            ON crawl_plans(plan_type, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_crawl_rounds_plan_status
            ON crawl_rounds(plan_id, status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_crawl_round_items_round_status
            ON crawl_round_items(round_id, status, item_order, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_ui_filter_presets_user_view
            ON ui_filter_presets(user_key, view_name, is_default, last_used_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ui_filter_history_user_view
            ON ui_filter_history(user_key, view_name, last_used_at DESC);
            """
        )
        self.conn.commit()

    def _migrate_schema(self) -> None:
        self._ensure_column("tasks", "plan_id", "INTEGER")
        self._ensure_column("tasks", "round_id", "INTEGER")
        self._ensure_column("tasks", "round_item_id", "INTEGER")
        self._ensure_column("tasks", "display_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("tasks", "progress_json", "TEXT NOT NULL DEFAULT '{}'")
        crawl_plans_create_sql = (
            """
            CREATE TABLE crawl_plans (
                id BIGSERIAL PRIMARY KEY,
                plan_type TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_by TEXT NOT NULL,
                schedule_kind TEXT NOT NULL DEFAULT 'manual',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}',
                last_dispatched_at TEXT,
                next_run_at TEXT,
                last_run_status TEXT NOT NULL DEFAULT '',
                last_run_task_id BIGINT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
            if self.backend == "postgres"
            else
            """
            CREATE TABLE crawl_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_type TEXT NOT NULL,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL,
                schedule_kind TEXT NOT NULL DEFAULT 'manual',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}',
                last_dispatched_at TEXT,
                next_run_at TEXT,
                last_run_status TEXT NOT NULL DEFAULT '',
                last_run_task_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        crawl_rounds_create_sql = (
            """
            CREATE TABLE crawl_rounds (
                id BIGSERIAL PRIMARY KEY,
                plan_id BIGINT NOT NULL,
                plan_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                payload_json TEXT NOT NULL DEFAULT '{}',
                context_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                current_task_id BIGINT,
                last_task_status TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
            if self.backend == "postgres"
            else
            """
            CREATE TABLE crawl_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                plan_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                payload_json TEXT NOT NULL DEFAULT '{}',
                context_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                current_task_id INTEGER,
                last_task_status TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        crawl_round_items_create_sql = (
            """
            CREATE TABLE crawl_round_items (
                id BIGSERIAL PRIMARY KEY,
                round_id BIGINT NOT NULL,
                item_key TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                item_order INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                current_task_id BIGINT,
                last_task_id BIGINT,
                last_task_status TEXT NOT NULL DEFAULT '',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(round_id, item_key)
            )
            """
            if self.backend == "postgres"
            else
            """
            CREATE TABLE crawl_round_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id INTEGER NOT NULL,
                item_key TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                item_order INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                current_task_id INTEGER,
                last_task_id INTEGER,
                last_task_status TEXT NOT NULL DEFAULT '',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(round_id, item_key)
            )
            """
        )
        self._ensure_table(
            "crawl_plans",
            crawl_plans_create_sql,
        )
        self._ensure_table(
            "crawl_rounds",
            crawl_rounds_create_sql,
        )
        self._ensure_table(
            "crawl_round_items",
            crawl_round_items_create_sql,
        )
        ui_filter_presets_create_sql = (
            """
            CREATE TABLE ui_filter_presets (
                id BIGSERIAL PRIMARY KEY,
                user_key TEXT NOT NULL,
                view_name TEXT NOT NULL,
                preset_name TEXT NOT NULL,
                route_payload_json TEXT NOT NULL DEFAULT '{}',
                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                UNIQUE(user_key, view_name, preset_name)
            )
            """
            if self.backend == "postgres"
            else
            """
            CREATE TABLE ui_filter_presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                view_name TEXT NOT NULL,
                preset_name TEXT NOT NULL,
                route_payload_json TEXT NOT NULL DEFAULT '{}',
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                UNIQUE(user_key, view_name, preset_name)
            )
            """
        )
        ui_filter_history_create_sql = (
            """
            CREATE TABLE ui_filter_history (
                id BIGSERIAL PRIMARY KEY,
                user_key TEXT NOT NULL,
                view_name TEXT NOT NULL,
                route_hash TEXT NOT NULL,
                route_payload_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                UNIQUE(user_key, view_name, route_hash)
            )
            """
            if self.backend == "postgres"
            else
            """
            CREATE TABLE ui_filter_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_key TEXT NOT NULL,
                view_name TEXT NOT NULL,
                route_hash TEXT NOT NULL,
                route_payload_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                UNIQUE(user_key, view_name, route_hash)
            )
            """
        )
        self._ensure_table("ui_filter_presets", ui_filter_presets_create_sql)
        self._ensure_table("ui_filter_history", ui_filter_history_create_sql)

    def _ensure_table(self, table_name: str, create_sql: str) -> None:
        if not self._table_exists(table_name):
            self._execute(create_sql)

    def _ensure_column(self, table_name: str, column_name: str, column_sql: str) -> None:
        columns = self._table_columns(table_name)
        if column_name not in columns:
            if self.backend == "sqlite":
                self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
            else:
                self._execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_sql}")

    def create_task(
        self,
        *,
        task_type: str,
        payload: dict[str, Any],
        created_by: str = "manual",
        priority: int = 100,
        schedule_type: str = "manual",
        schedule_expr: str = "",
        next_run_at: str | None = None,
        worker_type: str | None = None,
        plan_id: int | None = None,
        round_id: int | None = None,
        round_item_id: int | None = None,
        display_name: str = "",
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utcnow()
        now_iso = now.isoformat()
        normalized_schedule = _normalize_schedule_type(schedule_type)
        resolved_worker = _normalize_worker_type(task_type, worker_type)
        if normalized_schedule == "once":
            if not next_run_at:
                raise ValueError("next_run_at is required for once tasks")
            next_run_at = _parse_dt(next_run_at).isoformat()
        elif normalized_schedule == "weekly":
            if not schedule_expr:
                raise ValueError("schedule_expr is required for weekly tasks")
        with self._tx() as cur:
            task_id = self._insert_and_get_id(
                cur,
                """
                INSERT INTO tasks (
                    plan_id, round_id, round_item_id, task_type, worker_type, display_name, status, priority, created_by,
                    payload_json, progress_json, schedule_type, schedule_expr, last_run_at,
                    next_run_at, lease_owner, lease_expires_at, attempt_count, last_error,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', ?, ?)
                """,
                (
                    plan_id,
                    round_id,
                    round_item_id,
                    task_type,
                    resolved_worker,
                    display_name or "",
                    int(priority),
                    created_by,
                    _json_dump(payload),
                    _json_dump(progress or {}),
                    normalized_schedule,
                    schedule_expr or "",
                    None,
                    next_run_at,
                    None,
                    None,
                    now_iso,
                    now_iso,
                ),
            )
        return self.get_task(task_id)

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        row = self._fetchone("SELECT * FROM tasks WHERE id = ?", (int(task_id),))
        if row is None:
            return None
        return self._hydrate_task_row(row)

    def list_tasks(
        self,
        *,
        status: str = "",
        worker_type: str = "",
        limit: int = 100,
        plan_id: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status.strip().lower())
        if worker_type:
            clauses.append("worker_type = ?")
            params.append(worker_type.strip().lower())
        if plan_id is not None:
            clauses.append("plan_id = ?")
            params.append(int(plan_id))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._fetchall(
            f"""
            SELECT * FROM tasks
            {where_sql}
            ORDER BY
                CASE status
                    WHEN 'running' THEN 0
                    WHEN 'leased' THEN 1
                    WHEN 'pending' THEN 2
                    WHEN 'failed' THEN 3
                    ELSE 4
                END,
                priority ASC,
                updated_at DESC,
                id DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        return [self._hydrate_task_row(row) for row in rows]

    def list_task_runs(self, *, task_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if task_id is None:
            rows = self._fetchall(
                """
                SELECT tr.*, t.plan_id, t.display_name, t.status AS task_status
                FROM task_runs tr
                LEFT JOIN tasks t ON t.id = tr.task_id
                ORDER BY tr.started_at DESC, tr.id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
        else:
            rows = self._fetchall(
                """
                SELECT tr.*, t.plan_id, t.display_name, t.status AS task_status
                FROM task_runs tr
                LEFT JOIN tasks t ON t.id = tr.task_id
                WHERE tr.task_id = ?
                ORDER BY tr.started_at DESC, tr.id DESC
                LIMIT ?
                """,
                (int(task_id), int(limit)),
            )
        items = []
        for row in rows:
            item = dict(row)
            run_status = str(item.get("status") or "").strip().lower()
            task_status = str(item.get("task_status") or "").strip().lower()
            item["run_status"] = run_status
            if run_status in {"queued", "pending", "running", "leased", "retrying"} and task_status in {"completed", "failed", "cancelled", "skipped"}:
                item["status"] = task_status
            item["command"] = _json_load(item.pop("command_json", "[]"), [])
            item["payload"] = _json_load(item.pop("payload_json", "{}"), {})
            item["result"] = _json_load(item.pop("result_json", "{}"), {})
            items.append(item)
        return items

    def heartbeat_worker(
        self,
        *,
        worker_name: str,
        worker_type: str,
        status: str,
        current_task_id: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        heartbeat_at = utcnow_iso()
        payload = details or {}
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO workers (worker_name, worker_type, status, current_task_id, heartbeat_at, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_name) DO UPDATE SET
                    worker_type = excluded.worker_type,
                    status = excluded.status,
                    current_task_id = excluded.current_task_id,
                    heartbeat_at = excluded.heartbeat_at,
                    details_json = excluded.details_json
                """,
                (
                    worker_name,
                    worker_type,
                    status,
                    current_task_id,
                    heartbeat_at,
                    _json_dump(payload),
                ),
            )
        return {
            "worker_name": worker_name,
            "worker_type": worker_type,
            "status": status,
            "current_task_id": current_task_id,
            "heartbeat_at": heartbeat_at,
            "details": payload,
        }

    def list_workers(self, *, max_age_seconds: int | None = None) -> list[dict[str, Any]]:
        rows = self._fetchall("SELECT * FROM workers ORDER BY heartbeat_at DESC, worker_name ASC")
        cutoff: datetime | None = None
        if max_age_seconds is not None:
            cutoff = utcnow() - timedelta(seconds=int(max_age_seconds))
        items: list[dict[str, Any]] = []
        for row in rows:
            heartbeat_at = _parse_dt(row["heartbeat_at"])
            if cutoff is not None and (heartbeat_at is None or heartbeat_at < cutoff):
                continue
            items.append(
                {
                    **dict(row),
                    "details": _json_load(row["details_json"], {}),
                }
            )
        return items

    def prune_stale_workers(self, *, max_age_seconds: int = 7200) -> int:
        cutoff = (utcnow() - timedelta(seconds=int(max_age_seconds))).isoformat()
        if self.backend == "sqlite":
            rowcount = self.conn.execute(
                "DELETE FROM workers WHERE heartbeat_at < ?",
                (cutoff,),
            ).rowcount
            self.conn.commit()
        else:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute("DELETE FROM workers WHERE heartbeat_at < %s", (cutoff,))
                    rowcount = cur.rowcount
        return int(rowcount or 0)

    def list_ui_filter_presets(self, *, user_key: str, view_name: str) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT *
            FROM ui_filter_presets
            WHERE user_key = ? AND view_name = ?
            ORDER BY is_default DESC, last_used_at DESC, preset_name ASC
            """,
            (str(user_key), str(view_name)),
        )
        return [
            {
                **dict(row),
                "route_payload": _json_load(row["route_payload_json"], {}),
                "is_default": bool(row["is_default"]),
            }
            for row in rows
        ]

    def create_ui_filter_preset(
        self,
        *,
        user_key: str,
        view_name: str,
        preset_name: str,
        route_payload: dict[str, Any],
        is_default: bool = False,
    ) -> dict[str, Any]:
        now_iso = utcnow_iso()
        normalized_name = str(preset_name or "").strip()
        if not normalized_name:
            raise ValueError("preset_name is required")
        with self._tx() as cur:
            if is_default:
                cur.execute(
                    """
                    UPDATE ui_filter_presets
                    SET is_default = 0,
                        updated_at = ?
                    WHERE user_key = ? AND view_name = ?
                    """,
                    (now_iso, str(user_key), str(view_name)),
                )
            existing_row = cur.execute(
                """
                SELECT id
                FROM ui_filter_presets
                WHERE user_key = ? AND view_name = ? AND preset_name = ?
                """,
                (str(user_key), str(view_name), normalized_name),
            ).fetchone()
            if existing_row is not None:
                preset_id = int(existing_row["id"])
                cur.execute(
                    """
                    UPDATE ui_filter_presets
                    SET route_payload_json = ?,
                        is_default = ?,
                        updated_at = ?,
                        last_used_at = ?
                    WHERE id = ?
                    """,
                    (
                        _json_dump(route_payload or {}),
                        1 if is_default else 0,
                        now_iso,
                        now_iso,
                        preset_id,
                    ),
                )
            else:
                preset_id = self._insert_and_get_id(
                    cur,
                    """
                    INSERT INTO ui_filter_presets (
                        user_key, view_name, preset_name, route_payload_json, is_default,
                        created_at, updated_at, last_used_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(user_key),
                        str(view_name),
                        normalized_name,
                        _json_dump(route_payload or {}),
                        1 if is_default else 0,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
        return self.get_ui_filter_preset(preset_id)

    def get_ui_filter_preset(self, preset_id: int) -> dict[str, Any] | None:
        row = self._fetchone("SELECT * FROM ui_filter_presets WHERE id = ?", (int(preset_id),))
        if row is None:
            return None
        return {
            **dict(row),
            "route_payload": _json_load(row["route_payload_json"], {}),
            "is_default": bool(row["is_default"]),
        }

    def update_ui_filter_preset(
        self,
        preset_id: int,
        *,
        user_key: str,
        view_name: str,
        preset_name: str | None = None,
        route_payload: dict[str, Any] | None = None,
        is_default: bool | None = None,
        mark_used: bool = False,
    ) -> dict[str, Any] | None:
        current = self.get_ui_filter_preset(int(preset_id))
        if current is None:
            return None
        if current["user_key"] != str(user_key) or current["view_name"] != str(view_name):
            return None
        now_iso = utcnow_iso()
        next_name = str(preset_name or current["preset_name"]).strip()
        next_payload = route_payload if route_payload is not None else current["route_payload"]
        next_default = bool(current["is_default"] if is_default is None else is_default)
        next_used = now_iso if mark_used or route_payload is not None else str(current["last_used_at"] or now_iso)
        with self._tx() as cur:
            if next_default:
                cur.execute(
                    """
                    UPDATE ui_filter_presets
                    SET is_default = 0,
                        updated_at = ?
                    WHERE user_key = ? AND view_name = ? AND id <> ?
                    """,
                    (now_iso, str(user_key), str(view_name), int(preset_id)),
                )
            cur.execute(
                """
                UPDATE ui_filter_presets
                SET preset_name = ?,
                    route_payload_json = ?,
                    is_default = ?,
                    updated_at = ?,
                    last_used_at = ?
                WHERE id = ?
                """,
                (
                    next_name,
                    _json_dump(next_payload or {}),
                    1 if next_default else 0,
                    now_iso,
                    next_used,
                    int(preset_id),
                ),
            )
        return self.get_ui_filter_preset(int(preset_id))

    def delete_ui_filter_preset(self, preset_id: int, *, user_key: str, view_name: str) -> bool:
        with self._tx() as cur:
            cur.execute(
                "DELETE FROM ui_filter_presets WHERE id = ? AND user_key = ? AND view_name = ?",
                (int(preset_id), str(user_key), str(view_name)),
            )
            rowcount = getattr(cur._cursor, "rowcount", 0)
        return bool(rowcount)

    def list_ui_filter_history(
        self,
        *,
        user_key: str,
        view_name: str,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT *
            FROM ui_filter_history
            WHERE user_key = ? AND view_name = ?
            ORDER BY last_used_at DESC, id DESC
            LIMIT ?
            """,
            (str(user_key), str(view_name), int(limit)),
        )
        return [
            {
                **dict(row),
                "route_payload": _json_load(row["route_payload_json"], {}),
                "summary": _json_load(row["summary_json"], {}),
            }
            for row in rows
        ]

    def record_ui_filter_history(
        self,
        *,
        user_key: str,
        view_name: str,
        route_payload: dict[str, Any],
        summary: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        now_iso = utcnow_iso()
        route_hash = _route_hash(route_payload or {})
        with self._tx() as cur:
            cur.execute(
                """
                INSERT INTO ui_filter_history (
                    user_key, view_name, route_hash, route_payload_json, summary_json,
                    created_at, updated_at, last_used_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_key, view_name, route_hash) DO UPDATE SET
                    route_payload_json = excluded.route_payload_json,
                    summary_json = excluded.summary_json,
                    updated_at = excluded.updated_at,
                    last_used_at = excluded.last_used_at
                """,
                (
                    str(user_key),
                    str(view_name),
                    route_hash,
                    _json_dump(route_payload or {}),
                    _json_dump(summary or {}),
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
            overflow_rows = cur.execute(
                """
                SELECT id
                FROM ui_filter_history
                WHERE user_key = ? AND view_name = ?
                ORDER BY last_used_at DESC, id DESC
                """,
                (str(user_key), str(view_name)),
            ).fetchall()
            overflow_ids = [int(row["id"]) for row in overflow_rows[24:]]
            for history_id in overflow_ids:
                cur.execute("DELETE FROM ui_filter_history WHERE id = ?", (history_id,))
        return self.list_ui_filter_history(user_key=str(user_key), view_name=str(view_name))

    def cancel_task(self, task_id: int) -> dict[str, Any] | None:
        now = utcnow_iso()
        with self._tx() as cur:
            task_row = cur.execute(
                "SELECT round_id, round_item_id FROM tasks WHERE id = ?",
                (int(task_id),),
            ).fetchone()
            cur.execute(
                """
                UPDATE tasks
                SET status = 'cancelled',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, int(task_id)),
            )
            if task_row and task_row["round_item_id"] is not None:
                cur.execute(
                    """
                    UPDATE crawl_round_items
                    SET status = 'pending',
                        current_task_id = NULL,
                        last_task_id = ?,
                        last_task_status = 'cancelled',
                        last_error = 'task_cancelled',
                        finished_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (int(task_id), now, now, int(task_row["round_item_id"])),
                )
            cur.execute(
                """
                UPDATE crawl_rounds
                SET current_task_id = NULL,
                    last_task_status = 'cancelled',
                    updated_at = ?
                WHERE current_task_id = ?
                """,
                (now, int(task_id)),
            )
            cur.execute(
                """
                UPDATE task_runs
                SET status = 'cancelled',
                    finished_at = ?,
                    error_text = CASE
                        WHEN error_text = '' THEN 'task_cancelled'
                        ELSE error_text
                    END
                WHERE task_id = ?
                  AND status IN ('leased', 'running')
                """,
                (now, int(task_id)),
            )
            if task_row and task_row["round_id"] is not None:
                self._recalculate_crawl_round_state(cur, round_id=int(task_row["round_id"]), now_iso=now)
        return self.get_task(task_id)

    def retry_task(self, task_id: int) -> dict[str, Any] | None:
        now = utcnow_iso()
        with self._tx() as cur:
            task_row = cur.execute(
                "SELECT round_id, round_item_id FROM tasks WHERE id = ?",
                (int(task_id),),
            ).fetchone()
            cur.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NULL,
                    last_error = '',
                    updated_at = ?
                WHERE id = ?
                """,
                (now, int(task_id)),
            )
            if task_row and task_row["round_item_id"] is not None:
                cur.execute(
                    """
                    UPDATE crawl_round_items
                    SET status = 'pending',
                        current_task_id = NULL,
                        last_task_status = 'pending',
                        last_error = '',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, int(task_row["round_item_id"])),
                )
            cur.execute(
                """
                UPDATE crawl_rounds
                SET current_task_id = NULL,
                    last_task_status = 'pending',
                    updated_at = ?
                WHERE current_task_id = ?
                """,
                (now, int(task_id)),
            )
            if task_row and task_row["round_id"] is not None:
                self._recalculate_crawl_round_state(cur, round_id=int(task_row["round_id"]), now_iso=now)
        return self.get_task(task_id)

    def release_expired_leases(self, *, lease_timeout_seconds: int = 3600) -> int:
        now_iso = utcnow().isoformat()
        with self._tx() as cur:
            rows = cur.execute(
                """
                SELECT id, round_id, round_item_id
                FROM tasks
                WHERE status IN ('leased', 'running')
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                """,
                (now_iso,),
            ).fetchall()
            task_ids = [int(row["id"]) for row in rows]
            if not task_ids:
                return 0

            placeholders = ", ".join("?" for _ in task_ids)
            cur.execute(
                f"""
                UPDATE tasks
                SET status = 'pending',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?,
                    last_error = CASE
                        WHEN last_error = '' THEN 'lease_expired'
                        ELSE last_error || '; lease_expired'
                    END
                WHERE id IN ({placeholders})
                """,
                (now_iso, *task_ids),
            )
            round_item_ids = [int(row["round_item_id"]) for row in rows if row["round_item_id"] is not None]
            if round_item_ids:
                placeholders = ", ".join("?" for _ in round_item_ids)
                cur.execute(
                    f"""
                    UPDATE crawl_round_items
                    SET status = 'pending',
                        current_task_id = NULL,
                        last_task_status = 'failed',
                        last_error = CASE
                            WHEN last_error = '' THEN 'lease_expired'
                            ELSE last_error || '; lease_expired'
                        END,
                        finished_at = ?,
                        updated_at = ?
                    WHERE id IN ({placeholders})
                    """,
                    (now_iso, now_iso, *round_item_ids),
                )
            round_ids = sorted({int(row["round_id"]) for row in rows if row["round_id"] is not None})
            for round_id in round_ids:
                self._recalculate_crawl_round_state(cur, round_id=round_id, now_iso=now_iso)
            cur.execute(
                f"""
                UPDATE task_runs
                SET status = 'failed',
                    finished_at = ?,
                    error_text = CASE
                        WHEN error_text = '' THEN 'lease_expired'
                        ELSE error_text
                    END
                WHERE task_id IN ({placeholders})
                  AND status IN ('leased', 'running')
                """,
                (now_iso, *task_ids),
            )
        return len(task_ids)

    def refresh_task_lease(
        self,
        *,
        task_id: int,
        lease_owner: str,
        lease_timeout_seconds: int = 3600,
    ) -> bool:
        now = utcnow()
        now_iso = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=int(lease_timeout_seconds))).isoformat()
        with self._tx() as cur:
            row = cur.execute(
                """
                SELECT id
                FROM tasks
                WHERE id = ?
                  AND lease_owner = ?
                  AND status IN ('leased', 'running')
                """,
                (int(task_id), lease_owner),
            ).fetchone()
            if row is None:
                return False
            cur.execute(
                """
                UPDATE tasks
                SET lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (lease_expires_at, now_iso, int(task_id)),
            )
        return True

    def lease_next_task(
        self,
        *,
        worker_type: str,
        lease_owner: str,
        lease_timeout_seconds: int = 3600,
    ) -> LeaseResult:
        now = utcnow()
        now_iso = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=int(lease_timeout_seconds))).isoformat()
        with self._tx() as cur:
            row = cur.execute(
                """
                SELECT *
                FROM tasks
                WHERE worker_type = ?
                  AND status = 'pending'
                  AND (next_run_at IS NULL OR next_run_at <= ?)
                ORDER BY priority ASC, created_at ASC, id ASC
                LIMIT 1
                """,
                (worker_type, now_iso),
            ).fetchone()
            if row is None:
                return LeaseResult(task=None, run_id=None)

            task_id = int(row["id"])
            attempt_number = int(row["attempt_count"] or 0) + 1
            cur.execute(
                """
                UPDATE tasks
                SET status = 'leased',
                    lease_owner = ?,
                    lease_expires_at = ?,
                    attempt_count = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (lease_owner, lease_expires_at, attempt_number, now_iso, task_id),
            )
            run_id = self._insert_and_get_id(
                cur,
                """
                INSERT INTO task_runs (
                    task_id, task_type, worker_type, status, started_at, finished_at,
                    lease_owner, attempt_number, command_json, payload_json, result_json, error_text
                ) VALUES (?, ?, ?, 'leased', ?, NULL, ?, ?, '[]', ?, '{}', '')
                """,
                (
                    task_id,
                    row["task_type"],
                    row["worker_type"],
                    now_iso,
                    lease_owner,
                    attempt_number,
                    row["payload_json"],
                ),
            )
        task = self.get_task(task_id)
        return LeaseResult(task=task, run_id=run_id)

    def mark_task_running(self, *, task_id: int, run_id: int, command: list[str]) -> None:
        now = utcnow_iso()
        with self._tx() as cur:
            row = cur.execute("SELECT round_id, round_item_id FROM tasks WHERE id = ?", (int(task_id),)).fetchone()
            cur.execute(
                "UPDATE tasks SET status = 'running', updated_at = ? WHERE id = ?",
                (now, int(task_id)),
            )
            cur.execute(
                """
                UPDATE task_runs
                SET status = 'running',
                    command_json = ?,
                    started_at = ?
                WHERE id = ?
                """,
                (_json_dump(command), now, int(run_id)),
            )
            if row and row["round_item_id"] is not None:
                cur.execute(
                    """
                    UPDATE crawl_round_items
                    SET status = 'running',
                        last_task_status = 'running',
                        started_at = ?,
                        finished_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, int(row["round_item_id"])),
                )
            if row and row["round_id"] is not None:
                self._recalculate_crawl_round_state(cur, round_id=int(row["round_id"]), now_iso=now)

    def update_task_progress(self, *, task_id: int, progress: dict[str, Any]) -> dict[str, Any] | None:
        now = utcnow_iso()
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET progress_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (_json_dump(progress), now, int(task_id)),
            )
        return self.get_task(task_id)

    def create_crawl_round_items(self, round_id: int, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now_iso = utcnow_iso()
        with self._tx() as cur:
            for index, item in enumerate(items):
                cur.execute(
                    """
                    INSERT INTO crawl_round_items (
                        round_id, item_key, display_name, item_order, payload_json, status,
                        current_task_id, last_task_id, last_task_status, attempt_count,
                        last_error, result_json, started_at, finished_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', NULL, NULL, '', 0, '', '{}', NULL, NULL, ?, ?)
                    ON CONFLICT(round_id, item_key) DO NOTHING
                    """,
                    (
                        int(round_id),
                        str(item.get("item_key") or f"item-{index + 1}"),
                        str(item.get("display_name") or ""),
                        int(item.get("item_order") or index),
                        _json_dump(item.get("payload") or {}),
                        now_iso,
                        now_iso,
                    ),
                )
            self._recalculate_crawl_round_state(cur, round_id=int(round_id), now_iso=now_iso)
        return self.list_crawl_round_items(int(round_id))

    def get_crawl_round_item(self, item_id: int) -> dict[str, Any] | None:
        row = self._fetchone("SELECT * FROM crawl_round_items WHERE id = ?", (int(item_id),))
        if row is None:
            return None
        return self._hydrate_round_item_row(row)

    def list_crawl_round_items(self, round_id: int, *, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [int(round_id)]
        where_sql = "WHERE round_id = ?"
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            where_sql += f" AND status IN ({placeholders})"
            params.extend(str(status).strip().lower() for status in statuses)
        rows = self._fetchall(
            f"""
            SELECT *
            FROM crawl_round_items
            {where_sql}
            ORDER BY item_order ASC, id ASC
            """,
            tuple(params),
        )
        return [self._hydrate_round_item_row(row) for row in rows]

    def get_next_dispatchable_crawl_round_item(self, round_id: int) -> dict[str, Any] | None:
        row = self._fetchone(
            """
            SELECT *
            FROM crawl_round_items
            WHERE round_id = ?
              AND status = 'pending'
              AND current_task_id IS NULL
            ORDER BY item_order ASC, id ASC
            LIMIT 1
            """,
            (int(round_id),),
        )
        if row is None:
            return None
        return self._hydrate_round_item_row(row)

    def mark_crawl_round_item_dispatched(
        self,
        *,
        item_id: int,
        task_id: int,
        task_status: str = "pending",
    ) -> dict[str, Any] | None:
        now_iso = utcnow_iso()
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_round_items
                SET current_task_id = ?,
                    last_task_id = ?,
                    last_task_status = ?,
                    attempt_count = attempt_count + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (int(task_id), int(task_id), str(task_status or ""), now_iso, int(item_id)),
            )
            row = cur.execute(
                "SELECT round_id FROM crawl_round_items WHERE id = ?",
                (int(item_id),),
            ).fetchone()
            if row is not None:
                self._recalculate_crawl_round_state(cur, round_id=int(row["round_id"]), now_iso=now_iso)
        return self.get_crawl_round_item(item_id)

    def _summarize_crawl_round_items(self, cur, *, round_id: int) -> dict[str, Any]:
        rows = cur.execute(
            """
            SELECT status, COUNT(*) AS item_count
            FROM crawl_round_items
            WHERE round_id = ?
            GROUP BY status
            """,
            (int(round_id),),
        ).fetchall()
        counts = {str(row["status"]): int(row["item_count"] or 0) for row in rows}
        total_items = sum(counts.values())
        completed_items = counts.get("completed", 0)
        skipped_items = counts.get("skipped", 0)
        running_items = counts.get("running", 0)
        pending_items = counts.get("pending", 0)
        latest_progress_row = cur.execute(
            """
            SELECT progress_json
            FROM tasks
            WHERE round_id = ?
              AND progress_json IS NOT NULL
              AND progress_json <> '{}'
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (int(round_id),),
        ).fetchone()
        summary = {
            "total_items": total_items,
            "completed_items": completed_items,
            "skipped_items": skipped_items,
            "running_items": running_items,
            "pending_items": pending_items,
            "completion_ratio": (completed_items + skipped_items) / total_items if total_items else 0.0,
        }
        if latest_progress_row is not None:
            summary["progress"] = _json_load(latest_progress_row["progress_json"], {})
        return summary

    def _recalculate_crawl_round_state(self, cur, *, round_id: int, now_iso: str) -> None:
        summary = self._summarize_crawl_round_items(cur, round_id=round_id)
        total_items = int(summary["total_items"])
        pending_items = int(summary["pending_items"])
        running_items = int(summary["running_items"])
        completed_items = int(summary["completed_items"])
        skipped_items = int(summary["skipped_items"])
        if total_items == 0:
            status = "active"
            finished_at = None
        elif pending_items > 0 or running_items > 0:
            status = "active"
            finished_at = None
        elif completed_items > 0:
            status = "completed"
            finished_at = now_iso
        elif skipped_items == total_items:
            status = "skipped"
            finished_at = now_iso
        else:
            status = "active"
            finished_at = None
        cur.execute(
            """
            UPDATE crawl_rounds
            SET status = ?,
                summary_json = ?,
                finished_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                _json_dump(summary),
                finished_at,
                now_iso,
                int(round_id),
            ),
        )

    def finish_task_run(
        self,
        *,
        task_id: int,
        run_id: int,
        final_status: str,
        result: dict[str, Any] | None = None,
        error_text: str = "",
    ) -> dict[str, Any]:
        status = _normalize_status(final_status)
        now = utcnow()
        now_iso = now.isoformat()
        task = self.get_task(task_id)
        if task is None:
            raise ValueError(f"task not found: {task_id}")
        progress_payload = (
            dict(result.get("progress") or {})
            if isinstance(result, dict) and isinstance(result.get("progress"), dict)
            else dict(task.get("progress") or {})
        )

        schedule_type = _normalize_schedule_type(task.get("schedule_type"))
        schedule_expr = str(task.get("schedule_expr") or "")
        next_run_at: str | None = None
        task_status = status
        if schedule_type in {"interval", "weekly"} and status in {"completed", "skipped"}:
            next_run_at = self._compute_task_next_run_at(now, schedule_type, schedule_expr)
            task_status = "pending" if next_run_at else status

        with self._tx() as cur:
            cur.execute(
                """
                UPDATE task_runs
                SET status = ?,
                    finished_at = ?,
                    result_json = ?,
                    error_text = ?
                WHERE id = ?
                """,
                (status, now_iso, _json_dump(result or {}), error_text, int(run_id)),
            )
            cur.execute(
                """
                UPDATE tasks
                SET status = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_run_at = ?,
                    progress_json = ?,
                    next_run_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    task_status,
                    now_iso,
                    _json_dump(progress_payload),
                    next_run_at,
                    error_text,
                    now_iso,
                    int(task_id),
                ),
            )
            if task.get("plan_id"):
                cur.execute(
                    """
                    UPDATE crawl_plans
                    SET last_run_status = ?,
                        last_run_task_id = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (status, int(task_id), now_iso, int(task["plan_id"])),
                )
            round_id = int(task["round_id"]) if task.get("round_id") else None
            round_item_id = int(task["round_item_id"]) if task.get("round_item_id") else None
            if round_item_id is not None:
                if status in {"completed", "skipped"}:
                    cur.execute(
                        """
                        UPDATE crawl_round_items
                        SET status = ?,
                            current_task_id = NULL,
                            last_task_id = ?,
                            last_task_status = ?,
                            last_error = ?,
                            result_json = ?,
                            finished_at = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            status,
                            int(task_id),
                            status,
                            error_text,
                            _json_dump(result or {}),
                            now_iso,
                            now_iso,
                            round_item_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE crawl_round_items
                        SET status = 'pending',
                            current_task_id = NULL,
                            last_task_id = ?,
                            last_task_status = ?,
                            last_error = ?,
                            result_json = ?,
                            finished_at = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            int(task_id),
                            status,
                            error_text,
                            _json_dump(result or {}),
                            now_iso,
                            now_iso,
                            round_item_id,
                        ),
                    )
            if task.get("round_id"):
                cur.execute(
                    """
                    UPDATE crawl_rounds
                    SET current_task_id = NULL,
                        last_task_status = ?,
                        summary_json = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        _json_dump(result or {}),
                        now_iso,
                        round_id,
                    ),
                )
            if round_id is not None:
                self._recalculate_crawl_round_state(cur, round_id=round_id, now_iso=now_iso)
        return self.get_task(task_id) or {}

    def _compute_task_next_run_at(self, current_time: datetime, schedule_type: str, schedule_expr: str) -> str | None:
        expr = str(schedule_expr or "").strip()
        if schedule_type == "interval":
            if not expr:
                return None
            seconds = int(expr)
            return (_ensure_utc(current_time) + timedelta(seconds=seconds)).isoformat()
        if schedule_type == "weekly":
            if not expr:
                return None
            payload = _json_load(expr, {})
            return compute_plan_next_run_at("weekly", payload, reference_time=_ensure_utc(current_time))
        return None

    def get_status_counts(self) -> dict[str, int]:
        rows = self._fetchall("SELECT status, COUNT(*) AS count FROM tasks GROUP BY status")
        return {str(row["status"]): int(row["count"] or 0) for row in rows}

    def create_crawl_plan(
        self,
        *,
        plan_type: str,
        name: str,
        created_by: str,
        payload: dict[str, Any],
        schedule_kind: str = "manual",
        schedule_json: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        normalized_type = str(plan_type or "").strip().lower()
        if normalized_type not in SUPPORTED_PLAN_TYPES:
            raise ValueError(f"unsupported plan_type: {normalized_type}")
        normalized_kind = _normalize_schedule_kind(schedule_kind)
        normalized_schedule_json = _validate_schedule_json(normalized_kind, schedule_json)
        now = utcnow()
        now_iso = now.isoformat()
        next_run_at = compute_plan_next_run_at(normalized_kind, normalized_schedule_json, reference_time=now) if enabled else None
        with self._tx() as cur:
            plan_id = self._insert_and_get_id(
                cur,
                """
                INSERT INTO crawl_plans (
                    plan_type, name, enabled, created_by, schedule_kind, schedule_json,
                    payload_json, last_dispatched_at, next_run_at, last_run_status,
                    last_run_task_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_type,
                    str(name or "").strip() or normalized_type,
                    bool(enabled),
                    created_by,
                    normalized_kind,
                    _json_dump(normalized_schedule_json),
                    _json_dump(payload or {}),
                    None,
                    next_run_at,
                    "",
                    None,
                    now_iso,
                    now_iso,
                ),
            )
        return self.get_crawl_plan(plan_id)

    def create_crawl_round(
        self,
        *,
        plan_id: int,
        plan_type: str,
        payload: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        status: str = "active",
    ) -> dict[str, Any]:
        normalized_type = str(plan_type or "").strip().lower()
        if normalized_type not in SUPPORTED_PLAN_TYPES:
            raise ValueError(f"unsupported plan_type: {normalized_type}")
        normalized_status = str(status or "active").strip().lower() or "active"
        if normalized_status not in SUPPORTED_ROUND_STATUSES:
            raise ValueError(f"unsupported round status: {normalized_status}")
        now_iso = utcnow_iso()
        with self._tx() as cur:
            round_id = self._insert_and_get_id(
                cur,
                """
                INSERT INTO crawl_rounds (
                    plan_id, plan_type, status, payload_json, context_json, summary_json,
                    current_task_id, last_task_status, started_at, finished_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, '{}', NULL, '', ?, NULL, ?, ?)
                """,
                (
                    int(plan_id),
                    normalized_type,
                    normalized_status,
                    _json_dump(payload or {}),
                    _json_dump(context or {}),
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
        return self.get_crawl_round(round_id)

    def get_crawl_round(self, round_id: int) -> dict[str, Any] | None:
        row = self._fetchone("SELECT * FROM crawl_rounds WHERE id = ?", (int(round_id),))
        if row is None:
            return None
        return self._hydrate_round_row(row)

    def get_open_crawl_round_for_plan(self, plan_id: int) -> dict[str, Any] | None:
        row = self._fetchone(
            """
            SELECT *
            FROM crawl_rounds
            WHERE plan_id = ?
              AND status = 'active'
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (int(plan_id),),
        )
        if row is None:
            return None
        return self._hydrate_round_row(row)

    def update_crawl_round(
        self,
        round_id: int,
        *,
        status: str | None = None,
        context: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
        current_task_id: int | None = None,
        last_task_status: str | None = None,
        finished_at: str | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_crawl_round(round_id)
        if existing is None:
            return None
        next_status = existing["status"] if status is None else str(status).strip().lower()
        if next_status not in SUPPORTED_ROUND_STATUSES:
            raise ValueError(f"unsupported round status: {next_status}")
        next_context = existing["context"] if context is None else context
        next_summary = existing["summary"] if summary is None else summary
        next_current_task_id = existing.get("current_task_id") if current_task_id is None else current_task_id
        next_last_task_status = existing.get("last_task_status") if last_task_status is None else last_task_status
        next_finished_at = existing.get("finished_at") if finished_at is None else finished_at
        now_iso = utcnow_iso()
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_rounds
                SET status = ?,
                    context_json = ?,
                    summary_json = ?,
                    current_task_id = ?,
                    last_task_status = ?,
                    finished_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    next_status,
                    _json_dump(next_context or {}),
                    _json_dump(next_summary or {}),
                    next_current_task_id,
                    str(next_last_task_status or ""),
                    next_finished_at,
                    now_iso,
                    int(round_id),
                ),
            )
        return self.get_crawl_round(round_id)

    def get_crawl_plan(self, plan_id: int) -> dict[str, Any] | None:
        row = self._fetchone("SELECT * FROM crawl_plans WHERE id = ?", (int(plan_id),))
        if row is None:
            return None
        return self._hydrate_plan_row(row)

    def list_crawl_plans(self, *, enabled: bool | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if enabled is not None:
            clauses.append("enabled = ?")
            params.append(bool(enabled))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._fetchall(
            f"""
            SELECT *
            FROM crawl_plans
            {where_sql}
            ORDER BY enabled DESC, updated_at DESC, id DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        )
        return [self._hydrate_plan_row(row) for row in rows]

    def update_crawl_plan(
        self,
        plan_id: int,
        *,
        name: str | None = None,
        enabled: bool | None = None,
        schedule_kind: str | None = None,
        schedule_json: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        last_run_status: str | None = None,
        last_run_task_id: int | None = None,
    ) -> dict[str, Any] | None:
        plan = self.get_crawl_plan(plan_id)
        if plan is None:
            return None
        next_name = (name if name is not None else plan["name"]).strip() or plan["plan_type"]
        next_enabled = plan["enabled"] if enabled is None else bool(enabled)
        next_schedule_kind = _normalize_schedule_kind(schedule_kind or plan["schedule_kind"])
        next_schedule_json = _validate_schedule_json(
            next_schedule_kind,
            schedule_json if schedule_json is not None else plan["schedule_json"],
        )
        next_payload = payload if payload is not None else plan["payload"]
        now = utcnow()
        now_iso = now.isoformat()
        next_run_at = compute_plan_next_run_at(next_schedule_kind, next_schedule_json, reference_time=now) if next_enabled else None
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_plans
                SET name = ?,
                    enabled = ?,
                    schedule_kind = ?,
                    schedule_json = ?,
                    payload_json = ?,
                    next_run_at = ?,
                    last_run_status = COALESCE(?, last_run_status),
                    last_run_task_id = COALESCE(?, last_run_task_id),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    next_name,
                    bool(next_enabled),
                    next_schedule_kind,
                    _json_dump(next_schedule_json),
                    _json_dump(next_payload or {}),
                    next_run_at,
                    last_run_status,
                    last_run_task_id,
                    now_iso,
                    int(plan_id),
                ),
            )
        return self.get_crawl_plan(plan_id)

    def pause_crawl_plan(self, plan_id: int) -> dict[str, Any] | None:
        return self.update_crawl_plan(plan_id, enabled=False)

    def resume_crawl_plan(self, plan_id: int) -> dict[str, Any] | None:
        return self.update_crawl_plan(plan_id, enabled=True)

    def set_crawl_plan_next_run_at(self, plan_id: int, next_run_at: str | None) -> dict[str, Any] | None:
        plan = self.get_crawl_plan(plan_id)
        if plan is None:
            return None
        normalized_next_run_at = None
        if next_run_at:
            normalized_next_run_at = _parse_dt(next_run_at).isoformat()
        now_iso = utcnow_iso()
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_plans
                SET next_run_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (normalized_next_run_at, now_iso, int(plan_id)),
            )
        return self.get_crawl_plan(plan_id)

    def list_due_crawl_plans(self, *, now: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
        now_iso = _ensure_utc(now or utcnow()).isoformat()
        rows = self._fetchall(
            """
            SELECT *
            FROM crawl_plans
            WHERE enabled = ?
              AND schedule_kind <> 'manual'
              AND next_run_at IS NOT NULL
              AND next_run_at <= ?
            ORDER BY next_run_at ASC, updated_at ASC, id ASC
            LIMIT ?
            """,
            (True, now_iso, int(limit)),
        )
        return [self._hydrate_plan_row(row) for row in rows]

    def mark_crawl_plan_dispatched(self, *, plan_id: int, task_id: int, dispatched_at: datetime | None = None) -> dict[str, Any] | None:
        plan = self.get_crawl_plan(plan_id)
        if plan is None:
            return None
        now = _ensure_utc(dispatched_at or utcnow())
        now_iso = now.isoformat()
        next_run_at = compute_plan_next_run_at(plan["schedule_kind"], plan["schedule_json"], reference_time=now)
        if plan["schedule_kind"] in {"manual", "once"}:
            next_run_at = None
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_plans
                SET last_dispatched_at = ?,
                    next_run_at = ?,
                    last_run_status = 'dispatched',
                    last_run_task_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (now_iso, next_run_at, int(task_id), now_iso, int(plan_id)),
            )
        return self.get_crawl_plan(plan_id)

    def mark_crawl_round_dispatched(
        self,
        *,
        round_id: int,
        task_id: int,
        task_status: str = "pending",
    ) -> dict[str, Any] | None:
        now_iso = utcnow_iso()
        with self._tx() as cur:
            cur.execute(
                """
                UPDATE crawl_rounds
                SET current_task_id = ?,
                    last_task_status = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (int(task_id), str(task_status or ""), now_iso, int(round_id)),
            )
        return self.get_crawl_round(round_id)

    def list_active_tasks_for_plan(self, plan_id: int) -> list[dict[str, Any]]:
        rows = self._fetchall(
            """
            SELECT *
            FROM tasks
            WHERE plan_id = ?
              AND status IN ('pending', 'leased', 'running')
            ORDER BY created_at DESC, id DESC
            """,
            (int(plan_id),),
        )
        return [self._hydrate_task_row(row) for row in rows]

    def _hydrate_task_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row)
        payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
        payload["progress"] = _json_load(payload.pop("progress_json", "{}"), {})
        return payload

    def _hydrate_plan_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row)
        payload["enabled"] = bool(payload.get("enabled"))
        payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
        payload["schedule_json"] = _json_load(payload.get("schedule_json"), {})
        return payload

    def _hydrate_round_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row)
        payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
        payload["context"] = _json_load(payload.pop("context_json", "{}"), {})
        payload["summary"] = _json_load(payload.pop("summary_json", "{}"), {})
        return payload

    def _hydrate_round_item_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row)
        payload["payload"] = _json_load(payload.pop("payload_json", "{}"), {})
        payload["result"] = _json_load(payload.pop("result_json", "{}"), {})
        return payload


__all__ = [
    "LeaseResult",
    "OpsStore",
    "SUPPORTED_ROUND_STATUSES",
    "SUPPORTED_PLAN_TYPES",
    "SUPPORTED_ROUND_ITEM_STATUSES",
    "SUPPORTED_TASK_SCHEDULE_TYPES",
    "TASK_TYPE_TO_WORKER",
    "compute_plan_next_run_at",
    "get_ops_db_config",
    "get_ops_db_path",
    "utcnow",
    "utcnow_iso",
]
