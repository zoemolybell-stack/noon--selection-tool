from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.config import DatabaseConfig
from tools.cleanup_ops_history import (
    build_cleanup_plan,
    execute_cleanup,
    reconcile_keyword_runtime_runs,
    resolve_actions,
    resolve_mode,
)


class DummyConfig:
    def __init__(self, is_postgres: bool = False) -> None:
        self.is_postgres = is_postgres
        self.is_sqlite = not is_postgres


def iso(hours_ago: int = 0, days_ago: int = 0) -> str:
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago, days=days_ago)
    return ts.isoformat()


class CleanupOpsHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "ops.db"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE workers (
                worker_name TEXT PRIMARY KEY,
                worker_type TEXT,
                status TEXT,
                task_id INTEGER,
                heartbeat_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY,
                plan_id INTEGER,
                round_id INTEGER,
                round_item_id INTEGER,
                task_type TEXT,
                status TEXT,
                created_by TEXT,
                display_name TEXT,
                created_at TEXT,
                updated_at TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                last_error TEXT,
                payload_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                status TEXT,
                finished_at TEXT,
                error_text TEXT
            )
            """
        )

        cur.execute(
            "INSERT INTO workers(worker_name, worker_type, status, task_id, heartbeat_at) VALUES (?, ?, ?, ?, ?)",
            ("stale-worker", "keyword", "idle", 11, iso(hours_ago=7)),
        )
        cur.execute(
            "INSERT INTO workers(worker_name, worker_type, status, task_id, heartbeat_at) VALUES (?, ?, ?, ?, ?)",
            ("live-worker", "category", "idle", 12, iso(hours_ago=1)),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, None, None, None, "warehouse_sync", "completed", "smoke", "smoke task", iso(days_ago=8), iso(days_ago=8), None, None, None, "{}"),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (2, 9, 1, 1, "category_ready_scan", "failed", "plan", "plan linked failed", iso(days_ago=6), iso(days_ago=6), None, None, None, "{}"),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (3, 9, 1, 1, "category_ready_scan", "completed", "plan", "plan linked completed", iso(days_ago=6), iso(days_ago=6), None, None, None, "{}"),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (4, None, None, None, "category_ready_scan", "running", "test", "stranded running", iso(hours_ago=5), iso(hours_ago=5), "stale-worker", None, None, "{}"),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (5, None, None, None, "category_ready_scan", "running", "test", "missing lease owner", iso(hours_ago=4), iso(hours_ago=4), "ghost-worker", iso(hours_ago=2), None, "{}"),
        )
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (6, None, None, None, "keyword_monitor", "failed", "scheduler", "duplicate monitor lock", iso(hours_ago=2), iso(hours_ago=2), None, None, "keyword monitor is already running: pid=123, snapshot=s1", "{\"snapshot\":\"s1\"}"),
        )
        for task_id in (1, 2, 3, 4):
            cur.execute("INSERT INTO task_runs(task_id, status, finished_at, error_text) VALUES (?, ?, ?, ?)", (task_id, "completed", iso(hours_ago=1), ""))
        cur.execute("INSERT INTO task_runs(task_id, status, finished_at, error_text) VALUES (?, ?, ?, ?)", (5, "running", None, ""))
        cur.execute("INSERT INTO task_runs(task_id, status, finished_at, error_text) VALUES (?, ?, ?, ?)", (6, "failed", iso(hours_ago=1), ""))
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_mode_resolution_is_mutually_exclusive(self) -> None:
        self.assertEqual(resolve_mode(workers_only=True, running_only=False, history_only=False), "workers")
        self.assertEqual(resolve_mode(workers_only=False, running_only=True, history_only=False), "running")
        self.assertEqual(resolve_mode(workers_only=False, running_only=False, history_only=True), "history")
        self.assertEqual(resolve_mode(workers_only=False, running_only=False, history_only=False), "full")
        with self.assertRaises(ValueError):
            resolve_mode(workers_only=True, running_only=True, history_only=False)

    def test_history_retention_excludes_plan_linked_by_default(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )
        self.assertEqual(plan.stale_worker_names, ["stale-worker"])
        self.assertEqual(plan.stranded_running_task_ids, [4, 5])
        self.assertEqual(plan.duplicate_lock_failed_task_ids, [6])
        self.assertEqual(plan.duplicate_lock_skip_reason_by_task_id[6], "active_monitor")
        self.assertEqual(plan.history_task_ids, [1])

    def test_duplicate_lock_reclassification_requires_provably_terminal_state(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO tasks(id, plan_id, round_id, round_item_id, task_type, status, created_by, display_name, created_at, updated_at, lease_owner, lease_expires_at, last_error, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                7,
                None,
                None,
                None,
                "keyword_monitor",
                "failed",
                "scheduler",
                "duplicate monitor with lease",
                iso(hours_ago=1),
                iso(hours_ago=1),
                "live-worker",
                (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                "keyword monitor is already running: pid=321, snapshot=s2",
                "{\"snapshot\":\"s2\"}",
            ),
        )
        cur.execute("INSERT INTO task_runs(task_id, status, finished_at, error_text) VALUES (?, ?, ?, ?)", (7, "failed", iso(hours_ago=1), ""))
        self.conn.commit()

        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )

        self.assertEqual(plan.duplicate_lock_failed_task_ids, [6])
        self.assertEqual(plan.duplicate_lock_skip_reason_by_task_id[6], "active_monitor")
        self.assertNotIn(7, plan.duplicate_lock_failed_task_ids)

    def test_workers_only_does_not_delete_tasks(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )
        deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs = execute_cleanup(
            self.conn,
            DummyConfig(False),
            plan,
            delete_workers=True,
            cancel_stranded_running=False,
            delete_history=False,
            reclassify_duplicate_failures=False,
        )
        self.assertEqual((deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs), (1, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        remaining_tasks = self.conn.execute("SELECT COUNT(*) AS count FROM tasks").fetchone()["count"]
        self.assertEqual(remaining_tasks, 6)

    def test_running_only_cancels_stranded_task(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )
        deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs = execute_cleanup(
            self.conn,
            DummyConfig(False),
            plan,
            delete_workers=False,
            cancel_stranded_running=True,
            delete_history=False,
            reclassify_duplicate_failures=False,
        )
        self.assertEqual((deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs), (0, 0, 0, 2, 1, 0, 0))
        self.assertEqual((reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs), (0, 0, 0))
        status = self.conn.execute("SELECT status FROM tasks WHERE id = 4").fetchone()["status"]
        self.assertEqual(status, "cancelled")
        missing_owner_status = self.conn.execute("SELECT status FROM tasks WHERE id = 5").fetchone()["status"]
        self.assertEqual(missing_owner_status, "cancelled")

    def test_history_only_prunes_orphan_and_smoke_by_default(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )
        deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs = execute_cleanup(
            self.conn,
            DummyConfig(False),
            plan,
            delete_workers=False,
            cancel_stranded_running=False,
            delete_history=True,
            reclassify_duplicate_failures=False,
        )
        self.assertEqual((deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs), (0, 1, 1, 0, 0, 0, 0, 0, 0, 0))
        task_ids = [row["id"] for row in self.conn.execute("SELECT id FROM tasks ORDER BY id").fetchall()]
        self.assertEqual(task_ids, [2, 3, 4, 5, 6])

    def test_plan_linked_history_requires_explicit_flag(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=True,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
        )
        self.assertEqual(plan.history_task_ids, [1, 2])

    def test_post_deploy_reconcile_reclassifies_duplicate_lock_failures(self) -> None:
        plan = build_cleanup_plan(
            conn=self.conn,
            stale_worker_seconds=6 * 60 * 60,
            orphan_terminal_days=7,
            smoke_terminal_hours=6,
            plan_linked_terminal_days=5,
            include_plan_linked_terminal=False,
            smoke_substrings=("smoke", "predeploy", "main_window_sleep_plan"),
            deploy_started_at=datetime.now(timezone.utc),
            post_deploy_reconcile=True,
        )
        with mock.patch("tools.cleanup_ops_history.reconcile_keyword_runtime_runs", return_value=0):
            deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs, reclassified_duplicate_tasks, reclassified_duplicate_task_runs, reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs = execute_cleanup(
                self.conn,
                DummyConfig(False),
                plan,
                delete_workers=False,
                cancel_stranded_running=False,
                delete_history=False,
                reclassify_duplicate_failures=True,
            )
        self.assertEqual((deleted_workers, deleted_task_runs, deleted_tasks, cancelled_running_tasks, reconciled_task_runs), (0, 0, 0, 0, 0))
        self.assertEqual(reclassified_duplicate_tasks, 1)
        self.assertEqual(reclassified_duplicate_task_runs, 1)
        self.assertEqual((reconciled_terminal_task_runs, reconciled_keyword_source_runs, reconciled_keyword_warehouse_runs), (0, 0, 0))
        task_status = self.conn.execute("SELECT status FROM tasks WHERE id = 6").fetchone()["status"]
        task_last_error = self.conn.execute("SELECT last_error FROM tasks WHERE id = 6").fetchone()["last_error"]
        run_status = self.conn.execute("SELECT status FROM task_runs WHERE task_id = 6").fetchone()["status"]
        run_error = self.conn.execute("SELECT error_text FROM task_runs WHERE task_id = 6").fetchone()["error_text"]
        self.assertEqual(task_status, "skipped")
        self.assertEqual(run_status, "skipped")
        self.assertEqual(task_last_error, "active_monitor")
        self.assertEqual(run_error, "active_monitor")

    def test_reconcile_keyword_runtime_runs_uses_source_truth_for_warehouse_log(self) -> None:
        source_db = Path(self.tempdir.name) / "product_store.db"
        warehouse_db = Path(self.tempdir.name) / "warehouse.db"
        source_finished_at = iso(hours_ago=1)

        source_conn = sqlite3.connect(source_db)
        source_conn.row_factory = sqlite3.Row
        source_conn.execute(
            """
            CREATE TABLE keyword_runs (
                id INTEGER PRIMARY KEY,
                status TEXT,
                snapshot_id TEXT,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        source_conn.execute(
            """
            INSERT INTO keyword_runs(id, status, snapshot_id, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (101, "partial", "snap-terminal", iso(hours_ago=3), source_finished_at),
        )
        source_conn.commit()
        source_conn.close()

        warehouse_conn = sqlite3.connect(warehouse_db)
        warehouse_conn.row_factory = sqlite3.Row
        warehouse_conn.execute(
            """
            CREATE TABLE keyword_runs_log (
                id INTEGER PRIMARY KEY,
                source_run_id INTEGER,
                snapshot_id TEXT,
                status TEXT,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        warehouse_conn.execute(
            """
            INSERT INTO keyword_runs_log(id, source_run_id, snapshot_id, status, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, 101, "snap-terminal", "running", iso(hours_ago=3), ""),
        )
        warehouse_conn.execute(
            """
            INSERT INTO keyword_runs_log(id, source_run_id, snapshot_id, status, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (2, 999, "snap-orphan", "running", iso(days_ago=14), iso(days_ago=13)),
        )
        warehouse_conn.commit()
        warehouse_conn.close()

        reconciled = reconcile_keyword_runtime_runs(
            DatabaseConfig(backend="sqlite", source_env="test", sqlite_path=warehouse_db),
            table_name="keyword_runs_log",
            deploy_started_at=datetime.now(timezone.utc),
            live_snapshot_ids=[],
            finished_at_text=iso(),
            source_config=DatabaseConfig(backend="sqlite", source_env="test", sqlite_path=source_db),
        )
        self.assertEqual(reconciled, 2)

        warehouse_conn = sqlite3.connect(warehouse_db)
        warehouse_conn.row_factory = sqlite3.Row
        row_terminal = warehouse_conn.execute(
            "SELECT status, finished_at FROM keyword_runs_log WHERE id = 1"
        ).fetchone()
        row_orphan = warehouse_conn.execute(
            "SELECT status, finished_at FROM keyword_runs_log WHERE id = 2"
        ).fetchone()
        warehouse_conn.close()

        self.assertEqual(row_terminal["status"], "partial")
        self.assertEqual(row_terminal["finished_at"], source_finished_at)
        self.assertEqual(row_orphan["status"], "cancelled")
        self.assertTrue(str(row_orphan["finished_at"] or "").strip())


if __name__ == "__main__":
    unittest.main()
