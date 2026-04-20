from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import datetime, timedelta, timezone

from ops.crawler_control import dispatch_due_plans, launch_plan_now
from ops.task_store import OpsStore


class OpsTaskStoreTests(unittest.TestCase):
    def test_create_lease_run_and_complete_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"product_count": 50, "persist": True},
                    created_by="test",
                )
                lease = store.lease_next_task(worker_type="category", lease_owner="worker-a")
                self.assertIsNotNone(lease.task)
                self.assertIsNotNone(lease.run_id)
                store.mark_task_running(task_id=int(task["id"]), run_id=int(lease.run_id), command=["python", "run_ready_category_scan.py"])
                completed = store.finish_task_run(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    final_status="completed",
                    result={"returncode": 0},
                )
                self.assertEqual(completed["status"], "completed")
                self.assertEqual(store.get_status_counts().get("completed"), 1)
                runs = store.list_task_runs(task_id=int(task["id"]))
                self.assertEqual(len(runs), 1)
                self.assertEqual(runs[0]["status"], "completed")
            finally:
                store.close()

    def test_interval_task_reschedules_after_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="warehouse_sync",
                    payload={"reason": "interval"},
                    created_by="test",
                    schedule_type="interval",
                    schedule_expr="300",
                )
                lease = store.lease_next_task(worker_type="sync", lease_owner="sync-worker")
                self.assertIsNotNone(lease.task)
                rescheduled = store.finish_task_run(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    final_status="completed",
                    result={"returncode": 0},
                )
                self.assertEqual(rescheduled["status"], "pending")
                self.assertTrue(rescheduled["next_run_at"])
            finally:
                store.close()

    def test_cancel_and_retry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="keyword_once",
                    payload={"keyword": "dash camera"},
                    created_by="test",
                )
                lease = store.lease_next_task(worker_type="keyword", lease_owner="worker-a")
                store.mark_task_running(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "keyword_main.py", "--step", "scrape"],
                )
                cancelled = store.cancel_task(int(task["id"]))
                self.assertEqual(cancelled["status"], "cancelled")
                runs = store.list_task_runs(task_id=int(task["id"]))
                self.assertEqual(runs[0]["status"], "cancelled")
                self.assertEqual(runs[0]["error_text"], "task_cancelled")
                retried = store.retry_task(int(task["id"]))
                self.assertEqual(retried["status"], "pending")
            finally:
                store.close()

    def test_refresh_task_lease_extends_expiry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"product_count": 50, "persist": True},
                    created_by="test",
                )
                lease = store.lease_next_task(worker_type="category", lease_owner="worker-a", lease_timeout_seconds=30)
                leased_task = store.get_task(int(task["id"]))
                first_expiry = leased_task["lease_expires_at"]
                self.assertTrue(first_expiry)
                refreshed = store.refresh_task_lease(
                    task_id=int(task["id"]),
                    lease_owner="worker-a",
                    lease_timeout_seconds=300,
                )
                self.assertTrue(refreshed)
                updated_task = store.get_task(int(task["id"]))
                self.assertGreater(updated_task["lease_expires_at"], first_expiry)
                self.assertEqual(lease.run_id, 1)
            finally:
                store.close()

    def test_prune_stale_workers_removes_old_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                store.heartbeat_worker(
                    worker_name="worker-old",
                    worker_type="category",
                    status="idle",
                )
                store.conn.execute(
                    "UPDATE workers SET heartbeat_at = '2000-01-01T00:00:00+00:00' WHERE worker_name = 'worker-old'"
                )
                store.conn.commit()
                removed = store.prune_stale_workers(max_age_seconds=60)
                self.assertEqual(removed, 1)
                workers = store.list_workers()
                self.assertFalse(any(item["worker_name"] == "worker-old" for item in workers))
            finally:
                store.close()

    def test_list_workers_can_hide_stale_rows_without_pruning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                store.heartbeat_worker(
                    worker_name="worker-fresh",
                    worker_type="category",
                    status="running",
                )
                store.heartbeat_worker(
                    worker_name="worker-stale",
                    worker_type="sync",
                    status="idle",
                )
                store.conn.execute(
                    "UPDATE workers SET heartbeat_at = '2000-01-01T00:00:00+00:00' WHERE worker_name = 'worker-stale'"
                )
                store.conn.commit()

                workers = store.list_workers(max_age_seconds=60)

                self.assertTrue(any(item["worker_name"] == "worker-fresh" for item in workers))
                self.assertFalse(any(item["worker_name"] == "worker-stale" for item in workers))
            finally:
                store.close()

    def test_release_expired_leases_closes_open_task_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"product_count": 50, "persist": True},
                    created_by="test",
                )
                lease = store.lease_next_task(
                    worker_type="category",
                    lease_owner="worker-a",
                    lease_timeout_seconds=30,
                )
                store.mark_task_running(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "run_ready_category_scan.py"],
                )
                store.conn.execute(
                    "UPDATE tasks SET lease_expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?",
                    (int(task["id"]),),
                )
                store.conn.commit()

                released = store.release_expired_leases()
                self.assertEqual(released, 1)

                refreshed = store.get_task(int(task["id"]))
                self.assertEqual(refreshed["status"], "pending")
                self.assertIn("lease_expired", refreshed["last_error"])

                runs = store.list_task_runs(task_id=int(task["id"]))
                self.assertEqual(runs[0]["status"], "failed")
                self.assertEqual(runs[0]["error_text"], "lease_expired")
                self.assertTrue(runs[0]["finished_at"])
            finally:
                store.close()

    def test_list_task_runs_uses_terminal_task_status_when_run_row_is_stale_running(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="warehouse_sync",
                    payload={"reason": "stale-run-row"},
                    created_by="test",
                )
                lease = store.lease_next_task(worker_type="sync", lease_owner="sync-worker")
                store.mark_task_running(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "run_shared_warehouse_sync.py"],
                )
                store.conn.execute(
                    "UPDATE tasks SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (int(task["id"]),),
                )
                store.conn.commit()

                runs = store.list_task_runs(task_id=int(task["id"]))

                self.assertEqual(runs[0]["run_status"], "running")
                self.assertEqual(runs[0]["status"], "cancelled")
            finally:
                store.close()

    def test_crawl_plan_lifecycle_and_launch_creates_bound_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="Night Ready Scan",
                    created_by="test",
                    schedule_kind="weekly",
                    schedule_json={"days": ["mo", "fr"], "time": "09:30"},
                    payload={
                        "categories": ["sports", "baby"],
                        "default_product_count_per_leaf": 120,
                        "category_overrides": {"sports": 180},
                    },
                )
                self.assertEqual(plan["plan_type"], "category_ready_scan")
                self.assertTrue(plan["enabled"])
                self.assertEqual(plan["payload"]["category_overrides"]["sports"], 180)

                listed = store.list_crawl_plans()
                self.assertEqual(listed[0]["id"], plan["id"])

                updated = store.update_crawl_plan(
                    int(plan["id"]),
                    name="Night Ready Scan v2",
                    payload={
                        "categories": ["sports"],
                        "default_product_count_per_leaf": 90,
                        "category_overrides": {"sports": 140},
                    },
                )
                self.assertEqual(updated["name"], "Night Ready Scan v2")
                self.assertEqual(updated["payload"]["default_product_count_per_leaf"], 90)

                paused = store.pause_crawl_plan(int(plan["id"]))
                self.assertFalse(paused["enabled"])

                resumed = store.resume_crawl_plan(int(plan["id"]))
                self.assertTrue(resumed["enabled"])

                task = launch_plan_now(store, int(plan["id"]), created_by="crawler_console_test")
                self.assertEqual(task["plan_id"], int(plan["id"]))
                self.assertIn("display_name", task)
                self.assertEqual(task["task_type"], "category_ready_scan")
                self.assertEqual(task["payload"]["default_product_count_per_leaf"], 90)
            finally:
                store.close()

    def test_dispatch_due_plans_handles_once_interval_and_weekly(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)

                once_plan = store.create_crawl_plan(
                    plan_type="keyword_batch",
                    name="One shot",
                    created_by="test",
                    schedule_kind="once",
                    schedule_json={"run_at": (now - timedelta(minutes=5)).isoformat()},
                    payload={"keywords": ["dog toys"], "platforms": ["noon"]},
                )

                interval_plan = store.create_crawl_plan(
                    plan_type="category_single",
                    name="Every 2h",
                    created_by="test",
                    schedule_kind="interval",
                    schedule_json={"seconds": 7200},
                    payload={"category": "pets", "product_count": 80},
                )
                store.conn.execute(
                    "UPDATE crawl_plans SET next_run_at = ? WHERE id = ?",
                    ((now - timedelta(minutes=1)).isoformat(), int(interval_plan["id"])),
                )
                store.conn.commit()

                weekly_plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Weekly monitor",
                    created_by="test",
                    schedule_kind="weekly",
                    schedule_json={"days": ["mo", "we"], "time": "08:30"},
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                )
                store.conn.execute(
                    "UPDATE crawl_plans SET next_run_at = ? WHERE id = ?",
                    ((now - timedelta(minutes=1)).isoformat(), int(weekly_plan["id"])),
                )
                store.conn.commit()

                due = store.list_due_crawl_plans(now=now, limit=10)
                self.assertEqual({plan["id"] for plan in due}, {once_plan["id"], interval_plan["id"], weekly_plan["id"]})

                dispatched = dispatch_due_plans(store)
                self.assertEqual(len(dispatched), 3)

                refreshed_once = store.get_crawl_plan(int(once_plan["id"]))
                refreshed_interval = store.get_crawl_plan(int(interval_plan["id"]))
                refreshed_weekly = store.get_crawl_plan(int(weekly_plan["id"]))

                self.assertEqual(refreshed_once["last_run_status"], "dispatched")
                self.assertIsNone(refreshed_once["next_run_at"])

                self.assertEqual(refreshed_interval["last_run_status"], "dispatched")
                self.assertTrue(refreshed_interval["next_run_at"])
                self.assertGreater(
                    datetime.fromisoformat(refreshed_interval["next_run_at"]),
                    now,
                )

                self.assertEqual(refreshed_weekly["last_run_status"], "dispatched")
                self.assertTrue(refreshed_weekly["next_run_at"])
                self.assertGreater(
                    datetime.fromisoformat(refreshed_weekly["next_run_at"]),
                    now,
                )
            finally:
                store.close()

    def test_launch_plan_reuses_open_round_until_round_is_completed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="Resume Ready Scan",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={
                        "categories": ["sports", "pets"],
                        "default_product_count_per_leaf": 120,
                        "category_overrides": {"sports": 180},
                    },
                )

                first_task = launch_plan_now(store, int(plan["id"]), created_by="test")
                self.assertIsNotNone(first_task.get("round_id"))
                first_round_id = int(first_task["round_id"])

                active_round = store.get_open_crawl_round_for_plan(int(plan["id"]))
                self.assertIsNotNone(active_round)
                self.assertEqual(int(active_round["id"]), first_round_id)

                store.cancel_task(int(first_task["id"]))
                resumed_task = launch_plan_now(store, int(plan["id"]), created_by="test")
                self.assertEqual(int(resumed_task["round_id"]), first_round_id)
                self.assertNotEqual(int(resumed_task["id"]), int(first_task["id"]))

                lease = store.lease_next_task(worker_type="category", lease_owner="worker-a")
                self.assertIsNotNone(lease.task)
                self.assertEqual(int(lease.task["id"]), int(resumed_task["id"]))
                store.mark_task_running(
                    task_id=int(resumed_task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "run_ready_category_scan.py"],
                )
                store.finish_task_run(
                    task_id=int(resumed_task["id"]),
                    run_id=int(lease.run_id),
                    final_status="completed",
                    result={"returncode": 0, "progress": {"completed_units": 14}},
                )

                active_round = store.get_crawl_round(first_round_id)
                self.assertIsNotNone(active_round)
                self.assertEqual(active_round["status"], "active")
                self.assertEqual(active_round["last_task_status"], "completed")
                self.assertEqual(active_round["summary"]["completed_items"], 1)
                self.assertEqual(active_round["summary"]["pending_items"], 1)
                self.assertEqual(active_round["summary"]["progress"]["completed_units"], 14)

                next_task = launch_plan_now(store, int(plan["id"]), created_by="test")
                self.assertEqual(int(next_task["round_id"]), first_round_id)
                self.assertNotEqual(int(next_task["id"]), int(resumed_task["id"]))
            finally:
                store.close()

    def test_keyword_monitor_round_items_split_by_baseline_seed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            baseline_path = Path(temp_dir) / "baseline.txt"
            baseline_path.write_text("dog toys\ncat bed\n", encoding="utf-8")
            monitor_config_path = Path(temp_dir) / "monitor.json"
            monitor_config_path.write_text(
                json.dumps({"baseline_file": str(baseline_path)}, ensure_ascii=False),
                encoding="utf-8",
            )
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Seed Monitor",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"monitor_config": str(monitor_config_path)},
                )

                first_task = launch_plan_now(store, int(plan["id"]), created_by="test")
                round_id = int(first_task["round_id"])
                items = store.list_crawl_round_items(round_id)
                self.assertEqual(len(items), 2)
                self.assertEqual(items[0]["payload"]["monitor_seed_keyword"], "dog toys")
                self.assertEqual(items[1]["payload"]["monitor_seed_keyword"], "cat bed")

                lease = store.lease_next_task(worker_type="keyword", lease_owner="worker-a")
                self.assertIsNotNone(lease.task)
                self.assertEqual(int(lease.task["id"]), int(first_task["id"]))
                store.mark_task_running(
                    task_id=int(first_task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "run_keyword_monitor.py"],
                )
                store.finish_task_run(
                    task_id=int(first_task["id"]),
                    run_id=int(lease.run_id),
                    final_status="completed",
                    result={"returncode": 0},
                )

                second_task = launch_plan_now(store, int(plan["id"]), created_by="test")
                self.assertEqual(int(second_task["round_id"]), round_id)
                self.assertNotEqual(int(second_task["id"]), int(first_task["id"]))
                self.assertEqual(second_task["payload"]["monitor_seed_keyword"], "cat bed")
            finally:
                store.close()

    def test_launch_plan_now_returns_skipped_when_keyword_monitor_lock_is_active(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Active Monitor",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                )

                with patch("ops.crawler_control._keyword_monitor_lock_active", return_value=True):
                    result = launch_plan_now(store, int(plan["id"]), created_by="test")

                self.assertEqual(result["status"], "skipped")
                self.assertEqual(result["skip_reason"], "active_monitor")
                self.assertIn("already active", result["skip_detail"])
                self.assertIsNone(result["task_id"])
                self.assertEqual(result["plan_id"], int(plan["id"]))
                self.assertEqual(result["plan_type"], "keyword_monitor")

                self.assertEqual(len(store.list_tasks(limit=20)), 0)
            finally:
                store.close()

    def test_launch_plan_now_returns_skipped_when_category_lock_is_active(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="Active Category Crawl",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"categories": ["sports"], "default_product_count_per_leaf": 120},
                )

                with patch(
                    "ops.crawler_control._category_crawl_lock_detail",
                    return_value="category crawler is already active: category=sports, pid=123",
                ):
                    result = launch_plan_now(store, int(plan["id"]), created_by="test")

                self.assertEqual(result["status"], "skipped")
                self.assertEqual(result["skip_reason"], "active_category_crawl")
                self.assertIn("category=sports", result["skip_detail"])
                self.assertIsNone(result["task_id"])
                self.assertEqual(result["plan_id"], int(plan["id"]))
                self.assertEqual(result["plan_type"], "category_ready_scan")
                self.assertEqual(len(store.list_tasks(limit=20)), 0)
            finally:
                store.close()

    def test_launch_plan_now_returns_skipped_when_active_task_already_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Active Monitor Plan",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                )
                active_task = store.create_task(
                    task_type="keyword_monitor",
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                    created_by="test",
                    plan_id=int(plan["id"]),
                    display_name="active keyword monitor",
                )
                result = launch_plan_now(store, int(plan["id"]), created_by="test")

                self.assertEqual(result["status"], "skipped")
                self.assertEqual(result["skip_reason"], "active_monitor")
                self.assertIn("already active", result["skip_detail"])
                self.assertIn(str(int(active_task["id"])), result["skip_detail"])
                self.assertIsNone(result["task_id"])
                self.assertEqual(result["plan_id"], int(plan["id"]))
                self.assertEqual(result["plan_type"], "keyword_monitor")
                self.assertEqual(len(store.list_tasks(limit=20)), 1)
            finally:
                store.close()

    def test_dispatch_due_plans_skips_duplicate_lock_plans_without_creating_tasks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                keyword_plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Keyword Monitor",
                    created_by="test",
                    schedule_kind="once",
                    schedule_json={"run_at": "2026-03-30T09:55:00+00:00"},
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                )
                category_plan = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="Category Scan",
                    created_by="test",
                    schedule_kind="once",
                    schedule_json={"run_at": "2026-03-30T09:55:00+00:00"},
                    payload={"categories": ["sports"], "default_product_count_per_leaf": 120},
                )

                with (
                    patch("ops.crawler_control._keyword_monitor_lock_active", return_value=True),
                    patch("ops.crawler_control._category_crawl_lock_detail", return_value="category crawler is already active: category=sports, pid=123"),
                ):
                    dispatched = dispatch_due_plans(store)

                self.assertEqual(len(dispatched), 2)
                self.assertEqual(
                    {item["skip_reason"] for item in dispatched},
                    {"active_monitor", "active_category_crawl"},
                )
                self.assertTrue(all(item["status"] == "skipped" for item in dispatched))
                self.assertEqual(store.list_tasks(limit=20), [])
                self.assertEqual(store.get_crawl_plan(int(keyword_plan["id"]))["last_run_status"], "")
                self.assertEqual(store.get_crawl_plan(int(category_plan["id"]))["last_run_status"], "")
                self.assertEqual(
                    store.get_crawl_plan(int(keyword_plan["id"]))["next_run_at"],
                    "2026-03-30T09:55:00+00:00",
                )
                self.assertEqual(
                    store.get_crawl_plan(int(category_plan["id"]))["next_run_at"],
                    "2026-03-30T09:55:00+00:00",
                )
            finally:
                store.close()

    def test_dispatch_due_plans_skips_plans_with_existing_active_tasks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                keyword_plan = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="Keyword Monitor",
                    created_by="test",
                    schedule_kind="once",
                    schedule_json={"run_at": "2026-03-30T09:55:00+00:00"},
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                )
                category_plan = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="Category Scan",
                    created_by="test",
                    schedule_kind="once",
                    schedule_json={"run_at": "2026-03-30T09:55:00+00:00"},
                    payload={"categories": ["sports"], "default_product_count_per_leaf": 120},
                )
                keyword_task = store.create_task(
                    task_type="keyword_monitor",
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                    created_by="test",
                    plan_id=int(keyword_plan["id"]),
                    display_name="keyword monitor active",
                )
                category_task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"categories": ["sports"], "default_product_count_per_leaf": 120},
                    created_by="test",
                    plan_id=int(category_plan["id"]),
                    display_name="category scan active",
                )
                self.assertEqual(keyword_task["status"], "pending")
                self.assertEqual(category_task["status"], "pending")

                dispatched = dispatch_due_plans(store)

                self.assertEqual(len(dispatched), 2)
                self.assertEqual(
                    {item["skip_reason"] for item in dispatched},
                    {"active_monitor", "active_category_crawl"},
                )
                self.assertTrue(all(item["status"] == "skipped" for item in dispatched))
                self.assertEqual(len(store.list_tasks(limit=20)), 2)
                self.assertEqual(store.get_crawl_plan(int(keyword_plan["id"]))["last_run_status"], "")
                self.assertEqual(store.get_crawl_plan(int(category_plan["id"]))["last_run_status"], "")
                self.assertEqual(
                    store.get_crawl_plan(int(keyword_plan["id"]))["next_run_at"],
                    "2026-03-30T09:55:00+00:00",
                )
                self.assertEqual(
                    store.get_crawl_plan(int(category_plan["id"]))["next_run_at"],
                    "2026-03-30T09:55:00+00:00",
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
