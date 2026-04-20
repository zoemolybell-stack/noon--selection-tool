from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
import os
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.task_store import OpsStore
from run_task_scheduler import build_task_command, execute_task, maybe_run_followup_sync, _worker_details, _worker_name


class RunTaskSchedulerTests(unittest.TestCase):
    def test_remote_category_worker_name_and_details_use_remote_identity(self):
        with patch.dict(
            os.environ,
            {
                "NOON_WORKER_NODE_ROLE": "remote_category",
                "NOON_WORKER_NODE_HOST": "crawler-pc",
            },
            clear=False,
        ):
            worker_name = _worker_name("worker", "category")
            details = _worker_details("category", poll_seconds=10)

        self.assertTrue(worker_name.startswith("remote-category-crawler-pc-"))
        self.assertEqual(details["node_role"], "remote_category")
        self.assertEqual(details["node_host"], "crawler-pc")
        self.assertEqual(details["poll_seconds"], 10)

    def test_category_ready_scan_no_longer_runs_duplicate_followup_sync(self):
        task = {
            "id": 99,
            "task_type": "category_ready_scan",
            "payload": {
                "categories": ["beauty"],
                "persist": True,
                "default_product_count_per_leaf": 200,
            },
        }
        self.assertIsNone(maybe_run_followup_sync(task))

    def test_build_task_command_supports_crawler_plan_payloads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            previous_ops = os.environ.get("NOON_OPS_DB")
            os.environ["NOON_OPS_DB"] = str(Path(temp_dir) / "ops.db")
            try:
                keyword_batch_command = build_task_command(
                    {
                        "id": 11,
                        "task_type": "keyword_batch",
                        "payload": {
                            "keywords": ["dog toys", "cat litter box"],
                            "platforms": ["noon", "amazon"],
                            "noon_count": 20,
                            "amazon_count": 10,
                            "persist": True,
                            "snapshot": "keyword_batch_plan_9_round_2",
                            "resume": True,
                        },
                    }
                )
                self.assertIn("--keywords-file", keyword_batch_command)
                self.assertIn("--platforms", keyword_batch_command)
                self.assertIn("--persist", keyword_batch_command)
                self.assertIn("--snapshot", keyword_batch_command)
                self.assertIn("keyword_batch_plan_9_round_2", keyword_batch_command)
                self.assertIn("--resume", keyword_batch_command)

                category_single_command = build_task_command(
                    {
                        "id": 12,
                        "task_type": "category_single",
                        "payload": {
                            "category": "sports",
                            "product_count": 80,
                            "persist": True,
                            "snapshot": "category_plan_3_round_7",
                            "resume": True,
                        },
                    }
                )
                self.assertIn("--category", category_single_command)
                self.assertIn("sports", category_single_command)
                self.assertIn("--noon-count", category_single_command)
                self.assertIn("80", category_single_command)
                self.assertIn("--snapshot", category_single_command)
                self.assertIn("--resume", category_single_command)

                category_ready_command = build_task_command(
                    {
                        "id": 13,
                        "task_type": "category_ready_scan",
                        "payload": {
                            "categories": ["sports", "baby"],
                            "default_product_count_per_leaf": 120,
                            "category_overrides": {"sports": 180},
                            "subcategory_overrides": {"fitness_rollers": 240},
                            "persist": True,
                            "export_excel": False,
                            "output_dir": str(Path(temp_dir) / "batch_scans" / "plan_1_round_9"),
                        },
                    }
                )
                self.assertIn("--product-count", category_ready_command)
                self.assertIn("120", category_ready_command)
                self.assertIn("--category-overrides-json", category_ready_command)
                self.assertIn("--subcategory-overrides-json", category_ready_command)
                self.assertIn("--no-export-excel", category_ready_command)
                self.assertIn("--output-dir", category_ready_command)
            finally:
                if previous_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = previous_ops

    def test_execute_task_refreshes_lease_during_long_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"product_count": 50, "persist": False},
                    created_by="test",
                )
                lease = store.lease_next_task(
                    worker_type="category",
                    lease_owner="worker-a",
                    lease_timeout_seconds=2,
                )
                store.mark_task_running(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "-c", "import time; time.sleep(3)"],
                )
            finally:
                store.close()

            observed = {"released": 0}

            def run_task():
                with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                    with patch(
                        "run_task_scheduler.build_task_command",
                        return_value=[sys.executable, "-c", "import time; print('start'); time.sleep(3); print('done')"],
                    ):
                        status, result, error_text = execute_task(
                            lease.task,
                            worker_name="worker-a",
                            poll_seconds=1,
                            lease_timeout_seconds=2,
                        )
                        observed["status"] = status
                        observed["result"] = result
                        observed["error_text"] = error_text

            thread = threading.Thread(target=run_task)
            thread.start()
            time.sleep(2.2)

            with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                store = OpsStore(db_path)
                try:
                    observed["released"] = store.release_expired_leases(lease_timeout_seconds=2)
                finally:
                    store.close()

            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(observed["released"], 0)
            self.assertEqual(observed["status"], "completed")
            self.assertEqual(observed["result"]["returncode"], 0)
            self.assertEqual(observed["error_text"], "")

    def test_execute_task_stops_when_task_is_cancelled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"product_count": 50, "persist": False},
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
                    command=["python", "-c", "import time; time.sleep(30)"],
                )
            finally:
                store.close()

            observed = {}

            def run_task():
                with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                    with patch(
                        "run_task_scheduler.build_task_command",
                        return_value=[sys.executable, "-c", "import time; print('start'); time.sleep(30)"],
                    ):
                        status, result, error_text = execute_task(
                            lease.task,
                            worker_name="worker-a",
                            poll_seconds=1,
                            lease_timeout_seconds=30,
                        )
                        observed["status"] = status
                        observed["result"] = result
                        observed["error_text"] = error_text

            thread = threading.Thread(target=run_task)
            thread.start()
            time.sleep(1.5)

            with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                store = OpsStore(db_path)
                try:
                    cancelled = store.cancel_task(int(task["id"]))
                finally:
                    store.close()

            self.assertEqual(cancelled["status"], "cancelled")
            thread.join(timeout=8)
            self.assertFalse(thread.is_alive())
            self.assertEqual(observed["status"], "cancelled")
            self.assertEqual(observed["error_text"], "task_cancelled_by_operator")
            self.assertEqual(observed["result"]["cancel_reason"], "task_cancelled_by_operator")

    def test_execute_task_marks_keyword_monitor_active_message_as_skipped_even_if_not_last_line(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="keyword_monitor",
                    payload={"monitor_config": "config/keyword_monitor_defaults.json"},
                    created_by="test",
                )
                lease = store.lease_next_task(
                    worker_type="keyword",
                    lease_owner="worker-a",
                    lease_timeout_seconds=30,
                )
                store.mark_task_running(
                    task_id=int(task["id"]),
                    run_id=int(lease.run_id),
                    command=["python", "-c", "import sys; sys.exit(1)"],
                )
            finally:
                store.close()

            observed = {}

            def run_task():
                with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                    with patch(
                        "run_task_scheduler.build_task_command",
                        return_value=[
                            sys.executable,
                            "-c",
                            "import sys; sys.stderr.write('keyword monitor is already running: pid=123\\n'); sys.stderr.write('follow up line\\n'); sys.stderr.flush(); sys.exit(1)",
                        ],
                    ):
                        status, result, error_text = execute_task(
                            lease.task,
                            worker_name="worker-a",
                            poll_seconds=1,
                            lease_timeout_seconds=30,
                        )
                        observed["status"] = status
                        observed["result"] = result
                        observed["error_text"] = error_text

            thread = threading.Thread(target=run_task)
            thread.start()
            thread.join(timeout=8)
            self.assertFalse(thread.is_alive())
            self.assertEqual(observed["status"], "skipped")
            self.assertEqual(observed["error_text"], "active_monitor")
            self.assertEqual(observed["result"]["skip_reason"], "active_monitor")
            self.assertEqual(observed["result"]["skip_detail"], "follow up line")

    def test_execute_task_marks_category_duplicate_lock_as_skipped(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            store = OpsStore(db_path)
            try:
                task = store.create_task(
                    task_type="category_ready_scan",
                    payload={"categories": ["sports"], "product_count": 50, "persist": True},
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
                    command=["python", "-c", "import sys; sys.exit(1)"],
                )
            finally:
                store.close()

            observed = {}

            def run_task():
                with patch.dict(os.environ, {"NOON_OPS_DB": str(db_path)}):
                    with patch(
                        "run_task_scheduler.build_task_command",
                        return_value=[
                            sys.executable,
                            "-c",
                            "import sys; sys.stderr.write('category crawler is already running: category=sports, pid=123\\n'); sys.stderr.write('lock held\\n'); sys.stderr.flush(); sys.exit(1)",
                        ],
                    ):
                        status, result, error_text = execute_task(
                            lease.task,
                            worker_name="worker-a",
                            poll_seconds=1,
                            lease_timeout_seconds=30,
                        )
                        observed["status"] = status
                        observed["result"] = result
                        observed["error_text"] = error_text

            thread = threading.Thread(target=run_task)
            thread.start()
            thread.join(timeout=8)
            self.assertFalse(thread.is_alive())
            self.assertEqual(observed["status"], "skipped")
            self.assertEqual(observed["error_text"], "active_category_crawl")
            self.assertEqual(observed["result"]["skip_reason"], "active_category_crawl")
            self.assertEqual(observed["result"]["skip_detail"], "lock held")


if __name__ == "__main__":
    unittest.main()
