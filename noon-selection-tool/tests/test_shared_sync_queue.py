from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.shared_sync_queue import enqueue_and_wait_for_warehouse_sync, enqueue_or_reuse_warehouse_sync_task
from ops.task_store import OpsStore


class SharedSyncQueueTests(unittest.TestCase):
    def test_enqueue_or_reuse_creates_expected_warehouse_sync_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ops_db = Path(temp_dir) / "ops.db"
            previous = os.environ.get("NOON_OPS_DB")
            os.environ["NOON_OPS_DB"] = str(ops_db)
            try:
                store, task, reused_existing = enqueue_or_reuse_warehouse_sync_task(
                    actor="category_batch_scan",
                    reason="category_batch_incremental_sync",
                    trigger_db="postgresql://stage-db",
                    warehouse_db="postgresql://warehouse-db",
                    source_node="node-a",
                    snapshot_id="snap-1",
                    created_by="category_batch_scan",
                )
                store.close()
            finally:
                if previous is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = previous

            self.assertFalse(reused_existing)
            self.assertEqual(task["task_type"], "warehouse_sync")
            self.assertEqual(task["worker_type"], "sync")
            self.assertEqual(task["payload"]["source_node"], "node-a")
            self.assertEqual(task["payload"]["snapshot_id"], "snap-1")

    def test_enqueue_or_reuse_reuses_existing_active_warehouse_sync_task(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ops_db = Path(temp_dir) / "ops.db"
            previous = os.environ.get("NOON_OPS_DB")
            os.environ["NOON_OPS_DB"] = str(ops_db)
            try:
                seed_store = OpsStore()
                try:
                    existing = seed_store.create_task(
                        task_type="warehouse_sync",
                        payload={"reason": "existing"},
                        created_by="seed",
                        worker_type="sync",
                    )
                finally:
                    seed_store.close()

                store, task, reused_existing = enqueue_or_reuse_warehouse_sync_task(
                    actor="category_batch_scan",
                    reason="category_batch_incremental_sync",
                    trigger_db="postgresql://stage-db",
                    warehouse_db="postgresql://warehouse-db",
                    source_node="node-a",
                    snapshot_id="snap-1",
                    created_by="category_batch_scan",
                )
                store.close()
            finally:
                if previous is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = previous

            self.assertTrue(reused_existing)
            self.assertEqual(task["id"], existing["id"])

    def test_enqueue_or_reuse_uses_canonical_sync_db_refs_when_configured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ops_db = Path(temp_dir) / "ops.db"
            previous_ops = os.environ.get("NOON_OPS_DB")
            previous_trigger = os.environ.get("NOON_SHARED_SYNC_TRIGGER_DB_REF")
            previous_warehouse = os.environ.get("NOON_SHARED_SYNC_WAREHOUSE_DB_REF")
            os.environ["NOON_OPS_DB"] = str(ops_db)
            os.environ["NOON_SHARED_SYNC_TRIGGER_DB_REF"] = "postgresql://noon:secret@postgres:5432/noon_stage"
            os.environ["NOON_SHARED_SYNC_WAREHOUSE_DB_REF"] = "postgresql://noon:secret@postgres:5432/noon_warehouse"
            try:
                store, task, reused_existing = enqueue_or_reuse_warehouse_sync_task(
                    actor="category_batch_scan",
                    reason="category_batch_incremental_sync",
                    trigger_db="postgresql://noon:secret@host.docker.internal:55432/noon_stage",
                    warehouse_db="postgresql://noon:secret@host.docker.internal:55432/noon_warehouse",
                    source_node="node-a",
                    snapshot_id="snap-1",
                    created_by="category_batch_scan",
                )
                store.close()
            finally:
                if previous_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = previous_ops
                if previous_trigger is None:
                    os.environ.pop("NOON_SHARED_SYNC_TRIGGER_DB_REF", None)
                else:
                    os.environ["NOON_SHARED_SYNC_TRIGGER_DB_REF"] = previous_trigger
                if previous_warehouse is None:
                    os.environ.pop("NOON_SHARED_SYNC_WAREHOUSE_DB_REF", None)
                else:
                    os.environ["NOON_SHARED_SYNC_WAREHOUSE_DB_REF"] = previous_warehouse

            self.assertFalse(reused_existing)
            self.assertEqual(task["payload"]["trigger_db"], "postgresql://noon:secret@postgres:5432/noon_stage")
            self.assertEqual(task["payload"]["warehouse_db"], "postgresql://noon:secret@postgres:5432/noon_warehouse")

    def test_enqueue_and_wait_returns_terminal_payload(self):
        fake_store = mock.Mock()
        fake_store.list_tasks.side_effect = [
            [{"id": 44, "task_type": "warehouse_sync", "status": "running", "created_at": "2026-04-13T00:00:00+00:00"}],
            [],
            [],
        ]
        fake_store.get_task.side_effect = [
            {"id": 44, "status": "running"},
            {"id": 44, "status": "completed"},
        ]
        fake_store.list_task_runs.side_effect = [
            [],
            [
                {
                    "id": 101,
                    "status": "completed",
                    "result": {
                        "reason": "category_batch_incremental_sync",
                        "sync_state": {"status": "completed"},
                    },
                    "error_text": "",
                }
            ],
        ]

        with (
            mock.patch("ops.shared_sync_queue.OpsStore", return_value=fake_store),
            mock.patch("ops.shared_sync_queue.time.sleep", return_value=None),
        ):
            payload = enqueue_and_wait_for_warehouse_sync(
                actor="category_batch_scan",
                reason="category_batch_incremental_sync",
                trigger_db="postgresql://stage-db",
                warehouse_db="postgresql://warehouse-db",
                snapshot_id="snap-2",
                source_node="node-b",
                poll_seconds=1,
                wait_timeout_seconds=3,
            )

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["task_id"], 44)
        self.assertTrue(payload["reused_existing"])
        self.assertEqual(payload["sync_state"]["status"], "completed")

    def test_enqueue_and_wait_payload_uses_canonical_sync_db_refs_when_configured(self):
        fake_store = mock.Mock()
        fake_store.list_tasks.return_value = []
        fake_store.create_task.return_value = {
            "id": 45,
            "status": "pending",
            "task_type": "warehouse_sync",
        }
        fake_store.get_task.return_value = {"id": 45, "status": "completed"}
        fake_store.list_task_runs.return_value = [
            {
                "id": 102,
                "status": "completed",
                "result": {
                    "reason": "category_batch_incremental_sync",
                    "sync_state": {"status": "completed"},
                },
                "error_text": "",
            }
        ]

        with (
            mock.patch("ops.shared_sync_queue.OpsStore", return_value=fake_store),
            mock.patch("ops.shared_sync_queue.time.sleep", return_value=None),
            mock.patch.dict(
                os.environ,
                {
                    "NOON_SHARED_SYNC_TRIGGER_DB_REF": "postgresql://noon:secret@postgres:5432/noon_stage",
                    "NOON_SHARED_SYNC_WAREHOUSE_DB_REF": "postgresql://noon:secret@postgres:5432/noon_warehouse",
                },
                clear=False,
            ),
        ):
            payload = enqueue_and_wait_for_warehouse_sync(
                actor="category_batch_scan",
                reason="category_batch_incremental_sync",
                trigger_db="postgresql://noon:secret@host.docker.internal:55432/noon_stage",
                warehouse_db="postgresql://noon:secret@host.docker.internal:55432/noon_warehouse",
                snapshot_id="snap-2",
                source_node="node-b",
                poll_seconds=1,
                wait_timeout_seconds=3,
            )

        self.assertEqual(payload["trigger_db"], "postgresql://noon:secret@postgres:5432/noon_stage")
        self.assertEqual(payload["warehouse_db"], "postgresql://noon:secret@postgres:5432/noon_warehouse")
        create_payload = fake_store.create_task.call_args.kwargs["payload"]
        self.assertEqual(create_payload["trigger_db"], "postgresql://noon:secret@postgres:5432/noon_stage")
        self.assertEqual(create_payload["warehouse_db"], "postgresql://noon:secret@postgres:5432/noon_warehouse")


if __name__ == "__main__":
    unittest.main()
