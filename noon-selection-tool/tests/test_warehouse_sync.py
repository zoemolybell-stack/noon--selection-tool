from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import warehouse_sync


class WarehouseSyncTests(unittest.TestCase):
    def test_acquire_sync_lock_writes_running_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            lock_path = base / "warehouse_sync.lock"
            state_path = base / "warehouse_sync_status.json"

            payload = warehouse_sync.acquire_sync_lock(
                actor="main_window",
                reason="manual_test",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
            )

            self.assertEqual(payload["status"], "running")
            self.assertTrue(lock_path.exists())
            self.assertTrue(state_path.exists())
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state_payload["status"], "running")
            self.assertEqual(state_payload["actor"], "main_window")

    def test_second_acquire_skips_while_lock_active(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            lock_path = base / "warehouse_sync.lock"
            state_path = base / "warehouse_sync_status.json"

            first = warehouse_sync.acquire_sync_lock(
                actor="main_window",
                reason="first",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
            )
            second = warehouse_sync.acquire_sync_lock(
                actor="keyword_window",
                reason="second",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
            )

            self.assertEqual(first["status"], "running")
            self.assertEqual(second["status"], "skipped")
            self.assertEqual(second["skip_reason"], "lock_active")

    def test_stale_lock_is_replaced(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            lock_path = base / "warehouse_sync.lock"
            state_path = base / "warehouse_sync_status.json"
            stale_payload = {
                "lock_token": "stale-token",
                "actor": "old_window",
                "reason": "old",
                "expires_at": (datetime.now() - timedelta(minutes=5)).isoformat(),
            }
            lock_path.write_text(json.dumps(stale_payload), encoding="utf-8")

            payload = warehouse_sync.acquire_sync_lock(
                actor="main_window",
                reason="replace_stale",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
            )

            self.assertEqual(payload["status"], "running")
            self.assertNotEqual(payload["lock_token"], "stale-token")
            current_lock = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(current_lock["lock_token"], payload["lock_token"])

    def test_finalize_sync_state_releases_lock_and_records_completion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            lock_path = base / "warehouse_sync.lock"
            state_path = base / "warehouse_sync_status.json"

            running = warehouse_sync.acquire_sync_lock(
                actor="main_window",
                reason="complete",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
            )
            completed = warehouse_sync.finalize_sync_state(
                lock_token=running["lock_token"],
                final_status="completed",
                actor="main_window",
                reason="complete",
                warehouse_db="warehouse.db",
                lock_path=lock_path,
                state_path=state_path,
                metadata={"result": "ok"},
            )

            self.assertEqual(completed["status"], "completed")
            self.assertFalse(lock_path.exists())
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state_payload["status"], "completed")
            self.assertEqual(state_payload["metadata"]["result"], "ok")


if __name__ == "__main__":
    unittest.main()
