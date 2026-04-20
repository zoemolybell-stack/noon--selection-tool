from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.category_resilience import (
    classify_category_failure,
    evaluate_category_failover_state,
    evaluate_category_round_guardrail,
)
from ops.task_store import OpsStore


class CategoryResilienceTests(unittest.TestCase):
    def test_classify_category_failure_maps_access_denied(self):
        payload = classify_category_failure(
            error_text="Access Denied by Akamai",
            result={"stderr_tail": ["Access Denied", "Reference #18.4f8f"]},
        )
        self.assertEqual(payload["failure_category"], "access_denied")
        self.assertTrue(payload["last_error"].startswith("access_denied:"))

    def test_classify_category_failure_maps_db_tunnel_failure(self):
        payload = classify_category_failure(
            error_text="psycopg.OperationalError: could not connect to server at host.docker.internal:55432 Network is unreachable",
            result={},
        )
        self.assertEqual(payload["failure_category"], "db_tunnel_unavailable")

    def test_evaluate_category_failover_state_enters_fallback_when_remote_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            state_path = Path(temp_dir) / "failover.json"
            store = OpsStore(db_path)
            try:
                now = datetime.now(timezone.utc)
                with (
                    mock.patch.dict(os.environ, {"NOON_REMOTE_CATEGORY_NODE_ENABLED": "true"}, clear=False),
                    mock.patch("ops.category_resilience.CATEGORY_FAILOVER_STATE_PATH", state_path),
                ):
                    payload = evaluate_category_failover_state(store, reference_time=now, persist=True)
                self.assertEqual(payload["remote_category_node_state"], "missing")
                self.assertEqual(payload["category_failover_state"], "fallback_active")
                self.assertTrue(payload["fallback_should_accept_tasks"])
                self.assertTrue(payload["last_category_failover_at"])
            finally:
                store.close()

    def test_evaluate_category_failover_state_waits_for_failback_stability(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops.db"
            state_path = Path(temp_dir) / "failover.json"
            store = OpsStore(db_path)
            try:
                recovered_at = datetime.now(timezone.utc)
                previous = {
                    "mode": "fallback_active",
                    "remote_healthy_since": (recovered_at - timedelta(minutes=5)).isoformat(),
                    "last_failover_at": (recovered_at - timedelta(minutes=10)).isoformat(),
                }
                state_path.write_text(__import__("json").dumps(previous), encoding="utf-8")
                store.heartbeat_worker(
                    worker_name="remote-category-node-1",
                    worker_type="category",
                    status="idle",
                    details={
                        "node_role": "remote_category",
                        "node_host": "crawler-pc",
                        "health_state": "healthy",
                        "chrome_ready": True,
                        "db_tunnel_ready": True,
                    },
                )
                with (
                    mock.patch.dict(os.environ, {"NOON_REMOTE_CATEGORY_NODE_ENABLED": "true"}, clear=False),
                    mock.patch("ops.category_resilience.CATEGORY_FAILOVER_STATE_PATH", state_path),
                ):
                    payload = evaluate_category_failover_state(store, reference_time=recovered_at, persist=True)
                self.assertEqual(payload["category_failover_state"], "fallback_active")
                self.assertFalse(payload["fallback_has_running_task"])
                self.assertTrue(payload["fallback_should_accept_tasks"])
            finally:
                store.close()

    def test_evaluate_category_round_guardrail_aborts_after_consecutive_infra_failures(self):
        round_items = [
            {"id": 1, "item_order": 0, "status": "failed", "result": {"failure_category": "db_tunnel_unavailable"}},
            {"id": 2, "item_order": 1, "status": "failed", "result": {"failure_category": "chrome_unavailable"}},
            {"id": 3, "item_order": 2, "status": "failed", "result": {"failure_category": "warehouse_sync_failed"}},
            {"id": 4, "item_order": 3, "status": "pending", "result": {}},
        ]
        payload = evaluate_category_round_guardrail(round_items)
        self.assertTrue(payload["abort"])
        self.assertEqual(payload["reason"], "consecutive_infra_failures")
        self.assertEqual(payload["failure_category"], "warehouse_sync_failed")

    def test_evaluate_category_round_guardrail_aborts_when_remote_missing_without_fallback(self):
        round_items = [
            {"id": 1, "item_order": 0, "status": "completed", "result": {}},
            {"id": 2, "item_order": 1, "status": "pending", "result": {}},
        ]
        payload = evaluate_category_round_guardrail(
            round_items,
            failover_state={
                "remote_category_node_enabled": True,
                "remote_category_node_state": "missing",
                "fallback_should_accept_tasks": False,
                "fallback_worker_count": 0,
            },
        )
        self.assertTrue(payload["abort"])
        self.assertEqual(payload["reason"], "remote_node_unhealthy_without_fallback")
        self.assertEqual(payload["failure_category"], "node_unavailable")


if __name__ == "__main__":
    unittest.main()
