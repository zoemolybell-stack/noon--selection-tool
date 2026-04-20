from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_beta import app as web_app


def create_minimal_warehouse_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE product_identity (platform TEXT NOT NULL, product_id TEXT NOT NULL)")
        conn.execute(
            """
            CREATE TABLE observation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                scraped_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE source_databases (
                source_label TEXT PRIMARY KEY,
                db_path TEXT NOT NULL,
                source_scope TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                source_product_count INTEGER DEFAULT 0,
                source_observation_count INTEGER DEFAULT 0,
                source_keyword_count INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE keyword_runs_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_type TEXT,
                trigger_mode TEXT,
                seed_keyword TEXT,
                snapshot_id TEXT,
                status TEXT,
                keyword_count INTEGER,
                started_at TEXT,
                finished_at TEXT,
                metadata_json TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO source_databases(source_label, db_path, source_scope, imported_at) VALUES ('category_db', 'category.db', 'category_stage', '2026-03-29T22:00:00')"
        )
        conn.execute(
            "INSERT INTO source_databases(source_label, db_path, source_scope, imported_at) VALUES ('keyword_db', 'keyword.db', 'keyword_stage', '2026-03-29T22:05:00')"
        )
        conn.execute(
            "INSERT INTO observation_events(platform, product_id, source_type, scraped_at) VALUES ('noon', 'p1', 'category', '2026-03-29T22:00:00')"
        )
        conn.commit()
    finally:
        conn.close()


class WebTaskApiTests(unittest.TestCase):
    def test_task_api_roundtrip_and_system_health(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_db = Path(temp_dir) / "warehouse.db"
            ops_db = Path(temp_dir) / "ops.db"
            create_minimal_warehouse_db(warehouse_db)

            prev_warehouse = os.environ.get("NOON_WAREHOUSE_DB")
            prev_ops = os.environ.get("NOON_OPS_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(warehouse_db)
            os.environ["NOON_OPS_DB"] = str(ops_db)
            try:
                with mock.patch.object(web_app, "ensure_web_read_models", return_value=None):
                    client = TestClient(web_app.app, base_url="http://localhost")
                    create_response = client.post(
                        "/api/tasks",
                        json={
                            "task_type": "category_ready_scan",
                            "payload": {"product_count": 50, "persist": True},
                            "created_by": "test",
                            "priority": 10,
                        },
                    )
                    self.assertEqual(create_response.status_code, 200)
                    task_id = create_response.json()["id"]

                    list_response = client.get("/api/tasks")
                    self.assertEqual(list_response.status_code, 200)
                    self.assertEqual(list_response.json()["items"][0]["id"], task_id)

                    workers_response = client.get("/api/workers")
                    self.assertEqual(workers_response.status_code, 200)
                    self.assertIn("items", workers_response.json())

                    runs_response = client.get("/api/task-runs")
                    self.assertEqual(runs_response.status_code, 200)

                    cancel_response = client.post(f"/api/tasks/{task_id}/cancel")
                    self.assertEqual(cancel_response.status_code, 200)
                    self.assertEqual(cancel_response.json()["status"], "cancelled")

                    retry_response = client.post(f"/api/tasks/{task_id}/retry")
                    self.assertEqual(retry_response.status_code, 200)
                    self.assertEqual(retry_response.json()["status"], "pending")

                    health_response = client.get("/api/system/health")
                    self.assertEqual(health_response.status_code, 200)
                    payload = health_response.json()
                    self.assertEqual(payload["status"], "ok")
                    self.assertTrue(payload["ops"]["ops_db"].endswith("ops.db"))
                    self.assertIn("shared_sync", payload)
            finally:
                if prev_warehouse is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = prev_warehouse
                if prev_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = prev_ops

    def test_operator_cannot_manage_tasks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_db = Path(temp_dir) / "warehouse.db"
            ops_db = Path(temp_dir) / "ops.db"
            create_minimal_warehouse_db(warehouse_db)

            prev_warehouse = os.environ.get("NOON_WAREHOUSE_DB")
            prev_ops = os.environ.get("NOON_OPS_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(warehouse_db)
            os.environ["NOON_OPS_DB"] = str(ops_db)
            try:
                with mock.patch.object(web_app, "ensure_web_read_models", return_value=None):
                    client = TestClient(web_app.app)
                    headers = {"Cf-Access-Authenticated-User-Email": "huangqing@ykwen.cn"}
                    create_response = client.post(
                        "/api/tasks",
                        headers=headers,
                        json={
                            "task_type": "category_ready_scan",
                            "payload": {"product_count": 50, "persist": True},
                            "created_by": "test",
                            "priority": 10,
                        },
                    )
                    self.assertEqual(create_response.status_code, 403)
                    self.assertEqual(create_response.json()["detail"], "admin role required")
            finally:
                if prev_warehouse is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = prev_warehouse
                if prev_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = prev_ops


if __name__ == "__main__":
    unittest.main()
