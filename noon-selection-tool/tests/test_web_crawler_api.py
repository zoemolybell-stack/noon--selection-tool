from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient
from starlette.requests import Request


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_beta import app as web_app
from ops import keyword_control_state
from config.product_store import ProductStore


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
        conn.commit()
    finally:
        conn.close()


class WebCrawlerApiTests(unittest.TestCase):
    def test_keyword_control_api_roundtrip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_db = Path(temp_dir) / "warehouse.db"
            ops_db = Path(temp_dir) / "ops.db"
            product_store_db = Path(temp_dir) / "keyword_product_store.db"
            create_minimal_warehouse_db(warehouse_db)
            monitor_config = Path(temp_dir) / "keyword_monitor_pet_sports.json"
            baseline_file = Path(temp_dir) / "pet_sports_baseline.txt"
            baseline_file.write_text("dog toys\ncat litter\nadidas shoes\n", encoding="utf-8")
            monitor_config.write_text(
                json.dumps({"baseline_file": str(baseline_file)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sidecar_dir = Path(temp_dir) / "runtime_data" / "crawler_control" / "keyword_controls"

            prev_warehouse = os.environ.get("NOON_WAREHOUSE_DB")
            prev_ops = os.environ.get("NOON_OPS_DB")
            prev_product_store = os.environ.get("NOON_PRODUCT_STORE_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(warehouse_db)
            os.environ["NOON_OPS_DB"] = str(ops_db)
            os.environ["NOON_PRODUCT_STORE_DB"] = str(product_store_db)
            try:
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir),
                    mock.patch.object(web_app, "KEYWORD_PRODUCT_STORE_PATH", product_store_db),
                ):
                    sidecar_dir.mkdir(parents=True, exist_ok=True)
                    client = TestClient(web_app.app, base_url="http://localhost")

                    get_response = client.get(
                        "/api/crawler/keyword-controls",
                        params={"monitor_config": str(monitor_config)},
                    )
                    self.assertEqual(get_response.status_code, 200)
                    self.assertEqual(
                        [item["keyword"] for item in get_response.json()["baseline_keywords"]],
                        ["dog toys", "cat litter", "adidas shoes"],
                    )

                    add_baseline_response = client.post(
                        "/api/crawler/keyword-controls/baseline",
                        json={
                            "monitor_config": str(monitor_config),
                            "keywords": ["foam roller"],
                            "mode": "add",
                        },
                    )
                    self.assertEqual(add_baseline_response.status_code, 200)
                    self.assertIn(
                        "foam roller",
                        [item["keyword"] for item in add_baseline_response.json()["baseline_keywords"]],
                    )
                    self.assertEqual(add_baseline_response.json()["registered_count"], 1)
                    self.assertEqual(add_baseline_response.json()["baseline_additions"], [])
                    self.assertEqual(
                        baseline_file.read_text(encoding="utf-8").splitlines(),
                        ["dog toys", "cat litter", "adidas shoes", "", "foam roller"],
                    )
                    store = ProductStore(product_store_db)
                    try:
                        tracked_keywords = [item["keyword"] for item in store.list_keywords(status="active")]
                    finally:
                        store.close()
                    self.assertIn("foam roller", tracked_keywords)

                    add_exclusion_response = client.post(
                        "/api/crawler/keyword-controls/disable",
                        json={
                            "monitor_config": str(monitor_config),
                            "keyword": "dog toys",
                            "blocked_sources": ["baseline", "generated", "tracked", "manual"],
                            "reason": "seed cleanup",
                        },
                    )
                    self.assertEqual(add_exclusion_response.status_code, 200)
                    self.assertEqual(add_exclusion_response.json()["disabled_keywords"][0]["keyword"], "dog toys")
                    self.assertEqual(
                        add_exclusion_response.json()["disabled_keywords"][0]["blocked_sources"],
                        ["baseline", "generated", "tracked", "manual"],
                    )
                    self.assertNotIn("dog toys", baseline_file.read_text(encoding="utf-8"))
            finally:
                if prev_warehouse is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = prev_warehouse
                if prev_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = prev_ops
                if prev_product_store is None:
                    os.environ.pop("NOON_PRODUCT_STORE_DB", None)
                else:
                    os.environ["NOON_PRODUCT_STORE_DB"] = prev_product_store

    def test_keyword_control_new_routes_and_active_keywords(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_db = Path(temp_dir) / "warehouse.db"
            ops_db = Path(temp_dir) / "ops.db"
            product_store_db = Path(temp_dir) / "keyword_product_store.db"
            create_minimal_warehouse_db(warehouse_db)
            monitor_config = Path(temp_dir) / "keyword_monitor_pet_sports.json"
            baseline_file = Path(temp_dir) / "pet_sports_baseline.txt"
            baseline_file.write_text("dog toys\ncat litter\nadidas shoes\n", encoding="utf-8")
            monitor_config.write_text(
                json.dumps(
                    {
                        "baseline_file": str(baseline_file),
                        "expand_source_types": ["generated", "manual"],
                        "crawl_stale_hours": 24,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            sidecar_dir = Path(temp_dir) / "runtime_data" / "crawler_control" / "keyword_controls"

            store = ProductStore(product_store_db)
            try:
                store.upsert_keyword(
                    "dog toys",
                    display_keyword="dog toys",
                    tracking_mode="tracked",
                    source_type="baseline",
                    priority=10,
                    metadata={"registration_source": "baseline", "root_seed_keyword": "dog toys"},
                )
                store.upsert_keyword(
                    "dog shampoo",
                    display_keyword="dog shampoo",
                    tracking_mode="tracked",
                    source_type="generated",
                    priority=20,
                    metadata={
                        "registration_source": "generated",
                        "parent_source_type": "baseline",
                        "root_seed_keyword": "dog toys",
                        "seed_keyword": "dog toys",
                    },
                )
                store.upsert_keyword(
                    "cat bed",
                    display_keyword="cat bed",
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=30,
                    metadata={"registration_source": "manual", "root_seed_keyword": "cat bed"},
                )
            finally:
                store.close()

            prev_warehouse = os.environ.get("NOON_WAREHOUSE_DB")
            prev_ops = os.environ.get("NOON_OPS_DB")
            prev_product_store = os.environ.get("NOON_PRODUCT_STORE_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(warehouse_db)
            os.environ["NOON_OPS_DB"] = str(ops_db)
            os.environ["NOON_PRODUCT_STORE_DB"] = str(product_store_db)
            try:
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir),
                    mock.patch.object(web_app, "KEYWORD_PRODUCT_STORE_PATH", product_store_db),
                ):
                    sidecar_dir.mkdir(parents=True, exist_ok=True)
                    client = TestClient(web_app.app, base_url="http://localhost")

                    disable_response = client.post(
                        "/api/crawler/keyword-controls/disable",
                        json={
                            "monitor_config": str(monitor_config),
                            "keyword": "dog toys",
                            "blocked_sources": ["tracked"],
                            "reason": "temporary stop",
                        },
                    )
                    self.assertEqual(disable_response.status_code, 200)
                    self.assertEqual(disable_response.json()["disabled_keywords"][0]["keyword"], "dog toys")

                    restore_response = client.post(
                        "/api/crawler/keyword-controls/restore",
                        json={
                            "monitor_config": str(monitor_config),
                            "keyword": "dog toys",
                        },
                    )
                    self.assertEqual(restore_response.status_code, 200)
                    self.assertEqual(restore_response.json()["disabled_keywords"], [])

                    roots_response = client.post(
                        "/api/crawler/keyword-controls/roots",
                        json={
                            "monitor_config": str(monitor_config),
                            "keyword": "dog",
                            "blocked_sources": ["generated"],
                            "reason": "root stop",
                            "match_mode": "contains",
                            "mode": "upsert",
                        },
                    )
                    self.assertEqual(roots_response.status_code, 200)
                    self.assertEqual(roots_response.json()["blocked_roots"][0]["root_keyword"], "dog")

                    roots_get_response = client.get(
                        "/api/crawler/keyword-controls/roots",
                        params={"monitor_config": str(monitor_config)},
                    )
                    self.assertEqual(roots_get_response.status_code, 200)
                    self.assertEqual(roots_get_response.json()["blocked_roots"][0]["root_keyword"], "dog")

                    active_response = client.get(
                        "/api/crawler/keyword-controls/active-keywords",
                        params={"monitor_config": str(monitor_config)},
                    )
                    self.assertEqual(active_response.status_code, 200)
                    active_keywords = [item["keyword"] for item in active_response.json()["items"]]
                    self.assertIn("dog toys", active_keywords)
                    self.assertIn("adidas shoes", active_keywords)
                    self.assertIn("cat bed", active_keywords)
                    self.assertNotIn("dog shampoo", active_keywords)
                    self.assertGreaterEqual(active_response.json()["summary"]["blocked_expand_count"], 1)
            finally:
                if prev_warehouse is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = prev_warehouse
                if prev_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = prev_ops
                if prev_product_store is None:
                    os.environ.pop("NOON_PRODUCT_STORE_DB", None)
                else:
                    os.environ["NOON_PRODUCT_STORE_DB"] = prev_product_store

    def test_crawler_plan_api_roundtrip(self):
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

                    catalog_response = client.get("/api/crawler/catalog")
                    self.assertEqual(catalog_response.status_code, 200)
                    self.assertIn("ready_categories", catalog_response.json())

                    create_response = client.post(
                        "/api/crawler/plans",
                        json={
                            "plan_type": "keyword_batch",
                            "name": "Pets batch",
                            "created_by": "test",
                            "schedule_kind": "weekly",
                            "schedule_json": {"days": ["mo", "fr"], "time": "09:00"},
                            "payload": {
                                "keywords": ["dog toys", "cat litter"],
                                "platforms": ["noon", "amazon"],
                                "noon_count": 20,
                                "amazon_count": 10,
                                "persist": True,
                            },
                            "enabled": True,
                        },
                    )
                    self.assertEqual(create_response.status_code, 200)
                    plan_id = create_response.json()["id"]

                    plans_response = client.get("/api/crawler/plans")
                    self.assertEqual(plans_response.status_code, 200)
                    self.assertEqual(plans_response.json()["items"][0]["id"], plan_id)

                    patch_response = client.patch(
                        f"/api/crawler/plans/{plan_id}",
                        json={
                            "name": "Pets batch v2",
                            "enabled": False,
                        },
                    )
                    self.assertEqual(patch_response.status_code, 200)
                    self.assertEqual(patch_response.json()["name"], "Pets batch v2")
                    self.assertFalse(patch_response.json()["enabled"])

                    resume_response = client.post(f"/api/crawler/plans/{plan_id}/resume")
                    self.assertEqual(resume_response.status_code, 200)
                    self.assertTrue(resume_response.json()["enabled"])

                    launch_response = client.post(f"/api/crawler/plans/{plan_id}/launch")
                    self.assertEqual(launch_response.status_code, 200)
                    task_id = launch_response.json()["id"]

                    runs_response = client.get("/api/crawler/runs")
                    self.assertEqual(runs_response.status_code, 200)
                    self.assertEqual(runs_response.json()["items"][0]["task_id"], task_id)

                    run_detail_response = client.get(f"/api/crawler/runs/{task_id}")
                    self.assertEqual(run_detail_response.status_code, 200)
                    self.assertEqual(run_detail_response.json()["task_id"], task_id)

                    pause_response = client.post(f"/api/crawler/plans/{plan_id}/pause")
                    self.assertEqual(pause_response.status_code, 200)
                    self.assertFalse(pause_response.json()["enabled"])
            finally:
                if prev_warehouse is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = prev_warehouse
                if prev_ops is None:
                    os.environ.pop("NOON_OPS_DB", None)
                else:
                    os.environ["NOON_OPS_DB"] = prev_ops

    def test_resolve_ui_user_key_distinguishes_lan_clients(self):
        first_scope = {
            "type": "http",
            "scheme": "http",
            "method": "GET",
            "path": "/api/ui/filter-presets",
            "raw_path": b"/api/ui/filter-presets",
            "query_string": b"",
            "headers": [
                (b"host", b"192.168.100.20:8865"),
                (b"x-forwarded-for", b"192.168.100.101"),
            ],
            "client": ("192.168.100.101", 43210),
            "server": ("192.168.100.20", 8865),
        }
        second_scope = {
            **first_scope,
            "headers": [
                (b"host", b"192.168.100.20:8865"),
                (b"x-forwarded-for", b"192.168.100.102"),
            ],
            "client": ("192.168.100.102", 43211),
        }
        first_key, first_is_local = web_app.resolve_ui_user_key(Request(first_scope))
        second_key, second_is_local = web_app.resolve_ui_user_key(Request(second_scope))

        self.assertFalse(first_is_local)
        self.assertFalse(second_is_local)
        self.assertTrue(first_key.startswith("lan-"))
        self.assertTrue(second_key.startswith("lan-"))
        self.assertNotEqual(first_key, second_key)
        self.assertNotEqual(first_key, "anonymous")

    def test_operator_cannot_access_crawler_admin_api(self):
        headers = {"Cf-Access-Authenticated-User-Email": "huangqing@ykwen.cn"}
        client = TestClient(web_app.app)

        response = client.get("/api/crawler/catalog", headers=headers)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"], "admin role required")

    def test_operator_session_uses_registry_role(self):
        headers = {"Cf-Access-Authenticated-User-Email": "huangqing@ykwen.cn"}
        client = TestClient(web_app.app)

        response = client.get("/api/session", headers=headers)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["email"], "huangqing@ykwen.cn")
        self.assertEqual(payload["role"], "operator")
        self.assertFalse(payload["allow_role_switch"])

    def test_dashboard_payload_redacts_storage_paths(self):
        with mock.patch.object(
            web_app,
            "fetch_one",
            side_effect=[
                {
                    "product_count": 12,
                    "keyword_count": 8,
                    "overlap_count": 3,
                    "last_imported_at": "2026-04-02T10:00:00",
                },
                {
                    "warehouse_last_imported_at": "2026-04-02T10:00:00",
                    "category_last_imported_at": "2026-04-02T09:50:00",
                    "keyword_last_imported_at": "2026-04-02T09:55:00",
                    "category_last_observed_at": "2026-04-02T09:49:00",
                    "keyword_last_observed_at": "2026-04-02T09:54:00",
                },
            ],
        ), mock.patch.object(
            web_app,
            "build_global_scope_payload",
            return_value={
                "scope_path": "",
                "scope_label": "全平台 / 大盘",
                "summary": {"product_count": 12},
                "delivery_breakdown": [],
                "child_categories": [],
            },
        ), mock.patch.object(
            web_app,
            "build_daily_import_series",
            return_value={"days": [], "category_products": [], "keyword_products": []},
        ), mock.patch.object(
            web_app,
            "build_dashboard_source_coverage",
            return_value=[],
        ), mock.patch.object(
            web_app,
            "build_dashboard_recent_imports",
            return_value=[],
        ), mock.patch.object(
            web_app,
            "build_dashboard_recent_keyword_runs",
            return_value=[],
        ), mock.patch.object(
            web_app,
            "read_sync_state",
            return_value={
                "status": "completed",
                "updated_at": "2026-04-02T10:00:00",
                "warehouse_db": "D:/secret/runtime/warehouse.db",
                "trigger_db": "D:/secret/runtime/product_store.db",
                "metadata": {},
            },
        ):
            payload = web_app.compute_dashboard_payload()

        self.assertNotIn("warehouse_db", payload["overview"])
        self.assertEqual(payload["sync_visibility"]["shared_sync"]["warehouse_db"], "warehouse.db")
        self.assertEqual(payload["sync_visibility"]["shared_sync"]["trigger_db"], "product_store.db")

    def test_build_daily_import_series_uses_first_seen_and_source_coverage(self):
        today = datetime.now().date()
        day_today = today.isoformat()
        day_yesterday = (today - timedelta(days=1)).isoformat()
        with mock.patch.object(
            web_app,
            "fetch_all",
            return_value=[
                {"observed_day": day_yesterday, "category_count": 1, "keyword_count": 0},
                {"observed_day": day_today, "category_count": 1, "keyword_count": 2},
            ],
        ):
            web_app.STATIC_PAYLOAD_CACHE.clear()
            payload = web_app.build_daily_import_series(days=2)

        self.assertEqual(payload["days"], [day_yesterday, day_today])
        self.assertEqual(payload["category_products"], [1, 1])
        self.assertEqual(payload["keyword_products"], [0, 2])


if __name__ == "__main__":
    unittest.main()
