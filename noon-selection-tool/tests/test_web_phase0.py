from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_beta import app as web_app


def create_minimal_warehouse_db(db_path: Path) -> None:
    now_utc = datetime.now().replace(microsecond=0)
    fresh_import = now_utc.isoformat()
    stale_stage_import = (now_utc - timedelta(days=2)).isoformat()
    recent_category_observed = (now_utc - timedelta(minutes=45)).isoformat()
    recent_keyword_observed = (now_utc - timedelta(minutes=30)).isoformat()
    recent_run_started = (now_utc - timedelta(minutes=20)).isoformat()
    recent_run_finished = (now_utc - timedelta(minutes=15)).isoformat()
    conn = sqlite3.connect(str(db_path))
    try:
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
            CREATE TABLE product_identity (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE product_category_membership (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                category_path TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE product_keyword_membership (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                keyword TEXT NOT NULL
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
            """
            INSERT INTO source_databases (
                source_label, db_path, source_scope, imported_at, source_product_count, source_observation_count, source_keyword_count
            ) VALUES
                ('category_db', 'category.db', 'category_stage', ?, 100, 200, 0),
                ('keyword_db', 'keyword.db', 'keyword_stage', ?, 50, 120, 30),
                ('warehouse_sync_db', 'warehouse.db', 'warehouse_sync', ?, 150, 320, 30)
            """
            ,
            (stale_stage_import, stale_stage_import, fresh_import),
        )
        conn.execute(
            """
            INSERT INTO observation_events (platform, product_id, source_type, scraped_at) VALUES
                ('noon', 'p1', 'category', ?),
                ('noon', 'p2', 'keyword', ?)
            """
            ,
            (recent_category_observed, recent_keyword_observed),
        )
        conn.execute(
            """
            INSERT INTO keyword_runs_log (
                run_type, trigger_mode, seed_keyword, snapshot_id, status, keyword_count, started_at, finished_at, metadata_json
            ) VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            ,
            (
                "crawl",
                "manual",
                "shoe",
                "snap1",
                "completed",
                10,
                recent_run_started,
                recent_run_finished,
                '{"quality_state":"full","quality_reasons":["all_platforms_ok"],"crawl_status":"completed","platform_stats":{"noon":{"status":"completed","products_count":10},"amazon":{"status":"completed","products_count":5}}}',
            ),
        )
        conn.commit()
    finally:
        conn.close()


class WebPhaseZeroTests(unittest.TestCase):
    @staticmethod
    def _fake_idle_store():
        class FakeStore:
            db_path = Path("ops.db")

            def list_tasks(self, status=None, worker_type=None, limit=None):
                return []

            def prune_stale_workers(self, max_age_seconds=None):
                return 0

            def list_workers(self, max_age_seconds=None):
                return []

            def get_status_counts(self):
                return {}

            def list_task_runs(self, task_id=None, limit=None):
                return []

            def close(self):
                return None

        return FakeStore()

    @staticmethod
    def _fake_worker_store(worker_items):
        class FakeStore:
            db_path = Path("ops.db")

            def __init__(self, items):
                self._items = items

            def prune_stale_workers(self, max_age_seconds=None):
                return 0

            def list_workers(self, max_age_seconds=None):
                return list(self._items)

            def get_status_counts(self):
                return {}

            def list_task_runs(self, task_id=None, limit=None):
                return []

            def list_tasks(self, status=None, worker_type=None, limit=None):
                return []

            def close(self):
                return None

        return FakeStore(worker_items)

    def test_health_degrades_when_warehouse_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_db = Path(temp_dir) / "missing-warehouse.db"
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(missing_db)
            try:
                web_app.prepare_web_read_models()
                payload = web_app.health()
            finally:
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous

        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["detail"], "warehouse_db_missing")
        self.assertEqual(payload["warehouse_db"], "missing-warehouse.db")
        self.assertEqual(payload["product_count"], 0)
        self.assertEqual(payload["observation_count"], 0)

    def test_neutralize_csv_cell_blocks_formula_prefixes(self):
        self.assertEqual(web_app.neutralize_csv_cell("=1+1"), "'=1+1")
        self.assertEqual(web_app.neutralize_csv_cell("+SUM(A1:A2)"), "'+SUM(A1:A2)")
        self.assertEqual(web_app.neutralize_csv_cell("-10"), "'-10")
        self.assertEqual(web_app.neutralize_csv_cell("@cmd"), "'@cmd")
        self.assertEqual(web_app.neutralize_csv_cell("\t=hidden"), "'\t=hidden")
        self.assertEqual(web_app.neutralize_csv_cell("https://example.com"), "https://example.com")
        self.assertEqual(web_app.neutralize_csv_cell(12.5), 12.5)

    def test_export_products_csv_neutralizes_crawler_text(self):
        rows = [
            {
                "platform": "noon",
                "product_id": "sku-1",
                "title": "=SUM(1,1)",
                "brand": "@brand",
                "seller_name": "+seller",
                "latest_price": 12.5,
                "latest_original_price": 15.0,
                "latest_currency": "SAR",
                "latest_rating": 4.8,
                "latest_review_count": 5,
                "latest_visible_bsr_rank": 12,
                "latest_signal_count": 3,
                "latest_delivery_type": "express",
                "latest_delivery_eta_signal_text": "-ETA",
                "latest_is_ad": 1,
                "latest_observed_category_path": "Home > Baby",
                "latest_source_type": "category",
                "latest_source_value": "Home > Baby",
                "latest_observed_at": "2026-03-29T22:00:00",
                "product_url": "https://www.noon.com/item",
            }
        ]
        with mock.patch.object(web_app, "fetch_products_rows", return_value=rows):
            response = web_app.export_products_csv(limit=10)

        decoded = response.body.decode("utf-8-sig")
        parsed_rows = list(csv.DictReader(io.StringIO(decoded)))
        self.assertEqual(parsed_rows[0]["title"], "'=SUM(1,1)")
        self.assertEqual(parsed_rows[0]["brand"], "'@brand")
        self.assertEqual(parsed_rows[0]["seller_name"], "'+seller")
        self.assertEqual(parsed_rows[0]["latest_delivery_eta_signal_text"], "'-ETA")
        self.assertEqual(parsed_rows[0]["product_url"], "https://www.noon.com/item")

    def test_workers_api_normalizes_remote_category_workers(self):
        previous = os.environ.get("NOON_REMOTE_CATEGORY_NODE_ENABLED")
        os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = "true"
        try:
            with mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_worker_store([])):
                payload = web_app.workers()
        finally:
            if previous is None:
                os.environ.pop("NOON_REMOTE_CATEGORY_NODE_ENABLED", None)
            else:
                os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = previous

        self.assertIn("summary", payload)
        self.assertEqual(payload["summary"]["remote_category_node_enabled"], True)
        self.assertEqual(payload["summary"]["worker_count"], 0)
        self.assertEqual(payload["summary"]["category_worker_heartbeat_state"], "missing")

    def test_workers_api_reports_remote_category_heartbeat_metadata(self):
        worker_items = [
            {
                "worker_name": "remote-category-1",
                "worker_type": "category",
                "status": "running",
                "current_task_id": 101,
                "heartbeat_at": "2026-04-13T08:30:00+08:00",
                "details": {
                    "node_role": "remote_category",
                    "node_host": "category-node-01",
                },
            },
            {
                "worker_name": "keyword-1",
                "worker_type": "keyword",
                "status": "idle",
                "current_task_id": None,
                "heartbeat_at": "2026-04-13T08:30:00+08:00",
                "details": {},
            },
        ]
        previous = os.environ.get("NOON_REMOTE_CATEGORY_NODE_ENABLED")
        os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = "true"
        try:
            with mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_worker_store(worker_items)):
                payload = web_app.workers()
        finally:
            if previous is None:
                os.environ.pop("NOON_REMOTE_CATEGORY_NODE_ENABLED", None)
            else:
                os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = previous

        self.assertEqual(payload["summary"]["remote_category_node_enabled"], True)
        self.assertEqual(payload["summary"]["worker_count"], 2)
        self.assertEqual(payload["summary"]["category_worker_count"], 1)
        self.assertEqual(payload["summary"]["remote_category_worker_count"], 1)
        self.assertEqual(payload["summary"]["category_worker_heartbeat_state"], "present")
        self.assertEqual(payload["summary"]["remote_category_hosts"], ["category-node-01"])
        self.assertEqual(payload["items"][0]["node_role"], "remote_category")
        self.assertEqual(payload["items"][0]["node_host"], "category-node-01")
        self.assertTrue(payload["items"][0]["is_remote_category_worker"])
        self.assertEqual(payload["items"][0]["category_worker_mode"], "remote_category")

    def test_runs_summary_exposes_freshness_and_shared_sync_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            create_minimal_warehouse_db(db_path)
            watchdog_path = Path(temp_dir) / "watchdog_latest.json"
            watchdog_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "generated_at": "2026-03-29T22:10:00+08:00",
                            "status": "warning",
                            "release": "runtime-hardening-r1",
                            "issue_count": 1,
                            "report_missing_dates": ["20260328"],
                            "latest_report_age_hours": 2.5,
                        },
                        "issues": [
                            {
                                "check": "report_freshness",
                                "severity": "warning",
                                "message": "daily report is missing or stale",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            previous_remote_category = os.environ.get("NOON_REMOTE_CATEGORY_NODE_ENABLED")
            previous_watchdog_path = web_app.WATCHDOG_LATEST_JSON_PATH
            os.environ["NOON_WAREHOUSE_DB"] = str(db_path)
            os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = "true"
            try:
                web_app.WATCHDOG_LATEST_JSON_PATH = watchdog_path
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(
                        web_app,
                        "read_sync_state",
                        return_value={
                            "status": "failed",
                            "actor": "keyword_window",
                            "reason": "monitor",
                            "requested_at": "2026-03-29T22:01:00",
                            "started_at": "2026-03-29T22:02:00",
                            "finished_at": "2026-03-29T22:04:00",
                            "updated_at": "2026-03-29T22:05:00",
                            "error": "builder_failed",
                            "skip_reason": "",
                            "warehouse_db": str(db_path),
                            "trigger_db": "keyword.db",
                            "metadata": {},
                        },
                    ),
                    mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_idle_store()),
                ):
                    payload = web_app.runs_summary()
            finally:
                web_app.WATCHDOG_LATEST_JSON_PATH = previous_watchdog_path
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous
                if previous_remote_category is None:
                    os.environ.pop("NOON_REMOTE_CATEGORY_NODE_ENABLED", None)
                else:
                    os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = previous_remote_category

        self.assertIn("freshness", payload)
        self.assertEqual(payload["freshness"]["shared_sync"]["status"], "failed")
        self.assertEqual(payload["freshness"]["shared_sync"]["state"], "failed")
        self.assertEqual(payload["freshness"]["overall_state"], "warning")
        self.assertEqual(payload["freshness"]["truth_source"], "warehouse_visibility_plus_shared_sync")
        self.assertEqual(payload["freshness"]["warehouse_visible_freshness"], "fresh")
        self.assertEqual(payload["freshness"]["stage_import_freshness"], "stale")
        self.assertEqual(payload["freshness"]["operator_freshness_state"], "stale")
        self.assertIn("warehouse-visible state only", payload["freshness"]["visibility_scope_note"])
        self.assertIn("Shared warehouse sync failed recently", payload["freshness"]["shared_sync"]["status_summary"])
        self.assertEqual(payload["freshness"]["category_stage"]["scope"], "category_stage")
        self.assertEqual(payload["freshness"]["keyword_stage"]["scope"], "keyword_stage")
        self.assertTrue(payload["freshness"]["category_stage"]["diagnosis_summary"])
        self.assertTrue(payload["freshness"]["category_stage"]["recommended_action"])
        self.assertTrue(payload["freshness"]["warehouse"]["status_summary"])
        self.assertTrue(payload["freshness"]["warnings"])
        self.assertTrue(payload["freshness"]["diagnostic_notes"])
        self.assertEqual(payload["freshness"]["shared_sync"]["warehouse_db"], "warehouse.db")
        self.assertEqual(payload["freshness"]["shared_sync"]["trigger_db"], "keyword.db")
        self.assertEqual(payload["freshness"]["shared_sync"]["requested_at"], "2026-03-29T22:01:00")
        self.assertEqual(payload["freshness"]["shared_sync"]["started_at"], "2026-03-29T22:02:00")
        self.assertEqual(payload["freshness"]["shared_sync"]["finished_at"], "2026-03-29T22:04:00")
        self.assertEqual(payload["watchdog"]["status"], "warning")
        self.assertEqual(payload["watchdog"]["issue_count"], 1)
        self.assertEqual(payload["keyword_quality"]["latest_quality_state"], "full")
        self.assertEqual(payload["keyword_quality"]["latest_terminal_quality_state"], "full")
        self.assertEqual(payload["keyword_quality"]["operator_quality_state"], "full")
        self.assertEqual(payload["keyword_quality"]["live_batch_state"], "idle")
        self.assertEqual(payload["keyword_quality"]["truth_source"], "keyword_snapshot_batch_aggregate")
        self.assertEqual(payload["keyword_quality"]["active_snapshot_count"], 0)
        self.assertEqual(payload["keyword_quality"]["stale_running_snapshot_count"], 0)
        self.assertEqual(payload["keyword_quality"]["quality_state_breakdown"]["full"], 1)

    def test_keyword_quality_idle_overlay_demotes_stale_active_count(self):
        with (
            mock.patch.object(
                web_app,
                "build_keyword_quality_truth_model",
                return_value={
                    "truth_source": "keyword_snapshot_batch_aggregate",
                    "active_snapshot_count": 1,
                    "stale_running_snapshot_count": 0,
                    "live_batch_state": "idle",
                    "operator_quality_state": "partial",
                    "latest_terminal_batch_state": "partial",
                },
            ),
            mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_idle_store()),
            mock.patch.object(web_app, "collect_lock_payload", return_value={"state": "idle"}),
            mock.patch.object(web_app, "collect_summary_payload", return_value={"exists": True}),
            mock.patch.object(web_app, "collect_batch_logs", return_value={"latest_log_age_seconds": None}),
        ):
            payload = web_app._summarize_keyword_quality([])

        self.assertEqual(payload["active_snapshot_count"], 0)
        self.assertEqual(payload["stale_running_snapshot_count"], 1)
        self.assertEqual(payload["live_batch_state"], "idle")

    def test_runs_summary_normalizes_shared_sync_skip_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            create_minimal_warehouse_db(db_path)
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            previous_watchdog_path = web_app.WATCHDOG_LATEST_JSON_PATH
            os.environ["NOON_WAREHOUSE_DB"] = str(db_path)
            try:
                web_app.WATCHDOG_LATEST_JSON_PATH = Path(temp_dir) / "watchdog_latest.json"
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(
                        web_app,
                        "read_sync_state",
                        return_value={
                            "status": "skipped",
                            "skip_reason": "lock_active",
                            "actor": "shared_sync",
                            "reason": "lock_active",
                            "requested_at": "2026-03-29T22:01:00",
                            "started_at": "2026-03-29T22:02:00",
                            "finished_at": "2026-03-29T22:04:00",
                            "updated_at": "2026-03-29T22:05:00",
                            "warehouse_db": str(db_path),
                            "trigger_db": "keyword.db",
                            "metadata": {},
                        },
                    ),
                    mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_idle_store()),
                ):
                    payload = web_app.runs_summary()
            finally:
                web_app.WATCHDOG_LATEST_JSON_PATH = previous_watchdog_path
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous

        self.assertEqual(payload["freshness"]["shared_sync"]["status"], "skipped")
        self.assertEqual(payload["freshness"]["shared_sync"]["state"], "skipped_due_to_active_lock")
        self.assertEqual(payload["freshness"]["warehouse_visible_freshness"], "fresh")
        self.assertEqual(payload["freshness"]["stage_import_freshness"], "stale")
        self.assertEqual(payload["freshness"]["operator_freshness_state"], "delayed")
        self.assertEqual(payload["freshness"]["overall_state"], "attention")

    def test_system_health_exposes_keyword_quality_and_watchdog(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            create_minimal_warehouse_db(db_path)
            watchdog_path = Path(temp_dir) / "watchdog_latest.json"
            watchdog_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "generated_at": "2026-03-29T22:10:00+08:00",
                            "status": "critical",
                            "issue_count": 2,
                            "report_missing_dates": ["20260328", "20260329"],
                        },
                        "issues": [
                            {"check": "stale_workers", "severity": "warning", "message": "1 stale worker rows"},
                            {"check": "api_health", "severity": "critical", "message": "health endpoint failed"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            previous_remote_category = os.environ.get("NOON_REMOTE_CATEGORY_NODE_ENABLED")
            previous_watchdog_path = web_app.WATCHDOG_LATEST_JSON_PATH
            os.environ["NOON_WAREHOUSE_DB"] = str(db_path)
            os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = "true"
            try:
                web_app.WATCHDOG_LATEST_JSON_PATH = watchdog_path
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(
                        web_app,
                        "read_sync_state",
                        return_value={
                            "status": "failed",
                            "error": "test_sync_failed",
                            "updated_at": "2026-03-29T22:10:00+08:00",
                        },
                    ),
                    mock.patch.object(
                        web_app,
                        "connect_ops_store",
                        return_value=self._fake_worker_store(
                            [
                                {
                                    "worker_name": "remote-category-1",
                                    "worker_type": "category",
                                    "status": "running",
                                    "current_task_id": 77,
                                    "heartbeat_at": "2026-03-29T22:09:00+08:00",
                                    "details": {"node_role": "remote_category", "node_host": "category-node-01"},
                                },
                                {
                                    "worker_name": "keyword-1",
                                    "worker_type": "keyword",
                                    "status": "running",
                                    "current_task_id": 88,
                                    "heartbeat_at": "2026-03-29T22:09:00+08:00",
                                    "details": {},
                                },
                            ]
                        ),
                    ),
                ):
                    payload = web_app.system_health()
            finally:
                web_app.WATCHDOG_LATEST_JSON_PATH = previous_watchdog_path
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous
                if previous_remote_category is None:
                    os.environ.pop("NOON_REMOTE_CATEGORY_NODE_ENABLED", None)
                else:
                    os.environ["NOON_REMOTE_CATEGORY_NODE_ENABLED"] = previous_remote_category

        self.assertIn("watchdog", payload)
        self.assertIn("keyword_quality", payload)
        self.assertIn("sync_visibility", payload)
        self.assertEqual(payload["watchdog"]["status"], "critical")
        self.assertEqual(payload["watchdog"]["severity_breakdown"]["critical"], 1)
        self.assertEqual(payload["keyword_quality"]["latest_quality_state"], "full")
        self.assertEqual(payload["keyword_quality"]["latest_terminal_quality_state"], "full")
        self.assertEqual(payload["keyword_quality"]["operator_quality_state"], "full")
        self.assertEqual(payload["sync_visibility"]["shared_sync"]["state"], "failed")
        self.assertIn("worker_summary", payload["ops"])
        self.assertEqual(payload["ops"]["worker_summary"]["remote_category_node_enabled"], True)
        self.assertEqual(payload["ops"]["worker_summary"]["category_worker_heartbeat_state"], "present")
        self.assertEqual(payload["ops"]["worker_summary"]["remote_category_worker_count"], 1)
        self.assertEqual(payload["ops"]["worker_summary"]["category_worker_hosts"], ["category-node-01"])

    def test_runs_summary_prefers_batch_truth_for_operator_active_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            create_minimal_warehouse_db(db_path)
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute(
                    """
                    INSERT INTO keyword_runs_log (
                        run_type, trigger_mode, seed_keyword, snapshot_id, status, keyword_count, started_at, finished_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "crawl",
                        "monitor",
                        "ghost",
                        "ghost-snapshot",
                        "running",
                        10,
                        datetime.now().isoformat(),
                        None,
                        json.dumps({"quality_state": "full", "quality_summary": {"state": "full"}}),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            os.environ["NOON_WAREHOUSE_DB"] = str(db_path)
            try:
                with (
                    mock.patch.object(web_app, "ensure_web_read_models", return_value=None),
                    mock.patch.object(web_app, "connect_ops_store", return_value=self._fake_idle_store()),
                    mock.patch.object(
                        web_app,
                        "_summarize_keyword_quality",
                        return_value={
                            "truth_source": "keyword_snapshot_batch_aggregate",
                            "active_snapshot_count": 0,
                            "stale_running_snapshot_count": 1,
                            "live_batch_state": "idle",
                            "operator_quality_state": "full",
                            "latest_terminal_batch_state": "full",
                        },
                    ),
                ):
                    payload = web_app.runs_summary()
            finally:
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous

        self.assertEqual(payload["health"]["active_keyword_run_count"], 0)
        self.assertEqual(payload["health"]["incomplete_keyword_run_count"], 0)
        self.assertEqual(payload["health"]["active_keyword_run_row_count"], 1)
        self.assertEqual(payload["health"]["incomplete_keyword_run_row_count"], 1)


if __name__ == "__main__":
    unittest.main()
