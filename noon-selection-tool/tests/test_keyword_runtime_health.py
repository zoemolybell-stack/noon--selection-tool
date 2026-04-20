from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.keyword_runtime_health import build_health_payload, build_quality_payload
from scrapers.base_scraper import build_keyword_result_stem


class KeywordRuntimeHealthTests(unittest.TestCase):
    def test_build_health_payload_reads_runtime_summary_and_warehouse(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            runtime_db = temp_root / "product_store.db"
            warehouse_db = temp_root / "warehouse.db"
            monitor_dir = temp_root / "monitor"
            batch_log_dir = monitor_dir / "batch_logs"
            batch_log_dir.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(runtime_db)
            conn.executescript(
                """
                CREATE TABLE keywords(id INTEGER PRIMARY KEY, keyword TEXT);
                CREATE TABLE keyword_edges(id INTEGER PRIMARY KEY, parent_keyword TEXT, child_keyword TEXT);
                CREATE TABLE crawl_observations(id INTEGER PRIMARY KEY, product_id TEXT);
                CREATE TABLE keyword_runs(
                    id INTEGER PRIMARY KEY,
                    run_type TEXT,
                    trigger_mode TEXT,
                    seed_keyword TEXT,
                    snapshot_id TEXT,
                    status TEXT,
                    keyword_count INTEGER,
                    started_at TEXT,
                    finished_at TEXT
                );
                CREATE TABLE keyword_metrics_snapshots(id INTEGER PRIMARY KEY, keyword TEXT);
                INSERT INTO keywords(keyword) VALUES('dog food');
                INSERT INTO keyword_runs(id, run_type, trigger_mode, seed_keyword, snapshot_id, status, keyword_count, started_at, finished_at)
                VALUES(1, 'monitor', 'scheduled', 'dog food', 'snap-1', 'completed', 10, '2026-03-29T10:00:00', '2026-03-29T10:05:00');
                """
            )
            conn.commit()
            conn.close()

            warehouse_conn = sqlite3.connect(warehouse_db)
            warehouse_conn.executescript(
                """
                CREATE TABLE source_databases(
                    source_label TEXT PRIMARY KEY,
                    source_scope TEXT,
                    imported_at TEXT,
                    source_product_count INTEGER,
                    source_observation_count INTEGER,
                    source_keyword_count INTEGER
                );
                CREATE TABLE product_identity(id INTEGER PRIMARY KEY);
                CREATE TABLE observation_events(id INTEGER PRIMARY KEY);
                CREATE TABLE product_keyword_membership(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_catalog(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_runs_log(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_metric_snapshots(id INTEGER PRIMARY KEY);
                INSERT INTO source_databases(source_label, source_scope, imported_at, source_product_count, source_observation_count, source_keyword_count)
                VALUES('keyword_stage', 'keyword_stage', '2026-03-29T10:06:00', 10, 20, 5);
                INSERT INTO keyword_catalog(id) VALUES(1);
                """
            )
            warehouse_conn.commit()
            warehouse_conn.close()

            (monitor_dir / "keyword_monitor.lock").write_text(
                json.dumps(
                    {
                        "pid": 999999,
                        "snapshot_id": "snap-2",
                        "started_at": "2026-03-29T10:10:00",
                        "updated_at": "2026-03-29T10:11:00",
                        "current_stage": "crawling",
                        "stage_note": "keywords:100",
                    }
                ),
                encoding="utf-8",
            )
            (monitor_dir / "keyword_monitor_last_run.json").write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "crawl_status": "completed",
                        "snapshot_id": "snap-1",
                        "started_at": "2026-03-29T10:00:00",
                        "updated_at": "2026-03-29T10:04:00",
                        "finished_at": "2026-03-29T10:05:00",
                        "current_stage": "completed",
                        "persisted_product_count": 20,
                        "errors": [],
                        "warehouse_sync": {"status": "completed"},
                        "quality_source_breakdown": {
                            "crawl": {
                                "state": "full",
                                "reason_codes": ["noon_success"],
                                "primary_reason": "noon_success",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            batch_log = batch_log_dir / "batch.out.log"
            batch_log.write_text("line1\nline2\nline3\n", encoding="utf-8")
            now = os.path.getmtime(batch_log)
            os.utime(batch_log, (now, now))

            payload = build_health_payload(
                db_path=runtime_db,
                monitor_dir=monitor_dir,
                warehouse_db_path=warehouse_db,
                preview_lines=2,
                runtime_code_files=(temp_root / "keyword_main.py",),
            )

            self.assertEqual(payload["runtime"]["stats"]["keywords"], 1)
            self.assertEqual(payload["last_summary"]["persisted_product_count"], 20)
            self.assertEqual(payload["last_summary"]["quality_source_breakdown"]["crawl"]["state"], "full")
            self.assertEqual(payload["warehouse"]["table_counts"]["keyword_catalog"], 1)
            self.assertEqual(payload["lock"]["state"], "stale_lock")
            self.assertEqual(payload["lock"]["current_stage"], "crawling")
            self.assertEqual(payload["last_summary"]["current_stage"], "completed")
            self.assertEqual(payload["warehouse_lag"]["state"], "up_to_date")
            self.assertEqual(payload["warehouse_lag"]["keyword_gap"], -4)
            self.assertEqual(payload["runtime_code"]["state"], "stale_lock")
            self.assertEqual(payload["restart_verification"]["state"], "stale_lock")
            self.assertEqual(payload["operator_hint"]["code"], "stale_lock_detected")
            self.assertEqual(len(payload["batch_logs"]["latest_previews"]), 1)

    def test_build_quality_payload_classifies_bs4_unavailable_to_root_cause(self):
        payload = build_quality_payload(
            {
                "quality_state": "degraded",
                "quality_reasons": ["beautifulsoup4_unavailable", "amazon_bsr_missing"],
                "quality_evidence": ["beautifulsoup4_unavailable", "amazon_bsr_missing"],
                "quality_source_breakdown": {
                    "amazon": {
                        "state": "degraded",
                        "status": "failed",
                        "reason_codes": ["beautifulsoup4_unavailable"],
                        "primary_reason": "beautifulsoup4_unavailable",
                    }
                },
            },
            live_batch_state="running",
            latest_terminal_quality_state="degraded",
            operator_quality_state="degraded",
        )

        self.assertEqual(payload["state"], "degraded")
        self.assertNotIn("beautifulsoup4_unavailable", payload["quality_reasons"])
        self.assertIn(payload["quality_reasons"][0], {"runtime_import_error", "dependency_missing"})
        self.assertEqual(payload["quality_source_breakdown"]["amazon"]["reason_codes"][0], payload["quality_reasons"][0])
        self.assertEqual(payload["quality_source_breakdown"]["amazon"]["primary_reason"], payload["quality_reasons"][0])
        self.assertEqual(payload["latest_quality_state"], "degraded")
        self.assertEqual(payload["operator_quality_state"], "degraded")

    def test_build_health_payload_marks_running_logs_as_active_and_detects_code_drift(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            runtime_db = temp_root / "product_store.db"
            warehouse_db = temp_root / "warehouse.db"
            monitor_dir = temp_root / "monitor"
            batch_log_dir = monitor_dir / "batch_logs"
            batch_log_dir.mkdir(parents=True, exist_ok=True)
            tracked_runtime_file = temp_root / "keyword_main.py"
            tracked_runtime_file.write_text("# runtime marker\n", encoding="utf-8")
            modified_dt = datetime(2026, 3, 29, 10, 12, 0)
            modified_ts = modified_dt.timestamp()
            os.utime(tracked_runtime_file, (modified_ts, modified_ts))

            conn = sqlite3.connect(runtime_db)
            conn.executescript(
                """
                CREATE TABLE keywords(id INTEGER PRIMARY KEY, keyword TEXT);
                CREATE TABLE keyword_edges(id INTEGER PRIMARY KEY, parent_keyword TEXT, child_keyword TEXT);
                CREATE TABLE crawl_observations(id INTEGER PRIMARY KEY, product_id TEXT);
                CREATE TABLE keyword_runs(
                    id INTEGER PRIMARY KEY,
                    run_type TEXT,
                    trigger_mode TEXT,
                    seed_keyword TEXT,
                    snapshot_id TEXT,
                    status TEXT,
                    keyword_count INTEGER,
                    started_at TEXT,
                    finished_at TEXT
                );
                CREATE TABLE keyword_metrics_snapshots(id INTEGER PRIMARY KEY, keyword TEXT);
                """
            )
            conn.commit()
            conn.close()

            warehouse_conn = sqlite3.connect(warehouse_db)
            warehouse_conn.executescript(
                """
                CREATE TABLE source_databases(
                    source_label TEXT PRIMARY KEY,
                    source_scope TEXT,
                    imported_at TEXT,
                    source_product_count INTEGER,
                    source_observation_count INTEGER,
                    source_keyword_count INTEGER
                );
                CREATE TABLE product_identity(id INTEGER PRIMARY KEY);
                CREATE TABLE observation_events(id INTEGER PRIMARY KEY);
                CREATE TABLE product_keyword_membership(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_catalog(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_runs_log(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_metric_snapshots(id INTEGER PRIMARY KEY);
                """
            )
            warehouse_conn.commit()
            warehouse_conn.close()

            current_pid = os.getpid()
            (monitor_dir / "keyword_monitor.lock").write_text(
                json.dumps(
                    {
                        "pid": current_pid,
                        "snapshot_id": "snap-active",
                        "started_at": "2026-03-29T10:10:00",
                        "updated_at": "2026-03-29T10:11:00",
                        "status": "running",
                        "current_stage": "crawling",
                    }
                ),
                encoding="utf-8",
            )
            recent_log = batch_log_dir / "batch.err.log"
            recent_log.write_text("progress line\n", encoding="utf-8")
            now = os.path.getmtime(recent_log)
            os.utime(recent_log, (now, now))

            payload = build_health_payload(
                db_path=runtime_db,
                monitor_dir=monitor_dir,
                warehouse_db_path=warehouse_db,
                preview_lines=2,
                runtime_code_files=(tracked_runtime_file,),
            )

            self.assertEqual(payload["monitor_state"], "running")
            self.assertEqual(payload["activity_state"], "running_active_logs")
            self.assertEqual(payload["lock"]["current_stage"], "crawling")
            self.assertIsNotNone(payload["batch_logs"]["latest_log_age_seconds"])
            self.assertEqual(payload["warehouse_lag"]["state"], "missing_keyword_import")
            self.assertEqual(payload["runtime_code"]["state"], "running_process_predates_disk_code")
            self.assertTrue(payload["runtime_code"]["needs_restart_to_apply_code"])
            self.assertTrue(payload["runtime_code"]["tracked_files"][0]["modified_after_process_start"])
            self.assertEqual(payload["restart_verification"]["state"], "restart_required_after_completion")
            self.assertEqual(payload["operator_hint"]["code"], "active_batch_restart_after_completion")

    def test_build_health_payload_marks_running_semantics_verified_after_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            runtime_db = temp_root / "product_store.db"
            warehouse_db = temp_root / "warehouse.db"
            monitor_dir = temp_root / "monitor"
            batch_log_dir = monitor_dir / "batch_logs"
            snapshot_root = temp_root / "snapshots" / "snap-active" / "amazon"
            batch_log_dir.mkdir(parents=True, exist_ok=True)
            snapshot_root.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(runtime_db)
            conn.executescript(
                """
                CREATE TABLE keywords(id INTEGER PRIMARY KEY, keyword TEXT);
                CREATE TABLE keyword_edges(id INTEGER PRIMARY KEY, parent_keyword TEXT, child_keyword TEXT);
                CREATE TABLE crawl_observations(id INTEGER PRIMARY KEY, product_id TEXT);
                CREATE TABLE keyword_runs(
                    id INTEGER PRIMARY KEY,
                    run_type TEXT,
                    trigger_mode TEXT,
                    seed_keyword TEXT,
                    snapshot_id TEXT,
                    status TEXT,
                    keyword_count INTEGER,
                    started_at TEXT,
                    finished_at TEXT
                );
                CREATE TABLE keyword_metrics_snapshots(id INTEGER PRIMARY KEY, keyword TEXT);
                """
            )
            conn.commit()
            conn.close()

            warehouse_conn = sqlite3.connect(warehouse_db)
            warehouse_conn.executescript(
                """
                CREATE TABLE source_databases(
                    source_label TEXT PRIMARY KEY,
                    source_scope TEXT,
                    imported_at TEXT,
                    source_product_count INTEGER,
                    source_observation_count INTEGER,
                    source_keyword_count INTEGER
                );
                CREATE TABLE product_identity(id INTEGER PRIMARY KEY);
                CREATE TABLE observation_events(id INTEGER PRIMARY KEY);
                CREATE TABLE product_keyword_membership(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_catalog(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_runs_log(id INTEGER PRIMARY KEY);
                CREATE TABLE keyword_metric_snapshots(id INTEGER PRIMARY KEY);
                """
            )
            warehouse_conn.commit()
            warehouse_conn.close()

            tracked_runtime_file = temp_root / "keyword_main.py"
            tracked_runtime_file.write_text("# runtime marker\n", encoding="utf-8")
            modified_dt = datetime(2026, 3, 29, 10, 9, 0)
            modified_ts = modified_dt.timestamp()
            os.utime(tracked_runtime_file, (modified_ts, modified_ts))

            current_pid = os.getpid()
            (monitor_dir / "keyword_monitor.lock").write_text(
                json.dumps(
                    {
                        "pid": current_pid,
                        "snapshot_id": "snap-active",
                        "started_at": "2026-03-29T10:10:00",
                        "updated_at": "2026-03-29T10:11:00",
                        "status": "running",
                        "current_stage": "crawling",
                        "stage_note": "amazon",
                    }
                ),
                encoding="utf-8",
            )
            recent_log = batch_log_dir / "batch.err.log"
            recent_log.write_text("progress line\n", encoding="utf-8")
            now = os.path.getmtime(recent_log)
            os.utime(recent_log, (now, now))

            keyword = "dog leash"
            result_path = snapshot_root / f"{build_keyword_result_stem(keyword)}.json"
            result_path.write_text(
                json.dumps(
                    {
                        "keyword": keyword,
                        "products": [],
                        "total_results": 0,
                        "_meta": {
                            "platform": "amazon",
                            "keyword": keyword,
                            "scraped_at": "2026-03-29T10:11:30",
                        },
                    }
                ),
                encoding="utf-8",
            )

            payload = build_health_payload(
                db_path=runtime_db,
                monitor_dir=monitor_dir,
                warehouse_db_path=warehouse_db,
                preview_lines=2,
                runtime_code_files=(tracked_runtime_file,),
                result_sample_size=1,
            )

            self.assertEqual(payload["runtime_code"]["state"], "running_process_matches_disk_code")
            self.assertEqual(payload["result_files"]["state"], "current_semantics")
            self.assertEqual(payload["result_files"]["platforms"]["amazon"]["state"], "current_semantics")
            self.assertEqual(payload["restart_verification"]["state"], "running_semantics_verified")


if __name__ == "__main__":
    unittest.main()
