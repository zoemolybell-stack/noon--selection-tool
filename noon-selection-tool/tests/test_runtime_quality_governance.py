from __future__ import annotations

import argparse
import json
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

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional in some environments
    pd = None

import keyword_main
import run_task_scheduler
import tools.run_nas_watchdog_agent as watchdog
import tools.write_crawl_report as report
from analysis.keyword_quality_summary import summarize_recent_keyword_runs
from config.product_store import ProductStore
from config.settings import Settings


def build_settings(temp_root: Path, snapshot_id: str = "test_snapshot") -> Settings:
    settings = Settings()
    settings.set_runtime_scope("keyword")
    settings.set_data_dir(temp_root / "runtime_data" / "keyword")
    settings.set_product_store_db_path(temp_root / "runtime_data" / "keyword" / "product_store.db")
    settings.set_snapshot_id(snapshot_id)
    for subdir in keyword_main.KEYWORD_SNAPSHOT_SUBDIRS:
        (settings.snapshot_dir / subdir).mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "processed").mkdir(parents=True, exist_ok=True)
    return settings


def write_result(settings: Settings, platform: str, keyword: str, payload: dict) -> Path:
    path = keyword_main._result_file_path(settings, platform, keyword)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


class KeywordQualityGovernanceTests(unittest.IsolatedAsyncioTestCase):
    def test_summarize_recent_keyword_runs_handles_mixed_timezone_timestamps(self):
        summary = summarize_recent_keyword_runs(
            [
                {
                    "run_type": "crawl",
                    "trigger_mode": "monitor",
                    "seed_keyword": "pets",
                    "snapshot_id": "snap-live",
                    "status": "running",
                    "keyword_count": 20,
                    "started_at": "2026-04-11T10:00:00+00:00",
                    "finished_at": "",
                    "metadata_json": json.dumps(
                        {
                            "quality_state": "full",
                            "quality_summary": {"state": "full"},
                            "platform_stats": {"noon": {"status": "completed"}},
                        }
                    ),
                },
                {
                    "run_type": "analyze",
                    "trigger_mode": "monitor",
                    "seed_keyword": "sports",
                    "snapshot_id": "snap-terminal",
                    "status": "failed",
                    "keyword_count": 20,
                    "started_at": "2026-04-11T17:30:00",
                    "finished_at": "2026-04-11T17:35:00",
                    "metadata_json": json.dumps(
                        {
                            "quality_state": "degraded",
                            "quality_summary": {"state": "degraded"},
                            "errors": ["analysis_empty"],
                        }
                    ),
                },
            ]
        )

        self.assertEqual(summary["live_batch_snapshot_id"], "snap-live")
        self.assertEqual(summary["latest_terminal_batch_snapshot_id"], "snap-terminal")
        self.assertEqual(summary["operator_quality_state"], "degraded")

    def test_summarize_recent_keyword_runs_prunes_generic_runtime_and_stale_analysis_empty(self):
        crawl_metadata = {
            "quality_state": "partial",
            "quality_reasons": [
                "noon:runtime_error",
                "noon:page_recognition_failed",
                "analysis:analysis_empty",
            ],
            "quality_summary": {
                "state": "partial",
                "quality_evidence": [
                    "selector_miss:plp-product-box-name",
                    "url:https://www.noon.com/search?q=test&page=2",
                ],
                "quality_source_breakdown": {
                    "noon": {
                        "state": "partial",
                        "status": "partial",
                        "reason_codes": ["runtime_error", "page_recognition_failed", "partial_results"],
                        "failure_details": [],
                        "evidence": ["selector_miss:plp-product-box-name"],
                    },
                    "analysis": {
                        "state": "unknown",
                        "status": "unknown",
                        "reason_codes": ["analysis_empty"],
                        "available": False,
                    },
                },
            },
        }
        analyze_metadata = {
            "quality_state": "full",
            "quality_summary": {
                "state": "full",
                "quality_evidence": ["google_trends_missing"],
                "quality_source_breakdown": {
                    "analysis": {
                        "state": "full",
                        "status": "full",
                        "reason_codes": ["google_trends_missing"],
                        "available": True,
                        "rows": 12,
                    }
                },
            },
        }
        summary = summarize_recent_keyword_runs(
            [
                {
                    "run_type": "crawl",
                    "trigger_mode": "monitor",
                    "seed_keyword": "test",
                    "snapshot_id": "snap-cleanup",
                    "status": "partial",
                    "keyword_count": 10,
                    "started_at": "2026-04-11T10:00:00+00:00",
                    "finished_at": "2026-04-11T10:05:00+00:00",
                    "metadata_json": json.dumps(crawl_metadata),
                },
                {
                    "run_type": "analyze",
                    "trigger_mode": "monitor",
                    "seed_keyword": "test",
                    "snapshot_id": "snap-cleanup",
                    "status": "completed",
                    "keyword_count": 10,
                    "started_at": "2026-04-11T10:05:10+00:00",
                    "finished_at": "2026-04-11T10:05:20+00:00",
                    "metadata_json": json.dumps(analyze_metadata),
                },
            ]
        )

        self.assertEqual(summary["operator_quality_state"], "partial")
        self.assertNotIn("runtime_error", summary["operator_quality_reasons"])
        self.assertNotIn("analysis_empty", summary["operator_quality_reasons"])
        self.assertIn("page_recognition_failed", summary["operator_quality_reasons"])
        analysis_payload = summary["operator_quality_source_breakdown"]["analysis"]
        self.assertEqual(analysis_payload["state"], "full")
        self.assertTrue(analysis_payload["available"])
        self.assertEqual(analysis_payload["rows"], 12)

    def test_summarize_recent_keyword_runs_clears_operator_reasons_for_full_batch(self):
        summary = summarize_recent_keyword_runs(
            [
                {
                    "run_type": "crawl",
                    "trigger_mode": "monitor",
                    "seed_keyword": "foam roller",
                    "snapshot_id": "snap-full-clean",
                    "status": "completed",
                    "keyword_count": 12,
                    "started_at": "2026-04-13T03:10:00+00:00",
                    "finished_at": "2026-04-13T03:12:00+00:00",
                    "metadata_json": json.dumps(
                        {
                            "quality_state": "full",
                            "quality_summary": {
                                "state": "full",
                                "quality_evidence": ["google_trends_missing", "analysis_empty"],
                                "quality_source_breakdown": {
                                    "crawl": {
                                        "state": "full",
                                        "status": "completed",
                                        "reason_codes": ["analysis_empty"],
                                        "primary_reason": "analysis_empty",
                                        "evidence": ["analysis_empty"],
                                    },
                                    "analysis": {
                                        "state": "full",
                                        "status": "full",
                                        "reason_codes": ["google_trends_missing", "analysis_empty"],
                                        "primary_reason": "google_trends_missing",
                                        "available": True,
                                        "rows": 18,
                                    },
                                },
                            },
                        }
                    ),
                }
            ]
        )

        self.assertEqual(summary["operator_quality_state"], "full")
        self.assertEqual(summary["latest_terminal_batch_state"], "full")
        self.assertEqual(summary["operator_quality_reasons"], [])
        self.assertEqual(summary["operator_quality_evidence"], [])
        self.assertEqual(summary["latest_terminal_quality_reasons"], [])
        self.assertEqual(summary["latest_terminal_quality_evidence"], [])
        self.assertEqual(
            summary["operator_quality_source_breakdown"]["analysis"]["reason_codes"],
            [],
        )
        self.assertEqual(
            summary["operator_quality_source_breakdown"]["crawl"]["reason_codes"],
            [],
        )

    @unittest.skipIf(pd is None, "pandas unavailable")
    def test_analysis_quality_summary_detects_google_and_bsr_sources(self):
        frame = pd.DataFrame(
            [
                {"keyword": "a", "has_google_trends": False, "amazon_bsr_count": 0},
                {"keyword": "b", "has_google_trends": True, "amazon_bsr_count": 3},
            ]
        )

        summary = keyword_main._summarize_analysis_quality(frame)

        self.assertEqual(summary["state"], "full")
        self.assertTrue(summary["google_trends_available"])
        self.assertTrue(summary["amazon_bsr_available"])

    def test_keyword_quality_summary_treats_zero_results_as_quality_neutral(self):
        with mock.patch.object(
            keyword_main,
            "_summarize_analysis_quality",
            return_value={
                "state": "full",
                "available": True,
                "rows": 8,
                "google_trends_available": True,
                "amazon_bsr_available": True,
                "quality_flags": [],
                "quality_evidence": [],
            },
        ):
            summary = keyword_main._summarize_keyword_quality(
                {
                    "platforms": ["noon", "amazon"],
                    "platform_stats": {
                        "noon": {
                            "status": "zero_results",
                            "products_count": 0,
                            "total_results": 0,
                            "error_evidence": ["zero_results:no_product_cards_page_1"],
                            "zero_result_evidence": ["zero_results:no_product_cards_page_1"],
                            "failure_details": [],
                        },
                        "amazon": {
                            "status": "completed",
                            "products_count": 10,
                            "total_results": 120,
                            "error_evidence": [],
                            "zero_result_evidence": [],
                            "failure_details": [],
                        },
                    },
                }
            )

        self.assertEqual(summary["state"], "full")
        self.assertNotIn("noon:zero_results", summary["quality_reasons"])
        self.assertEqual(summary["quality_source_breakdown"]["noon"]["state"], "full")
        self.assertEqual(summary["quality_source_breakdown"]["runtime"]["state"], "full")

    async def test_run_scrape_marks_degraded_when_bs4_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="quality_snapshot")
            args = argparse.Namespace(
                keyword="shoes",
                keywords_file=None,
                platforms=["amazon"],
                noon_count=5,
                amazon_count=5,
                persist=True,
                tracking_mode="adhoc",
                priority=20,
                limit=None,
                stale_hours=None,
            )

            async def fake_step_scrape(
                settings_obj,
                keywords,
                *,
                platforms=None,
                noon_count=None,
                amazon_count=None,
            ):
                write_result(
                    settings_obj,
                    "amazon",
                    keywords[0],
                    {
                        "keyword": keywords[0],
                        "products": [],
                        "page_state": "results",
                        "zero_result_evidence": [],
                        "error_evidence": ["beautifulsoup4_unavailable"],
                        "error": "beautifulsoup4_unavailable",
                        "total_results": 0,
                        "suggested_keywords": [],
                    },
                )

            with (
                mock.patch.object(keyword_main.keyword_core, "step_scrape", side_effect=fake_step_scrape),
                mock.patch.object(keyword_main.keyword_core, "_persist_keyword_results", return_value={"amazon": 0}),
                mock.patch.object(
                    keyword_main,
                    "_capture_live_suggestion_edges",
                    return_value={
                        "platform_files": 0,
                        "parent_keywords": 0,
                        "discovered_keywords": 0,
                        "recorded_edges": 0,
                        "rejected_keywords": 0,
                    },
                ),
                mock.patch.object(keyword_main, "_sync_warehouse", return_value={"status": "completed", "reason": "crawl"}),
            ):
                summary = await keyword_main.run_scrape(settings, args, keywords=["sample keyword"], sync_warehouse=False)

            self.assertEqual(summary["quality_state"], "degraded")
            self.assertEqual(summary["quality_summary"]["state"], "degraded")
            self.assertTrue(summary["quality_summary"]["signals"]["beautifulsoup4_unavailable"])
            self.assertEqual(summary["platform_stats"]["amazon"]["quality_state"], "degraded")

            conn = sqlite3.connect(settings.product_store_db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT metadata_json FROM keyword_runs WHERE run_type = 'crawl' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            metadata = json.loads(row["metadata_json"])
            self.assertEqual(metadata["quality_state"], "degraded")
            self.assertEqual(metadata["quality_summary"]["state"], "degraded")

    def test_run_monitor_persists_quality_summary_from_analysis(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="monitor_quality_snapshot")
            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keywords(
                    ["yoga mat", "foam roller"],
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                )
            finally:
                store.close()

            args = argparse.Namespace(noon_count=5, amazon_count=5)
            monitor_profile = {
                "baseline_file": None,
                "tracked_priority": 30,
                "expand_limit": 0,
                "expand_stale_hours": 72,
                "expand_source_types": ["manual", "expanded"],
                "expand_platforms": ["noon", "amazon"],
                "crawl_platforms": ["noon", "amazon"],
                "crawl_stale_hours": 24,
                "crawl_limit": 10,
                "crawl_batch_size": 10,
                "crawl_sync_interval_seconds": 0,
                "monitor_report": False,
            }

            batch_summary = {
                "status": "completed",
                "persisted_product_count": 2,
                "persist_counts": {"noon": 2},
                "platform_stats": {
                    "noon": {
                        "status": "completed",
                        "attempts": 1,
                        "retry_count": 0,
                        "keyword_count": 2,
                        "products_count": 2,
                        "persisted_count": 2,
                        "result_files": ["seed.json"],
                        "total_results": 2,
                        "error": "",
                        "zero_result_evidence": [],
                        "failed_keywords": [],
                        "zero_result_keywords": [],
                        "attempt_history": [{"attempt": 1, "status": "completed"}],
                        "live_suggestion_files": 0,
                        "live_suggestion_parents": 0,
                        "live_suggestion_keywords": 0,
                        "live_suggestion_edges": 0,
                        "live_suggestion_rejected": 0,
                        "quality_state": "full",
                        "quality_flags": [],
                        "quality_evidence": [],
                    }
                },
                "errors": [],
                "quality_state": "full",
                "quality_summary": {
                    "state": "full",
                    "crawl_state": "full",
                    "analysis_state": "unknown",
                    "platforms": {"noon": {"state": "full"}},
                    "signals": {
                        "noon_success": True,
                        "amazon_success": None,
                        "amazon_bsr_available": None,
                        "google_trends_available": None,
                        "beautifulsoup4_unavailable": False,
                    },
                    "quality_flags": [],
                    "quality_evidence": [],
                    "analysis": {
                        "state": "unknown",
                        "available": False,
                        "rows": 0,
                        "google_trends_available": False,
                        "amazon_bsr_available": False,
                        "quality_flags": ["analysis_empty"],
                        "quality_evidence": ["analysis_empty"],
                    },
                },
            }

            frame = pd.DataFrame(
                [
                    {"keyword": "yoga mat", "has_google_trends": True, "amazon_bsr_count": 2},
                    {"keyword": "foam roller", "has_google_trends": False, "amazon_bsr_count": 0},
                ]
            )

            with (
                mock.patch.object(keyword_main, "_load_monitor_profile", return_value=monitor_profile),
                mock.patch.object(keyword_main, "run_scrape", new=mock.AsyncMock(return_value=batch_summary)),
                mock.patch.object(keyword_main, "run_analyze", return_value=frame),
                mock.patch.object(keyword_main, "_sync_warehouse", side_effect=[{"status": "completed", "reason": "monitor_batch_1"}, {"status": "completed", "reason": "monitor"}]),
            ):
                summary = keyword_main.run_monitor(settings, args)

            self.assertEqual(summary["quality_state"], "full")
            self.assertEqual(summary["quality_summary"]["analysis_state"], "full")
            self.assertTrue(summary["quality_summary"]["signals"]["google_trends_available"])
            self.assertTrue(summary["quality_summary"]["signals"]["amazon_bsr_available"])

            conn = sqlite3.connect(settings.product_store_db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT metadata_json FROM keyword_runs WHERE run_type = 'monitor' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            metadata = json.loads(row["metadata_json"])
            self.assertEqual(metadata["quality_state"], "full")
            self.assertEqual(metadata["quality_summary"]["analysis_state"], "full")


class ReportAndWatchdogQualityTests(unittest.TestCase):
    def test_write_crawl_report_includes_keyword_quality_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            monitor_dir = temp_root / "runtime_data" / "keyword" / "monitor"
            monitor_dir.mkdir(parents=True, exist_ok=True)
            monitor_summary_path = monitor_dir / "keyword_monitor_last_run.json"
            monitor_summary_path.write_text(
                json.dumps(
                    {
                        "quality_state": "degraded",
                        "quality_summary": {
                            "state": "degraded",
                            "crawl_state": "degraded",
                            "analysis_state": "partial",
                            "platforms": {
                                "amazon": {
                                    "state": "degraded",
                                    "status": "failed",
                                    "evidence": ["beautifulsoup4_unavailable"],
                                }
                            },
                            "signals": {
                                "noon_success": True,
                                "amazon_success": False,
                                "amazon_bsr_available": False,
                                "google_trends_available": False,
                                "beautifulsoup4_unavailable": True,
                            },
                            "quality_flags": ["beautifulsoup4_unavailable"],
                            "quality_evidence": ["beautifulsoup4_unavailable"],
                            "analysis": {
                                "state": "partial",
                                "available": True,
                                "rows": 2,
                                "google_trends_available": False,
                                "amazon_bsr_available": False,
                                "quality_flags": ["google_trends_missing", "amazon_bsr_missing"],
                                "quality_evidence": ["google_trends_missing", "amazon_bsr_missing"],
                            },
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            previous_runtime_root = report.KEYWORD_RUNTIME_ROOT
            previous_summary_path = report.KEYWORD_MONITOR_SUMMARY_PATH
            previous_health = report.safe_request_json
            previous_snapshots = report.load_snapshots
            previous_select = report.select_snapshot_before
            previous_release = report.current_release_label
            try:
                report.KEYWORD_RUNTIME_ROOT = temp_root / "runtime_data" / "keyword"
                report.KEYWORD_MONITOR_SUMMARY_PATH = str(monitor_summary_path)
                def fake_safe_request_json(path: str):
                    if path == "/api/health":
                        return ({"status": "ok", "warehouse_db": "postgres", "product_count": 1, "observation_count": 2}, None)
                    if path == "/api/system/health":
                        return ({"status": "ok", "ops": {"worker_count": 0, "workers": [], "recent_runs": []}}, None)
                    if path == "/api/dashboard":
                        return ({"overview": {"product_count": 1, "keyword_count": 1, "overlap_count": 0, "last_sync_at": None}, "scope": {"child_categories": []}}, None)
                    if path == "/api/tasks?limit=500":
                        return ({"items": []}, None)
                    return ({}, None)
                report.safe_request_json = fake_safe_request_json
                report.load_snapshots = lambda *_args, **_kwargs: []
                report.select_snapshot_before = lambda *_args, **_kwargs: None
                report.current_release_label = lambda: "test-release"

                body, metadata = report.build_report()

                self.assertEqual(metadata["keyword_quality_summary"]["state"], "degraded")
                self.assertEqual(metadata["runtime_operator_quality_state"], "degraded")
                self.assertEqual(metadata["runtime_live_batch_state"], "idle")
                self.assertIn("watchdog_alert_summary", metadata)
                self.assertIn("Keyword Quality Summary", body)
                self.assertTrue(
                    any(
                        reason in body
                        for reason in ("runtime_import_error", "dependency_missing")
                    )
                )
            finally:
                report.KEYWORD_RUNTIME_ROOT = previous_runtime_root
                report.KEYWORD_MONITOR_SUMMARY_PATH = previous_summary_path
                report.safe_request_json = previous_health
                report.load_snapshots = previous_snapshots
                report.select_snapshot_before = previous_select
                report.current_release_label = previous_release

    def test_watchdog_emits_keyword_quality_and_sync_backlog_issues(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            monitor_dir = temp_root / "runtime_data" / "keyword" / "monitor"
            monitor_dir.mkdir(parents=True, exist_ok=True)
            (monitor_dir / "keyword_monitor_last_run.json").write_text(
                json.dumps(
                    {
                        "quality_state": "degraded",
                        "quality_summary": {
                            "state": "degraded",
                            "crawl_state": "degraded",
                            "analysis_state": "partial",
                            "signals": {
                                "noon_success": True,
                                "amazon_success": False,
                                "amazon_bsr_available": False,
                                "google_trends_available": False,
                                "beautifulsoup4_unavailable": True,
                            },
                            "quality_flags": ["beautifulsoup4_unavailable"],
                            "quality_evidence": ["beautifulsoup4_unavailable"],
                            "platforms": {},
                            "analysis": {
                                "state": "partial",
                                "available": True,
                                "rows": 1,
                                "google_trends_available": False,
                                "amazon_bsr_available": False,
                                "quality_flags": ["google_trends_missing", "amazon_bsr_missing"],
                                "quality_evidence": ["google_trends_missing", "amazon_bsr_missing"],
                            },
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            health_payload = {
                "status": "ok",
                "warehouse_db": "postgres",
                "product_count": 10,
                "observation_count": 20,
            }
            system_health = {
                "status": "ok",
                "ops": {
                    "ops_db": "postgres",
                    "task_status_counts": {"running": 1},
                    "worker_count": 3,
                    "workers": [
                        {
                            "worker_name": "keyword-worker",
                            "worker_type": "keyword",
                            "status": "running",
                            "current_task_id": 1,
                            "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                        }
                    ],
                    "recent_runs": [
                        {
                            "id": 1,
                            "task_id": 1,
                            "task_type": "warehouse_sync",
                            "worker_type": "sync",
                            "status": "completed",
                            "started_at": datetime.now(timezone.utc).isoformat(),
                            "finished_at": datetime.now(timezone.utc).isoformat(),
                            "result": {
                                "skip_reason": "lock_active",
                                "reason": "alternating slot",
                            },
                        }
                    ],
                },
                "shared_sync": {
                    "status": "running",
                    "skip_reason": "lock_active",
                    "reason": "alternating slot",
                },
            }

            previous_root = watchdog.REPORT_ROOT
            previous_state = watchdog.STATE_DIR
            previous_daily = watchdog.DAILY_DIR
            previous_latest_json = watchdog.LATEST_JSON
            previous_latest_md = watchdog.LATEST_MD
            previous_runtime_root = watchdog.KEYWORD_RUNTIME_ROOT
            previous_summary_path = watchdog.KEYWORD_MONITOR_SUMMARY_PATH
            previous_safe_http_json = watchdog.safe_http_json
            previous_systemctl_state = watchdog.systemctl_state
            previous_load_latest_report_stamp = watchdog.load_latest_report_stamp
            previous_report_file_dates = watchdog.report_file_dates
            previous_collect_tasks = watchdog.collect_tasks
            try:
                watchdog.REPORT_ROOT = temp_root / "shared" / "report" / "crawl"
                watchdog.STATE_DIR = watchdog.REPORT_ROOT / "state"
                watchdog.DAILY_DIR = watchdog.REPORT_ROOT / "daily"
                watchdog.LATEST_JSON = watchdog.STATE_DIR / "watchdog_latest.json"
                watchdog.LATEST_MD = watchdog.STATE_DIR / "watchdog_latest.md"
                watchdog.RUNS_FILE = watchdog.STATE_DIR / "watchdog_runs.jsonl"
                watchdog.KEYWORD_RUNTIME_ROOT = temp_root / "runtime_data" / "keyword"
                watchdog.KEYWORD_MONITOR_SUMMARY_PATH = str(monitor_dir / "keyword_monitor_last_run.json")

                def fake_safe_http_json(path: str, timeout: int = 0):
                    if path == "/api/health":
                        return health_payload, None
                    if path == "/api/system/health":
                        return system_health, None
                    return {}, None

                watchdog.safe_http_json = fake_safe_http_json
                watchdog.systemctl_state = lambda unit, **kwargs: {"unit": unit, "active": True, "state": "active", "substate": "running", "error": None}
                watchdog.load_latest_report_stamp = lambda: (datetime.now(timezone.utc) - timedelta(hours=2), "test-release")
                watchdog.report_file_dates = lambda: {datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).date().strftime("%Y%m%d"): ["crawl_report_test.md"]}
                watchdog.collect_tasks = lambda: []

                markdown, payload = watchdog.build_watchdog()

                issue_checks = {item["check"] for item in payload["issues"]}
                self.assertIn("keyword_quality", issue_checks)
                self.assertIn("shared_sync_backlog", issue_checks)
                self.assertEqual(payload["summary"]["status"], "warning")
                self.assertEqual(payload["checks"]["keyword_quality"]["state"], "degraded")
                self.assertEqual(payload["summary"]["current_alert_summary"]["history_vs_current"]["historical_report_gap"], True)
                self.assertEqual(payload["summary"]["current_alert_summary"]["history_vs_current"]["current_report_chain_problem"], False)
                self.assertFalse(payload["checks"]["sync_backlog"]["ok"])
                self.assertIn("Keyword Quality", markdown)
            finally:
                watchdog.REPORT_ROOT = previous_root
                watchdog.STATE_DIR = previous_state
                watchdog.DAILY_DIR = previous_daily
                watchdog.LATEST_JSON = previous_latest_json
                watchdog.LATEST_MD = previous_latest_md
                watchdog.KEYWORD_RUNTIME_ROOT = previous_runtime_root
                watchdog.KEYWORD_MONITOR_SUMMARY_PATH = previous_summary_path
                watchdog.safe_http_json = previous_safe_http_json
                watchdog.systemctl_state = previous_systemctl_state
                watchdog.load_latest_report_stamp = previous_load_latest_report_stamp
                watchdog.report_file_dates = previous_report_file_dates
                watchdog.collect_tasks = previous_collect_tasks

    def test_watchdog_ignores_backfilled_report_dates_when_files_exist(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            daily_dir = temp_root / "shared" / "report" / "crawl" / "daily"
            daily_dir.mkdir(parents=True, exist_ok=True)
            for day_key in ("20260407", "20260408", "20260409"):
                md_path = daily_dir / f"crawl_report_{day_key}_1015.md"
                json_path = daily_dir / f"crawl_report_{day_key}_1015.json"
                md_path.write_text(f"# backfill {day_key}\n", encoding="utf-8")
                json_path.write_text(
                    json.dumps({"report_date": f"{day_key[:4]}-{day_key[4:6]}-{day_key[6:]}", "backfilled": True}, ensure_ascii=False),
                    encoding="utf-8",
                )

            health_payload = {"status": "ok", "warehouse_db": "postgres", "product_count": 10, "observation_count": 20}
            system_health = {
                "status": "ok",
                "ops": {"worker_count": 3, "workers": [], "recent_runs": []},
                "shared_sync": {"status": "running", "skip_reason": "", "reason": "alternating slot"},
                "keyword_quality": {"operator_quality_state": "full", "latest_terminal_quality_state": "full", "live_batch_state": "running"},
            }

            previous_root = watchdog.REPORT_ROOT
            previous_state = watchdog.STATE_DIR
            previous_daily = watchdog.DAILY_DIR
            previous_latest_json = watchdog.LATEST_JSON
            previous_latest_md = watchdog.LATEST_MD
            previous_runtime_root = watchdog.KEYWORD_RUNTIME_ROOT
            previous_summary_path = watchdog.KEYWORD_MONITOR_SUMMARY_PATH
            previous_safe_http_json = watchdog.safe_http_json
            previous_systemctl_state = watchdog.systemctl_state
            previous_load_latest_report_stamp = watchdog.load_latest_report_stamp
            previous_report_file_dates = watchdog.report_file_dates
            previous_collect_tasks = watchdog.collect_tasks
            previous_shanghai_now = watchdog.shanghai_now
            try:
                watchdog.REPORT_ROOT = temp_root / "shared" / "report" / "crawl"
                watchdog.STATE_DIR = watchdog.REPORT_ROOT / "state"
                watchdog.DAILY_DIR = daily_dir
                watchdog.LATEST_JSON = watchdog.STATE_DIR / "watchdog_latest.json"
                watchdog.LATEST_MD = watchdog.STATE_DIR / "watchdog_latest.md"
                watchdog.RUNS_FILE = watchdog.STATE_DIR / "watchdog_runs.jsonl"
                watchdog.KEYWORD_RUNTIME_ROOT = temp_root / "runtime_data" / "keyword"
                watchdog.KEYWORD_MONITOR_SUMMARY_PATH = str(temp_root / "runtime_data" / "keyword" / "monitor" / "keyword_monitor_last_run.json")

                def fake_safe_http_json(path: str, timeout: int = 0):
                    if path == "/api/health":
                        return health_payload, None
                    if path == "/api/system/health":
                        return system_health, None
                    return {}, None

                fixed_now = datetime(2026, 4, 11, 9, 0, tzinfo=timezone(timedelta(hours=8)))
                watchdog.safe_http_json = fake_safe_http_json
                watchdog.systemctl_state = lambda unit, **kwargs: {"unit": unit, "active": True, "state": "active", "substate": "running", "error": None}
                watchdog.load_latest_report_stamp = lambda: (fixed_now.astimezone(timezone.utc) - timedelta(hours=2), "test-release")
                watchdog.report_file_dates = previous_report_file_dates
                watchdog.collect_tasks = lambda: []
                watchdog.shanghai_now = lambda: fixed_now

                markdown, payload = watchdog.build_watchdog()
                missing_dates = payload["summary"]["report_missing_dates"]
                self.assertNotIn("20260407", missing_dates)
                self.assertNotIn("20260408", missing_dates)
                self.assertNotIn("20260409", missing_dates)
                self.assertIn("20260410", missing_dates)
                self.assertIn("20260407", payload["checks"]["report_freshness"]["available_dates"])
                self.assertIn("20260408", payload["checks"]["report_freshness"]["available_dates"])
                self.assertIn("20260409", payload["checks"]["report_freshness"]["available_dates"])
                self.assertIn("Keyword Quality", markdown)
            finally:
                watchdog.REPORT_ROOT = previous_root
                watchdog.STATE_DIR = previous_state
                watchdog.DAILY_DIR = previous_daily
                watchdog.LATEST_JSON = previous_latest_json
                watchdog.LATEST_MD = previous_latest_md
                watchdog.KEYWORD_RUNTIME_ROOT = previous_runtime_root
                watchdog.KEYWORD_MONITOR_SUMMARY_PATH = previous_summary_path
                watchdog.safe_http_json = previous_safe_http_json
                watchdog.systemctl_state = previous_systemctl_state
                watchdog.load_latest_report_stamp = previous_load_latest_report_stamp
                watchdog.report_file_dates = previous_report_file_dates
                watchdog.collect_tasks = previous_collect_tasks
                watchdog.shanghai_now = previous_shanghai_now


class KeywordQualityRootCauseTests(unittest.TestCase):
    def test_batch_truth_prefers_degraded_crawl_over_full_register(self):
        recent_runs = [
            {
                "run_type": "register",
                "trigger_mode": "tracked",
                "seed_keyword": "sports bag",
                "snapshot_id": "snap-batch-1",
                "status": "completed",
                "keyword_count": 8,
                "started_at": "2026-04-11T10:10:00",
                "finished_at": "2026-04-11T10:11:00",
                "metadata_json": json.dumps(
                    {
                        "quality_state": "full",
                        "quality_summary": {
                            "state": "full",
                            "quality_evidence": ["all_platforms_ok"],
                            "quality_source_breakdown": {
                                "register": {"state": "full", "reason_codes": []}
                            },
                        },
                    }
                ),
            },
            {
                "run_type": "crawl",
                "trigger_mode": "tracked",
                "seed_keyword": "sports bag",
                "snapshot_id": "snap-batch-1",
                "status": "partial",
                "keyword_count": 8,
                "started_at": "2026-04-11T10:00:00",
                "finished_at": "2026-04-11T10:09:00",
                "metadata_json": json.dumps(
                    {
                        "quality_state": "degraded",
                        "quality_summary": {
                            "state": "degraded",
                            "quality_evidence": ["missing_result_file:amazon.json"],
                            "quality_source_breakdown": {
                                "amazon": {
                                    "state": "degraded",
                                    "reason_codes": ["result_contract_mismatch"],
                                    "primary_reason": "result_contract_mismatch",
                                },
                                "crawl": {
                                    "state": "degraded",
                                    "reason_codes": ["result_contract_mismatch"],
                                    "primary_reason": "result_contract_mismatch",
                                },
                            },
                        },
                    }
                ),
            },
        ]

        summary = summarize_recent_keyword_runs(recent_runs)

        self.assertEqual(summary["latest_terminal_batch_state"], "degraded")
        self.assertEqual(summary["operator_quality_state"], "degraded")
        self.assertEqual(summary["truth_source"], "keyword_snapshot_batch_aggregate")
        self.assertEqual(summary["active_snapshot_count"], 0)
        self.assertEqual(summary["stale_running_snapshot_count"], 0)
        self.assertIn("result_contract_mismatch", summary["batch_quality_reasons"])

    def test_quality_summary_maps_missing_result_file_to_result_contract_mismatch(self):
        payload = keyword_main._summarize_keyword_quality(
            {
                "platforms": ["amazon"],
                "platform_stats": {
                    "amazon": {
                        "status": "failed",
                        "products_count": 0,
                        "total_results": 0,
                        "error": "",
                        "error_evidence": ["missing_result_file:test.json"],
                        "zero_result_evidence": [],
                        "failure_details": [],
                    }
                },
            }
        )

        self.assertEqual(payload["state"], "degraded")
        amazon_breakdown = payload["quality_source_breakdown"]["amazon"]
        self.assertIn("result_contract_mismatch", amazon_breakdown["reason_codes"])
        self.assertEqual(amazon_breakdown["primary_reason"], "result_contract_mismatch")

    def test_quality_summary_maps_timeout_to_amazon_upstream_blocked(self):
        payload = keyword_main._summarize_keyword_quality(
            {
                "platforms": ["amazon"],
                "platform_stats": {
                    "amazon": {
                        "status": "failed",
                        "products_count": 0,
                        "total_results": 0,
                        "error": "page.goto: Timeout 30000ms exceeded",
                        "error_evidence": ["page.goto: Timeout 30000ms exceeded"],
                        "zero_result_evidence": [],
                        "failure_details": [],
                    }
                },
            }
        )

        self.assertEqual(payload["state"], "degraded")
        amazon_breakdown = payload["quality_source_breakdown"]["amazon"]
        self.assertIn("amazon_upstream_blocked", amazon_breakdown["reason_codes"])

    def test_quality_summary_maps_noon_timeout_to_timeout(self):
        payload = keyword_main._summarize_keyword_quality(
            {
                "platforms": ["noon"],
                "platform_stats": {
                    "noon": {
                        "status": "failed",
                        "products_count": 0,
                        "total_results": 0,
                        "error": "page.goto: Timeout 45000ms exceeded",
                        "error_evidence": ["timeout:TimeoutError", "page.goto: Timeout 45000ms exceeded"],
                        "zero_result_evidence": [],
                        "failure_details": [],
                    }
                },
            }
        )

        self.assertEqual(payload["state"], "degraded")
        noon_breakdown = payload["quality_source_breakdown"]["noon"]
        self.assertIn("timeout", noon_breakdown["reason_codes"])
        self.assertEqual(noon_breakdown["primary_reason"], "timeout")

    @unittest.skipIf(pd is None, "pandas unavailable")
    def test_quality_summary_treats_optional_analysis_signals_as_advisory_when_rows_exist(self):
        frame = pd.DataFrame(
            [
                {"keyword": "rope", "has_google_trends": False, "amazon_bsr_count": 0},
                {"keyword": "bag", "has_google_trends": False, "amazon_bsr_count": 0},
            ]
        )

        payload = keyword_main._summarize_keyword_quality(
            {
                "platforms": ["noon"],
                "platform_stats": {
                    "noon": {
                        "status": "completed",
                        "products_count": 20,
                        "total_results": 20,
                        "error": "",
                        "error_evidence": [],
                        "zero_result_evidence": [],
                        "failure_details": [],
                    }
                },
            },
            analysis_frame=frame,
        )

        self.assertEqual(payload["state"], "full")
        self.assertEqual(payload["analysis_state"], "full")
        self.assertIn("google_trends_missing", payload["quality_flags"])
        self.assertIn("amazon_bsr_missing", payload["quality_flags"])
        self.assertNotIn("analysis:google_trends_missing", payload["quality_reasons"])
        self.assertNotIn("analysis:amazon_bsr_missing", payload["quality_reasons"])

    def test_task_scheduler_maps_duplicate_monitor_to_skipped(self):
        task = {"id": 99, "worker_type": "keyword", "task_type": "keyword_monitor", "payload": {}}

        class FakeProcess:
            def poll(self):
                return 1

            def wait(self):
                return 1

        with (
            mock.patch.object(run_task_scheduler, "build_task_command", return_value=["python", "run_keyword_monitor.py"]),
            mock.patch.object(run_task_scheduler.subprocess, "Popen", return_value=FakeProcess()),
            mock.patch.object(run_task_scheduler, "_tail_lines", side_effect=[[], ["keyword monitor is already running: pid=123, snapshot=s1"]]),
        ):
            final_status, result, error_text = run_task_scheduler.execute_task(
                task,
                worker_name="keyword-worker",
                poll_seconds=10,
                lease_timeout_seconds=3600,
            )

        self.assertEqual(final_status, "skipped")
        self.assertEqual(error_text, "active_monitor")
        self.assertEqual(result["skip_reason"], "active_monitor")


if __name__ == "__main__":
    unittest.main()
