from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import keyword_main
from config.product_store import ProductStore
from config.settings import Settings
from ops import keyword_control_state
from scrapers.base_scraper import BaseScraper


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


class PlatformSnapshotTests(unittest.TestCase):
    def test_summarize_platform_snapshot_reports_zero_results(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            write_result(
                settings,
                "amazon",
                "rare shoes",
                {
                    "keyword": "rare shoes",
                    "products": [],
                    "page_state": "zero_results",
                    "zero_result_evidence": ["no results for"],
                    "error_evidence": [],
                    "total_results": 0,
                },
            )

            summary = keyword_main._summarize_platform_snapshot(settings, "amazon", ["rare shoes"])

            self.assertEqual(summary["status"], "zero_results")
            self.assertEqual(summary["products_count"], 0)
            self.assertEqual(summary["zero_result_keywords"], ["rare shoes"])
            self.assertIn("no results for", summary["zero_result_evidence"])

    def test_load_platform_result_payloads_rejects_incomplete_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            path = keyword_main._result_file_path(settings, "amazon", "broken shoes")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "keyword": "broken shoes",
                        "page_state": "results",
                        "total_results": 10,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            records = keyword_main._load_platform_result_payloads(settings, "amazon", ["broken shoes"])

            self.assertEqual(len(records), 1)
            self.assertIsNone(records[0]["payload"])
            self.assertIn("incomplete_json", records[0]["load_error"])

    def test_classify_platform_payload_marks_partial_with_structured_failure_details(self):
        payload = {
            "keyword": "spin bike",
            "products": [{"product_id": "P1", "title": "Bike"}],
            "page_state": "partial_results",
            "page_url": "https://www.noon.com/search/?q=spin+bike&page=2",
            "error_evidence": [
                "partial_results:no_more_cards_page_2",
                "selector_miss:plp-product-box-name",
            ],
        }

        summary = keyword_main._classify_platform_payload("noon", "spin bike", payload)

        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["failure_details"][0]["failure_category"], "page_recognition_failed")
        self.assertIn("selector_miss:plp-product-box-name", summary["error_evidence"])

    def test_load_platform_result_payloads_normalize_contract_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            write_result(
                settings,
                "amazon",
                "foam roller",
                {
                    "keyword": "foam roller",
                    "products": [{"product_id": "ASIN1", "title": "Foam Roller"}],
                    "page_state": "results",
                    "total_results": "1",
                },
            )

            records = keyword_main._load_platform_result_payloads(settings, "amazon", ["foam roller"])

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["load_error"], "")
            payload = records[0]["payload"]
            self.assertEqual(payload["suggested_keywords"], [])
            self.assertEqual(payload["error_evidence"], [])
            self.assertEqual(payload["zero_result_evidence"], [])
            self.assertEqual(payload["failure_details"], [])


class BaseScraperFailurePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_scrape_with_retry_persists_failure_payload_after_final_retry(self):
        class DummyFailingAmazonScraper(BaseScraper):
            async def scrape_keyword(self, keyword: str) -> dict:
                raise TimeoutError("page.goto: Timeout 30000ms exceeded")

        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="failure_snapshot")
            settings.max_retries = 1
            scraper = DummyFailingAmazonScraper("amazon", settings)

            result = await scraper.scrape_with_retry("yoga mat")

            self.assertIsNone(result)
            payload = scraper.load_result("yoga mat")
            self.assertIsInstance(payload, dict)
            self.assertEqual(payload["page_state"], "error")
            self.assertEqual(payload["failure_details"][0]["failure_category"], "timeout")
            self.assertIn("expected_result_file:", payload["error_evidence"][1])


class KeywordControlRuntimeTests(unittest.TestCase):
    def test_get_monitor_active_keywords_filters_disabled_keywords_and_blocked_roots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            config_path = temp_root / "keyword_monitor_test.json"
            baseline_path = temp_root / "baseline.txt"
            baseline_path.write_text("dog toys\nadidas running belt\nyoga mat\n", encoding="utf-8")
            config_path.write_text(
                json.dumps({"baseline_file": str(baseline_path)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            sidecar_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"
            product_store_path = temp_root / "runtime_data" / "keyword" / "product_store.db"
            product_store_path.parent.mkdir(parents=True, exist_ok=True)
            prev_product_store = os.environ.get("NOON_PRODUCT_STORE_DB")
            os.environ["NOON_PRODUCT_STORE_DB"] = str(product_store_path)
            try:
                with mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", sidecar_dir):
                    sidecar_dir.mkdir(parents=True, exist_ok=True)
                    keyword_control_state.update_monitor_disabled_keyword_rules(
                        str(config_path),
                        keywords=["dog toys"],
                        blocked_sources=["baseline", "generated", "tracked", "manual"],
                        reason="brand cleanup",
                        mode="disable",
                    )
                    keyword_control_state.update_monitor_blocked_root_rules(
                        str(config_path),
                        root_keywords=["adidas"],
                        blocked_sources=["baseline", "generated", "tracked", "manual"],
                        reason="brand root",
                        match_mode="contains",
                        mode="upsert",
                    )
                    payload = keyword_control_state.get_monitor_active_keywords(str(config_path))
            finally:
                if prev_product_store is None:
                    os.environ.pop("NOON_PRODUCT_STORE_DB", None)
                else:
                    os.environ["NOON_PRODUCT_STORE_DB"] = prev_product_store

            active_keywords = [item["keyword"] for item in payload["items"]]
            blocked_keywords = [item["keyword"] for item in payload["blocked_keywords"]["baseline"]]
            self.assertEqual(active_keywords, ["yoga mat"])
            self.assertIn("dog toys", blocked_keywords)
            self.assertIn("adidas running belt", blocked_keywords)


class PrepareSettingsTests(unittest.TestCase):
    def test_prepare_settings_creates_snapshot_directory_when_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "runtime_data" / "keyword"
            parser = keyword_main.build_parser()
            args = parser.parse_args(
                [
                    "--data-root",
                    str(data_root),
                    "--snapshot",
                    "keyword_monitor_plan_4_round_2",
                ]
            )

            settings = keyword_main.prepare_settings(args)

            self.assertEqual(settings.snapshot_id, "keyword_monitor_plan_4_round_2")
            self.assertTrue(settings.snapshot_dir.exists())
            for subdir in keyword_main.KEYWORD_SNAPSHOT_SUBDIRS:
                self.assertTrue((settings.snapshot_dir / subdir).exists())


class KeywordResumeTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_full_pipeline_resume_falls_back_to_pool_when_snapshot_keywords_are_corrupt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="resume_snapshot")
            keyword_main._keyword_pool_path(settings).write_text(
                json.dumps(["trail shoes"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            keyword_main._keyword_snapshot_path(settings).write_text('{"broken":', encoding="utf-8")

            args = argparse.Namespace(resume=True)

            async def fake_run_scrape(_settings, _args, keywords=None, **_kwargs):
                return {"status": "completed", "keywords": list(keywords or [])}

            with (
                mock.patch.object(keyword_main, "run_keywords") as run_keywords_mock,
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape) as run_scrape_mock,
                mock.patch.object(keyword_main, "run_analyze", return_value=None),
                mock.patch.object(keyword_main, "_sync_warehouse", return_value={"status": "completed"}),
                mock.patch.object(keyword_main.keyword_core, "_save_config_snapshot", return_value=None, create=True),
            ):
                await keyword_main.run_full_pipeline(settings, args)

            run_keywords_mock.assert_not_called()
            self.assertEqual(run_scrape_mock.await_args.kwargs["keywords"], ["trail shoes"])
            persisted_keywords = json.loads(keyword_main._keyword_snapshot_path(settings).read_text(encoding="utf-8"))
            self.assertEqual(persisted_keywords, ["trail shoes"])

    def test_read_keyword_pool_returns_empty_for_corrupt_json(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            pool_path = keyword_main._keyword_pool_path(settings)
            pool_path.write_text('["ok", ', encoding="utf-8")

            self.assertEqual(keyword_main._read_keyword_pool(settings), [])


class RunScrapeRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_scrape_retries_amazon_once_after_failed_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="retry_snapshot")
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
            attempts: list[int] = []

            async def fake_step_scrape(
                settings_obj,
                keywords,
                *,
                platforms=None,
                noon_count=None,
                amazon_count=None,
            ):
                attempts.append(len(attempts) + 1)
                if len(attempts) == 2:
                    write_result(
                        settings_obj,
                        "amazon",
                        keywords[0],
                        {
                            "keyword": keywords[0],
                            "products": [
                                {
                                    "product_id": "ASIN123456",
                                    "title": "Test Shoes",
                                    "product_url": "https://www.amazon.sa/dp/ASIN123456",
                                    "price": 100.0,
                                    "rating": 4.5,
                                    "review_count": 10,
                                    "search_rank": 1,
                                    "is_ad": False,
                                    "delivery_type": "prime",
                                    "badge_texts": ["Prime"],
                                }
                            ],
                            "page_state": "results",
                            "zero_result_evidence": [],
                            "error_evidence": [],
                            "total_results": 1,
                            "suggested_keywords": [],
                        },
                    )

            async def fake_sleep(_seconds: float):
                return None

            with (
                mock.patch.object(keyword_main.keyword_core, "step_scrape", side_effect=fake_step_scrape),
                mock.patch.object(
                    keyword_main.keyword_core,
                    "_persist_keyword_results",
                    return_value={"amazon": 3},
                ),
                mock.patch.object(
                    keyword_main,
                    "_capture_live_suggestion_edges",
                    return_value={
                        "platform_files": 1,
                        "parent_keywords": 0,
                        "discovered_keywords": 0,
                        "recorded_edges": 0,
                        "rejected_keywords": 0,
                    },
                ),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    return_value={"status": "completed", "reason": "crawl"},
                ),
                mock.patch.object(keyword_main.asyncio, "sleep", side_effect=fake_sleep),
            ):
                summary = await keyword_main.run_scrape(settings, args, sync_warehouse=True)

            self.assertEqual(summary["status"], "completed")
            self.assertEqual(attempts, [1, 2])
            self.assertEqual(summary["persist_counts"]["amazon"], 3)
            self.assertEqual(summary["success_platforms"], ["amazon"])

            amazon_stats = summary["platform_stats"]["amazon"]
            self.assertEqual(amazon_stats["attempts"], 2)
            self.assertEqual(amazon_stats["retry_count"], 1)
            self.assertEqual(amazon_stats["status"], "completed")
            self.assertEqual(amazon_stats["attempt_history"][0]["status"], "failed")
            self.assertEqual(amazon_stats["attempt_history"][1]["status"], "completed")

            conn = sqlite3.connect(settings.product_store_db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT metadata_json FROM keyword_runs WHERE run_type = 'crawl' ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
            metadata = json.loads(row["metadata_json"])
            self.assertEqual(metadata["platform_stats"]["amazon"]["attempts"], 2)
            self.assertEqual(metadata["persist_counts"]["amazon"], 3)

    async def test_run_scrape_marks_partial_when_sync_returns_failed_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="sync_failed_snapshot")
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
                        "products": [
                            {
                                "product_id": "ASIN123456",
                                "title": "Test Shoes",
                                "product_url": "https://www.amazon.sa/dp/ASIN123456",
                                "price": 100.0,
                                "rating": 4.5,
                                "review_count": 10,
                                "search_rank": 1,
                                "is_ad": False,
                                "delivery_type": "prime",
                                "badge_texts": ["Prime"],
                            }
                        ],
                        "page_state": "results",
                        "zero_result_evidence": [],
                        "error_evidence": [],
                        "total_results": 1,
                        "suggested_keywords": [],
                    },
                )

            with (
                mock.patch.object(keyword_main.keyword_core, "step_scrape", side_effect=fake_step_scrape),
                mock.patch.object(
                    keyword_main.keyword_core,
                    "_persist_keyword_results",
                    return_value={"amazon": 3},
                ),
                mock.patch.object(
                    keyword_main,
                    "_capture_live_suggestion_edges",
                    return_value={
                        "platform_files": 1,
                        "parent_keywords": 0,
                        "discovered_keywords": 0,
                        "recorded_edges": 0,
                        "rejected_keywords": 0,
                    },
                ),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    return_value={"status": "failed", "reason": "crawl", "error": "lock timeout"},
                ),
            ):
                summary = await keyword_main.run_scrape(settings, args, sync_warehouse=True)

            self.assertEqual(summary["status"], "partial")
            self.assertEqual(summary["warehouse_sync"]["status"], "failed")
            self.assertTrue(summary["errors"])


class WarehouseSyncAdoptionTests(unittest.TestCase):
    def test_sync_warehouse_skips_when_shared_lock_is_active(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="sync_skip_snapshot")
            completed = mock.Mock(
                returncode=0,
                stdout='WAREHOUSE_SYNC_RESULT={"status":"skipped","skip_reason":"lock_active","sync_state":{"status":"skipped"}}\n',
                stderr="",
            )
            with (
                mock.patch.object(keyword_main, "_official_keyword_db_path", return_value=settings.product_store_db_path),
                mock.patch.object(keyword_main.subprocess, "run", return_value=completed) as run_mock,
            ):
                payload = keyword_main._sync_warehouse(settings, reason="crawl", wait_for_lock=False)

            self.assertEqual(payload["status"], "skipped")
            self.assertEqual(payload["skip_reason"], "lock_active")
            self.assertEqual(payload["keyword_db"], str(settings.product_store_db_path.resolve()))
            run_mock.assert_called_once()

    def test_sync_warehouse_finalizes_completed_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="sync_complete_snapshot")
            completed = mock.Mock(
                returncode=0,
                stdout='line1\nWAREHOUSE_SYNC_RESULT={"status":"completed","reason":"analyze","sync_state":{"status":"completed"}}\n',
                stderr="",
            )
            with (
                mock.patch.object(keyword_main, "_official_keyword_db_path", return_value=settings.product_store_db_path),
                mock.patch.object(keyword_main.subprocess, "run", return_value=completed) as run_mock,
            ):
                payload = keyword_main._sync_warehouse(settings, reason="analyze")

            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["sync_state"]["status"], "completed")
            run_mock.assert_called_once()


class MonitorSummaryTests(unittest.TestCase):
    def test_run_monitor_applies_keyword_control_exclusions_to_baseline_and_crawl(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            settings = build_settings(temp_root, snapshot_id="monitor_keyword_control_snapshot")
            baseline_file = temp_root / "baseline.txt"
            baseline_file.write_text("dog toys\ncat bed\n", encoding="utf-8")
            monitor_config = temp_root / "keyword_monitor_pet_sports.json"
            monitor_config.write_text(
                json.dumps({"baseline_file": str(baseline_file)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            keyword_control_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"

            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keyword(
                    "dog toys",
                    tracking_mode="tracked",
                    source_type="baseline",
                    priority=10,
                )
                store.upsert_keyword(
                    "cat bed",
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                )
            finally:
                store.close()

            seen_register_keywords: list[str] = []
            seen_batches: list[list[str]] = []

            async def fake_run_scrape(_settings, _args, keywords=None, **_kwargs):
                seen_batches.append(list(keywords or []))
                return {
                    "status": "completed",
                    "persisted_product_count": len(list(keywords or [])),
                    "persist_counts": {"noon": len(list(keywords or []))},
                    "platform_stats": {
                        "noon": {
                            "status": "completed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": len(list(keywords or [])),
                            "products_count": len(list(keywords or [])),
                            "persisted_count": len(list(keywords or [])),
                            "result_files": ["seed.json"],
                            "total_results": len(list(keywords or [])),
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
                        }
                    },
                    "errors": [],
                }

            def fake_run_register(_settings, register_args, sync_warehouse=False):
                seen_register_keywords.extend(
                    [item for item in str(register_args.keyword or "").split(",") if item]
                )
                return {"registered_count": len(seen_register_keywords)}

            with (
                mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", keyword_control_dir),
                mock.patch.object(
                    keyword_main,
                    "_load_monitor_profile",
                    return_value={
                        "monitor_config": str(monitor_config),
                        "baseline_file": str(baseline_file),
                        "tracked_priority": 30,
                        "expand_limit": 0,
                        "expand_stale_hours": 72,
                        "expand_source_types": ["manual", "baseline"],
                        "expand_platforms": ["noon", "amazon"],
                        "crawl_platforms": ["noon", "amazon"],
                        "crawl_stale_hours": 24,
                        "crawl_limit": 10,
                        "crawl_batch_size": 10,
                        "crawl_sync_interval_seconds": 0,
                        "monitor_report": False,
                    },
                ),
                mock.patch.object(keyword_main, "run_register", side_effect=fake_run_register),
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape),
                mock.patch.object(keyword_main, "run_analyze", return_value=[]),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    side_effect=[{"status": "completed", "reason": "monitor"}],
                ),
            ):
                keyword_control_dir.mkdir(parents=True, exist_ok=True)
                keyword_control_state.update_monitor_exclusion_rule(
                    str(monitor_config),
                    keyword="dog toys",
                    blocked_sources=["baseline"],
                    reason="seed cleanup",
                    mode="upsert",
                )
                summary = keyword_main.run_monitor(
                    settings,
                    argparse.Namespace(noon_count=5, amazon_count=5),
                )

            self.assertEqual(seen_register_keywords, ["cat bed"])
            self.assertEqual(seen_batches, [["cat bed"]])
            self.assertEqual(summary["baseline_excluded_count"], 1)
            self.assertEqual(summary["baseline_excluded_keywords"], ["dog toys"])
            self.assertEqual(summary["crawled_keyword_excluded_count"], 1)
            self.assertEqual(summary["crawled_keyword_excluded_keywords"], ["dog toys"])

    def test_run_monitor_applies_blocked_root_rules_to_baseline_expand_and_crawl(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            settings = build_settings(temp_root, snapshot_id="monitor_blocked_root_snapshot")
            baseline_file = temp_root / "baseline.txt"
            baseline_file.write_text("dog toys\ncat bed\n", encoding="utf-8")
            monitor_config = temp_root / "keyword_monitor_pet_sports.json"
            monitor_config.write_text(
                json.dumps({"baseline_file": str(baseline_file)}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            keyword_control_dir = temp_root / "runtime_data" / "crawler_control" / "keyword_controls"

            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keyword(
                    "dog toys",
                    tracking_mode="tracked",
                    source_type="baseline",
                    priority=10,
                    metadata={"registration_source": "baseline", "root_seed_keyword": "dog toys"},
                )
                store.upsert_keyword(
                    "dog shampoo",
                    tracking_mode="tracked",
                    source_type="generated",
                    priority=15,
                    metadata={
                        "registration_source": "generated",
                        "root_seed_keyword": "dog toys",
                        "seed_keyword": "dog toys",
                    },
                )
                store.upsert_keyword(
                    "cat bed",
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=20,
                    metadata={"registration_source": "manual", "root_seed_keyword": "cat bed"},
                )
            finally:
                store.close()

            seen_register_keywords: list[str] = []
            seen_expand_seeds: list[str] = []
            seen_batches: list[list[str]] = []

            async def fake_run_scrape(_settings, _args, keywords=None, **_kwargs):
                seen_batches.append(list(keywords or []))
                return {
                    "status": "completed",
                    "persisted_product_count": len(list(keywords or [])),
                    "persist_counts": {"noon": len(list(keywords or []))},
                    "platform_stats": {
                        "noon": {
                            "status": "completed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": len(list(keywords or [])),
                            "products_count": len(list(keywords or [])),
                            "persisted_count": len(list(keywords or [])),
                            "result_files": ["seed.json"],
                            "total_results": len(list(keywords or [])),
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
                        }
                    },
                    "errors": [],
                }

            def fake_run_register(_settings, register_args, sync_warehouse=False):
                seen_register_keywords.extend(
                    [item for item in str(register_args.keyword or "").split(",") if item]
                )
                return {"registered_count": len(seen_register_keywords)}

            def fake_expand_keyword_into_db(_settings, *, seed_keyword, **_kwargs):
                seen_expand_seeds.append(str(seed_keyword))
                return {"all": [], "rejected_count": 0, "recorded_edges": 0}

            with (
                mock.patch.object(keyword_control_state, "KEYWORD_CONTROL_DIR", keyword_control_dir),
                mock.patch.object(
                    keyword_main,
                    "_load_monitor_profile",
                    return_value={
                        "monitor_config": str(monitor_config),
                        "baseline_file": str(baseline_file),
                        "tracked_priority": 30,
                        "expand_limit": 10,
                        "expand_stale_hours": 72,
                        "expand_source_types": ["generated", "manual"],
                        "expand_platforms": ["noon", "amazon"],
                        "crawl_platforms": ["noon", "amazon"],
                        "crawl_stale_hours": 24,
                        "crawl_limit": 10,
                        "crawl_batch_size": 10,
                        "crawl_sync_interval_seconds": 0,
                        "monitor_report": False,
                    },
                ),
                mock.patch.object(keyword_main, "run_register", side_effect=fake_run_register),
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape),
                mock.patch.object(keyword_main, "run_analyze", return_value=[]),
                mock.patch.object(keyword_main, "expand_keyword_into_db", side_effect=fake_expand_keyword_into_db),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    side_effect=[{"status": "completed", "reason": "monitor"}],
                ),
            ):
                keyword_control_dir.mkdir(parents=True, exist_ok=True)
                keyword_control_state.update_monitor_blocked_root_rules(
                    str(monitor_config),
                    root_keywords=["dog toys"],
                    blocked_sources=["baseline", "generated"],
                    reason="root cleanup",
                    match_mode="exact",
                    mode="upsert",
                )
                summary = keyword_main.run_monitor(
                    settings,
                    argparse.Namespace(noon_count=5, amazon_count=5),
                )

            self.assertEqual(seen_register_keywords, ["cat bed"])
            self.assertEqual(seen_expand_seeds, ["cat bed"])
            self.assertEqual(seen_batches, [["cat bed"]])
            self.assertEqual(summary["baseline_excluded_count"], 1)
            self.assertEqual(summary["baseline_excluded_keywords"], ["dog toys"])
            self.assertEqual(summary["expanded_seed_excluded_count"], 1)
            self.assertEqual(summary["expanded_seed_excluded_keywords"], ["dog shampoo"])
            self.assertEqual(summary["crawled_keyword_excluded_count"], 2)
            self.assertEqual(summary["crawled_keyword_excluded_keywords"], ["dog toys", "dog shampoo"])

    def test_run_monitor_filters_batches_by_monitor_seed_keyword(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="monitor_seed_snapshot")
            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keyword(
                    "dog toys",
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                    metadata={"root_seed_keyword": "dog toys"},
                )
                store.upsert_keyword(
                    "dog shampoo",
                    tracking_mode="tracked",
                    source_type="expanded",
                    priority=10,
                    metadata={"root_seed_keyword": "dog toys", "seed_keyword": "dog toys"},
                )
                store.upsert_keyword(
                    "cat bed",
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                    metadata={"root_seed_keyword": "cat bed"},
                )
            finally:
                store.close()

            args = argparse.Namespace(noon_count=5, amazon_count=5, monitor_seed_keyword="dog toys")
            seen_batches: list[list[str]] = []

            async def fake_run_scrape(_settings, _args, keywords=None, **_kwargs):
                batch_keywords = list(keywords or [])
                seen_batches.append(batch_keywords)
                return {
                    "status": "completed",
                    "persisted_product_count": len(batch_keywords),
                    "persist_counts": {"noon": len(batch_keywords)},
                    "platform_stats": {
                        "noon": {
                            "status": "completed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": len(batch_keywords),
                            "products_count": len(batch_keywords),
                            "persisted_count": len(batch_keywords),
                            "result_files": ["seed.json"],
                            "total_results": len(batch_keywords),
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
                        }
                    },
                    "errors": [],
                }

            with (
                mock.patch.object(
                    keyword_main,
                    "_load_monitor_profile",
                    return_value={
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
                    },
                ),
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape),
                mock.patch.object(keyword_main, "run_analyze", return_value=[]),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    side_effect=[{"status": "completed", "reason": "monitor_batch_1"}, {"status": "completed", "reason": "monitor"}],
                ),
            ):
                summary = keyword_main.run_monitor(settings, args)

            self.assertEqual(seen_batches, [["dog toys", "dog shampoo"]])
            self.assertEqual(summary["monitor_seed_keyword"], "dog toys")
            self.assertEqual(summary["crawled_keyword_count"], 2)
            self.assertEqual(summary["processed_keyword_count"], 2)
            self.assertEqual(summary["persisted_product_count"], 2)

    def test_run_monitor_batches_crawl_and_accumulates_persisted_products(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="monitor_batch_snapshot")
            keywords = ["dog toys", "dog shampoo", "cat bed"]
            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keywords(
                    keywords,
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                )
            finally:
                store.close()

            args = argparse.Namespace(noon_count=5, amazon_count=5)
            seen_batches: list[list[str]] = []

            async def fake_run_scrape(_settings, _args, keywords=None, **_kwargs):
                batch_keywords = list(keywords or [])
                seen_batches.append(batch_keywords)
                if len(seen_batches) == 1:
                    return {
                        "status": "completed",
                        "persisted_product_count": 3,
                        "persist_counts": {"noon": 3},
                        "platform_stats": {
                            "noon": {
                                "status": "completed",
                                "attempts": 1,
                                "retry_count": 0,
                                "keyword_count": len(batch_keywords),
                                "products_count": 3,
                                "persisted_count": 3,
                                "result_files": ["batch1_noon.json"],
                                "total_results": 3,
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
                            }
                        },
                        "errors": [],
                    }
                return {
                    "status": "partial",
                    "persisted_product_count": 2,
                    "persist_counts": {"noon": 2, "amazon": 0},
                    "platform_stats": {
                        "noon": {
                            "status": "completed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": len(batch_keywords),
                            "products_count": 2,
                            "persisted_count": 2,
                            "result_files": ["batch2_noon.json"],
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
                        },
                        "amazon": {
                            "status": "failed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": len(batch_keywords),
                            "products_count": 0,
                            "persisted_count": 0,
                            "result_files": [],
                            "total_results": 0,
                            "error": "amazon:timeout",
                            "zero_result_evidence": [],
                            "failed_keywords": batch_keywords,
                            "zero_result_keywords": [],
                            "attempt_history": [{"attempt": 1, "status": "failed"}],
                            "live_suggestion_files": 0,
                            "live_suggestion_parents": 0,
                            "live_suggestion_keywords": 0,
                            "live_suggestion_edges": 0,
                            "live_suggestion_rejected": 0,
                        },
                    },
                    "errors": [{"stage": "crawl_platform", "error": "amazon:timeout", "platform": "amazon"}],
                }

            with (
                mock.patch.object(
                    keyword_main,
                    "_load_monitor_profile",
                    return_value={
                        "baseline_file": None,
                        "tracked_priority": 30,
                        "expand_limit": 0,
                        "expand_stale_hours": 72,
                        "expand_source_types": ["manual"],
                        "expand_platforms": ["noon", "amazon"],
                        "crawl_platforms": ["noon", "amazon"],
                        "crawl_stale_hours": 24,
                        "crawl_limit": 3,
                        "crawl_batch_size": 2,
                        "crawl_sync_interval_seconds": 0,
                        "monitor_report": False,
                    },
                ),
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape),
                mock.patch.object(keyword_main, "run_analyze", return_value=[]),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    side_effect=[
                        {"status": "completed", "reason": "monitor_batch_1"},
                        {"status": "completed", "reason": "monitor"},
                    ],
                ) as sync_mock,
            ):
                summary = keyword_main.run_monitor(settings, args)

            self.assertEqual(seen_batches, [["dog toys", "dog shampoo"], ["cat bed"]])
            self.assertEqual(summary["crawl_batch_count"], 2)
            self.assertEqual(summary["crawl_batches_completed"], 2)
            self.assertEqual(summary["processed_keyword_count"], 3)
            self.assertEqual(summary["persisted_product_count"], 5)
            self.assertEqual(summary["persist_counts"]["noon"], 5)
            self.assertEqual(summary["status"], "partial")
            self.assertEqual(summary["crawl_status"], "partial")
            self.assertEqual(summary["intermediate_sync_count"], 1)
            self.assertEqual(sync_mock.call_count, 3)
            self.assertEqual(summary["platform_stats"]["noon"]["persisted_count"], 5)
            self.assertEqual(summary["platform_stats"]["amazon"]["status"], "failed")

    def test_run_monitor_marks_partial_when_scrape_returns_partial(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="monitor_snapshot")
            store = ProductStore(settings.product_store_db_path)
            try:
                store.upsert_keywords(
                    ["shoes accessories"],
                    tracking_mode="tracked",
                    source_type="manual",
                    priority=10,
                )
            finally:
                store.close()

            args = argparse.Namespace(noon_count=5, amazon_count=5)

            async def fake_run_scrape(*_args, **_kwargs):
                return {
                    "status": "partial",
                    "persisted_product_count": 5,
                    "persist_counts": {"noon": 5, "amazon": 0},
                    "platform_stats": {
                        "noon": {
                            "status": "completed",
                            "attempts": 1,
                            "retry_count": 0,
                            "keyword_count": 1,
                            "products_count": 5,
                            "persisted_count": 5,
                            "result_files": ["noon.json"],
                            "total_results": 5,
                            "error": "",
                            "zero_result_evidence": [],
                            "failed_keywords": [],
                            "zero_result_keywords": [],
                            "attempt_history": [],
                        },
                        "amazon": {
                            "status": "failed",
                            "attempts": 2,
                            "retry_count": 1,
                            "keyword_count": 1,
                            "products_count": 0,
                            "persisted_count": 0,
                            "result_files": [],
                            "total_results": 0,
                            "error": "amazon:empty_result_without_evidence",
                            "zero_result_evidence": [],
                            "failed_keywords": ["shoes accessories"],
                            "zero_result_keywords": [],
                            "attempt_history": [
                                {"attempt": 1, "status": "failed"},
                                {"attempt": 2, "status": "failed"},
                            ],
                        },
                    },
                    "errors": [
                        {
                            "stage": "crawl_platform",
                            "error": "amazon:empty_result_without_evidence",
                            "platform": "amazon",
                        }
                    ],
                }

            with (
                mock.patch.object(
                    keyword_main,
                    "_load_monitor_profile",
                    return_value={
                        "baseline_file": None,
                        "tracked_priority": 30,
                        "expand_limit": 0,
                        "expand_stale_hours": 72,
                        "expand_source_types": ["manual"],
                        "expand_platforms": ["noon", "amazon"],
                        "crawl_platforms": ["noon", "amazon"],
                        "crawl_stale_hours": 24,
                        "crawl_limit": 1,
                        "monitor_report": False,
                    },
                ),
                mock.patch.object(keyword_main, "run_scrape", side_effect=fake_run_scrape),
                mock.patch.object(keyword_main, "run_analyze", return_value=[{"keyword": "shoes accessories"}]),
                mock.patch.object(
                    keyword_main,
                    "_sync_warehouse",
                    return_value={"status": "completed", "reason": "monitor"},
                ),
            ):
                summary = keyword_main.run_monitor(settings, args)

            self.assertEqual(summary["status"], "partial")
            self.assertEqual(summary["crawl_status"], "partial")
            self.assertEqual(summary["persist_counts"]["noon"], 5)
            self.assertEqual(summary["persist_counts"]["amazon"], 0)
            self.assertEqual(summary["platform_stats"]["amazon"]["retry_count"], 1)
            self.assertEqual(summary["analyzed_keyword_count"], 1)
            self.assertTrue(summary["errors"])

            summary_path = keyword_main._monitor_summary_path(settings)
            self.assertTrue(summary_path.exists())
            persisted_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted_summary["status"], "partial")
            self.assertEqual(persisted_summary["crawl_status"], "partial")


if __name__ == "__main__":
    unittest.main()
