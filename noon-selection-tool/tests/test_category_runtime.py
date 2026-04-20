from __future__ import annotations

import asyncio
import argparse
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module
import run_ready_category_scan as ready_scan_module
from config.settings import Settings
from scrapers.noon_category_card_capture import (
    build_category_product_cards_js,
    collect_category_product_payloads,
)
from scrapers.noon_category_crawler import NoonCategoryCrawler


def build_settings(temp_root: Path) -> Settings:
    settings = Settings()
    settings.set_runtime_scope("shared")
    settings.set_data_dir(temp_root / "data")
    settings.set_snapshot_id("category_test_snapshot")
    return settings


class CategoryWarehouseSyncTests(unittest.TestCase):
    def test_sync_category_warehouse_runs_shared_runner_for_official_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            settings.set_product_store_db_path(main_module._official_category_db_path(settings))
            completed = types.SimpleNamespace(
                returncode=0,
                stdout=(
                    'line1\n'
                    'WAREHOUSE_SYNC_RESULT={"status":"completed","reason":"category_persist",'
                    '"sync_state":{"status":"completed"}}\n'
                ),
                stderr="",
            )

            with mock.patch.object(main_module.subprocess, "run", return_value=completed) as run_mock:
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=3,
                    category_name="sports",
                )

            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["reason"], "category_persist")
            self.assertEqual(payload["persisted_count"], 3)
            self.assertEqual(payload["category_db"], str(main_module._official_category_db_path(settings).resolve()))
            self.assertEqual(payload["sync_state"]["status"], "completed")
            run_mock.assert_called_once_with(
                [
                    sys.executable,
                    str(settings.project_root / "run_shared_warehouse_sync.py"),
                    "--actor",
                    "category_window",
                    "--reason",
                    "category_persist",
                    "--trigger-db",
                    str(main_module._official_category_db_path(settings).resolve()),
                    "--warehouse-db",
                    str(settings.project_root / "data" / "analytics" / "warehouse.db"),
                ],
                cwd=str(settings.project_root),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )

    def test_sync_category_warehouse_skips_when_no_new_observations(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))

            with mock.patch.object(main_module.subprocess, "run") as run_mock:
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=0,
                )

            self.assertEqual(payload["status"], "skipped")
            self.assertEqual(payload["reason"], "no_new_category_observations")
            run_mock.assert_not_called()

    def test_sync_category_warehouse_skips_for_non_official_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            settings.set_product_store_db_path(Path(temp_dir) / "alt" / "product_store.db")

            with mock.patch.object(main_module.subprocess, "run") as run_mock:
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=5,
                )

            self.assertEqual(payload["status"], "skipped")
            self.assertEqual(payload["reason"], "non_official_category_db")
            run_mock.assert_not_called()

    def test_sync_category_warehouse_skips_when_shared_lock_is_active(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            settings.set_product_store_db_path(main_module._official_category_db_path(settings))
            completed = types.SimpleNamespace(
                returncode=0,
                stdout=(
                    'WAREHOUSE_SYNC_RESULT={"status":"skipped","reason":"category_persist",'
                    '"skip_reason":"lock_active","sync_state":{"status":"skipped","skip_reason":"lock_active"}}\n'
                ),
                stderr="",
            )

            with mock.patch.object(main_module.subprocess, "run", return_value=completed) as run_mock:
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=5,
                    category_name="sports",
                )

            self.assertEqual(payload["status"], "skipped")
            self.assertEqual(payload["reason"], "category_persist")
            self.assertEqual(payload["skip_reason"], "lock_active")
            self.assertEqual(payload["sync_state"]["skip_reason"], "lock_active")
            run_mock.assert_called_once()

    def test_sync_category_warehouse_reports_failed_shared_runner(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            settings.set_product_store_db_path(main_module._official_category_db_path(settings))
            completed = types.SimpleNamespace(
                returncode=1,
                stdout=(
                    'WAREHOUSE_SYNC_RESULT={"status":"failed","reason":"category_persist",'
                    '"sync_state":{"status":"failed"}}\n'
                    "warehouse start\n"
                    "importing\n"
                ),
                stderr="boom\n",
            )

            with mock.patch.object(main_module.subprocess, "run", return_value=completed):
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=9,
                    category_name="sports",
                )

            self.assertEqual(payload["status"], "failed")
            self.assertEqual(payload["reason"], "category_persist")
            self.assertEqual(payload["returncode"], 1)
            self.assertIn("boom", "\n".join(payload["log_tail"]))
            self.assertIn("boom", payload["error"])
            self.assertEqual(payload["sync_state"]["status"], "failed")

    def test_sync_category_warehouse_enqueues_shared_sync_when_remote_mode_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            settings.set_product_store_db_path(Path(temp_dir) / "remote-stage" / "product_store.db")

            with (
                mock.patch.object(main_module, "enqueue_shared_sync_enabled", return_value=True),
                mock.patch.object(
                    main_module,
                    "enqueue_and_wait_for_warehouse_sync",
                    return_value={
                        "status": "completed",
                        "reason": "category_persist",
                        "task_id": 91,
                        "sync_state": {"status": "completed"},
                    },
                ) as enqueue_mock,
            ):
                payload = main_module._sync_category_warehouse(
                    settings,
                    reason="category_persist",
                    persisted_count=4,
                    category_name="sports",
                )

            enqueue_mock.assert_called_once_with(
                actor="category_window",
                reason="category_persist",
                trigger_db=settings.product_store_db_ref,
                warehouse_db=settings.warehouse_db_ref,
                snapshot_id=settings.snapshot_id,
                created_by="category_window",
            )
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["task_id"], 91)
            self.assertEqual(payload["command"], ["enqueue_shared_warehouse_sync_task", "91"])


class CategoryBatchIncrementalSyncTests(unittest.TestCase):
    def test_incremental_sync_uses_shared_runner_result_without_double_lock(self):
        completed = types.SimpleNamespace(
            returncode=0,
            stdout=(
                'WAREHOUSE_SYNC_RESULT={"status":"completed","reason":"category_batch_incremental_sync",'
                '"sync_state":{"status":"completed"}}\n'
            ),
            stderr="",
        )

        with (
            mock.patch.object(ready_scan_module.subprocess, "run", return_value=completed) as run_mock,
            mock.patch.object(ready_scan_module.warehouse_sync, "acquire_sync_lock") as acquire_mock,
            mock.patch.object(ready_scan_module.warehouse_sync, "finalize_sync_state") as finalize_mock,
        ):
            payload, last_sync_at = ready_scan_module.maybe_incremental_category_sync(
                category_db=ROOT / "data" / "product_store.db",
                warehouse_db=ROOT / "data" / "analytics" / "warehouse.db",
                snapshot_id="ready_scan_test",
                category_name="sports",
                persisted_count=12,
                last_sync_at=None,
                force=False,
            )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["reason"], "category_batch_incremental_sync")
        self.assertEqual(payload["sync_state"]["status"], "completed")
        self.assertIsInstance(last_sync_at, float)
        run_mock.assert_called_once()
        acquire_mock.assert_not_called()
        finalize_mock.assert_not_called()

    def test_incremental_sync_surfaces_lock_active_from_shared_runner(self):
        completed = types.SimpleNamespace(
            returncode=0,
            stdout=(
                'WAREHOUSE_SYNC_RESULT={"status":"skipped","reason":"category_batch_incremental_sync",'
                '"skip_reason":"lock_active","sync_state":{"status":"skipped","skip_reason":"lock_active"}}\n'
            ),
            stderr="",
        )

        with mock.patch.object(ready_scan_module.subprocess, "run", return_value=completed):
            payload, _ = ready_scan_module.maybe_incremental_category_sync(
                category_db=ROOT / "data" / "product_store.db",
                warehouse_db=ROOT / "data" / "analytics" / "warehouse.db",
                snapshot_id="ready_scan_test",
                category_name="sports",
                persisted_count=12,
                last_sync_at=None,
                force=False,
            )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["skip_reason"], "lock_active")


class CategoryStepTests(unittest.TestCase):
    def test_run_category_step_exits_after_persist_when_sync_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            args = argparse.Namespace(
                category="sports",
                noon_count=10,
                max_depth=3,
                target_subcategory=None,
                persist=True,
                export_excel=False,
            )

            class FakeCrawler:
                def __init__(self, *_args, **kwargs):
                    self.on_subcategory_saved = kwargs.get("on_subcategory_saved")

                def run(self):
                    if self.on_subcategory_saved:
                        self.on_subcategory_saved("sports-leaf", Path(temp_dir) / "sports-leaf.json", {})
                    return {"category": "sports", "completed": 1}

            with (
                mock.patch("scrapers.noon_category_crawler.NoonCategoryCrawler", FakeCrawler),
                mock.patch.object(main_module.asyncio, "run", return_value={"category": "sports", "completed": 1}),
                mock.patch.object(main_module, "_persist_category_subcategory_file", return_value=7) as persist_mock,
                mock.patch.object(
                    main_module,
                    "_sync_category_warehouse",
                    return_value={"status": "failed", "reason": "category_persist", "log_tail": []},
                ) as sync_mock,
            ):
                with self.assertRaises(SystemExit) as exc:
                    main_module._run_category_step(settings, args)

            self.assertEqual(exc.exception.code, 1)
            persist_mock.assert_called_once_with(
                settings,
                category_name="sports",
                json_file=Path(temp_dir) / "sports-leaf.json",
            )
            sync_mock.assert_called_once_with(
                settings,
                reason="category_persist",
                persisted_count=7,
                category_name="sports",
            )


class CategoryCrawlerHardeningTests(unittest.TestCase):
    def test_collect_category_product_payloads_rewrites_marker_pattern_and_returns_cards(self):
        script = build_category_product_cards_js()
        self.assertNotIn("__DELIVERY_MARKER_PATTERN__", script)
        self.assertIn("product-noon-express", script)

        class FakePage:
            def __init__(self):
                self.script = None

            async def evaluate(self, script):
                self.script = script
                return [{"title": "Example", "cardText": "Express"}]

        fake_page = FakePage()
        result = asyncio.run(collect_category_product_payloads(fake_page))

        self.assertEqual(result, [{"title": "Example", "cardText": "Express"}])
        self.assertIsNotNone(fake_page.script)
        self.assertEqual(fake_page.script, script)

    def test_parse_products_delegates_through_shared_card_evidence_helper(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            raw_cards = [
                {
                    "title": "Example bottle",
                    "href": "/saudi-en/example/N12345678A/p/?o=abc",
                    "priceText": "SAR 12.00",
                    "wasText": "",
                    "ratingText": "4.2 (12 ratings)",
                    "sellerText": "",
                    "brandText": "Brand",
                    "cardText": "Free Delivery\nGet it by 15 April",
                    "signalTexts": ["Free Delivery", "Get it by 15 April"],
                    "deliveryMarkers": [{"merged": "noon-express"}],
                    "adMarkers": [],
                    "imgCount": 1,
                    "imageUrl": "https://example.com/a.jpg",
                    "isExpress": True,
                    "isBestSeller": False,
                    "isAd": False,
                }
            ]

            with (
                mock.patch("scrapers.noon_category_parsing.collect_category_product_payloads", return_value=raw_cards) as collect_mock,
                mock.patch.object(crawler, "_scrape_public_product_details", return_value=None) as detail_mock,
            ):
                products = asyncio.run(
                    crawler._parse_products(
                        object(),
                        rank_offset=2,
                        category_path="Sports",
                    )
                )

            self.assertEqual(len(products), 1)
            self.assertEqual(products[0]["title"], "Example bottle")
            self.assertEqual(products[0]["search_rank"], 3)
            self.assertEqual(products[0]["delivery_type"], "express")
            collect_mock.assert_called_once()
            detail_mock.assert_not_called()

    def test_subcategory_paths_use_hash_to_avoid_legacy_collisions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")

            first_name = "A/B"
            second_name = "AB"

            self.assertEqual(
                crawler._legacy_subcategory_file_path(first_name).name,
                crawler._legacy_subcategory_file_path(second_name).name,
            )
            self.assertNotEqual(
                crawler._subcategory_file_path(first_name).name,
                crawler._subcategory_file_path(second_name).name,
            )

    def test_subcategory_path_length_is_capped_and_stable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")

            sub_name = "Very Long Subcategory " * 20
            first_path = crawler._subcategory_file_path(sub_name)
            second_path = crawler._subcategory_file_path(sub_name)

            self.assertEqual(first_path.name, second_path.name)
            self.assertLessEqual(len(first_path.name), 120)
            self.assertTrue(first_path.name.endswith(".json"))

    def test_subcategory_done_requires_valid_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")

            crawler._save_subcategory(
                "Yoga Blocks",
                [{"product_id": "p1", "title": "Block"}],
                {"url": "https://example.com/blocks"},
            )
            self.assertTrue(crawler._is_subcategory_done("Yoga Blocks"))

            broken_path = crawler._subcategory_file_path("Broken Case")
            broken_path.write_text('{"subcategory": "Broken Case"', encoding="utf-8")
            self.assertFalse(crawler._is_subcategory_done("Broken Case"))

    def test_subcategory_done_and_existing_ids_support_legacy_file_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            sub_name = "Yoga/Blocks"
            legacy_path = crawler._legacy_subcategory_file_path(sub_name)
            self.assertNotEqual(legacy_path, crawler._subcategory_file_path(sub_name))

            legacy_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:00:00",
                        "products": [{"product_id": "legacy-1", "title": "Legacy Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            self.assertTrue(crawler._is_subcategory_done(sub_name))
            crawler._load_existing_sub_ids(sub_name)
            self.assertIn("legacy-1", crawler._seen_product_ids)

    def test_save_subcategory_lazily_migrates_legacy_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            sub_name = "Yoga/Blocks"
            legacy_path = crawler._legacy_subcategory_file_path(sub_name)
            current_path = crawler._subcategory_file_path(sub_name)
            self.assertNotEqual(legacy_path, current_path)

            legacy_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:00:00",
                        "products": [{"product_id": "legacy-1", "title": "Legacy Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            crawler._save_subcategory(
                sub_name,
                [{"product_id": "new-1", "title": "New Block"}],
                {"url": "https://example.com/blocks"},
            )

            self.assertTrue(current_path.exists())
            self.assertFalse(legacy_path.exists())
            payload = crawler._load_subcategory_payload_from_path(current_path, expected_sub_name=sub_name)
            self.assertIsNotNone(payload)
            self.assertEqual(payload["products"][0]["product_id"], "new-1")

    def test_load_seen_ids_prefers_current_subcategory_file_over_legacy_duplicate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            sub_name = "Yoga/Blocks"
            legacy_path = crawler._legacy_subcategory_file_path(sub_name)
            current_path = crawler._subcategory_file_path(sub_name)

            legacy_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:00:00",
                        "products": [{"product_id": "legacy-1", "title": "Legacy Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            current_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:05:00",
                        "products": [{"product_id": "current-1", "title": "Current Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            crawler._load_seen_ids()

            self.assertIn("current-1", crawler._seen_product_ids)
            self.assertNotIn("legacy-1", crawler._seen_product_ids)

    def test_merge_all_products_prefers_current_subcategory_file_over_legacy_duplicate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            sub_name = "Yoga/Blocks"
            legacy_path = crawler._legacy_subcategory_file_path(sub_name)
            current_path = crawler._subcategory_file_path(sub_name)

            legacy_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:00:00",
                        "products": [{"product_id": "legacy-1", "title": "Legacy Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            current_path.write_text(
                json.dumps(
                    {
                        "subcategory": sub_name,
                        "product_count": 1,
                        "scraped_at": "2026-03-29T00:05:00",
                        "products": [{"product_id": "current-1", "title": "Current Block"}],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            merged = crawler._merge_all_products()

            self.assertEqual([item["product_id"] for item in merged], ["current-1"])
            self.assertEqual(merged[0]["_subcategory"], sub_name)

    def test_subcategory_payload_name_match_uses_normalized_name_not_filename(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")

            self.assertTrue(
                crawler._is_valid_subcategory_payload(
                    {
                        "subcategory": "  yoga   blocks  ",
                        "product_count": 1,
                        "products": [{"product_id": "p1"}],
                    },
                    expected_sub_name="Yoga Blocks",
                )
            )
            self.assertFalse(
                crawler._is_valid_subcategory_payload(
                    {
                        "subcategory": "Yoga/Blocks",
                        "product_count": 1,
                        "products": [{"product_id": "p1"}],
                    },
                    expected_sub_name="Yoga Blocks",
                )
            )

    def test_merge_all_products_skips_invalid_files_and_dedupes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")

            crawler._save_subcategory(
                "Yoga Blocks",
                [
                    {"product_id": "p1", "title": "Block A"},
                    {"product_id": "p2", "title": "Block B"},
                ],
                {"url": "https://example.com/blocks"},
            )
            crawler._save_subcategory(
                "Yoga Mats",
                [
                    {"product_id": "p2", "title": "Duplicate Product"},
                    {"product_id": "p3", "title": "Mat C"},
                ],
                {"url": "https://example.com/mats"},
            )
            crawler._subcategory_file_path("Broken Case").write_text(
                '{"subcategory": "Broken Case"',
                encoding="utf-8",
            )

            merged = crawler._merge_all_products()

            self.assertEqual([item["product_id"] for item in merged], ["p1", "p2", "p3"])
            self.assertEqual(merged[0]["_subcategory"], "Yoga Blocks")
            self.assertEqual(merged[2]["_subcategory"], "Yoga Mats")

    def test_run_lock_blocks_second_live_owner(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            fake_lock_path = mock.MagicMock()
            fake_lock_path.exists.return_value = True
            fake_lock_path.__str__.return_value = str(Path(temp_dir) / "category_crawl.lock")
            crawler.lock_path = fake_lock_path

            with (
                mock.patch.object(
                    crawler,
                    "_read_json_file",
                    return_value={"pid": os.getpid(), "category": "sports"},
                ),
                mock.patch("scrapers.noon_category_crawler._pid_is_alive", return_value=True),
            ):
                with self.assertRaises(SystemExit):
                    crawler._acquire_run_lock()

    def test_run_lock_cleans_stale_lock_before_recreate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            original_open = os.open
            fake_lock_target = Path(temp_dir) / "recreated.lock"
            fake_lock_path = mock.MagicMock()
            fake_lock_path.exists.side_effect = [True, False]
            fake_lock_path.unlink = mock.Mock()
            fake_lock_path.__str__.return_value = str(fake_lock_target)
            crawler.lock_path = fake_lock_path

            with (
                mock.patch.object(
                    crawler,
                    "_read_json_file",
                    return_value={"pid": 999999, "category": "sports"},
                ),
                mock.patch("scrapers.noon_category_crawler._pid_is_alive", return_value=False),
                mock.patch("scrapers.noon_category_crawler.os.open", side_effect=lambda path, flags: original_open(path, flags)),
            ):
                payload = crawler._acquire_run_lock()

            self.assertEqual(payload["pid"], os.getpid())
            fake_lock_path.unlink.assert_called_once_with(missing_ok=True)
            self.assertEqual(str(fake_lock_target), str(fake_lock_path))


class CategoryCrawlerTraversalTests(unittest.TestCase):
    def test_load_runtime_category_map_prefers_shared_runtime_loader(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            runtime_map = {
                "categories": {
                    "sports": {
                        "basketball": {
                            "display_name": "Basketball",
                            "resolved_url": "https://www.noon.com/saudi-en/sports/team-sports/basketball/",
                            "source_urls": [],
                            "expected_path": ["Sports", "Team Sports", "Basketball"],
                        }
                    }
                }
            }

            with mock.patch(
                "scrapers.noon_category_crawler.load_runtime_category_map",
                return_value=runtime_map,
            ) as loader_mock:
                loaded = crawler._load_runtime_category_map()

            self.assertEqual(loaded, runtime_map)
            loader_mock.assert_called_once()

    def test_discover_category_tree_prefers_runtime_category_map(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            runtime_map = {
                "categories": {
                    "sports": {
                        "cfg-1": {
                            "display_name": "Yoga Blocks",
                            "subcategory_name": "Yoga Blocks",
                            "resolved_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                            "source_urls": [],
                            "expected_path": ["Sports", "Yoga", "Yoga Blocks"],
                            "platform_nav_path": ["Sports", "Yoga", "Yoga Blocks"],
                            "parent_config_id": "parent-1",
                        }
                    }
                }
            }

            with (
                mock.patch.object(crawler, "_load_runtime_category_map", return_value=runtime_map),
                mock.patch.object(crawler, "crawl_category_recursive", new=mock.AsyncMock()) as recursive_mock,
            ):
                discovered = asyncio.run(crawler.discover_category_tree())

            self.assertEqual(len(discovered), 1)
            self.assertEqual(discovered[0]["config_id"], "cfg-1")
            self.assertEqual(discovered[0]["source"], "runtime_category_map")
            recursive_mock.assert_not_called()

    def test_get_target_subcategory_prefers_runtime_record_by_config_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data")
            runtime_items = [
                {
                    "name": "Yoga Blocks",
                    "url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                    "source_urls": ["https://www.noon.com/saudi-en/sports/yoga-blocks/"],
                    "expected_path": ["Sports", "Yoga", "Yoga Blocks"],
                    "platform_nav_path": ["Sports", "Yoga", "Yoga Blocks"],
                    "config_id": "cfg-1",
                    "parent_config_id": "parent-1",
                    "source": "runtime_category_map",
                }
            ]

            with (
                mock.patch.object(crawler, "_build_runtime_subcategories", return_value=runtime_items),
                mock.patch.object(crawler, "discover_category_tree", new=mock.AsyncMock()) as discover_mock,
            ):
                resolved = asyncio.run(crawler._get_target_subcategory("cfg-1"))

            self.assertEqual(len(resolved), 1)
            self.assertEqual(resolved[0]["name"], "Yoga Blocks")
            self.assertEqual(resolved[0]["config_id"], "cfg-1")
            discover_mock.assert_not_called()

    def test_scrape_subcategory_returns_timeout_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data", max_products_per_sub=60)

            class FakePage:
                def __init__(self):
                    self.url = "https://www.noon.com/saudi-en/sports/yoga-blocks/"

                async def goto(self, *_args, **_kwargs):
                    raise RuntimeError("goto timeout")

                async def wait_for_timeout(self, _ms):
                    return None

                async def close(self):
                    return None

            class FakeContext:
                def __init__(self, page):
                    self.page = page

                async def new_page(self):
                    return self.page

                async def close(self):
                    return None

            with mock.patch.object(
                crawler,
                "_new_context",
                new=mock.AsyncMock(return_value=FakeContext(FakePage())),
            ):
                result = asyncio.run(
                    crawler.scrape_subcategory(
                        "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                        "Yoga Blocks",
                        expected_path=["Sports", "Yoga", "Yoga Blocks"],
                    )
                )

            self.assertEqual(result["products"], [])
            self.assertEqual(len(result["page_evidence"]), 1)
            self.assertEqual(result["page_evidence"][0]["failure_category"], "timeout")
            self.assertEqual(result["page_evidence"][0]["page_state"], "timeout")

    def test_scrape_subcategory_returns_page_recognition_failed_for_empty_first_page(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data", max_products_per_sub=60)

            class FakePage:
                def __init__(self):
                    self.url = "https://www.noon.com/saudi-en/sports/yoga-blocks/"

                async def goto(self, url, *_args, **_kwargs):
                    self.url = url
                    return None

                async def wait_for_timeout(self, _ms):
                    return None

                async def close(self):
                    return None

            class FakeContext:
                def __init__(self, page):
                    self.page = page

                async def new_page(self):
                    return self.page

                async def close(self):
                    return None

            with (
                mock.patch.object(
                    crawler,
                    "_new_context",
                    new=mock.AsyncMock(return_value=FakeContext(FakePage())),
                ),
                mock.patch.object(
                    crawler,
                    "_extract_breadcrumb",
                    new=mock.AsyncMock(
                        return_value={
                            "items": ["Sports", "Yoga", "Yoga Blocks"],
                            "links": [],
                            "path": "Sports > Yoga > Yoga Blocks",
                        }
                    ),
                ),
                mock.patch(
                    "scrapers.noon_category_crawler.extract_category_filter",
                    new=mock.AsyncMock(return_value={"links": [], "texts": ["Yoga Blocks"]}),
                ),
                mock.patch.object(crawler, "_parse_products", new=mock.AsyncMock(return_value=[])),
            ):
                result = asyncio.run(
                    crawler.scrape_subcategory(
                        "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                        "Yoga Blocks",
                        expected_path=["Sports", "Yoga", "Yoga Blocks"],
                    )
                )

            self.assertEqual(result["products"], [])
            self.assertEqual(result["breadcrumb_match_status"], "matched")
            self.assertEqual(len(result["page_evidence"]), 1)
            self.assertEqual(result["page_evidence"][0]["failure_category"], "page_recognition_failed")
            self.assertEqual(result["page_evidence"][0]["page_number"], 1)

    def test_scrape_subcategory_retries_access_denied_then_recovers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data", max_products_per_sub=60)

            class FakePage:
                def __init__(self):
                    self.url = "https://www.noon.com/saudi-en/sports/yoga-blocks/"

                async def goto(self, url, *_args, **_kwargs):
                    self.url = url
                    return None

                async def wait_for_timeout(self, _ms):
                    return None

                async def close(self):
                    return None

            class FakeContext:
                def __init__(self, page):
                    self.page = page

                async def new_page(self):
                    return self.page

                async def close(self):
                    return None

            with (
                mock.patch.object(
                    crawler,
                    "_new_context",
                    new=mock.AsyncMock(
                        side_effect=[
                            FakeContext(FakePage()),
                            FakeContext(FakePage()),
                        ]
                    ),
                ),
                mock.patch.object(crawler, "_warmup_context", new=mock.AsyncMock()),
                mock.patch.object(
                    crawler,
                    "_is_access_denied_page",
                    new=mock.AsyncMock(side_effect=[True, False, False]),
                ),
                mock.patch.object(
                    crawler,
                    "_extract_breadcrumb",
                    new=mock.AsyncMock(
                        return_value={
                            "items": ["Sports", "Yoga", "Yoga Blocks"],
                            "links": [],
                            "path": "Sports > Yoga > Yoga Blocks",
                        }
                    ),
                ),
                mock.patch(
                    "scrapers.noon_category_crawler.extract_category_filter",
                    new=mock.AsyncMock(return_value={"links": [], "texts": ["Yoga Blocks"]}),
                ),
                mock.patch.object(
                    crawler,
                    "_parse_products",
                    new=mock.AsyncMock(return_value=[{"product_id": "p1", "title": "Block"}]),
                ),
                mock.patch.object(
                    crawler,
                    "_handle_access_denied_retry",
                    new=mock.AsyncMock(),
                ) as retry_mock,
            ):
                result = asyncio.run(
                    crawler.scrape_subcategory(
                        "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                        "Yoga Blocks",
                        expected_path=["Sports", "Yoga", "Yoga Blocks"],
                    )
                )

            self.assertEqual(len(result["products"]), 1)
            self.assertEqual(result["products"][0]["product_id"], "p1")
            self.assertEqual(len(result["page_evidence"]), 1)
            self.assertEqual(result["page_evidence"][0]["failure_category"], "access_denied")
            self.assertEqual(result["page_evidence"][0]["page_state"], "blocked_warmup")
            retry_mock.assert_awaited_once()

    def test_run_includes_page_evidence_in_source_runs_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data", max_products_per_sub=60)
            saved_meta: list[dict] = []

            async def fake_discover():
                return [
                    {
                        "name": "Yoga Blocks",
                        "url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                        "source_urls": ["https://www.noon.com/saudi-en/sports/yoga-blocks/"],
                        "expected_path": ["Sports", "Yoga", "Yoga Blocks"],
                        "platform_nav_path": ["Sports", "Yoga", "Yoga Blocks"],
                        "config_id": "cfg-1",
                        "parent_config_id": "parent-1",
                    }
                ]

            async def fake_scrape(_url, _sub_name, expected_path=None):
                return {
                    "requested_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                    "resolved_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                    "effective_category_path": "Sports > Yoga > Yoga Blocks",
                    "breadcrumb": {"items": expected_path or [], "links": [], "path": "Sports > Yoga > Yoga Blocks"},
                    "category_filter": {"texts": ["Yoga Blocks"], "links": []},
                    "breadcrumb_match_status": "matched",
                    "products": [{"product_id": "p1", "title": "Block"}],
                    "page_evidence": [
                        {
                            "platform": "noon",
                            "source_type": "category",
                            "failure_category": "timeout",
                            "short_evidence": "page_timeout",
                            "page_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/?page=2",
                            "page_number": 2,
                            "page_state": "timeout",
                        }
                    ],
                }

            def fake_save(_sub_name, _products, meta):
                saved_meta.append(meta)

            with (
                mock.patch.object(crawler, "_acquire_run_lock"),
                mock.patch.object(crawler, "_release_run_lock"),
                mock.patch.object(crawler, "_start_browser", new=mock.AsyncMock()),
                mock.patch.object(crawler, "_stop_browser", new=mock.AsyncMock()),
                mock.patch.object(crawler, "discover_category_tree", new=fake_discover),
                mock.patch.object(crawler, "scrape_subcategory", new=fake_scrape),
                mock.patch.object(crawler, "_is_subcategory_done", return_value=False),
                mock.patch.object(crawler, "_save_subcategory", side_effect=fake_save),
                mock.patch.object(crawler, "_merge_all_products", return_value=[{"product_id": "p1", "title": "Block"}]),
                mock.patch("scrapers.noon_category_crawler.asyncio.sleep", new=mock.AsyncMock()),
            ):
                summary = asyncio.run(crawler.run())

            self.assertEqual(summary["completed"], 1)
            self.assertEqual(len(saved_meta), 1)
            self.assertEqual(saved_meta[0]["source_runs"][0]["page_evidence"][0]["failure_category"], "timeout")

    def test_run_marks_access_denied_without_products_as_failed_subcategory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = NoonCategoryCrawler("sports", Path(temp_dir) / "data", max_products_per_sub=60)

            async def fake_discover():
                return [
                    {
                        "name": "Yoga Blocks",
                        "url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                        "source_urls": ["https://www.noon.com/saudi-en/sports/yoga-blocks/"],
                        "expected_path": ["Sports", "Yoga", "Yoga Blocks"],
                        "platform_nav_path": ["Sports", "Yoga", "Yoga Blocks"],
                        "config_id": "cfg-1",
                        "parent_config_id": "parent-1",
                    }
                ]

            async def fake_scrape(_url, _sub_name, expected_path=None):
                return {
                    "requested_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                    "resolved_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                    "effective_category_path": "Sports > Yoga > Yoga Blocks",
                    "breadcrumb": {"items": expected_path or [], "links": [], "path": "Sports > Yoga > Yoga Blocks"},
                    "category_filter": {"texts": ["Yoga Blocks"], "links": []},
                    "breadcrumb_match_status": "matched",
                    "products": [],
                    "page_evidence": [
                        {
                            "platform": "noon",
                            "source_type": "category",
                            "failure_category": "access_denied",
                            "short_evidence": "akamai_access_denied_on_category_page_attempt_3",
                            "page_url": "https://www.noon.com/saudi-en/sports/yoga-blocks/",
                            "page_number": 1,
                            "page_state": "blocked",
                        }
                    ],
                }

            with (
                mock.patch.object(crawler, "_acquire_run_lock"),
                mock.patch.object(crawler, "_release_run_lock"),
                mock.patch.object(crawler, "_start_browser", new=mock.AsyncMock()),
                mock.patch.object(crawler, "_stop_browser", new=mock.AsyncMock()),
                mock.patch.object(crawler, "discover_category_tree", new=fake_discover),
                mock.patch.object(crawler, "scrape_subcategory", new=fake_scrape),
                mock.patch.object(crawler, "_is_subcategory_done", return_value=False),
                mock.patch.object(crawler, "_save_subcategory") as save_mock,
                mock.patch.object(crawler, "_merge_all_products", return_value=[]),
                mock.patch("scrapers.noon_category_crawler.asyncio.sleep", new=mock.AsyncMock()),
            ):
                summary = asyncio.run(crawler.run())

            self.assertEqual(summary["completed"], 0)
            self.assertEqual(len(summary["errors"]), 1)
            self.assertEqual(summary["errors"][0]["subcategory"], "Yoga Blocks")
            self.assertIn("access_denied:", summary["errors"][0]["error"])
            save_mock.assert_not_called()


class CategorySnapshotPreparationTests(unittest.TestCase):
    def test_prepare_snapshot_directory_creates_missing_snapshot_for_resume(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = Settings()
            settings.set_runtime_scope("shared")
            settings.set_data_dir(Path(temp_dir) / "data")
            args = argparse.Namespace(snapshot="category_plan_11_round_19_sports", resume=True)

            main_module._prepare_snapshot_directory(settings, args)

            self.assertEqual(settings.snapshot_id, "category_plan_11_round_19_sports")
            self.assertTrue(settings.snapshot_dir.exists())


if __name__ == "__main__":
    unittest.main()
