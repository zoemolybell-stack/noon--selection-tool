from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import run_ready_category_scan as ready_scan
from config.product_store import ProductStore
from ops import crawler_control


class ReadyCategoryScanTests(unittest.TestCase):
    def test_incremental_category_sync_lock_skip_does_not_advance_last_sync_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            category_db = temp_root / "product_store.db"
            warehouse_db = temp_root / "warehouse.db"
            completed = mock.Mock(
                returncode=0,
                stdout='WAREHOUSE_SYNC_RESULT={"status":"skipped","skip_reason":"lock_active","sync_state":{"status":"skipped"}}\n',
                stderr="",
            )
            with mock.patch.object(ready_scan.subprocess, "run", return_value=completed):
                payload, last_sync_at = ready_scan.maybe_incremental_category_sync(
                    category_db=category_db,
                    warehouse_db=warehouse_db,
                    snapshot_id="snap-1",
                    category_name="pets",
                    persisted_count=12,
                    last_sync_at=123.0,
                    force=False,
                )

            self.assertEqual(payload["status"], "skipped")
            self.assertEqual(payload["skip_reason"], "lock_active")
            self.assertEqual(last_sync_at, 123.0)

    def test_incremental_category_sync_enqueues_shared_sync_when_remote_mode_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            category_db = temp_root / "product_store.db"
            warehouse_db = temp_root / "warehouse.db"
            with (
                mock.patch.object(ready_scan, "enqueue_shared_sync_enabled", return_value=True),
                mock.patch.object(
                    ready_scan,
                    "enqueue_and_wait_for_warehouse_sync",
                    return_value={
                        "status": "completed",
                        "reason": "category_batch_incremental_sync",
                        "task_id": 77,
                        "sync_state": {"status": "completed"},
                    },
                ) as enqueue_mock,
            ):
                payload, last_sync_at = ready_scan.maybe_incremental_category_sync(
                    category_db=category_db,
                    warehouse_db=warehouse_db,
                    snapshot_id="snap-2",
                    category_name="pets",
                    persisted_count=12,
                    last_sync_at=None,
                    force=True,
                )

            enqueue_mock.assert_called_once_with(
                actor="category_batch_scan",
                reason="category_batch_incremental_sync",
                trigger_db=str(category_db),
                warehouse_db=str(warehouse_db),
                snapshot_id="snap-2",
                created_by="category_batch_scan",
                wait_timeout_seconds=ready_scan.DEFAULT_FINAL_SYNC_LOCK_WAIT_SECONDS,
            )
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["task_id"], 77)
            self.assertEqual(payload["command"], ["enqueue_shared_warehouse_sync_task", "77"])
            self.assertIsInstance(last_sync_at, float)

    def test_run_category_batch_persists_each_subcategory_incrementally(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "product_store.db"
            output_dir = temp_root / "batch"
            runtime_map_path = temp_root / "runtime_category_map.json"

            class FakeCrawler:
                def __init__(self, category, data_dir, max_products_per_sub=50, on_subcategory_saved=None, **_kwargs):
                    self.category = category
                    self.data_dir = Path(data_dir)
                    self.on_subcategory_saved = on_subcategory_saved

                async def run(self):
                    category_dir = self.data_dir / "monitoring" / "categories" / self.category / "subcategory"
                    category_dir.mkdir(parents=True, exist_ok=True)
                    for sub_name, product_id in (("Alpha", "p-alpha"), ("Beta", "p-beta")):
                        payload = {
                            "subcategory": sub_name,
                            "scraped_at": "2026-03-30T10:00:00",
                            "products": [
                                {
                                    "product_id": product_id,
                                    "title": f"Product {sub_name}",
                                    "price": 10,
                                    "rating": 4.5,
                                    "review_count": 2,
                                    "product_url": f"https://example.com/{product_id}",
                                    "category_path": f"Home > {sub_name}",
                                }
                            ],
                        }
                        json_file = category_dir / f"{sub_name}.json"
                        json_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                        if self.on_subcategory_saved:
                            self.on_subcategory_saved(sub_name, json_file, payload)
                    return {"category": self.category, "completed": 2}

            with (
                mock.patch.object(ready_scan, "DEFAULT_DB_PATH", db_path),
                mock.patch.object(ready_scan, "DEFAULT_RUNTIME_MAP_PATH", runtime_map_path),
                mock.patch.object(ready_scan, "NoonCategoryCrawler", FakeCrawler),
                mock.patch.object(ready_scan, "build_payload", return_value=({}, {})),
                mock.patch.object(ready_scan, "build_runtime_map", return_value={}),
                mock.patch.object(
                    ready_scan,
                    "maybe_incremental_category_sync",
                    side_effect=lambda **kwargs: (
                        {"status": "completed", "persisted_count": kwargs["persisted_count"]},
                        kwargs.get("last_sync_at"),
                    ),
                ) as sync_mock,
            ):
                summary = asyncio.run(
                    ready_scan.run_category_batch(
                        categories=["sports"],
                        output_dir=output_dir,
                        product_count=50,
                        export_excel=False,
                        persist=True,
                    )
                )

            self.assertEqual(summary["results"][0]["persisted_count"], 2)
            self.assertEqual(sync_mock.call_count, 3)
            self.assertEqual(runtime_map_path.read_text(encoding="utf-8"), "{}")

            store = ProductStore(db_path)
            try:
                product_alpha = store.get_product("p-alpha", platform="noon")
                product_beta = store.get_product("p-beta", platform="noon")
            finally:
                store.close()

            self.assertIsNotNone(product_alpha)
            self.assertIsNotNone(product_beta)

    def test_run_category_batch_applies_subcategory_overrides_after_category_scan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            output_dir = temp_root / "batch"

            runtime_map = {
                "categories": {
                    "sports": {
                        "sports_leaf_default": {
                            "config_id": "sports_leaf_default",
                            "display_name": "Sports Default",
                            "breadcrumb_path": "Home > Sports > Default",
                            "parent_config_id": "sports_parent",
                        },
                        "sports_leaf_override": {
                            "config_id": "sports_leaf_override",
                            "display_name": "Sports Override",
                            "breadcrumb_path": "Home > Sports > Override",
                            "parent_config_id": "sports_parent",
                        },
                    }
                }
            }

            class FakeCrawler:
                init_calls: list[tuple[str, int, str | None]] = []

                def __init__(self, category, data_dir, max_products_per_sub=50, max_depth=3, target_subcategory=None, on_subcategory_saved=None):
                    self.category = category
                    self.data_dir = Path(data_dir)
                    self.max_products_per_sub = max_products_per_sub
                    self.target_subcategory = target_subcategory
                    self.on_subcategory_saved = on_subcategory_saved
                    FakeCrawler.init_calls.append((category, max_products_per_sub, target_subcategory))

                async def run(self):
                    category_dir = self.data_dir / "monitoring" / "categories" / self.category / "subcategory"
                    category_dir.mkdir(parents=True, exist_ok=True)
                    sub_name = self.target_subcategory or "sports_leaf_default"
                    payload = {
                        "subcategory": sub_name,
                        "scraped_at": "2026-03-30T10:00:00",
                        "products": [
                            {
                                "product_id": f"{sub_name}-p",
                                "title": f"Product {sub_name}",
                                "price": 10,
                                "rating": 4.5,
                                "review_count": 2,
                                "product_url": f"https://example.com/{sub_name}",
                                "category_path": f"Home > {sub_name}",
                            }
                        ],
                    }
                    json_file = category_dir / f"{sub_name}.json"
                    json_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    if self.on_subcategory_saved:
                        self.on_subcategory_saved(sub_name, json_file, payload)
                    return {
                        "category": self.category,
                        "target_subcategory": self.target_subcategory,
                        "max_products_per_sub": self.max_products_per_sub,
                    }

            FakeCrawler.init_calls = []
            with (
                mock.patch.object(ready_scan, "NoonCategoryCrawler", FakeCrawler),
                mock.patch.object(ready_scan, "build_payload", return_value=({}, {})),
                mock.patch.object(ready_scan, "build_runtime_map", return_value={}),
                mock.patch.object(ready_scan, "load_runtime_category_map", return_value=runtime_map),
                mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map),
            ):
                summary = asyncio.run(
                    ready_scan.run_category_batch(
                        categories=["sports"],
                        output_dir=output_dir,
                        product_count=50,
                        category_overrides={"sports": 120},
                        subcategory_overrides={"sports_leaf_override": 220},
                        export_excel=False,
                        persist=False,
                    )
                )

            self.assertEqual(FakeCrawler.init_calls[0], ("sports", 120, None))
            self.assertEqual(FakeCrawler.init_calls[1], ("sports", 220, "sports_leaf_override"))
            self.assertEqual(summary["results"][0]["product_count_per_leaf"], 120)
            self.assertEqual(summary["results"][0]["subcategory_overrides"][0]["target_subcategory"], "sports_leaf_override")
            self.assertEqual(summary["results"][0]["override_runs"][0]["target_subcategory"], "sports_leaf_override")

    def test_run_category_batch_pushes_runtime_map_artifacts_when_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            output_dir = temp_root / "batch"
            runtime_map_path = temp_root / "runtime_category_map.json"

            class FakeCrawler:
                def __init__(self, category, data_dir, max_products_per_sub=50, on_subcategory_saved=None, **_kwargs):
                    self.category = category
                    self.data_dir = Path(data_dir)

                async def run(self):
                    category_dir = self.data_dir / "monitoring" / "categories" / self.category / "subcategory"
                    category_dir.mkdir(parents=True, exist_ok=True)
                    payload = {
                        "subcategory": "Alpha",
                        "scraped_at": "2026-03-30T10:00:00",
                        "products": [
                            {
                                "product_id": "p-alpha",
                                "title": "Product Alpha",
                                "price": 10,
                                "rating": 4.5,
                                "review_count": 2,
                                "product_url": "https://example.com/p-alpha",
                                "category_path": "Home > Alpha",
                            }
                        ],
                    }
                    json_file = category_dir / "Alpha.json"
                    json_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    return {"category": self.category, "completed": 1}

            with (
                mock.patch.object(ready_scan, "DEFAULT_RUNTIME_MAP_PATH", runtime_map_path),
                mock.patch.object(ready_scan, "NoonCategoryCrawler", FakeCrawler),
                mock.patch.object(ready_scan, "build_payload", return_value=({}, {})),
                mock.patch.object(ready_scan, "build_runtime_map", return_value={"categories": {"sports": {}}}),
                mock.patch.object(ready_scan, "artifact_sync_enabled", return_value=True),
                mock.patch.object(
                    ready_scan,
                    "push_runtime_category_artifacts",
                    return_value={"status": "completed", "remote_dir": "/remote/snap-1"},
                ) as artifact_sync_mock,
                mock.patch.dict("os.environ", {"NOON_WORKER_NODE_HOST": "crawler-pc"}, clear=False),
            ):
                summary = asyncio.run(
                    ready_scan.run_category_batch(
                        categories=["sports"],
                        output_dir=output_dir,
                        product_count=50,
                        export_excel=False,
                        persist=False,
                    )
                )

            artifact_sync_mock.assert_called_once_with(
                runtime_map_path=output_dir / "runtime_category_map.json",
                snapshot_id=output_dir.name,
                source_node="crawler-pc",
                batch_dir=output_dir,
            )
            self.assertEqual(summary["artifact_sync"]["status"], "completed")
            persisted_summary = json.loads((output_dir / "batch_scan_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted_summary["artifact_sync"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
