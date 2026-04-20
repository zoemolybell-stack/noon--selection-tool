from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.auto_dispatch import (
    AUTO_DISPATCH_INTERVAL_SECONDS,
    CANONICAL_CATEGORY_PLAN_NAME,
    CANONICAL_KEYWORD_PLAN_NAME,
    build_auto_dispatch_summary,
    sync_canonical_auto_plans,
)
from ops.task_store import OpsStore


class AutoDispatchTests(unittest.TestCase):
    def test_build_auto_dispatch_summary_reports_canonical_and_conflict(self) -> None:
        plans = [
            {
                "id": 33,
                "plan_type": "category_ready_scan",
                "name": CANONICAL_CATEGORY_PLAN_NAME,
                "enabled": True,
                "schedule_kind": "interval",
                "schedule_json": {"seconds": AUTO_DISPATCH_INTERVAL_SECONDS},
                "next_run_at": "2026-04-18T01:00:00+00:00",
                "last_run_status": "completed",
                "last_run_task_id": 1001,
                "updated_at": "2026-04-17T01:00:00+00:00",
                "payload": {},
            },
            {
                "id": 10,
                "plan_type": "keyword_monitor",
                "name": CANONICAL_KEYWORD_PLAN_NAME,
                "enabled": True,
                "schedule_kind": "interval",
                "schedule_json": {"seconds": AUTO_DISPATCH_INTERVAL_SECONDS},
                "next_run_at": "2026-04-18T01:00:00+00:00",
                "last_run_status": "completed",
                "last_run_task_id": 1002,
                "updated_at": "2026-04-17T01:00:00+00:00",
                "payload": {},
            },
        ]
        workers = [
            {
                "worker_type": "scheduler",
                "last_heartbeat_age_seconds": 12,
            }
        ]

        summary = build_auto_dispatch_summary(
            plans,
            worker_items=workers,
            host_alternating_service_active=True,
        )

        self.assertEqual(summary["auto_dispatch_entry"], "crawl_plans")
        self.assertTrue(summary["scheduler_heartbeat_ok"])
        self.assertEqual(summary["enabled_canonical_auto_plan_count"], 2)
        self.assertTrue(summary["auto_dispatch_conflict"])
        self.assertEqual(
            {item["family"] for item in summary["canonical_auto_plans"] if item["enabled"]},
            {"category", "keyword"},
        )

    def test_sync_canonical_auto_plans_reuses_aliases_and_disables_legacy_auto_plans(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = OpsStore(Path(temp_dir) / "ops.db")
            try:
                old_interval_category = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="12h category ready scan x500",
                    created_by="test",
                    schedule_kind="interval",
                    schedule_json={"seconds": 43200},
                    payload={"categories": ["pets"], "default_product_count_per_leaf": 500},
                    enabled=True,
                )
                old_interval_keyword = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="12h keyword pets sports ~4000",
                    created_by="test",
                    schedule_kind="interval",
                    schedule_json={"seconds": 43200},
                    payload={"monitor_config": "config/legacy.json", "noon_count": 30, "amazon_count": 30},
                    enabled=True,
                )
                reusable_category = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="remote-category-daily-all-categories-x1000",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"categories": ["pets"], "default_product_count_per_leaf": 1000},
                    enabled=False,
                )
                reusable_keyword = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name="alternating keyword pets-sports x300 postgres",
                    created_by="test",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"monitor_config": "config/legacy.json", "noon_count": 300, "amazon_count": 300},
                    enabled=False,
                )

                reference_time = datetime(2026, 4, 17, 1, 2, 3, tzinfo=timezone.utc)
                summary = sync_canonical_auto_plans(store, reference_time=reference_time)

                category_plan = store.get_crawl_plan(int(reusable_category["id"]))
                keyword_plan = store.get_crawl_plan(int(reusable_keyword["id"]))
                disabled_category = store.get_crawl_plan(int(old_interval_category["id"]))
                disabled_keyword = store.get_crawl_plan(int(old_interval_keyword["id"]))

                self.assertEqual(category_plan["name"], CANONICAL_CATEGORY_PLAN_NAME)
                self.assertTrue(category_plan["enabled"])
                self.assertEqual(category_plan["schedule_kind"], "interval")
                self.assertEqual(keyword_plan["name"], CANONICAL_KEYWORD_PLAN_NAME)
                self.assertTrue(keyword_plan["enabled"])
                self.assertEqual(keyword_plan["schedule_kind"], "interval")
                self.assertEqual(category_plan["next_run_at"], keyword_plan["next_run_at"])
                self.assertEqual(
                    category_plan["next_run_at"],
                    (reference_time.replace(microsecond=0) + timedelta(seconds=AUTO_DISPATCH_INTERVAL_SECONDS)).isoformat(),
                )
                self.assertFalse(disabled_category["enabled"])
                self.assertFalse(disabled_keyword["enabled"])
                self.assertIn(int(old_interval_category["id"]), summary["disabled_legacy_plan_ids"])
                self.assertIn(int(old_interval_keyword["id"]), summary["disabled_legacy_plan_ids"])

            finally:
                store.close()

    def test_sync_canonical_auto_plans_force_due_aligns_both_plans_to_now(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = OpsStore(Path(temp_dir) / "ops.db")
            try:
                reference_time = datetime(2026, 4, 17, 8, 30, 0, tzinfo=timezone.utc)
                summary = sync_canonical_auto_plans(store, reference_time=reference_time, force_due=True)
                plans = {item["name"]: item for item in store.list_crawl_plans(limit=20)}
                expected_due_at = reference_time.replace(microsecond=0).isoformat()
                self.assertTrue(summary["force_due"])
                self.assertEqual(plans[CANONICAL_CATEGORY_PLAN_NAME]["next_run_at"], expected_due_at)
                self.assertEqual(plans[CANONICAL_KEYWORD_PLAN_NAME]["next_run_at"], expected_due_at)
            finally:
                store.close()

    def test_sync_canonical_auto_plans_disables_duplicate_canonical_family_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = OpsStore(Path(temp_dir) / "ops.db")
            try:
                older = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name=CANONICAL_KEYWORD_PLAN_NAME,
                    created_by="older",
                    schedule_kind="once",
                    schedule_json={"run_at": "2026-04-17T08:00:00+00:00"},
                    payload={"monitor_config": "config/older.json", "noon_count": 50, "amazon_count": 50},
                    enabled=True,
                )
                newer = store.create_crawl_plan(
                    plan_type="keyword_monitor",
                    name=CANONICAL_KEYWORD_PLAN_NAME,
                    created_by="newer",
                    schedule_kind="interval",
                    schedule_json={"seconds": 86400},
                    payload={"monitor_config": "config/newer.json", "noon_count": 300, "amazon_count": 300},
                    enabled=True,
                )

                summary = sync_canonical_auto_plans(
                    store,
                    reference_time=datetime(2026, 4, 17, 9, 0, 0, tzinfo=timezone.utc),
                )
                updated_older = store.get_crawl_plan(int(older["id"]))
                updated_newer = store.get_crawl_plan(int(newer["id"]))

                self.assertFalse(updated_older["enabled"])
                self.assertEqual(updated_older["schedule_kind"], "manual")
                self.assertTrue(updated_newer["enabled"])
                self.assertEqual(updated_newer["name"], CANONICAL_KEYWORD_PLAN_NAME)
                self.assertIn(int(older["id"]), summary["disabled_duplicate_plan_ids"])
            finally:
                store.close()

    def test_sync_canonical_auto_plans_prefers_exact_canonical_name_over_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = OpsStore(Path(temp_dir) / "ops.db")
            try:
                canonical = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name=CANONICAL_CATEGORY_PLAN_NAME,
                    created_by="canonical",
                    schedule_kind="interval",
                    schedule_json={"seconds": 86400},
                    payload={"categories": ["pets"], "default_product_count_per_leaf": 1000},
                    enabled=True,
                )
                alias = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name="alternating category sports-pets priority 500-200 postgres",
                    created_by="alias",
                    schedule_kind="manual",
                    schedule_json={},
                    payload={"categories": ["pets"], "default_product_count_per_leaf": 200},
                    enabled=False,
                )
                summary = sync_canonical_auto_plans(
                    store,
                    reference_time=datetime(2026, 4, 17, 10, 0, 0, tzinfo=timezone.utc),
                )
                updated_canonical = store.get_crawl_plan(int(canonical["id"]))
                updated_alias = store.get_crawl_plan(int(alias["id"]))
                self.assertTrue(updated_canonical["enabled"])
                self.assertEqual(updated_canonical["name"], CANONICAL_CATEGORY_PLAN_NAME)
                self.assertFalse(updated_alias["enabled"])
                self.assertEqual(summary["canonical_auto_plans"][0]["plan_id"], int(canonical["id"]))
            finally:
                store.close()

    def test_sync_canonical_auto_plans_prefers_recently_dispatched_exact_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = OpsStore(Path(temp_dir) / "ops.db")
            try:
                stale = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name=CANONICAL_CATEGORY_PLAN_NAME,
                    created_by="stale",
                    schedule_kind="interval",
                    schedule_json={"seconds": 86400},
                    payload={"categories": ["pets"], "default_product_count_per_leaf": 1000},
                    enabled=True,
                )
                fresh = store.create_crawl_plan(
                    plan_type="category_ready_scan",
                    name=CANONICAL_CATEGORY_PLAN_NAME,
                    created_by="fresh",
                    schedule_kind="interval",
                    schedule_json={"seconds": 86400},
                    payload={"categories": ["sports"], "default_product_count_per_leaf": 1000},
                    enabled=True,
                )
                store.conn.execute(
                    "UPDATE crawl_plans SET last_dispatched_at=? WHERE id=?",
                    ("2026-04-11T00:00:00+00:00", int(stale["id"])),
                )
                store.conn.execute(
                    "UPDATE crawl_plans SET last_dispatched_at=? WHERE id=?",
                    ("2026-04-17T08:48:33+00:00", int(fresh["id"])),
                )
                store.conn.commit()
                summary = sync_canonical_auto_plans(
                    store,
                    reference_time=datetime(2026, 4, 17, 10, 30, 0, tzinfo=timezone.utc),
                )
                updated_stale = store.get_crawl_plan(int(stale["id"]))
                updated_fresh = store.get_crawl_plan(int(fresh["id"]))
                self.assertFalse(updated_stale["enabled"])
                self.assertTrue(updated_fresh["enabled"])
                self.assertEqual(summary["canonical_auto_plans"][0]["plan_id"], int(fresh["id"]))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
