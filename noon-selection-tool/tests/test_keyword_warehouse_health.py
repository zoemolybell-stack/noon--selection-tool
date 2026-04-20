from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.keyword_warehouse_health import build_keyword_warehouse_health_payload


class KeywordWarehouseHealthTests(unittest.TestCase):
    def test_build_keyword_warehouse_health_payload(self):
        payload = build_keyword_warehouse_health_payload(
            overview_row={
                "keyword_source_db_count": 1,
                "keyword_catalog_count": 205,
                "keyword_metric_snapshot_count": 7,
                "keyword_run_count": 24,
                "active_keyword_run_count": 2,
                "incomplete_keyword_run_count": 2,
                "last_keyword_imported_at": "2026-03-29T13:29:48.678951",
                "last_keyword_run_started_at": "2026-03-29T14:18:38.625207",
                "last_keyword_run_finished_at": "2026-03-29T14:17:37.677639",
            },
            status_rows=[
                {"status": "completed", "count": 20},
                {"status": "running", "count": 2},
                {"status": "failed", "count": 2},
            ],
            recent_imports=[
                {
                    "source_label": "runtime_data__keyword__product_store_db",
                    "imported_at": "2026-03-29T13:29:48.678951",
                    "source_keyword_count": 205,
                    "source_observation_count": 950,
                }
            ],
            recent_runs=[
                {
                    "run_type": "crawl",
                    "trigger_mode": "tracked",
                    "seed_keyword": "dog food",
                    "snapshot_id": "2026-03-29_133002",
                    "status": "running",
                    "keyword_count": 10,
                    "started_at": "2026-03-29T14:18:38.625207",
                    "finished_at": None,
                    "metadata_json": json.dumps(
                        {
                            "quality_state": "degraded",
                            "quality_summary": {
                                "state": "degraded",
                                "quality_evidence": ["beautifulsoup4_unavailable", "amazon_bsr_missing"],
                                "quality_flags": ["beautifulsoup4_unavailable"],
                            },
                            "quality_source_breakdown": {
                                "crawl": {
                                    "state": "degraded",
                                    "reason_codes": ["beautifulsoup4_unavailable"],
                                    "primary_reason": "beautifulsoup4_unavailable",
                                }
                            },
                        }
                    ),
                }
            ],
        )
        summary = payload["summary"]
        self.assertEqual(summary["keyword_source_db_count"], 1)
        self.assertEqual(summary["keyword_catalog_count"], 205)
        self.assertEqual(summary["active_keyword_run_count"], 1)
        self.assertEqual(summary["active_keyword_run_row_count"], 2)
        self.assertEqual(summary["incomplete_keyword_run_row_count"], 2)
        self.assertEqual(summary["status_breakdown"]["completed"], 20)
        self.assertEqual(payload["recent_runs"][0]["status"], "running")
        self.assertEqual(payload["recent_runs"][0]["quality_state"], "degraded")
        self.assertIn(payload["recent_runs"][0]["quality_reasons"][0], {"runtime_import_error", "dependency_missing"})
        self.assertIsNotNone(summary["last_keyword_import_age_seconds"])
        self.assertIn(summary["import_freshness_state"], {"fresh", "delayed", "stale", "unknown"})
        self.assertIn(summary["run_activity_state"], {"active", "recent", "cooldown", "idle", "unknown"})
        self.assertTrue(summary["lag_hint"])
        self.assertEqual(summary["latest_quality_state"], "degraded")
        self.assertEqual(summary["latest_terminal_quality_state"], "unknown")
        self.assertEqual(summary["latest_quality_source_breakdown"]["crawl"]["state"], "degraded")
        self.assertEqual(summary["live_batch_state"], "degraded")
        self.assertEqual(summary["operator_quality_state"], "degraded")
        self.assertIn(summary["quality_reasons"][0], {"runtime_import_error", "dependency_missing", "signal_missing:amazon_bsr"})
        self.assertNotIn("beautifulsoup4_unavailable", summary["quality_reasons"])
        self.assertEqual(summary["evidence"], summary["quality_evidence"])
        self.assertEqual(summary["quality_health_state"], "degraded")
        self.assertEqual(summary["quality_state_breakdown"]["degraded"], 1)
        self.assertIn("quality degraded", summary["quality_status_summary"])


if __name__ == "__main__":
    unittest.main()
