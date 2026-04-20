from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.keyword_opportunity_engine import (
    build_keyword_graph_payload,
    build_opportunity_items,
    build_opportunity_summary,
    build_quality_issue_summary,
    classify_opportunity,
    keyword_quality_flags,
)


class KeywordOpportunityEngineTests(unittest.TestCase):
    def test_classify_opportunity_prefers_supply_gap(self):
        item = classify_opportunity(
            {
                "keyword": "dog food",
                "display_keyword": "dog food",
                "latest_total_score": 8.2,
                "previous_total_score": 7.1,
                "supply_gap_ratio": 6.5,
                "competition_density": 1.2,
                "margin_war_pct": 24.0,
                "margin_peace_pct": 28.0,
                "amazon_total": 1200,
                "noon_total": 80,
                "matched_product_count": 35,
                "metadata_json": "{\"root_seed_keyword\":\"pet supplies\",\"expansion_depth\":1}",
            }
        )
        self.assertEqual(item["opportunity_type"], "supply_gap")
        self.assertEqual(item["root_seed_keyword"], "pet supplies")
        self.assertGreater(item["opportunity_score"], 40)
        self.assertIn("amazon_noon_gap", item["reason_codes"])
        self.assertTrue(item["action_hint"])
        self.assertIn(item["priority_band"], {"critical", "high", "medium", "watch"})
        self.assertIn(item["evidence_strength"], {"strong", "moderate", "emerging", "thin"})
        self.assertTrue(item["decision_summary"])

    def test_keyword_quality_flags_detects_noise(self):
        flags = keyword_quality_flags("shoes x x x 2025 2025")
        self.assertIn("fragment_token", flags)
        self.assertIn("duplicate_tokens", flags)

    def test_build_opportunity_items_sorts_by_score(self):
        rows = [
            {
                "keyword": "keyword a",
                "display_keyword": "keyword a",
                "latest_total_score": 5.0,
                "previous_total_score": 4.8,
                "supply_gap_ratio": 1.2,
                "competition_density": 6.0,
                "margin_war_pct": 8.0,
                "amazon_total": 100,
                "noon_total": 80,
                "matched_product_count": 5,
                "metadata_json": "{}",
            },
            {
                "keyword": "keyword b",
                "display_keyword": "keyword b",
                "latest_total_score": 7.5,
                "previous_total_score": 6.0,
                "supply_gap_ratio": 5.5,
                "competition_density": 1.0,
                "margin_war_pct": 20.0,
                "amazon_total": 600,
                "noon_total": 60,
                "matched_product_count": 20,
                "metadata_json": "{}",
            },
        ]
        items = build_opportunity_items(rows, limit=10)
        self.assertEqual(items[0]["keyword"], "keyword b")

    def test_build_opportunity_items_prefers_priority_band(self):
        rows = [
            {
                "keyword": "keyword watch",
                "display_keyword": "keyword watch",
                "latest_total_score": 7.2,
                "previous_total_score": 6.9,
                "supply_gap_ratio": 1.3,
                "competition_density": 1.8,
                "margin_war_pct": 7.0,
                "amazon_total": 90,
                "noon_total": 25,
                "matched_product_count": 10,
                "metadata_json": "{}",
            },
            {
                "keyword": "keyword critical",
                "display_keyword": "keyword critical",
                "latest_total_score": 7.0,
                "previous_total_score": 6.2,
                "supply_gap_ratio": 6.0,
                "competition_density": 1.0,
                "margin_war_pct": 18.0,
                "amazon_total": 600,
                "noon_total": 30,
                "matched_product_count": 22,
                "metadata_json": "{}",
            },
        ]
        items = build_opportunity_items(rows, limit=10)
        self.assertEqual(items[0]["keyword"], "keyword critical")
        self.assertEqual(items[0]["priority_band"], "critical")

    def test_build_opportunity_summary_counts_types_and_roots(self):
        items = build_opportunity_items(
            [
                {
                    "keyword": "trail running shoes",
                    "display_keyword": "trail running shoes",
                    "latest_total_score": 7.8,
                    "previous_total_score": 6.2,
                    "supply_gap_ratio": 5.0,
                    "competition_density": 1.1,
                    "margin_war_pct": 18.0,
                    "amazon_total": 500,
                    "noon_total": 50,
                    "matched_product_count": 18,
                    "metadata_json": "{\"root_seed_keyword\":\"sports shoes\",\"expansion_depth\":1}",
                },
                {
                    "keyword": "dog harness small",
                    "display_keyword": "dog harness small",
                    "latest_total_score": 5.9,
                    "previous_total_score": 5.6,
                    "supply_gap_ratio": 1.4,
                    "competition_density": 1.8,
                    "margin_war_pct": 9.0,
                    "amazon_total": 90,
                    "noon_total": 14,
                    "matched_product_count": 8,
                    "metadata_json": "{\"root_seed_keyword\":\"pet supplies\",\"expansion_depth\":2}",
                },
            ],
            limit=10,
        )
        summary = build_opportunity_summary(items, limit=10)
        self.assertEqual(summary["available_count"], 2)
        self.assertIn("watchlist", summary["type_counts"])
        self.assertIn("high", summary["priority_counts"])
        self.assertEqual(summary["top_root_keywords"][0]["count"], 1)
        self.assertGreater(summary["avg_opportunity_score"], 0)
        filtered_summary = build_opportunity_summary(
            items,
            limit=10,
            opportunity_type="watchlist",
            priority_band="high",
            evidence_strength="moderate",
        )
        self.assertEqual(filtered_summary["opportunity_type"], "watchlist")
        self.assertEqual(filtered_summary["priority_band"], "high")
        self.assertEqual(filtered_summary["evidence_strength"], "moderate")

    def test_build_quality_issue_summary_counts_flags(self):
        items = build_opportunity_items(
            [
                {
                    "keyword": "shoe x x 2025",
                    "display_keyword": "shoe x x 2025",
                    "latest_total_score": 5.2,
                    "previous_total_score": 5.0,
                    "supply_gap_ratio": 1.3,
                    "competition_density": 2.2,
                    "margin_war_pct": 4.0,
                    "amazon_total": 60,
                    "noon_total": 20,
                    "matched_product_count": 4,
                    "metadata_json": "{\"root_seed_keyword\":\"sports shoes\",\"expansion_depth\":2}",
                }
            ],
            limit=10,
        )
        summary = build_quality_issue_summary(items, limit=10)
        self.assertEqual(summary["available_count"], 1)
        self.assertIn("fragment_token", summary["flag_counts"])

    def test_build_keyword_graph_payload(self):
        payload = build_keyword_graph_payload(
            "pet supplies",
            edge_rows=[
                {
                    "depth": 1,
                    "parent_keyword": "pet supplies",
                    "child_keyword": "dog food",
                    "source_platform": "amazon",
                    "source_type": "autocomplete",
                    "discovered_at": "2026-03-29T10:00:00",
                },
                {
                    "depth": 2,
                    "parent_keyword": "dog food",
                    "child_keyword": "grain free dog food",
                    "source_platform": "noon",
                    "source_type": "autocomplete",
                    "discovered_at": "2026-03-29T10:05:00",
                },
            ],
            metric_rows=[
                {
                    "keyword": "pet supplies",
                    "display_keyword": "pet supplies",
                    "total_score": 7.0,
                    "metadata_json": "{\"expansion_depth\":0}",
                },
                {
                    "keyword": "dog food",
                    "display_keyword": "dog food",
                    "total_score": 8.0,
                    "metadata_json": "{\"expansion_depth\":1}",
                },
            ],
        )
        self.assertEqual(payload["root_keyword"], "pet supplies")
        self.assertEqual(payload["edge_count"], 2)
        self.assertEqual(payload["node_count"], 3)
        node = next(item for item in payload["nodes"] if item["keyword"] == "dog food")
        self.assertIn("priority_band", node)
        self.assertIn("evidence_strength", node)
        self.assertIn("decision_summary", node)


if __name__ == "__main__":
    unittest.main()
