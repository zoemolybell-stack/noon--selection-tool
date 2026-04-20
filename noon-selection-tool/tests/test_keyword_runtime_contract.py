from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import Settings
from keyword_runtime_contract import (
    build_failure_detail,
    classify_platform_payload,
    classify_platform_quality_reasons,
    load_platform_result_payloads,
    result_file_path,
    summarize_platform_snapshot,
)


def build_settings(temp_root: Path, snapshot_id: str = "contract_snapshot") -> Settings:
    settings = Settings()
    settings.set_runtime_scope("keyword")
    settings.set_data_dir(temp_root / "runtime_data" / "keyword")
    settings.set_product_store_db_path(temp_root / "runtime_data" / "keyword" / "product_store.db")
    settings.set_snapshot_id(snapshot_id)
    return settings


class KeywordRuntimeContractTests(unittest.TestCase):
    def test_summarize_platform_snapshot_persists_snapshot_id_and_expected_result_file_on_missing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir), snapshot_id="snapshot_123")

            summary = summarize_platform_snapshot(settings, "amazon", ["spin bike"])

            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["failed_keywords"], ["spin bike"])
            self.assertEqual(len(summary["failure_details"]), 1)
            detail = summary["failure_details"][0]
            self.assertEqual(detail["snapshot_id"], "snapshot_123")
            self.assertTrue(str(detail["expected_result_file"]).endswith(".json"))
            self.assertEqual(detail["platform"], "amazon")
            self.assertEqual(detail["keyword"], "spin bike")

    def test_load_platform_result_payloads_normalizes_payload_shape_from_base_contract(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            path = result_file_path(settings, "noon", "adjustable dumbbell")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "keyword": "adjustable dumbbell",
                        "products": [{"product_id": "N1", "title": "Dumbbell"}],
                        "total_results": "7",
                        "page_state": "results",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            payloads = load_platform_result_payloads(settings, "noon", ["adjustable dumbbell"])

            self.assertEqual(len(payloads), 1)
            self.assertEqual(payloads[0]["load_error"], "")
            payload = payloads[0]["payload"]
            self.assertEqual(payload["suggested_keywords"], [])
            self.assertEqual(payload["error_evidence"], [])
            self.assertEqual(payload["failure_details"], [])

    def test_classify_platform_payload_builds_structured_failure_when_partial_without_details(self):
        payload = {
            "keyword": "cycling bag",
            "products": [{"product_id": "N1"}],
            "page_state": "partial_results",
            "page_url": "https://www.noon.com/search?q=cycling+bag&page=3",
            "page_number": 3,
            "error_evidence": ["partial_results:no_more_cards_page_3", "selector_miss:plp-product-box-name"],
        }

        summary = classify_platform_payload("noon", "cycling bag", payload)

        self.assertEqual(summary["status"], "partial")
        self.assertEqual(len(summary["failure_details"]), 1)
        detail = summary["failure_details"][0]
        self.assertEqual(detail["failure_category"], "page_recognition_failed")
        self.assertEqual(detail["page_url"], "https://www.noon.com/search?q=cycling+bag&page=3")
        self.assertEqual(detail["page_number"], 3)
        self.assertEqual(detail["page_state"], "partial_results")

    def test_build_failure_detail_extends_base_contract_with_snapshot_id(self):
        detail = build_failure_detail(
            platform="amazon",
            keyword="pet stroller",
            failure_category="amazon_parse_failure",
            short_evidence="missing_result_file:pet_stroller.json",
            expected_result_file="D:/snapshots/amazon/pet_stroller.json",
            page_state="error",
            snapshot_id="snapshot_abc",
        )

        self.assertEqual(detail["snapshot_id"], "snapshot_abc")
        self.assertEqual(detail["platform"], "amazon")
        self.assertEqual(detail["keyword"], "pet stroller")

    def test_classify_platform_quality_reasons_maps_missing_result_file_to_result_contract_mismatch(self):
        reasons = classify_platform_quality_reasons(
            "amazon",
            status="failed",
            evidence=["missing_result_file:pet_stroller.json"],
        )

        self.assertEqual(reasons[0], "result_contract_mismatch")

    def test_classify_platform_quality_reasons_maps_noon_missing_result_file_to_result_contract_mismatch(self):
        reasons = classify_platform_quality_reasons(
            "noon",
            status="failed",
            evidence=["missing_result_file:foam_roller.json"],
        )

        self.assertEqual(reasons[0], "result_contract_mismatch")


if __name__ == "__main__":
    unittest.main()
