from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import keyword_main
from config.settings import Settings
from scrapers.base_scraper import BaseScraper, build_keyword_result_stem


class DummyScraper(BaseScraper):
    def __init__(self, settings):
        super().__init__("dummy", settings)
        self.calls: list[str] = []

    async def start_browser(self):
        return None

    async def stop_browser(self):
        return None

    async def random_delay(self):
        return None

    async def scrape_keyword(self, keyword: str) -> dict:
        self.calls.append(keyword)
        return {
            "products": [
                {
                    "product_id": "SKU-1",
                    "title": "Dummy Product",
                }
            ],
            "total_results": 1,
        }


def build_settings(temp_root: Path, snapshot_id: str = "result_safety") -> Settings:
    settings = Settings()
    settings.set_runtime_scope("keyword")
    settings.set_data_dir(temp_root / "runtime_data" / "keyword")
    settings.set_product_store_db_path(temp_root / "runtime_data" / "keyword" / "product_store.db")
    settings.set_snapshot_id(snapshot_id)
    return settings


class BaseScraperResultSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_windows_safe_filename_normalization_is_stable_and_unique(self):
        stem_one = build_keyword_result_stem('dog:food? bowl* xl')
        stem_two = build_keyword_result_stem('dog/food\\ bowl| xl')

        for stem in (stem_one, stem_two):
            self.assertNotRegex(stem, r'[<>:"/\\|?*]')
            self.assertTrue(stem.endswith(stem.split("__")[-1]))

        self.assertNotEqual(stem_one, stem_two)

    async def test_invalid_result_payload_is_not_treated_as_completed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            scraper = DummyScraper(settings)
            result_path = scraper._result_path("cat:tree?")
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text('{"products":', encoding="utf-8")

            self.assertFalse(scraper.is_completed("cat:tree?"))
            self.assertIsNone(scraper.load_result("cat:tree?"))

    async def test_run_rescrapes_when_existing_result_file_is_invalid(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            scraper = DummyScraper(settings)
            keyword = "dog/leash?"
            result_path = scraper._result_path(keyword)
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text('{"_meta":{"platform":"dummy","keyword":"dog/leash?"}}', encoding="utf-8")

            results = await scraper.run([keyword])

            self.assertEqual(scraper.calls, [keyword])
            self.assertEqual(len(results), 1)
            self.assertTrue(scraper.is_completed(keyword))
            persisted = scraper.load_result(keyword)
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted["_meta"]["keyword"], keyword)
            self.assertIn("products", persisted)

    async def test_keyword_main_result_path_uses_same_filename_semantics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            keyword = 'pet carrier: xl/large?'
            scraper = DummyScraper(settings)

            scraper_path = scraper._result_path(keyword)
            runtime_path = keyword_main._result_file_path(settings, "dummy", keyword)

            self.assertEqual(scraper_path.name, runtime_path.name)

    async def test_save_result_normalizes_shared_contract_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = build_settings(Path(temp_dir))
            scraper = DummyScraper(settings)
            keyword = "dog bed"

            scraper.save_result(
                keyword,
                {
                    "products": [{"product_id": "SKU-1", "title": "Dog Bed"}],
                    "total_results": "2",
                    "page_state": "results",
                    "error_evidence": ["marker_missing", "marker_missing"],
                    "failure_details": [
                        {
                            "failure_category": "runtime_error",
                            "short_evidence": " marker_missing ",
                        }
                    ],
                },
            )

            persisted = scraper.load_result(keyword)

            self.assertEqual(persisted["keyword"], keyword)
            self.assertEqual(persisted["suggested_keywords"], [])
            self.assertEqual(persisted["error_evidence"], ["marker_missing"])
            self.assertEqual(persisted["zero_result_evidence"], [])
            self.assertEqual(persisted["failure_details"][0]["platform"], "dummy")
            self.assertEqual(persisted["failure_details"][0]["keyword"], keyword)
            self.assertTrue(persisted["failure_details"][0]["expected_result_file"])


if __name__ == "__main__":
    unittest.main()
