from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import Settings
from scrapers.noon_scraper import NoonScraper, _classify_empty_results_page


class NoonScraperContractTests(unittest.TestCase):
    def test_empty_page_after_expected_total_is_treated_as_results_exhausted(self):
        outcome = _classify_empty_results_page(
            page_num=2,
            collected_count=30,
            total_results=30,
            target_count=300,
            visible_name_count=0,
        )

        self.assertEqual(outcome["kind"], "results_exhausted")
        self.assertEqual(outcome["page_state"], "results")
        self.assertEqual(outcome["failure_category"], "")

    def test_visible_names_without_parsed_products_is_parse_failure(self):
        outcome = _classify_empty_results_page(
            page_num=2,
            collected_count=24,
            total_results=80,
            target_count=300,
            visible_name_count=12,
        )

        self.assertEqual(outcome["kind"], "page_parse_failure")
        self.assertEqual(outcome["failure_category"], "page_recognition_failed")
        self.assertEqual(outcome["page_state"], "partial_results")
        self.assertIn("page_parse_failure:visible_names_12", outcome["error_evidence"])


class _FakeLocator:
    def __init__(self, count_value: int):
        self._count_value = count_value

    async def count(self):
        return self._count_value


class _FakeTimeoutPage:
    def __init__(self, *, current_url: str, visible_name_count: int):
        self.url = current_url
        self._visible_name_count = visible_name_count
        self.goto_calls = 0

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls += 1
        raise RuntimeError(f"Page.goto: Timeout {timeout}ms exceeded while loading {url}")

    async def wait_for_timeout(self, ms):
        return None

    def locator(self, selector):
        return _FakeLocator(self._visible_name_count)


class NoonScraperRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_with_visible_cards_is_salvaged(self):
        settings = Settings()
        scraper = NoonScraper(settings)
        page = _FakeTimeoutPage(
            current_url="https://www.noon.com/saudi-en/search/?q=foam%20roller&page=2",
            visible_name_count=6,
        )

        await scraper._goto_search_page_with_retry(
            page,
            "https://www.noon.com/saudi-en/search/?q=foam%20roller&page=2",
        )

        self.assertEqual(page.goto_calls, 1)

    async def test_timeout_without_visible_cards_still_raises(self):
        settings = Settings()
        scraper = NoonScraper(settings)
        page = _FakeTimeoutPage(
            current_url="https://www.noon.com/saudi-en/search/?q=foam%20roller&page=2",
            visible_name_count=0,
        )

        with self.assertRaises(RuntimeError):
            await scraper._goto_search_page_with_retry(
                page,
                "https://www.noon.com/saudi-en/search/?q=foam%20roller&page=2",
            )

        self.assertEqual(page.goto_calls, 2)


if __name__ == "__main__":
    unittest.main()
