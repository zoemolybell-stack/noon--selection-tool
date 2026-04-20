from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_beta.app import resolve_effective_signal_payload


class WebStickySignalTests(unittest.TestCase):
    def test_resolve_effective_signal_payload_prefers_sticky_text_when_latest_is_blank(self):
        summary = {
            "latest_observed_at": "2026-03-25T00:00:00+00:00",
            "sticky_sold_recently_text": "120+ sold recently",
            "sticky_sold_recently_seen_at": "2026-03-20T00:00:00+00:00",
            "sticky_stock_signal_text": "Only 2 left in stock",
            "sticky_stock_signal_seen_at": "2026-03-21T00:00:00+00:00",
        }
        latest_observation = {
            "scraped_at": "2026-03-25T00:00:00+00:00",
            "sold_recently_text": "",
            "stock_signal_text": "",
        }

        payload = resolve_effective_signal_payload(summary, latest_observation)

        self.assertEqual(payload["sold_recently_text"], "120+ sold recently")
        self.assertEqual(payload["sold_recently_text_last_seen_at"], "2026-03-20T00:00:00+00:00")
        self.assertTrue(payload["sold_recently_text_is_sticky"])
        self.assertEqual(payload["stock_signal_text"], "Only 2 left in stock")
        self.assertEqual(payload["stock_signal_text_last_seen_at"], "2026-03-21T00:00:00+00:00")
        self.assertTrue(payload["stock_signal_text_is_sticky"])


if __name__ == "__main__":
    unittest.main()
