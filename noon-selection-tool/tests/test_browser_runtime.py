from __future__ import annotations

import socket
import sys
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.browser_runtime import page_looks_access_denied, resolve_browser_cdp_endpoint


class BrowserRuntimeTests(unittest.TestCase):
    def test_resolve_browser_cdp_endpoint_prefers_ipv4(self):
        infos = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.65.254", 9222)),
        ]
        with mock.patch("scrapers.browser_runtime.socket.getaddrinfo", return_value=infos):
            resolved = resolve_browser_cdp_endpoint("http://host.docker.internal:9222")
        self.assertEqual(resolved, "http://192.168.65.254:9222")

    def test_page_looks_access_denied_detects_akamai_denial(self):
        self.assertTrue(
            page_looks_access_denied(
                title="Access Denied",
                body_text="You don't have permission to access this resource. Reference #18. errors.edgesuite.net",
            )
        )

    def test_page_looks_access_denied_ignores_normal_category_page(self):
        self.assertFalse(
            page_looks_access_denied(
                title="Pet Supplies KSA | Best Price Offers | Riyadh, Jeddah",
                body_text="Dog Supplies Cat Supplies Pet Food Best Price Offers",
            )
        )

    def test_page_looks_access_denied_detects_cloudflare_error_page(self):
        self.assertTrue(
            page_looks_access_denied(
                title="Bad gateway",
                body_text="Cloudflare Tunnel error The host is configured as a Cloudflare Tunnel and Cloudflare is currently unable to resolve it.",
            )
        )


if __name__ == "__main__":
    unittest.main()
