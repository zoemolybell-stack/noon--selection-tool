from __future__ import annotations

import unittest

import validate_tunnel_origin


class ValidateTunnelOriginTests(unittest.TestCase):
    def test_get_network_snapshot_handles_missing(self):
        self.assertEqual(validate_tunnel_origin.get_network_snapshot({}), {})

    def test_get_aliases_returns_list(self):
        snapshot = {
            "huihaokang-stable-net": {
                "Aliases": ["huihaokang-web", "web"],
            }
        }
        self.assertEqual(
            validate_tunnel_origin.get_aliases(snapshot, "huihaokang-stable-net"),
            ["huihaokang-web", "web"],
        )


if __name__ == "__main__":
    unittest.main()
