from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.progress_reporter import build_progress


class ProgressReporterTests(unittest.TestCase):
    def test_partial_visible_stage_sits_between_sync_and_final_visibility(self):
        payload = build_progress(
            "partial_visible",
            message="incremental sync already visible",
            metrics={"persisted_observations": 10},
        )

        stage_map = {item["key"]: item["status"] for item in payload["stages"]}

        self.assertEqual(stage_map["runtime_collecting"], "completed")
        self.assertEqual(stage_map["stage_persisted"], "completed")
        self.assertEqual(stage_map["warehouse_syncing"], "completed")
        self.assertEqual(stage_map["partial_visible"], "active")
        self.assertEqual(stage_map["web_visible"], "pending")


if __name__ == "__main__":
    unittest.main()
