from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import Settings


class SettingsNoonEnvTests(unittest.TestCase):
    def test_noon_prefixed_runtime_envs_are_respected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "NOON_RUNTIME_SCOPE": "keyword",
                "NOON_DATA_ROOT": str(Path(temp_dir) / "runtime_data"),
                "NOON_OPS_DB": str(Path(temp_dir) / "ops" / "ops.db"),
                "NOON_BROWSER_HEADLESS": "true",
                "NOON_BROWSER_PROFILE_ROOT": str(Path(temp_dir) / "profiles"),
                "NOON_BROWSER_EXECUTABLE_PATH": "/usr/bin/chromium",
                "NOON_WORKER_TYPE": "keyword",
                "NOON_MAX_CONCURRENT_TASKS": "2",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                settings = Settings()

            self.assertEqual(settings.runtime_scope_name, "keyword")
            self.assertEqual(settings.data_dir, Path(env["NOON_DATA_ROOT"]))
            self.assertEqual(settings.ops_db_path, Path(env["NOON_OPS_DB"]))
            self.assertTrue(settings.browser_headless)
            self.assertEqual(settings.browser_profile_root_path, Path(env["NOON_BROWSER_PROFILE_ROOT"]))
            self.assertEqual(settings.browser_executable_path_value, "/usr/bin/chromium")
            self.assertEqual(settings.worker_type, "keyword")
            self.assertEqual(settings.max_concurrent_tasks, 2)


if __name__ == "__main__":
    unittest.main()
