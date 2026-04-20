import tempfile
import textwrap
import unittest
from pathlib import Path

import validate_nas_env as validator


class ValidateNasEnvTests(unittest.TestCase):
    def test_example_env_passes_base_validation(self):
        env_path = Path(validator.ROOT / ".env.nas.example")
        result = validator.validate_env_file(env_path, require_cloudflare=False)
        self.assertEqual(result["status"], "completed")

    def test_cloudflare_mode_requires_token(self):
        env_path = Path(validator.ROOT / ".env.nas.example")
        result = validator.validate_env_file(env_path, require_cloudflare=True)
        self.assertEqual(result["status"], "failed")
        self.assertIn("TUNNEL_TOKEN", result["missing"])

    def test_non_container_paths_raise_warning(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / ".env.nas"
            env_file.write_text(textwrap.dedent("""
                NOON_WAREHOUSE_DB=D:/tmp/warehouse.db
                NOON_OPS_DB=D:/tmp/ops.db
                NOON_BROWSER_PROFILE_ROOT=D:/tmp/browser_profiles
                NOON_BROWSER_HEADLESS=false
                NOON_MAX_CONCURRENT_TASKS=1
                TASK_LEASE_TIMEOUT_SECONDS=3600
                WORKER_POLL_SECONDS=10
                SCHEDULER_POLL_SECONDS=15
            """).strip(), encoding="utf-8")
            result = validator.validate_env_file(env_file, require_cloudflare=False)
            self.assertEqual(result["status"], "completed")
            self.assertGreaterEqual(len(result["warnings"]), 3)


if __name__ == "__main__":
    unittest.main()
