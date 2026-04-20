import unittest

import run_local_compose_smoke as smoke


class LocalComposeSmokeTests(unittest.TestCase):
    def test_required_files_exist_in_repository(self):
        checks = smoke.file_checks()
        self.assertTrue(checks["Dockerfile"])
        self.assertTrue(checks["docker-compose.yml"])
        self.assertTrue(checks["run_task_scheduler.py"])
        self.assertTrue(checks["task_cli.py"])

    def test_docker_available_returns_boolean(self):
        self.assertIsInstance(smoke.docker_available(), bool)


if __name__ == "__main__":
    unittest.main()
