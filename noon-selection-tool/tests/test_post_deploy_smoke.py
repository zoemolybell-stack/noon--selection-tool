import unittest
from unittest.mock import patch

import run_post_deploy_smoke as smoke


class PostDeploySmokeTests(unittest.TestCase):
    @patch("run_post_deploy_smoke.get_json")
    def test_run_smoke_reads_core_endpoints(self, mock_get_json):
        mock_get_json.return_value = {"status": "ok"}
        result = smoke.run_smoke("http://127.0.0.1:8865", create_sync_smoke=False)
        self.assertEqual(result["status"], "completed")
        self.assertIn("/api/health", result["checks"])
        self.assertEqual(mock_get_json.call_count, 5)

    @patch("run_post_deploy_smoke.post_json")
    @patch("run_post_deploy_smoke.get_json")
    def test_run_smoke_can_create_sync_task(self, mock_get_json, mock_post_json):
        mock_get_json.return_value = {"status": "ok"}
        mock_post_json.return_value = {"id": 9, "task_type": "warehouse_sync", "status": "pending"}
        result = smoke.run_smoke("http://127.0.0.1:8865", create_sync_smoke=True)
        self.assertEqual(result["created_task"]["id"], 9)
        self.assertEqual(mock_post_json.call_count, 1)


if __name__ == "__main__":
    unittest.main()
