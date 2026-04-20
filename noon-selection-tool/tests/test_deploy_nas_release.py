from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import deploy_nas_release


class DeployNasReleaseTests(unittest.TestCase):
    def test_load_env_file_reads_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env.nas"
            env_path.write_text("COMPOSE_PROJECT_NAME=custom\nTUNNEL_TOKEN=abc\n", encoding="utf-8")
            values = deploy_nas_release.load_env_file(env_path)
            self.assertEqual(values["COMPOSE_PROJECT_NAME"], "custom")
            self.assertEqual(values["TUNNEL_TOKEN"], "abc")

    def test_build_compose_command_auto_enables_tunnel(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env.nas"
            env_path.write_text("TUNNEL_TOKEN=abc\n", encoding="utf-8")
            command, include_tunnel, scheduler_runtime = deploy_nas_release.build_compose_command(
                env_file=env_path,
                project_name="huihaokang-stable",
                tunnel_mode="auto",
                build_images=True,
            )
            self.assertTrue(include_tunnel)
            self.assertEqual(scheduler_runtime, "container")
            self.assertIn("--profile", command)
            self.assertIn("tunnel", command)
            self.assertIn("--remove-orphans", command)

    def test_build_compose_command_can_disable_tunnel(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env.nas"
            env_path.write_text("TUNNEL_TOKEN=abc\n", encoding="utf-8")
            command, include_tunnel, scheduler_runtime = deploy_nas_release.build_compose_command(
                env_file=env_path,
                project_name="huihaokang-stable",
                tunnel_mode="disabled",
                build_images=False,
            )
            self.assertFalse(include_tunnel)
            self.assertEqual(scheduler_runtime, "container")
            self.assertIn("--profile", command)
            self.assertIn("local-category", command)
            self.assertNotIn("tunnel", command)
            self.assertNotIn("--build", command)

    def test_build_compose_command_skips_local_category_worker_when_remote_node_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env.nas"
            env_path.write_text("NOON_REMOTE_CATEGORY_NODE_ENABLED=true\n", encoding="utf-8")
            command, include_tunnel, scheduler_runtime = deploy_nas_release.build_compose_command(
                env_file=env_path,
                project_name="huihaokang-stable",
                tunnel_mode="disabled",
                build_images=False,
            )
            self.assertFalse(include_tunnel)
            self.assertEqual(scheduler_runtime, "container")
            self.assertIn("keyword-worker", command)
            self.assertIn("sync-worker", command)
            self.assertNotIn("category-worker", command)
            self.assertNotIn("local-category", command)

    @mock.patch("deploy_nas_release.time.sleep")
    @mock.patch("deploy_nas_release.subprocess.run")
    def test_runtime_reconciliation_falls_back_to_web_container_on_missing_psycopg(self, run_mock, sleep_mock):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env.nas"
            env_path.write_text("NOON_OPS_DATABASE_URL=postgres://example\n", encoding="utf-8")

            host_failure = mock.Mock()
            host_failure.returncode = 1
            host_failure.stdout = ""
            host_failure.stderr = "ModuleNotFoundError: No module named 'psycopg'"

            container_success = mock.Mock()
            container_success.returncode = 0
            container_success.stdout = json.dumps({"status": "ok", "deleted_workers": 2})
            container_success.stderr = ""

            run_mock.side_effect = [host_failure, container_success]

            result = deploy_nas_release.run_post_deploy_runtime_reconciliation(
                env_file=env_path,
                deploy_started_at="2026-04-11T01:19:08.932186+00:00",
                wait_seconds=0,
            )

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["stdout"]["status"], "ok")
            self.assertIn("docker", result["command"][0])
            self.assertIn("huihaokang-web", result["command"])
            self.assertEqual(result["fallback_from_host_python"]["status"], "failed")

    @mock.patch("deploy_nas_release.fetch_json")
    def test_health_recheck_allows_remote_category_grace_window(self, fetch_json_mock):
        now_iso = deploy_nas_release.utc_now_iso()
        fetch_json_mock.side_effect = [
            {"status": "ok"},
            {
                "status": "ok",
                "shared_sync": {"state": "completed"},
                "keyword_quality": {"operator_quality_state": "full"},
                "ops": {
                    "worker_summary": {
                        "worker_count": 2,
                        "worker_type_counts": {"keyword": 1, "sync": 1},
                        "node_role_counts": {"keyword": 1, "sync": 1},
                        "remote_category_node_enabled": True,
                        "category_worker_count": 0,
                        "category_worker_heartbeat_state": "missing",
                        "category_worker_heartbeat_present": False,
                        "remote_category_worker_count": 0,
                        "local_category_worker_count": 0,
                        "remote_category_hosts": [],
                        "category_worker_hosts": [],
                    }
                },
            },
            {
                "items": [
                    {
                        "worker_name": "keyword-1",
                        "worker_type": "keyword",
                        "status": "running",
                        "current_task_id": 11,
                        "heartbeat_at": now_iso,
                        "details": {},
                    },
                    {
                        "worker_name": "sync-1",
                        "worker_type": "sync",
                        "status": "idle",
                        "current_task_id": None,
                        "heartbeat_at": now_iso,
                        "details": {},
                    },
                ]
            },
        ]

        result = deploy_nas_release.run_post_deploy_health_recheck(
            base_url="http://127.0.0.1:8865",
            expected_worker_types={"category", "keyword", "sync"},
            deploy_started_at=now_iso,
            remote_category_node_enabled=True,
            remote_category_grace_seconds=300,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["health_status"], "ok")
        self.assertEqual(result["system_status"], "ok")
        self.assertEqual(result["missing_worker_types"], [])
        self.assertEqual(result["category_worker_heartbeat_state"], "grace_pending")
        self.assertTrue(result["category_worker_grace_applied"])
        self.assertEqual(result["remote_category_node_enabled"], True)
        self.assertEqual(result["worker_count"], 2)


if __name__ == "__main__":
    unittest.main()
