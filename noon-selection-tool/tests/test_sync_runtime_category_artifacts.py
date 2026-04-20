from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import sync_runtime_category_artifacts as artifact_sync


class SyncRuntimeCategoryArtifactsTests(unittest.TestCase):
    def test_push_runtime_category_artifacts_skips_when_runtime_map_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_map_path = Path(temp_dir) / "missing.json"
            payload = artifact_sync.push_runtime_category_artifacts(
                runtime_map_path=runtime_map_path,
                snapshot_id="snap-1",
                source_node="crawler-pc",
            )
        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["reason"], "runtime_map_missing")

    def test_push_runtime_category_artifacts_skips_when_remote_not_configured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_map_path = Path(temp_dir) / "runtime_category_map.json"
            runtime_map_path.write_text(json.dumps({"categories": {}}), encoding="utf-8")
            payload = artifact_sync.push_runtime_category_artifacts(
                runtime_map_path=runtime_map_path,
                snapshot_id="snap-1",
                source_node="crawler-pc",
                remote_host="",
                remote_batch_root="",
            )
        self.assertEqual(payload["status"], "skipped")
        self.assertEqual(payload["reason"], "artifact_sync_not_configured")

    @mock.patch.object(artifact_sync.subprocess, "run")
    def test_push_runtime_category_artifacts_copies_runtime_map_and_metadata(self, run_mock):
        mkdir_proc = mock.Mock(returncode=0, stdout="", stderr="")
        scp_proc = mock.Mock(returncode=0, stdout="", stderr="")
        run_mock.side_effect = [mkdir_proc, scp_proc, scp_proc]

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_map_path = Path(temp_dir) / "runtime_category_map.json"
            runtime_map_path.write_text(json.dumps({"categories": {"sports": {}}}), encoding="utf-8")
            batch_dir = Path(temp_dir) / "batch"
            batch_dir.mkdir(parents=True, exist_ok=True)

            payload = artifact_sync.push_runtime_category_artifacts(
                runtime_map_path=runtime_map_path,
                snapshot_id="snap-1",
                source_node="crawler-pc",
                batch_dir=batch_dir,
                remote_batch_root="/remote/batch_scans",
                remote_host="192.168.100.20",
                remote_user="admin",
                remote_port=22,
            )

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["reason"], "artifact_sync_completed")
        self.assertEqual(payload["remote_dir"], "/remote/batch_scans/snap-1")
        self.assertEqual(payload["transferred_files"], ["runtime_category_map.json", "runtime_category_map.meta.json"])
        self.assertEqual(run_mock.call_count, 3)
        mkdir_command = run_mock.call_args_list[0].args[0]
        runtime_map_command = run_mock.call_args_list[1].args[0]
        metadata_command = run_mock.call_args_list[2].args[0]
        self.assertEqual(mkdir_command[0], "ssh")
        self.assertIn("-O", runtime_map_command)
        self.assertIn("-O", metadata_command)
        self.assertTrue(any(str(arg).startswith("UserKnownHostsFile=") for arg in mkdir_command))
        self.assertTrue(any(str(arg).startswith("UserKnownHostsFile=") for arg in runtime_map_command))


if __name__ == "__main__":
    unittest.main()
