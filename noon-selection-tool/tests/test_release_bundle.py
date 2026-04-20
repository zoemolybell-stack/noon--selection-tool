import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

import build_release_bundle as bundle


class ReleaseBundleTests(unittest.TestCase):
    def test_should_exclude_runtime_and_env_files(self):
        self.assertTrue(bundle.should_exclude(bundle.ROOT / "data" / "product_store.db"))
        self.assertTrue(bundle.should_exclude(bundle.ROOT / ".env"))
        self.assertTrue(bundle.should_exclude(bundle.ROOT / "runtime_data" / "sample.json"))
        self.assertFalse(bundle.should_exclude(bundle.ROOT / "Dockerfile"))

    def test_write_release_bundle_creates_archive_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path, manifest_path = bundle.write_release_bundle(Path(tmpdir), "test-release")
            self.assertTrue(archive_path.exists())
            self.assertTrue(manifest_path.exists())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["release_version"], "test-release")
            self.assertIn("docs/NAS_DEPLOYMENT_RUNBOOK.md", manifest["required_docs"])

            with ZipFile(archive_path) as zf:
                names = set(zf.namelist())
                self.assertIn("release-manifest.json", names)
                self.assertIn("docker-compose.yml", names)
                self.assertNotIn(".env", names)


if __name__ == "__main__":
    unittest.main()
