from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tools.write_crawl_report as report


class WriteCrawlReportTests(unittest.TestCase):
    def test_baseline_uses_latest_snapshot_before_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_root = Path(tmpdir)
            state_dir = report_root / "state"
            state_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            old_snapshot = {"ts": (now - timedelta(hours=25)).isoformat(), "product_count": 100, "observation_count": 200}
            mid_snapshot = {"ts": (now - timedelta(hours=3)).isoformat(), "product_count": 140, "observation_count": 260}
            latest_snapshot = {"ts": (now - timedelta(minutes=15)).isoformat(), "product_count": 150, "observation_count": 280}
            metrics_path = state_dir / "metrics_snapshots.jsonl"
            metrics_path.write_text(
                "\n".join(json.dumps(item) for item in (old_snapshot, mid_snapshot, latest_snapshot)),
                encoding="utf-8",
            )

            previous_metrics = report.METRICS_FILE
            previous_root = report.REPORT_ROOT
            previous_latest_symlink = report.CURRENT_SYMLINK
            try:
                report.METRICS_FILE = metrics_path
                report.REPORT_ROOT = report_root
                report.CURRENT_SYMLINK = report_root / "current"
                snapshots = report.load_snapshots(now - timedelta(hours=72), now)
                baseline = report.select_snapshot_before(snapshots, now - timedelta(hours=24))

                self.assertEqual(len(snapshots), 3)
                self.assertEqual(baseline["product_count"], 100)
                self.assertEqual(baseline["observation_count"], 200)
                self.assertEqual(report.format_delta(150, 100), "+50")
            finally:
                report.METRICS_FILE = previous_metrics
                report.REPORT_ROOT = previous_root
                report.CURRENT_SYMLINK = previous_latest_symlink

    def test_backfill_report_uses_report_date_and_preserves_latest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_root = Path(tmpdir)
            (report_root / "state").mkdir(parents=True, exist_ok=True)
            (report_root / "daily").mkdir(parents=True, exist_ok=True)
            (report_root / "latest.md").write_text("sentinel-latest", encoding="utf-8")
            (report_root / "latest.json").write_text(json.dumps({"sentinel": True}), encoding="utf-8")

            previous_root = report.REPORT_ROOT
            previous_state_dir = report.STATE_DIR
            previous_daily_dir = report.DAILY_DIR
            previous_current_symlink = report.CURRENT_SYMLINK
            previous_health = report.safe_request_json
            previous_snapshots = report.load_snapshots
            previous_select = report.select_snapshot_before
            previous_release = report.current_release_label
            try:
                report.REPORT_ROOT = report_root
                report.STATE_DIR = report_root / "state"
                report.DAILY_DIR = report_root / "daily"
                report.CURRENT_SYMLINK = report_root / "current"

                def fake_safe_request_json(path: str):
                    if path == "/api/health":
                        return ({"status": "ok", "warehouse_db": "postgres", "product_count": 1, "observation_count": 2}, None)
                    if path == "/api/system/health":
                        return ({"status": "ok", "ops": {"worker_count": 0, "workers": [], "recent_runs": []}}, None)
                    if path == "/api/dashboard":
                        return ({"overview": {"product_count": 1, "keyword_count": 1, "overlap_count": 0, "last_sync_at": None}, "scope": {"child_categories": []}}, None)
                    if path == "/api/tasks?limit=500":
                        return ({"items": []}, None)
                    return ({}, None)

                report.safe_request_json = fake_safe_request_json
                report.load_snapshots = lambda *_args, **_kwargs: []
                report.select_snapshot_before = lambda *_args, **_kwargs: None
                report.current_release_label = lambda: "test-release"

                report_day = datetime(2026, 4, 7, tzinfo=timezone.utc).date()
                body, metadata = report.build_report(report_day=report_day, backfilled=True)
                self.assertEqual(metadata["report_date"], "2026-04-07")
                self.assertEqual(metadata["report_kind"], "backfill")
                self.assertTrue(metadata["backfilled"])

                out_path = report.write_outputs(body, metadata, update_latest=False)
                self.assertTrue(out_path.name.startswith("crawl_report_20260407_"))
                self.assertEqual((report_root / "latest.md").read_text(encoding="utf-8"), "sentinel-latest")
                self.assertEqual(json.loads((report_root / "latest.json").read_text(encoding="utf-8"))["sentinel"], True)
                self.assertTrue((report_root / "daily" / out_path.name).exists())
                self.assertTrue((report_root / "daily" / out_path.with_suffix(".json").name).exists())
            finally:
                report.REPORT_ROOT = previous_root
                report.STATE_DIR = previous_state_dir
                report.DAILY_DIR = previous_daily_dir
                report.CURRENT_SYMLINK = previous_current_symlink
                report.safe_request_json = previous_health
                report.load_snapshots = previous_snapshots
                report.select_snapshot_before = previous_select
                report.current_release_label = previous_release


if __name__ == "__main__":
    unittest.main()
