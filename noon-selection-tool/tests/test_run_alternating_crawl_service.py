from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import tools.run_alternating_crawl_service as runner


class AlternatingCrawlServiceTests(unittest.TestCase):
    def test_launch_plan_raises_no_dispatchable_error_for_404_exhausted_plan(self) -> None:
        error = runner.RunnerApiError(
            "POST",
            "/api/crawler/plans/9/launch",
            404,
            '{"detail":"no dispatchable round items for plan: 9"}',
        )
        with mock.patch.object(runner, "request_json", side_effect=error):
            with self.assertRaises(runner.NoDispatchablePlanError):
                runner.launch_plan(9)

    def test_maybe_run_phase_sync_skips_duplicate_after_self_syncing_task(self) -> None:
        phase = {"key": "category", "plan_type": "category_ready_scan", "name": "Category", "payload": {}, "sync_reason": "category phase"}
        terminal = {"status": "completed", "task_type": "category_ready_scan"}
        with (
            mock.patch.object(runner, "create_sync_task") as create_sync_task_mock,
            mock.patch.object(runner, "wait_sync") as wait_sync_mock,
            mock.patch.object(runner, "log"),
            mock.patch.object(runner, "append_snapshot"),
        ):
            runner.maybe_run_phase_sync(1, phase, 123, terminal)

        create_sync_task_mock.assert_not_called()
        wait_sync_mock.assert_not_called()

    def test_maybe_run_phase_sync_triggers_single_formal_sync_for_non_self_syncing_task(self) -> None:
        phase = {"key": "category", "plan_type": "category_ready_scan", "name": "Category", "payload": {}, "sync_reason": "category phase"}
        terminal = {"status": "failed", "task_type": "category_ready_scan"}
        sync_terminal = {"status": "completed", "task_type": "warehouse_sync", "id": 456}
        with (
            mock.patch.object(runner, "create_sync_task", return_value=456) as create_sync_task_mock,
            mock.patch.object(runner, "wait_sync", return_value=sync_terminal) as wait_sync_mock,
            mock.patch.object(runner, "log"),
            mock.patch.object(runner, "append_snapshot"),
        ):
            runner.maybe_run_phase_sync(1, phase, 123, terminal)

        create_sync_task_mock.assert_called_once()
        wait_sync_mock.assert_called_once_with(456)

    def test_run_phase_skips_no_dispatchable_plan_without_crashing(self) -> None:
        phase = {
            "key": "category",
            "plan_type": "category_ready_scan",
            "name": "Category",
            "payload": {},
            "sync_reason": "category phase",
        }
        with (
            mock.patch.object(runner, "ensure_plan", return_value=9),
            mock.patch.object(runner, "find_active_plan_task", return_value=None),
            mock.patch.object(
                runner,
                "launch_plan",
                side_effect=runner.NoDispatchablePlanError(
                    "POST",
                    "/api/crawler/plans/9/launch",
                    404,
                    '{"detail":"no dispatchable round items for plan: 9"}',
                ),
            ),
            mock.patch.object(runner, "maybe_run_phase_sync") as maybe_run_phase_sync_mock,
            mock.patch.object(runner, "append_snapshot") as append_snapshot_mock,
            mock.patch.object(runner, "log"),
            mock.patch.object(runner.time, "sleep") as sleep_mock,
        ):
            runner.run_phase(1, 0, phase)

        maybe_run_phase_sync_mock.assert_not_called()
        sleep_mock.assert_called_once_with(runner.EMPTY_PLAN_SLEEP_SECONDS)
        appended_events = [call.args[0] for call in append_snapshot_mock.call_args_list]
        self.assertIn("phase_skipped", appended_events)
        self.assertIn("slot_ended", appended_events)


if __name__ == "__main__":
    unittest.main()
