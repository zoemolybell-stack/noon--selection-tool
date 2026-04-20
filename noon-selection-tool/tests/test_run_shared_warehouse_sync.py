from __future__ import annotations
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_shared_warehouse_sync import (
    DEFAULT_KEYWORD_STAGE_DB,
    DEFAULT_TRIGGER_DB,
    RESULT_PREFIX,
    _build_builder_command,
    _extract_embedded_result,
    main,
)


class SharedWarehouseSyncCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.python_executable = "python"
        self.builder_script = Path("D:/claude noon v1/noon-selection-tool/build_analytics_warehouse.py")
        self.warehouse_db = Path("D:/tmp/isolated_warehouse.db")

    def test_default_category_trigger_uses_default_keyword_selection(self) -> None:
        command = _build_builder_command(
            python_executable=self.python_executable,
            builder_script=self.builder_script,
            trigger_db=DEFAULT_TRIGGER_DB,
            warehouse_db=self.warehouse_db,
            builder_args=[],
        )

        self.assertIn("--warehouse-db", command)
        self.assertIn(str(self.warehouse_db), command)
        self.assertIn("--category-db", command)
        self.assertIn(str(DEFAULT_TRIGGER_DB.resolve()), command)
        self.assertNotIn("--keyword-dbs", command)

    def test_default_keyword_trigger_uses_explicit_keyword_db(self) -> None:
        command = _build_builder_command(
            python_executable=self.python_executable,
            builder_script=self.builder_script,
            trigger_db=DEFAULT_KEYWORD_STAGE_DB,
            warehouse_db=self.warehouse_db,
            builder_args=[],
        )

        self.assertIn("--warehouse-db", command)
        self.assertIn("--keyword-dbs", command)
        self.assertIn(str(DEFAULT_KEYWORD_STAGE_DB.resolve()), command)
        self.assertNotIn("--category-db", command)

    def test_custom_trigger_is_treated_as_shared_isolated_stage(self) -> None:
        trigger_db = Path("D:/tmp/local_test_stage.db")
        command = _build_builder_command(
            python_executable=self.python_executable,
            builder_script=self.builder_script,
            trigger_db=trigger_db,
            warehouse_db=self.warehouse_db,
            builder_args=["--replace"],
        )

        self.assertIn("--category-db", command)
        self.assertIn("--keyword-dbs", command)
        self.assertIn(str(trigger_db.resolve()), command)
        self.assertIn("--replace", command)

    def test_postgres_trigger_is_forwarded_as_explicit_stage_dsn(self) -> None:
        trigger_dsn = "postgresql://noon:noon_local_dev@localhost:5433/noon_stage"
        command = _build_builder_command(
            python_executable=self.python_executable,
            builder_script=self.builder_script,
            trigger_db=trigger_dsn,
            warehouse_db="postgresql://noon:noon_local_dev@localhost:5433/noon_warehouse",
            builder_args=[],
        )

        self.assertIn("--category-db", command)
        self.assertIn("--keyword-dbs", command)
        self.assertIn(trigger_dsn, command)
        self.assertIn("postgresql://noon:noon_local_dev@localhost:5433/noon_warehouse", command)

    def test_extract_embedded_result_returns_last_valid_payload(self) -> None:
        lines = [
            "04:35:39 [shared-warehouse-sync] INFO: shared warehouse sync skipped: lock_active",
            f"{RESULT_PREFIX}not-json",
            f'{RESULT_PREFIX}{{"status":"skipped","skip_reason":"lock_active"}}',
        ]

        payload = _extract_embedded_result(lines)

        self.assertEqual(payload, {"status": "skipped", "skip_reason": "lock_active"})

    def test_extract_embedded_result_returns_none_without_result_prefix(self) -> None:
        payload = _extract_embedded_result(["plain stdout", "another line"])
        self.assertIsNone(payload)

    def test_main_finalizes_lock_when_builder_crashes(self) -> None:
        with unittest.mock.patch("run_shared_warehouse_sync.parse_args") as parse_args_mock, \
             unittest.mock.patch("run_shared_warehouse_sync.warehouse_sync.acquire_sync_lock") as acquire_mock, \
             unittest.mock.patch("run_shared_warehouse_sync.subprocess.run", side_effect=RuntimeError("boom")), \
             unittest.mock.patch("run_shared_warehouse_sync.warehouse_sync.finalize_sync_state") as finalize_mock, \
             unittest.mock.patch("run_shared_warehouse_sync._emit_result") as emit_mock:
            parse_args_mock.return_value = unittest.mock.Mock(
                actor="tester",
                reason="sync-test",
                trigger_db=str(DEFAULT_TRIGGER_DB),
                warehouse_db=str(self.warehouse_db),
                builder_args=[],
            )
            acquire_mock.return_value = {"status": "running", "lock_token": "lock-1"}
            finalize_mock.return_value = {"status": "failed"}

            exit_code = main()

        self.assertEqual(exit_code, 1)
        finalize_mock.assert_called_once()
        emit_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
