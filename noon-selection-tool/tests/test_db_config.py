from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.config import (
    get_ops_database_config,
    get_product_store_database_config,
    get_warehouse_database_config,
    require_sqlite_database,
)
from config.settings import Settings


class DatabaseConfigTests(unittest.TestCase):
    def test_ops_defaults_to_sqlite_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            default_path = Path(temp_dir) / "ops.db"
            previous = os.environ.pop("NOON_OPS_DATABASE_URL", None)
            previous_path = os.environ.pop("NOON_OPS_DB", None)
            try:
                config = get_ops_database_config(default_path)
                self.assertEqual(config.backend, "sqlite")
                self.assertEqual(config.sqlite_path, default_path)
            finally:
                if previous is not None:
                    os.environ["NOON_OPS_DATABASE_URL"] = previous
                if previous_path is not None:
                    os.environ["NOON_OPS_DB"] = previous_path

    def test_ops_supports_postgres_url(self):
        previous = os.environ.get("NOON_OPS_DATABASE_URL")
        try:
            os.environ["NOON_OPS_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/noon_ops"
            config = get_ops_database_config(Path("unused.db"))
            self.assertEqual(config.backend, "postgres")
            self.assertEqual(config.dsn, "postgresql://user:pass@localhost:5432/noon_ops")
        finally:
            if previous is None:
                os.environ.pop("NOON_OPS_DATABASE_URL", None)
            else:
                os.environ["NOON_OPS_DATABASE_URL"] = previous

    def test_product_store_supports_postgres_url(self):
        previous = os.environ.get("NOON_PRODUCT_STORE_DATABASE_URL")
        try:
            os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/noon_stage"
            config = get_product_store_database_config(Path("unused.db"))
            self.assertEqual(config.backend, "postgres")
            self.assertEqual(config.dsn, "postgresql://user:pass@localhost:5432/noon_stage")
        finally:
            if previous is None:
                os.environ.pop("NOON_PRODUCT_STORE_DATABASE_URL", None)
            else:
                os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = previous

    def test_warehouse_path_still_uses_sqlite_env(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "warehouse.db"
            previous = os.environ.get("NOON_WAREHOUSE_DB")
            previous_url = os.environ.pop("NOON_WAREHOUSE_DATABASE_URL", None)
            try:
                os.environ["NOON_WAREHOUSE_DB"] = str(db_path)
                config = get_warehouse_database_config(Path("unused.db"))
                self.assertEqual(require_sqlite_database(config, "warehouse"), db_path)
            finally:
                if previous is None:
                    os.environ.pop("NOON_WAREHOUSE_DB", None)
                else:
                    os.environ["NOON_WAREHOUSE_DB"] = previous
                if previous_url is not None:
                    os.environ["NOON_WAREHOUSE_DATABASE_URL"] = previous_url

    def test_warehouse_supports_postgres_url(self):
        previous = os.environ.get("NOON_WAREHOUSE_DATABASE_URL")
        try:
            os.environ["NOON_WAREHOUSE_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/noon_warehouse"
            config = get_warehouse_database_config(Path("unused.db"))
            self.assertEqual(config.backend, "postgres")
            self.assertEqual(config.dsn, "postgresql://user:pass@localhost:5432/noon_warehouse")
        finally:
            if previous is None:
                os.environ.pop("NOON_WAREHOUSE_DATABASE_URL", None)
            else:
                os.environ["NOON_WAREHOUSE_DATABASE_URL"] = previous

    def test_settings_product_store_db_ref_prefers_postgres_url(self):
        previous = os.environ.get("NOON_PRODUCT_STORE_DATABASE_URL")
        try:
            os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/noon_stage"
            settings = Settings()
            self.assertEqual(settings.product_store_db_ref, "postgresql://user:pass@localhost:5432/noon_stage")
        finally:
            if previous is None:
                os.environ.pop("NOON_PRODUCT_STORE_DATABASE_URL", None)
            else:
                os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = previous

    def test_settings_warehouse_db_ref_prefers_postgres_url(self):
        previous = os.environ.get("NOON_WAREHOUSE_DATABASE_URL")
        try:
            os.environ["NOON_WAREHOUSE_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/noon_warehouse"
            settings = Settings()
            self.assertEqual(settings.warehouse_db_ref, "postgresql://user:pass@localhost:5432/noon_warehouse")
        finally:
            if previous is None:
                os.environ.pop("NOON_WAREHOUSE_DATABASE_URL", None)
            else:
                os.environ["NOON_WAREHOUSE_DATABASE_URL"] = previous


if __name__ == "__main__":
    unittest.main()
