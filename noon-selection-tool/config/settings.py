"""
全局配置

职责:
- 从 `.env` 读取基础配置
- 提供运行时数据目录 / 数据库路径
- 支持按运行作用域隔离关键词与类目链路
"""
from __future__ import annotations

import datetime
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings

from db.config import DatabaseConfig, database_config_from_reference, get_product_store_database_config, get_warehouse_database_config


class Settings(BaseSettings):
    # 代理配置（可选，推荐用于 Amazon / Temu）
    proxy_host: str = ""
    proxy_port: int = 0
    proxy_user: str = ""
    proxy_pass: str = ""

    # SHEIN API（Phase 1 预留）
    shein_api_key: str = ""
    shein_api_provider: str = "brightdata"  # brightdata / oxylabs / scraperapi

    # 业务参数
    ksa_exchange_rate: float = 1.90
    shipping_rate_cbm: float = 2000
    shipping_rate_cbm_peacetime: float = 950
    default_ad_rate: float = 0.05
    default_return_rate: float = 0.03

    # 爬虫参数
    min_delay: float = 2.0
    max_delay: float = 5.0
    max_retries: int = 3
    concurrent_browsers: int = 3
    products_per_keyword: int = 100

    # 漏斗参数
    funnel_top_n: int = 100

    # 运行时隔离配置
    runtime_scope: str = Field(
        default="shared",
        validation_alias=AliasChoices("runtime_scope", "RUNTIME_SCOPE", "NOON_RUNTIME_SCOPE"),
    )  # shared / keyword / category / <custom>
    data_root: str = Field(
        default="",
        validation_alias=AliasChoices("data_root", "DATA_ROOT", "NOON_DATA_ROOT"),
    )
    product_store_db: str = Field(
        default="",
        validation_alias=AliasChoices("product_store_db", "PRODUCT_STORE_DB", "NOON_PRODUCT_STORE_DB"),
    )
    warehouse_db: str = Field(
        default="",
        validation_alias=AliasChoices("warehouse_db", "WAREHOUSE_DB", "NOON_WAREHOUSE_DB"),
    )
    ops_db: str = Field(
        default="",
        validation_alias=AliasChoices("ops_db", "OPS_DB", "NOON_OPS_DB"),
    )
    browser_headless: bool = Field(
        default=False,
        validation_alias=AliasChoices("browser_headless", "BROWSER_HEADLESS", "NOON_BROWSER_HEADLESS"),
    )
    browser_profile_root: str = Field(
        default="",
        validation_alias=AliasChoices("browser_profile_root", "BROWSER_PROFILE_ROOT", "NOON_BROWSER_PROFILE_ROOT"),
    )
    browser_channel: str = Field(
        default="",
        validation_alias=AliasChoices("browser_channel", "BROWSER_CHANNEL", "NOON_BROWSER_CHANNEL"),
    )
    browser_executable_path: str = Field(
        default="",
        validation_alias=AliasChoices(
            "browser_executable_path",
            "BROWSER_EXECUTABLE_PATH",
            "NOON_BROWSER_EXECUTABLE_PATH",
        ),
    )
    browser_cdp_endpoint: str = Field(
        default="",
        validation_alias=AliasChoices(
            "browser_cdp_endpoint",
            "BROWSER_CDP_ENDPOINT",
            "NOON_BROWSER_CDP_ENDPOINT",
        ),
    )
    browser_warmup_url: str = Field(
        default="",
        validation_alias=AliasChoices(
            "browser_warmup_url",
            "BROWSER_WARMUP_URL",
            "NOON_BROWSER_WARMUP_URL",
            "NOON_CATEGORY_WARMUP_URL",
        ),
    )
    worker_type: str = Field(
        default="",
        validation_alias=AliasChoices("worker_type", "WORKER_TYPE", "NOON_WORKER_TYPE"),
    )
    max_concurrent_tasks: int = Field(
        default=1,
        validation_alias=AliasChoices(
            "max_concurrent_tasks",
            "MAX_CONCURRENT_TASKS",
            "NOON_MAX_CONCURRENT_TASKS",
        ),
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def proxy_url(self) -> str | None:
        if not self.proxy_host:
            return None
        auth = f"{self.proxy_user}:{self.proxy_pass}@" if self.proxy_user else ""
        return f"http://{auth}{self.proxy_host}:{self.proxy_port}"

    @property
    def project_root(self) -> Path:
        return Path(__file__).parent.parent

    @property
    def shared_data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def runtime_scope_name(self) -> str:
        if hasattr(self, "_runtime_scope_override"):
            return self._runtime_scope_override
        return (self.runtime_scope or "shared").strip().lower() or "shared"

    def set_runtime_scope(self, scope: str):
        """手动指定运行作用域。"""
        normalized = (scope or "shared").strip().lower() or "shared"
        object.__setattr__(self, "_runtime_scope_override", normalized)

    def set_data_dir(self, path: str | Path):
        """手动指定运行数据根目录。"""
        object.__setattr__(self, "_data_dir_override", Path(path).expanduser())

    def set_product_store_db_path(self, path: str | Path):
        """手动指定产品库数据库引用（SQLite 路径或 Postgres DSN）。"""
        object.__setattr__(self, "_product_store_db_override", str(path).strip())

    def set_warehouse_db_path(self, path: str | Path):
        object.__setattr__(self, "_warehouse_db_override", str(path).strip())

    def _default_data_dir_for_scope(self, scope: str) -> Path:
        if scope == "shared":
            return self.shared_data_dir
        return self.project_root / "runtime_data" / scope

    @property
    def data_dir(self) -> Path:
        override = getattr(self, "_data_dir_override", None)
        if override is None and self.data_root:
            override = Path(self.data_root).expanduser()

        data_dir = override or self._default_data_dir_for_scope(self.runtime_scope_name)
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir

    @property
    def monitoring_dir(self) -> Path:
        return self.data_dir / "monitoring"

    @property
    def exports_dir(self) -> Path:
        return self.data_dir / "exports"

    @property
    def product_store_db_path(self) -> Path:
        override = getattr(self, "_product_store_db_override", None)
        if override is not None:
            try:
                config = database_config_from_reference(override, default_source_env="explicit_override")
                if config.is_sqlite:
                    db_path = config.sqlite_path_or_raise("Settings.product_store_db_path")
                else:
                    db_path = self.data_dir / "product_store.db"
            except ValueError:
                db_path = self.data_dir / "product_store.db"
        elif self.product_store_db:
            db_path = Path(self.product_store_db).expanduser()
        else:
            db_path = self.data_dir / "product_store.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path

    @property
    def product_store_database_config(self) -> DatabaseConfig:
        override = getattr(self, "_product_store_db_override", None)
        if override is not None:
            return database_config_from_reference(override, default_source_env="explicit_override")
        return get_product_store_database_config(self.data_dir / "product_store.db")

    @property
    def product_store_db_ref(self) -> str:
        return self.product_store_database_config.as_reference("Settings.product_store_db_ref")

    @property
    def warehouse_db_path(self) -> Path:
        override = getattr(self, "_warehouse_db_override", None)
        if override is not None:
            try:
                config = database_config_from_reference(override, default_source_env="explicit_override")
                if config.is_sqlite:
                    db_path = config.sqlite_path_or_raise("Settings.warehouse_db_path")
                else:
                    db_path = self.project_root / "data" / "analytics" / "warehouse.db"
            except ValueError:
                db_path = self.project_root / "data" / "analytics" / "warehouse.db"
        elif self.warehouse_db:
            db_path = Path(self.warehouse_db).expanduser()
        else:
            db_path = self.project_root / "data" / "analytics" / "warehouse.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path

    @property
    def warehouse_database_config(self) -> DatabaseConfig:
        override = getattr(self, "_warehouse_db_override", None)
        if override is not None:
            return database_config_from_reference(override, default_source_env="explicit_override")
        return get_warehouse_database_config(self.project_root / "data" / "analytics" / "warehouse.db")

    @property
    def warehouse_db_ref(self) -> str:
        return self.warehouse_database_config.as_reference("Settings.warehouse_db_ref")

    @property
    def ops_db_path(self) -> Path:
        db_path = Path(self.ops_db).expanduser() if self.ops_db else (self.data_dir / "ops" / "ops.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path

    @property
    def browser_profile_root_path(self) -> Path:
        if self.browser_profile_root:
            profile_root = Path(self.browser_profile_root).expanduser()
        else:
            profile_root = self.data_dir / ".browser_profiles"
        profile_root.mkdir(parents=True, exist_ok=True)
        return profile_root

    @property
    def browser_executable_path_value(self) -> str:
        return str(self.browser_executable_path or "").strip()

    @property
    def browser_cdp_endpoint_value(self) -> str:
        return str(self.browser_cdp_endpoint or "").strip()

    @property
    def browser_warmup_url_value(self) -> str:
        return str(self.browser_warmup_url or "").strip()

    def set_snapshot_id(self, sid: str):
        """手动指定快照 ID（用于断点续跑同一快照）。"""
        object.__setattr__(self, "_snapshot_id", sid)

    @property
    def snapshot_id(self) -> str:
        """当次运行的快照 ID，格式 YYYY-MM-DD_HHMMSS。"""
        if not hasattr(self, "_snapshot_id"):
            object.__setattr__(
                self,
                "_snapshot_id",
                datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S"),
            )
        return self._snapshot_id

    @property
    def snapshot_dir(self) -> Path:
        """当次运行的数据目录。"""
        snap_dir = self.data_dir / "snapshots" / self.snapshot_id
        snap_dir.mkdir(parents=True, exist_ok=True)
        return snap_dir
