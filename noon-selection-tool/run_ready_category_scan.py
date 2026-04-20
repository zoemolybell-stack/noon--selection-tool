import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.product_store import ProductStore
from db.config import database_config_from_reference, get_product_store_database_config, get_warehouse_database_config
from ops.crawler_control import build_runtime_category_index, load_runtime_category_map
from ops.progress_reporter import TaskProgressReporter, get_task_progress_reporter_from_env
from ops.shared_sync_queue import enqueue_and_wait_for_warehouse_sync, enqueue_shared_sync_enabled
from output.crawl_data_exporter import export_crawl_data
from scrapers.noon_category_crawler import NoonCategoryCrawler
from tools.build_fused_taxonomy_from_scan import build_payload
from tools.build_runtime_category_map import build_runtime_map
from tools.sync_runtime_category_artifacts import artifact_sync_enabled, push_runtime_category_artifacts
import warehouse_sync


ROOT = Path(__file__).parent
REPORTS_DIR = ROOT / "data" / "reports"
DEFAULT_DB_PATH = ROOT / "data" / "product_store.db"
DEFAULT_HOME_NAV_PATH = ROOT / "config" / "noon_home_navigation_saudi_en.json"
DEFAULT_CATEGORY_TREE_PATH = ROOT / "config" / "category_tree.json"
DEFAULT_RUNTIME_MAP_PATH = ROOT / "config" / "runtime_category_map.json"
DEFAULT_INCREMENTAL_SYNC_SECONDS = int(os.getenv("CATEGORY_INCREMENTAL_SYNC_SECONDS") or "0")
DEFAULT_FINAL_SYNC_LOCK_WAIT_SECONDS = int(os.getenv("CATEGORY_FINAL_SYNC_LOCK_WAIT_SECONDS") or "180")
DEFAULT_SYNC_LOCK_RETRY_SECONDS = int(os.getenv("SYNC_LOCK_RETRY_SECONDS") or "5")
DEFAULT_WAREHOUSE_DB_PATH = ROOT / "data" / "analytics" / "warehouse.db"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ready-category-scan")
SYNC_RESULT_PREFIX = "WAREHOUSE_SYNC_RESULT="


def _normalize_positive_int(value: object, *, field_name: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{field_name} must be a positive integer") from exc
    if normalized <= 0:
        raise SystemExit(f"{field_name} must be a positive integer")
    return normalized


def _normalize_subcategory_overrides(
    raw_overrides: object,
    *,
    runtime_index: dict[str, object],
    default_depth: int,
) -> dict[str, int]:
    if raw_overrides in (None, ""):
        return {}
    if not isinstance(raw_overrides, dict):
        raise SystemExit("subcategory_overrides_json must be a JSON object keyed by runtime_category_map config_id")

    valid_config_ids = set((runtime_index.get("by_config_id") or {}).keys())
    normalized: dict[str, int] = {}
    for key, value in raw_overrides.items():
        config_id = str(key or "").strip()
        if not config_id:
            continue
        if config_id not in valid_config_ids:
            raise SystemExit(f"unknown config_id in subcategory_overrides_json: {config_id}")
        if isinstance(value, dict):
            raw_depth = value.get("product_count") or value.get("default_product_count_per_leaf") or default_depth
        else:
            raw_depth = value or default_depth
        normalized[config_id] = _normalize_positive_int(raw_depth, field_name=f"subcategory_overrides[{config_id}]")
    return normalized


def _subcategory_override_items_for_category(
    *,
    category_name: str,
    subcategory_overrides: dict[str, int],
    runtime_index: dict[str, object],
) -> list[tuple[str, int, dict[str, object]]]:
    by_config_id = runtime_index.get("by_config_id") or {}
    items: list[tuple[str, int, dict[str, object]]] = []
    for config_id, product_count in subcategory_overrides.items():
        record = by_config_id.get(config_id)
        if not isinstance(record, dict):
            continue
        if str(record.get("top_level_category") or "").strip() != category_name:
            continue
        items.append((config_id, product_count, record))
    items.sort(key=lambda item: (str(item[2].get("display_name") or ""), item[0]))
    return items


def _normalize_db_reference(reference: str | Path, *, component_name: str) -> str:
    config = database_config_from_reference(reference, default_source_env="explicit")
    return config.as_reference(component_name)


def _configured_stage_db_ref(explicit_ref: str | Path | None = None) -> str:
    if explicit_ref is not None:
        return _normalize_db_reference(explicit_ref, component_name="run_ready_category_scan.stage_db")
    return get_product_store_database_config(DEFAULT_DB_PATH).as_reference("run_ready_category_scan.stage_db")


def _configured_warehouse_db_ref(explicit_ref: str | Path | None = None) -> str:
    if explicit_ref is not None:
        return _normalize_db_reference(explicit_ref, component_name="run_ready_category_scan.warehouse_db")
    return get_warehouse_database_config(DEFAULT_WAREHOUSE_DB_PATH).as_reference("run_ready_category_scan.warehouse_db")


def find_latest_readiness_report() -> Path:
    candidates = sorted(REPORTS_DIR.glob("scan_readiness_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"未找到 readiness 报告：{REPORTS_DIR}")
    return candidates[0]


def load_scan_plan(report_path: Path, categories_arg: str | None) -> list[str]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if categories_arg:
        requested = [item.strip() for item in categories_arg.split(",") if item.strip()]
        return requested
    return report.get("status_buckets", {}).get("ready_for_scan", [])


def persist_category(category_dir: Path, category_name: str, snapshot_id: str, *, db_path: Path) -> int:
    store = ProductStore(db_path)
    try:
        return store.import_category_directory(
            category_dir,
            platform="noon",
            snapshot_id=snapshot_id,
            category_name=category_name,
        )
    finally:
        store.close()


def persist_subcategory_file(
    store: ProductStore,
    *,
    json_file: Path,
    category_name: str,
    snapshot_id: str,
) -> int:
    return store.import_category_file(
        json_file,
        platform="noon",
        snapshot_id=snapshot_id,
        category_name=category_name,
    )


def maybe_incremental_category_sync(
    *,
    category_db: str | Path,
    warehouse_db: str | Path,
    snapshot_id: str,
    category_name: str,
    persisted_count: int,
    last_sync_at: float | None,
    force: bool = False,
) -> tuple[dict[str, object] | None, float | None]:
    now = time.time()
    current_db = _normalize_db_reference(category_db, component_name="run_ready_category_scan.category_db")
    resolved_warehouse_db = _normalize_db_reference(warehouse_db, component_name="run_ready_category_scan.warehouse_db")
    sync_reason = "category_batch_incremental_sync"

    if persisted_count <= 0:
        return None, last_sync_at
    if not force and last_sync_at and (now - last_sync_at) < DEFAULT_INCREMENTAL_SYNC_SECONDS:
        return None, last_sync_at

    if enqueue_shared_sync_enabled():
        payload = enqueue_and_wait_for_warehouse_sync(
            actor="category_batch_scan",
            reason=sync_reason,
            trigger_db=current_db,
            warehouse_db=resolved_warehouse_db,
            snapshot_id=snapshot_id,
            created_by="category_batch_scan",
            wait_timeout_seconds=DEFAULT_FINAL_SYNC_LOCK_WAIT_SECONDS if force else None,
        )
        payload["persisted_count"] = persisted_count
        payload["category_db"] = current_db
        payload["warehouse_db"] = resolved_warehouse_db
        payload["command"] = ["enqueue_shared_warehouse_sync_task", str(payload.get("task_id") or "")]
        if str(payload.get("status") or "").strip().lower() == "completed":
            return payload, time.time()
        return payload, last_sync_at

    command = [
        sys.executable,
        str(ROOT / "run_shared_warehouse_sync.py"),
        "--actor",
        "category_batch_scan",
        "--reason",
        sync_reason,
        "--trigger-db",
        current_db,
        "--warehouse-db",
        resolved_warehouse_db,
    ]

    deadline = now + (DEFAULT_FINAL_SYNC_LOCK_WAIT_SECONDS if force else 0)
    while True:
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        stderr_lines = [line for line in completed.stderr.splitlines() if line.strip()]
        log_lines = stdout_lines + stderr_lines
        result_payload: dict[str, object] | None = None
        for line in reversed(stdout_lines):
            if line.startswith(SYNC_RESULT_PREFIX):
                try:
                    result_payload = json.loads(line[len(SYNC_RESULT_PREFIX):])
                except json.JSONDecodeError:
                    result_payload = None
                break

        payload = {
            "status": str((result_payload or {}).get("status") or ("failed" if completed.returncode != 0 else "completed")),
            "reason": str((result_payload or {}).get("reason") or sync_reason),
            "persisted_count": persisted_count,
            "category_db": str(current_db),
            "warehouse_db": str(resolved_warehouse_db),
            "returncode": completed.returncode,
            "log_tail": log_lines[-20:],
            "error": "\n".join(log_lines[-20:]) if completed.returncode != 0 else "",
            "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
            "sync_state": (result_payload or {}).get("sync_state") or {},
        }
        status = str(payload.get("status") or "").strip().lower()
        skip_reason = str(payload.get("skip_reason") or "").strip().lower()
        if status == "completed":
            return payload, time.time()
        if status == "skipped" and skip_reason == "lock_active" and force and time.time() < deadline:
            time.sleep(max(DEFAULT_SYNC_LOCK_RETRY_SECONDS, 1))
            continue
        return payload, last_sync_at


async def run_category_batch(
    categories: list[str],
    output_dir: Path,
    product_count: int,
    category_overrides: dict[str, int] | None = None,
    subcategory_overrides: dict[str, int] | None = None,
    export_excel: bool = False,
    persist: bool = False,
    progress_reporter: TaskProgressReporter | None = None,
    db_path: Path | None = None,
    warehouse_db: Path | None = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    exports_dir = output_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    snapshot_id = output_dir.name
    started = time.time()
    results = []
    category_overrides = category_overrides or {}
    subcategory_overrides = subcategory_overrides or {}
    total_persisted = 0
    persisted_subcategories = 0
    last_sync_status = ""
    stage_db = _configured_stage_db_ref(db_path)
    target_warehouse_db = _configured_warehouse_db_ref(warehouse_db)
    runtime_index = build_runtime_category_index(load_runtime_category_map())

    if progress_reporter:
        progress_reporter.update(
            "runtime_collecting",
            message="category batch scan started",
            metrics={
                "categories_total": len(categories),
                "categories_completed": 0,
                "persisted_subcategories": 0,
                "persisted_observations": 0,
            },
                details={
                    "categories": categories,
                    "default_product_count_per_leaf": product_count,
                    "category_overrides": category_overrides,
                    "subcategory_overrides": subcategory_overrides,
                },
            )

    for idx, category in enumerate(categories, 1):
        category_product_count = int(category_overrides.get(category) or product_count)
        category_subcategory_overrides = _subcategory_override_items_for_category(
            category_name=category,
            subcategory_overrides=subcategory_overrides,
            runtime_index=runtime_index,
        )
        logger.info("=== [%s/%s] category scan start: %s ===", idx, len(categories), category)
        store = ProductStore(stage_db) if persist else None
        persisted_count = 0 if persist else None
        last_incremental_sync_at: float | None = None
        sync_events: list[dict[str, object]] = []
        override_runs: list[dict[str, object]] = []
        try:
            if progress_reporter:
                progress_reporter.update(
                    "runtime_collecting",
                    message=f"collecting {category}",
                    metrics={
                        "categories_total": len(categories),
                        "categories_completed": idx - 1,
                        "persisted_subcategories": persisted_subcategories,
                        "persisted_observations": total_persisted,
                    },
                    details={
                        "current_category": category,
                        "current_product_count_per_leaf": category_product_count,
                        "subcategory_override_count": len(category_subcategory_overrides),
                    },
                )

            def on_subcategory_saved(sub_name: str, json_file: Path, _payload: dict) -> None:
                nonlocal persisted_count
                nonlocal last_incremental_sync_at
                nonlocal total_persisted
                nonlocal persisted_subcategories
                nonlocal last_sync_status
                if store is None:
                    return
                imported = persist_subcategory_file(
                    store,
                    json_file=json_file,
                    category_name=category,
                    snapshot_id=snapshot_id,
                )
                persisted_count += imported
                total_persisted += imported
                persisted_subcategories += 1
                logger.info(
                    "subcategory persisted: %s/%s -> +%s observations (running_total=%s)",
                    category,
                    sub_name,
                    imported,
                    persisted_count,
                )
                if progress_reporter:
                    progress_reporter.update(
                        "stage_persisted",
                        message=f"persisted {category}/{sub_name}",
                        metrics={
                            "categories_total": len(categories),
                            "categories_completed": idx - 1,
                            "persisted_subcategories": persisted_subcategories,
                            "persisted_observations": total_persisted,
                        },
                        details={"current_category": category, "current_subcategory": sub_name, "imported_count": imported},
                    )
                sync_payload, last_incremental_sync_at = maybe_incremental_category_sync(
                    category_db=stage_db,
                    warehouse_db=target_warehouse_db,
                    snapshot_id=snapshot_id,
                    category_name=category,
                    persisted_count=persisted_count,
                    last_sync_at=last_incremental_sync_at,
                    force=False,
                )
                if sync_payload:
                    sync_events.append(sync_payload)
                    last_sync_status = str(sync_payload.get("status") or "")
                    if progress_reporter:
                        progress_reporter.update(
                            "partial_visible" if last_sync_status == "completed" else "warehouse_syncing",
                            message=f"sync {last_sync_status or 'unknown'} for {category}",
                            metrics={
                                "categories_total": len(categories),
                                "categories_completed": idx - 1,
                                "persisted_subcategories": persisted_subcategories,
                                "persisted_observations": total_persisted,
                            },
                            details={
                                "current_category": category,
                                "skip_reason": str(sync_payload.get("skip_reason") or ""),
                            },
                        )

            crawler = NoonCategoryCrawler(
                category,
                output_dir,
                max_products_per_sub=category_product_count,
                on_subcategory_saved=on_subcategory_saved if persist else None,
            )
            result = await crawler.run()

            for override_config_id, override_product_count, override_record in category_subcategory_overrides:
                target_name = str(override_record.get("display_name") or override_record.get("config_id") or override_config_id)
                if progress_reporter:
                    progress_reporter.update(
                        "runtime_collecting",
                        message=f"override {category}/{target_name}",
                        metrics={
                            "categories_total": len(categories),
                            "categories_completed": idx - 1,
                            "persisted_subcategories": persisted_subcategories,
                            "persisted_observations": total_persisted,
                        },
                        details={
                            "current_category": category,
                            "target_subcategory": override_config_id,
                            "target_subcategory_display_name": target_name,
                            "target_product_count_per_leaf": override_product_count,
                        },
                    )
                override_crawler = NoonCategoryCrawler(
                    category,
                    output_dir,
                    max_products_per_sub=override_product_count,
                    target_subcategory=override_config_id,
                    on_subcategory_saved=on_subcategory_saved if persist else None,
                )
                try:
                    override_result = await override_crawler.run()
                    override_runs.append(
                        {
                            "target_subcategory": override_config_id,
                            "display_name": target_name,
                            "product_count_per_leaf": override_product_count,
                            "result": override_result,
                        }
                    )
                except Exception as exc:
                    override_runs.append(
                        {
                            "target_subcategory": override_config_id,
                            "display_name": target_name,
                            "product_count_per_leaf": override_product_count,
                            "error": str(exc),
                        }
                    )
                    logger.warning("subcategory override crawl failed %s/%s: %s", category, override_config_id, exc)

            if persist:
                sync_payload, last_incremental_sync_at = maybe_incremental_category_sync(
                    category_db=stage_db,
                    warehouse_db=target_warehouse_db,
                    snapshot_id=snapshot_id,
                    category_name=category,
                    persisted_count=persisted_count or 0,
                    last_sync_at=last_incremental_sync_at,
                    force=True,
                )
                if sync_payload:
                    sync_events.append(sync_payload)
                    last_sync_status = str(sync_payload.get("status") or last_sync_status or "")

            category_dir = output_dir / "monitoring" / "categories" / category
            excel_path = None

            if export_excel and category_dir.exists():
                excel_path = exports_dir / f"category_{category}_{product_count}.xlsx"
                try:
                    export_crawl_data([category_dir], excel_path, ["noon"], mode="category")
                except Exception as exc:
                    logger.warning("Excel export failed %s: %s", category, exc)
                    excel_path = None

            results.append(
                {
                    "category": category,
                    "result": result,
                    "override_runs": override_runs,
                    "subcategory_overrides": [
                        {
                            "target_subcategory": override_config_id,
                            "display_name": override_record.get("display_name") or override_record.get("config_id") or override_config_id,
                            "product_count_per_leaf": override_product_count,
                        }
                        for override_config_id, override_product_count, override_record in category_subcategory_overrides
                    ],
                    "category_dir": str(category_dir),
                    "excel_path": str(excel_path) if excel_path else None,
                    "persisted_count": persisted_count,
                    "product_count_per_leaf": category_product_count,
                    "sync_events": sync_events,
                }
            )
            if progress_reporter:
                progress_reporter.update(
                    "runtime_collecting",
                    message=f"completed {category}",
                    metrics={
                        "categories_total": len(categories),
                        "categories_completed": idx,
                        "persisted_subcategories": persisted_subcategories,
                        "persisted_observations": total_persisted,
                    },
                    details={"current_category": category, "current_product_count_per_leaf": category_product_count},
                )
        finally:
            if store is not None:
                store.close()

    elapsed_minutes = round((time.time() - started) / 60, 1)
    summary = {
        "started_at": datetime.now().isoformat(),
        "snapshot_id": snapshot_id,
        "categories": categories,
        "product_count_per_leaf": product_count,
        "category_overrides": category_overrides,
        "subcategory_overrides": subcategory_overrides,
        "elapsed_minutes": elapsed_minutes,
        "results": results,
    }
    if progress_reporter:
        progress_reporter.update(
            "web_visible" if last_sync_status == "completed" else "stage_persisted",
            message="category batch scan completed",
            metrics={
                "categories_total": len(categories),
                "categories_completed": len(categories),
                "persisted_subcategories": persisted_subcategories,
                "persisted_observations": total_persisted,
            },
            details={"last_sync_status": last_sync_status},
            completed=True,
        )
    summary_path = output_dir / "batch_scan_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        fused_payload, gap_report = build_payload(output_dir, DEFAULT_HOME_NAV_PATH)
        (output_dir / "fused_platform_taxonomy.json").write_text(
            json.dumps(fused_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (output_dir / "taxonomy_gap_report.json").write_text(
            json.dumps(gap_report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("已生成 fused taxonomy 与 gap report")
    except Exception as exc:
        logger.warning("生成 fused taxonomy 失败: %s", exc)

    try:
        runtime_map = build_runtime_map(output_dir, DEFAULT_CATEGORY_TREE_PATH)
        runtime_map_path = output_dir / "runtime_category_map.json"
        runtime_map_path.write_text(
            json.dumps(runtime_map, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        DEFAULT_RUNTIME_MAP_PATH.write_text(
            json.dumps(runtime_map, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if artifact_sync_enabled():
            artifact_sync_payload = push_runtime_category_artifacts(
                runtime_map_path=runtime_map_path,
                snapshot_id=snapshot_id,
                source_node=str(
                    os.getenv("NOON_WORKER_NODE_HOST")
                    or os.getenv("COMPUTERNAME")
                    or os.getenv("HOSTNAME")
                    or "local-category-node"
                ),
                batch_dir=output_dir,
            )
            summary["artifact_sync"] = artifact_sync_payload
            if str(artifact_sync_payload.get("status") or "").strip().lower() != "completed":
                logger.warning("runtime category artifacts sync not completed: %s", artifact_sync_payload)
        logger.info("已更新 runtime category map: %s", DEFAULT_RUNTIME_MAP_PATH)
    except Exception as exc:
        logger.warning("生成 runtime category map 失败: %s", exc)
    logger.info("批量扫描完成，汇总已写入：%s", summary_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="? readiness ?????? Noon ??")
    parser.add_argument("--report", type=Path, default=None, help="readiness ????????? data/reports ?????")
    parser.add_argument("--categories", type=str, default=None, help="??????????????? readiness ?? ready_for_scan")
    parser.add_argument("--product-count", type=int, default=100, help="???????????????")
    parser.add_argument("--category-overrides-json", type=str, default=None, help="JSON ????????? product_count")
    parser.add_argument("--subcategory-overrides-json", type=str, default=None, help="JSON ????????? runtime_category_map config_id")
    parser.add_argument("--output-dir", type=Path, default=None, help="??????? data/batch_scans/<timestamp>")
    parser.add_argument("--db-path", type=Path, default=None, help="category stage DB ?????????? product_store.db")
    parser.add_argument("--warehouse-db", type=Path, default=None, help="warehouse DB ?????????? analytics/warehouse.db")
    parser.add_argument("--no-export-excel", action="store_true", help="?? Excel ??")
    parser.add_argument("--persist", action="store_true", help="????? stage store")
    return parser.parse_args()


def main():
    args = parse_args()
    report_path = args.report or find_latest_readiness_report()
    categories = load_scan_plan(report_path, args.categories)
    category_overrides: dict[str, int] = {}
    runtime_index = build_runtime_category_index(load_runtime_category_map())
    subcategory_overrides: dict[str, int] = {}
    if not categories:
        raise SystemExit("未找到可扫描类目，请检查 readiness 报告或 --categories 参数")
    if args.category_overrides_json:
        try:
            raw_overrides = json.loads(args.category_overrides_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"category_overrides_json 不是合法 JSON: {exc}") from exc
        if not isinstance(raw_overrides, dict):
            raise SystemExit("category_overrides_json 必须是对象，例如 {\"sports\": 200}")
        for key, value in raw_overrides.items():
            if not isinstance(key, str):
                raise SystemExit("category_overrides_json 的类目名必须是字符串")
            try:
                normalized_value = int(value)
            except (TypeError, ValueError) as exc:
                raise SystemExit(f"category_overrides_json 中 {key} 的 product_count 无效: {value}") from exc
            if normalized_value <= 0:
                raise SystemExit(f"category_overrides_json 中 {key} 的 product_count 必须大于 0")
            category_overrides[key.strip()] = normalized_value
    if args.subcategory_overrides_json:
        try:
            raw_subcategory_overrides = json.loads(args.subcategory_overrides_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"subcategory_overrides_json 涓嶆槸鍚堟硶 JSON: {exc}") from exc
        subcategory_overrides = _normalize_subcategory_overrides(
            raw_subcategory_overrides,
            runtime_index=runtime_index,
            default_depth=args.product_count,
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (ROOT / "data" / "batch_scans" / f"ready_scan_{timestamp}")

    logger.info("使用 readiness 报告：%s", report_path)
    logger.info("本次扫描类目：%s", ", ".join(categories))
    logger.info("输出目录：%s", output_dir)

    asyncio.run(
        run_category_batch(
            categories=categories,
            output_dir=output_dir,
            product_count=args.product_count,
            category_overrides=category_overrides,
            subcategory_overrides=subcategory_overrides,
            export_excel=not args.no_export_excel,
            persist=args.persist,
            progress_reporter=get_task_progress_reporter_from_env(),
            db_path=args.db_path,
            warehouse_db=args.warehouse_db,
        )
    )


if __name__ == "__main__":
    main()
