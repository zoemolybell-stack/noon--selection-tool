from __future__ import annotations

import json
import os
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ops.crawler_runtime_contract import (
    ACTIVE_CATEGORY_CRAWL_SKIP_REASON,
    ACTIVE_MONITOR_SKIP_REASON,
)
from ops.task_store import OpsStore, SUPPORTED_PLAN_TYPES, compute_plan_next_run_at


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
REPORTS_DIR = ROOT / "data" / "reports"
BATCH_SCANS_DIR = ROOT / "data" / "batch_scans"
KEYWORD_MONITOR_LOCK_PATH = ROOT / "runtime_data" / "keyword" / "monitor" / "keyword_monitor.lock"
CATEGORY_CRAWL_LOCK_DIR = ROOT / "data" / "monitoring" / "categories"
DEFAULT_CATEGORY_PRODUCT_COUNT = 100
DEFAULT_KEYWORD_NOON_COUNT = 30
DEFAULT_KEYWORD_AMAZON_COUNT = 30
logger = logging.getLogger(__name__)

PLAN_LABELS = {
    "category_single": "Single Category / 单类目",
    "category_ready_scan": "Ready Categories / 批量类目",
    "keyword_batch": "Keyword Batch / 关键词批量",
    "keyword_monitor": "Keyword Monitor / 关键词监控",
}


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = (result.stdout or "").strip()
        return bool(output and not output.startswith("INFO:") and (f'"{pid}"' in output or f",{pid}," in output))
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _keyword_monitor_lock_active() -> bool:
    if not KEYWORD_MONITOR_LOCK_PATH.exists():
        return False
    payload = _safe_json_load(KEYWORD_MONITOR_LOCK_PATH, {})
    if not isinstance(payload, dict):
        return False
    try:
        pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    if _pid_is_alive(pid):
        return True
    KEYWORD_MONITOR_LOCK_PATH.unlink(missing_ok=True)
    return False


def _keyword_monitor_skip_dispatch_result(plan: dict[str, Any], *, created_by: str, skip_detail: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "skip_reason": ACTIVE_MONITOR_SKIP_REASON,
        "skip_detail": skip_detail,
        "plan_id": int(plan["id"]),
        "plan_type": str(plan.get("plan_type") or ""),
        "plan_name": str(plan.get("name") or ""),
        "task_id": None,
        "created_by": created_by,
    }


def _category_crawl_lock_path(category_name: str) -> Path:
    return CATEGORY_CRAWL_LOCK_DIR / str(category_name).strip() / "category_crawl.lock"


def _category_crawl_lock_detail(plan: dict[str, Any]) -> str:
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    payload = dict(plan.get("payload") or {})
    categories: list[str] = []
    if plan_type == "category_single":
        category_name = str(payload.get("category") or "").strip()
        if category_name:
            categories.append(category_name)
    elif plan_type == "category_ready_scan":
        raw_categories = payload.get("categories") or load_ready_categories()
        if isinstance(raw_categories, str):
            raw_categories = [item.strip() for item in raw_categories.split(",") if item.strip()]
        categories.extend(str(item).strip() for item in raw_categories if str(item).strip())
    for category_name in categories:
        lock_path = _category_crawl_lock_path(category_name)
        if not lock_path.exists():
            continue
        payload = _safe_json_load(lock_path, {})
        if not isinstance(payload, dict):
            continue
        try:
            pid = int(payload.get("pid") or 0)
        except (TypeError, ValueError):
            pid = 0
        if _pid_is_alive(pid):
            return f"category crawler is already active: category={category_name}, pid={pid}, lock={lock_path}"
        lock_path.unlink(missing_ok=True)
    return ""


def _category_crawl_skip_dispatch_result(plan: dict[str, Any], *, created_by: str, skip_detail: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "skip_reason": ACTIVE_CATEGORY_CRAWL_SKIP_REASON,
        "skip_detail": skip_detail,
        "plan_id": int(plan["id"]),
        "plan_type": str(plan.get("plan_type") or ""),
        "plan_name": str(plan.get("name") or ""),
        "task_id": None,
        "created_by": created_by,
    }


def _plan_active_skip_reason(plan_type: str) -> str:
    if plan_type in {"keyword_monitor", "keyword_batch"}:
        return ACTIVE_MONITOR_SKIP_REASON
    if plan_type in {"category_single", "category_ready_scan"}:
        return ACTIVE_CATEGORY_CRAWL_SKIP_REASON
    return ""


def _plan_active_skip_detail(plan: dict[str, Any], active_tasks: list[dict[str, Any]]) -> str:
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    task_ids = [int(task["id"]) for task in active_tasks if task.get("id") is not None]
    task_ids_text = ",".join(str(task_id) for task_id in sorted(task_ids))
    if plan_type.startswith("keyword_"):
        return f"keyword monitor already active: active_task_ids={task_ids_text}"
    if plan_type.startswith("category_"):
        return f"category crawler already active: active_task_ids={task_ids_text}"
    return f"plan already active: active_task_ids={task_ids_text}"


def _safe_json_load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _runtime_category_map_candidate_paths() -> list[Path]:
    candidates: list[Path] = [CONFIG_DIR / "runtime_category_map.json"]
    if BATCH_SCANS_DIR.exists():
        candidates.extend(
            sorted(
                BATCH_SCANS_DIR.rglob("runtime_category_map.json"),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
        )
    return candidates


def _runtime_category_map_score(payload: dict[str, Any], path: Path) -> tuple[int, int, float]:
    categories = payload.get("categories", {})
    if not isinstance(categories, dict):
        return (0, 0, path.stat().st_mtime)
    category_count = len(categories)
    subcategory_count = 0
    for item in categories.values():
        if isinstance(item, dict):
            subcategory_count += len(item)
    return (category_count, subcategory_count, path.stat().st_mtime)


def load_runtime_category_map() -> dict[str, Any]:
    fallback_payload: dict[str, Any] | None = None
    best_payload: dict[str, Any] | None = None
    best_score = (-1, -1, -1.0)
    for path in _runtime_category_map_candidate_paths():
        payload = _safe_json_load(path, {})
        if not isinstance(payload, dict):
            continue
        payload = dict(payload)
        payload["_source_path"] = str(path)
        if fallback_payload is None:
            fallback_payload = payload
        score = _runtime_category_map_score(payload, path)
        if score[0] <= 0:
            continue
        if score > best_score:
            best_score = score
            best_payload = payload
    return best_payload or fallback_payload or {}


def _normalize_positive_int(value: Any, *, field_name: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer") from exc
    if normalized <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return normalized


def flatten_runtime_category_map(runtime_map: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    runtime_map = runtime_map if runtime_map is not None else load_runtime_category_map()
    categories = runtime_map.get("categories", {}) if isinstance(runtime_map, dict) else {}
    flattened: list[dict[str, Any]] = []
    if not isinstance(categories, dict):
        return flattened

    for top_level_category, subcategories in sorted(categories.items(), key=lambda item: str(item[0])):
        if not isinstance(subcategories, dict):
            continue
        for config_id, record in sorted(subcategories.items(), key=lambda item: str(item[0])):
            if not isinstance(record, dict):
                continue
            normalized_config_id = str(record.get("config_id") or config_id or "").strip()
            if not normalized_config_id:
                continue
            flattened.append(
                {
                    "config_id": normalized_config_id,
                    "display_name": str(
                        record.get("display_name")
                        or record.get("config_name")
                        or record.get("subcategory_name")
                        or normalized_config_id
                    ).strip(),
                    "breadcrumb_path": str(record.get("breadcrumb_path") or "").strip(),
                    "parent_config_id": str(record.get("parent_config_id") or "").strip() or None,
                    "top_level_category": str(top_level_category).strip(),
                }
            )
    return flattened


def build_runtime_category_index(runtime_map: dict[str, Any] | None = None) -> dict[str, Any]:
    runtime_map = runtime_map if runtime_map is not None else load_runtime_category_map()
    subcategories = flatten_runtime_category_map(runtime_map)
    by_config_id: dict[str, dict[str, Any]] = {}
    by_top_level_category: dict[str, list[dict[str, Any]]] = {}
    for item in subcategories:
        config_id = str(item["config_id"])
        by_config_id[config_id] = item
        by_top_level_category.setdefault(str(item["top_level_category"]), []).append(item)
    for items in by_top_level_category.values():
        items.sort(key=lambda item: (str(item.get("display_name") or ""), str(item.get("config_id") or "")))
    return {
        "subcategories": subcategories,
        "by_config_id": by_config_id,
        "by_top_level_category": by_top_level_category,
    }


def load_ready_categories() -> list[str]:
    reports = sorted(REPORTS_DIR.glob("scan_readiness_*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    if not reports:
        return sorted(load_runtime_category_map().get("categories", {}).keys())
    payload = _safe_json_load(reports[0], {})
    ready = payload.get("status_buckets", {}).get("ready_for_scan", [])
    if isinstance(ready, list) and ready:
        return [str(item).strip() for item in ready if str(item).strip()]
    return sorted(load_runtime_category_map().get("categories", {}).keys())


def list_monitor_configs() -> list[dict[str, str]]:
    items = []
    for path in sorted(CONFIG_DIR.glob("keyword_monitor*.json")):
        if path.name == "keyword_monitor_defaults.json":
            continue
        items.append(
            {
                "name": path.name,
                "path": str(path),
                "label": path.stem.replace("_", " "),
            }
        )
    default_path = CONFIG_DIR / "keyword_monitor_defaults.json"
    if default_path.exists():
        items.insert(
            0,
            {
                "name": default_path.name,
                "path": str(default_path),
                "label": "keyword monitor defaults",
            },
        )
    return items


def _resolve_monitor_config_path(raw_path: str) -> Path:
    candidate = Path(str(raw_path or "").strip())
    if candidate.is_absolute():
        return candidate
    relative_to_config = (CONFIG_DIR / candidate).resolve()
    if relative_to_config.exists():
        return relative_to_config
    return (ROOT / candidate).resolve()


def _load_keyword_list(path: Path) -> list[str]:
    if not path.exists():
        return []
    seen: set[str] = set()
    items: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        token = str(raw_line).strip().lower()
        if not token or token.startswith("#") or token in seen:
            continue
        seen.add(token)
        items.append(token)
    return items


def _load_monitor_seed_keywords(payload: dict[str, Any]) -> list[str]:
    monitor_config = str(payload.get("monitor_config") or "").strip()
    config_path = _resolve_monitor_config_path(monitor_config) if monitor_config else (CONFIG_DIR / "keyword_monitor_defaults.json")
    config_payload = _safe_json_load(config_path, {})
    baseline_file_raw = str(config_payload.get("baseline_file") or "").strip()
    if baseline_file_raw:
        baseline_path = Path(baseline_file_raw)
        if not baseline_path.is_absolute():
            baseline_path = (config_path.parent / baseline_path).resolve()
    else:
        baseline_path = CONFIG_DIR / "keyword_baseline_seeds.txt"
    return _load_keyword_list(baseline_path)


def build_crawler_catalog() -> dict[str, Any]:
    runtime_map = load_runtime_category_map()
    category_map = runtime_map.get("categories", {}) if isinstance(runtime_map, dict) else {}
    runtime_index = build_runtime_category_index(runtime_map)
    return {
        "generated_at": _now_iso(),
        "ready_categories": load_ready_categories(),
        "runtime_category_map": {
            "path": str(runtime_map.get("_source_path") or (CONFIG_DIR / "runtime_category_map.json")),
            "generated_at": runtime_map.get("generated_at") if isinstance(runtime_map, dict) else "",
            "category_count": len(category_map),
            "subcategory_count": len(runtime_index["subcategories"]),
            "categories": sorted(category_map.keys()),
        },
        "subcategory_catalog": runtime_index["subcategories"],
        "monitor_configs": list_monitor_configs(),
        "defaults": {
            "category_default_product_count_per_leaf": DEFAULT_CATEGORY_PRODUCT_COUNT,
            "keyword_noon_count": DEFAULT_KEYWORD_NOON_COUNT,
            "keyword_amazon_count": DEFAULT_KEYWORD_AMAZON_COUNT,
        },
    }


def _normalize_keyword_batch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    keywords = payload.get("keywords") or []
    if isinstance(keywords, str):
        keywords = [item.strip() for item in keywords.splitlines() if item.strip()]
    keywords = sorted({str(item).strip().lower() for item in keywords if str(item).strip()})
    if not keywords:
        raise ValueError("keywords is required for keyword_batch")
    platforms = payload.get("platforms") or ["noon", "amazon"]
    if isinstance(platforms, str):
        platforms = [item.strip() for item in platforms.replace(",", " ").split() if item.strip()]
    return {
        "keywords": keywords,
        "platforms": [str(item).strip().lower() for item in platforms if str(item).strip()],
        "noon_count": int(payload.get("noon_count") or DEFAULT_KEYWORD_NOON_COUNT),
        "amazon_count": int(payload.get("amazon_count") or DEFAULT_KEYWORD_AMAZON_COUNT),
        "persist": bool(payload.get("persist", True)),
        "reason": str(payload.get("reason") or ""),
    }


def _normalize_keyword_monitor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    monitor_config = str(payload.get("monitor_config") or "").strip()
    if not monitor_config:
        raise ValueError("monitor_config is required for keyword_monitor")
    normalized = {
        "monitor_config": monitor_config,
        "noon_count": int(payload.get("noon_count") or DEFAULT_KEYWORD_NOON_COUNT),
        "amazon_count": int(payload.get("amazon_count") or DEFAULT_KEYWORD_AMAZON_COUNT),
        "persist": bool(payload.get("persist", True)),
        "monitor_report": bool(payload.get("monitor_report", False)),
        "reason": str(payload.get("reason") or ""),
    }
    monitor_seed_keyword = str(payload.get("monitor_seed_keyword") or "").strip().lower()
    if monitor_seed_keyword:
        normalized["monitor_seed_keyword"] = monitor_seed_keyword
    return normalized


def _normalize_category_single_payload(payload: dict[str, Any]) -> dict[str, Any]:
    category = str(payload.get("category") or "").strip()
    target_subcategory = str(payload.get("target_subcategory") or "").strip()
    if target_subcategory:
        runtime_index = build_runtime_category_index()
        subcategory_record = runtime_index["by_config_id"].get(target_subcategory)
        if not subcategory_record:
            raise ValueError(f"unknown target_subcategory config_id: {target_subcategory}")
        target_top_level_category = str(subcategory_record.get("top_level_category") or "").strip()
        if category and target_top_level_category and category != target_top_level_category:
            raise ValueError(
                f"target_subcategory {target_subcategory} does not belong to category {category}"
            )
        if not category:
            category = target_top_level_category
    if not category:
        raise ValueError("category is required for category_single")
    return {
        "category": category,
        "target_subcategory": target_subcategory,
        "product_count": int(payload.get("product_count") or DEFAULT_CATEGORY_PRODUCT_COUNT),
        "persist": bool(payload.get("persist", True)),
        "export_excel": bool(payload.get("export_excel", False)),
        "reason": str(payload.get("reason") or ""),
    }


def _normalize_category_ready_scan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    categories = payload.get("categories") or []
    if isinstance(categories, str):
        categories = [item.strip() for item in categories.split(",") if item.strip()]
    default_depth = _normalize_positive_int(
        payload.get("default_product_count_per_leaf")
        or payload.get("product_count")
        or DEFAULT_CATEGORY_PRODUCT_COUNT
    , field_name="default_product_count_per_leaf")
    overrides = payload.get("category_overrides") or {}
    normalized_overrides: dict[str, int] = {}
    if isinstance(overrides, dict):
        for key, value in overrides.items():
            category_key = str(key or "").strip()
            if not category_key:
                continue
            if isinstance(value, dict):
                normalized_overrides[category_key] = _normalize_positive_int(
                    value.get("product_count")
                    or value.get("default_product_count_per_leaf")
                    or default_depth
                    , field_name=f"category_overrides[{category_key}]"
                )
            else:
                normalized_overrides[category_key] = _normalize_positive_int(
                    value or default_depth,
                    field_name=f"category_overrides[{category_key}]",
                )

    runtime_index = build_runtime_category_index()
    valid_config_ids = set(runtime_index["by_config_id"].keys())
    requested_categories = [str(item).strip() for item in categories if str(item).strip()]
    effective_categories = requested_categories or load_ready_categories()
    effective_category_set = {str(item).strip() for item in effective_categories if str(item).strip()}
    raw_subcategory_overrides = payload.get("subcategory_overrides") or {}
    normalized_subcategory_overrides: dict[str, int] = {}
    if raw_subcategory_overrides:
        if not isinstance(raw_subcategory_overrides, dict):
            raise ValueError("subcategory_overrides must be an object keyed by runtime_category_map config_id")
        for key, value in raw_subcategory_overrides.items():
            config_id = str(key or "").strip()
            if not config_id:
                continue
            if config_id not in valid_config_ids:
                raise ValueError(f"unknown subcategory config_id in subcategory_overrides: {config_id}")
            record = runtime_index["by_config_id"].get(config_id) or {}
            top_level_category = str(record.get("top_level_category") or "").strip()
            if effective_category_set and top_level_category and top_level_category not in effective_category_set:
                raise ValueError(
                    f"subcategory_overrides[{config_id}] does not belong to selected categories"
                )
            if isinstance(value, dict):
                raw_depth = value.get("product_count") or value.get("default_product_count_per_leaf") or default_depth
            else:
                raw_depth = value or default_depth
            normalized_subcategory_overrides[config_id] = _normalize_positive_int(
                raw_depth,
                field_name=f"subcategory_overrides[{config_id}]",
            )
    return {
        "categories": [str(item).strip() for item in categories if str(item).strip()],
        "default_product_count_per_leaf": default_depth,
        "category_overrides": normalized_overrides,
        "subcategory_overrides": normalized_subcategory_overrides,
        "persist": bool(payload.get("persist", True)),
        "export_excel": bool(payload.get("export_excel", False)),
        "reason": str(payload.get("reason") or ""),
    }


def normalize_plan_payload(plan_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized_type = str(plan_type or "").strip().lower()
    payload = dict(payload or {})
    if normalized_type == "keyword_batch":
        return _normalize_keyword_batch_payload(payload)
    if normalized_type == "keyword_monitor":
        return _normalize_keyword_monitor_payload(payload)
    if normalized_type == "category_single":
        return _normalize_category_single_payload(payload)
    if normalized_type == "category_ready_scan":
        return _normalize_category_ready_scan_payload(payload)
    raise ValueError(f"unsupported plan_type: {normalized_type}")


def build_task_payload_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return normalize_plan_payload(str(plan.get("plan_type") or ""), dict(plan.get("payload") or {}))


def _build_round_context(plan: dict[str, Any], round_id: int) -> dict[str, Any]:
    plan_id = int(plan["id"])
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    context: dict[str, Any] = {"resume": True}
    if plan_type == "category_ready_scan":
        context["output_dir"] = str(ROOT / "data" / "batch_scans" / f"plan_{plan_id}_round_{round_id}")
    elif plan_type == "category_single":
        context["snapshot"] = f"category_plan_{plan_id}_round_{round_id}"
    elif plan_type in {"keyword_batch", "keyword_monitor"}:
        context["snapshot"] = f"{plan_type}_plan_{plan_id}_round_{round_id}"
    return context


def _ensure_open_round(store: OpsStore, plan: dict[str, Any]) -> dict[str, Any]:
    open_round = store.get_open_crawl_round_for_plan(int(plan["id"]))
    if open_round is not None:
        if not store.list_crawl_round_items(int(open_round["id"])):
            store.create_crawl_round_items(int(open_round["id"]), _build_round_items(plan, open_round))
        refreshed = store.get_crawl_round(int(open_round["id"]))
        if refreshed is not None:
            return refreshed
        return open_round
    round_payload = build_task_payload_from_plan(plan)
    created_round = store.create_crawl_round(
        plan_id=int(plan["id"]),
        plan_type=str(plan["plan_type"]),
        payload=round_payload,
    )
    context = _build_round_context(plan, int(created_round["id"]))
    updated_round = store.update_crawl_round(int(created_round["id"]), context=context) or created_round
    store.create_crawl_round_items(int(updated_round["id"]), _build_round_items(plan, updated_round))
    return store.get_crawl_round(int(updated_round["id"])) or updated_round


def _build_round_items(plan: dict[str, Any], crawl_round: dict[str, Any]) -> list[dict[str, Any]]:
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    payload = dict(crawl_round.get("payload") or {})
    items: list[dict[str, Any]] = []
    if plan_type == "category_ready_scan":
        categories = payload.get("categories") or load_ready_categories()
        overrides = payload.get("category_overrides") or {}
        for index, category in enumerate(categories):
            category_name = str(category).strip()
            if not category_name:
                continue
            category_payload = {
                **payload,
                "categories": [category_name],
                "category_overrides": (
                    {category_name: int(overrides[category_name])}
                    if category_name in overrides
                    else {}
                ),
            }
            items.append(
                {
                    "item_key": category_name,
                    "display_name": f"Category / {category_name}",
                    "item_order": index,
                    "payload": category_payload,
                }
            )
        return items
    if plan_type == "category_single":
        category_name = str(payload.get("category") or "").strip()
        return [
            {
                "item_key": category_name or "category-single",
                "display_name": f"Category / {category_name}" if category_name else "Category / single",
                "item_order": 0,
                "payload": payload,
            }
        ]
    if plan_type == "keyword_batch":
        keywords = payload.get("keywords") or []
        for index, keyword in enumerate(keywords):
            token = str(keyword).strip()
            if not token:
                continue
            items.append(
                {
                    "item_key": token,
                    "display_name": f"Keyword / {token}",
                    "item_order": index,
                    "payload": {
                        **payload,
                        "keywords": [token],
                    },
                }
            )
        return items
    if plan_type == "keyword_monitor":
        monitor_config = str(payload.get("monitor_config") or "").strip() or "monitor"
        seed_keywords = _load_monitor_seed_keywords(payload)
        if seed_keywords:
            for index, seed_keyword in enumerate(seed_keywords):
                items.append(
                    {
                        "item_key": f"seed-{index + 1:04d}",
                        "display_name": f"Monitor Seed / {seed_keyword}",
                        "item_order": index,
                        "payload": {
                            **payload,
                            "monitor_seed_keyword": seed_keyword,
                        },
                    }
                )
            return items
        return [
            {
                "item_key": monitor_config,
                "display_name": f"Monitor / {Path(monitor_config).name}",
                "item_order": 0,
                "payload": payload,
            }
        ]
    return []


def build_task_payload_for_round(plan: dict[str, Any], round_item: dict[str, Any], round_context: dict[str, Any]) -> dict[str, Any]:
    payload = normalize_plan_payload(str(plan.get("plan_type") or ""), dict(round_item.get("payload") or {}))
    context = dict(round_context or {})
    item_key = str(round_item.get("item_key") or "").strip()
    if context.get("output_dir"):
        output_dir = Path(str(context["output_dir"]))
        payload["output_dir"] = str(output_dir / item_key) if item_key else str(output_dir)
    if context.get("snapshot"):
        payload["snapshot"] = f"{context['snapshot']}_{item_key}" if item_key else str(context["snapshot"])
    if context.get("resume"):
        payload["resume"] = True
    payload["_round_id"] = int(round_context.get("id") or 0) if round_context.get("id") else None
    payload["_round_item_id"] = int(round_item.get("id") or 0) if round_item.get("id") else None
    return payload


def build_task_display_name(plan: dict[str, Any], round_item: dict[str, Any] | None = None) -> str:
    if round_item is not None:
        item_label = str(round_item.get("display_name") or "").strip()
        if item_label:
            return f"{plan.get('name')} 路 {item_label}"
    payload = dict(plan.get("payload") or {})
    plan_type = str(plan.get("plan_type") or "")
    if plan_type == "category_single":
        return f"{plan.get('name')} · {payload.get('category')}"
    if plan_type == "category_ready_scan":
        categories = payload.get("categories") or []
        if categories:
            return f"{plan.get('name')} · {len(categories)} categories"
        return f"{plan.get('name')} · ready scan"
    if plan_type == "keyword_batch":
        keywords = payload.get("keywords") or []
        return f"{plan.get('name')} · {len(keywords)} keywords"
    if plan_type == "keyword_monitor":
        return f"{plan.get('name')} · monitor"
    return str(plan.get("name") or plan_type)


def launch_plan_now(store: OpsStore, plan_id: int, *, created_by: str = "crawler_console") -> dict[str, Any]:
    plan = store.get_crawl_plan(plan_id)
    if plan is None:
        raise ValueError(f"plan not found: {plan_id}")
    active = store.list_active_tasks_for_plan(plan_id)
    if active:
        plan_type = str(plan.get("plan_type") or "").strip().lower()
        skip_reason = _plan_active_skip_reason(plan_type)
        if skip_reason:
            skip_detail = _plan_active_skip_detail(plan, active)
            logger.info("plan launch skipped due to active task: plan_id=%s detail=%s", plan_id, skip_detail)
            if skip_reason == ACTIVE_MONITOR_SKIP_REASON:
                return _keyword_monitor_skip_dispatch_result(plan, created_by=created_by, skip_detail=skip_detail)
            return _category_crawl_skip_dispatch_result(plan, created_by=created_by, skip_detail=skip_detail)
        return active[0]
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    if plan_type == "keyword_monitor" and _keyword_monitor_lock_active():
        skip_detail = "keyword monitor is already active; launch skipped"
        logger.info("keyword monitor launch skipped: plan_id=%s detail=%s", plan_id, skip_detail)
        return _keyword_monitor_skip_dispatch_result(plan, created_by=created_by, skip_detail=skip_detail)
    if plan_type in {"category_single", "category_ready_scan"}:
        skip_detail = _category_crawl_lock_detail(plan)
        if skip_detail:
            logger.info("category crawl launch skipped: plan_id=%s detail=%s", plan_id, skip_detail)
            return _category_crawl_skip_dispatch_result(plan, created_by=created_by, skip_detail=skip_detail)
    crawl_round = _ensure_open_round(store, plan)
    round_item = store.get_next_dispatchable_crawl_round_item(int(crawl_round["id"]))
    if round_item is None:
        updated_round = store.get_crawl_round(int(crawl_round["id"]))
        if updated_round is not None and updated_round.get("status") != "active":
            crawl_round = _ensure_open_round(store, plan)
            round_item = store.get_next_dispatchable_crawl_round_item(int(crawl_round["id"]))
    if round_item is None:
        raise ValueError(f"no dispatchable round items for plan: {plan_id}")
    task = store.create_task(
        task_type=str(plan["plan_type"]),
        payload=build_task_payload_for_round(plan, round_item, {**crawl_round.get("context", {}), "id": crawl_round["id"]}),
        created_by=created_by,
        priority=10,
        plan_id=int(plan_id),
        round_id=int(crawl_round["id"]),
        round_item_id=int(round_item["id"]),
        display_name=build_task_display_name(plan, round_item),
    )
    store.mark_crawl_plan_dispatched(plan_id=int(plan_id), task_id=int(task["id"]))
    store.mark_crawl_round_dispatched(round_id=int(crawl_round["id"]), task_id=int(task["id"]))
    store.mark_crawl_round_item_dispatched(item_id=int(round_item["id"]), task_id=int(task["id"]))
    return task


def dispatch_due_plans(store: OpsStore) -> list[dict[str, Any]]:
    dispatched = []
    for plan in store.list_due_crawl_plans(limit=50):
        active_tasks = store.list_active_tasks_for_plan(int(plan["id"]))
        if active_tasks:
            plan_type = str(plan.get("plan_type") or "").strip().lower()
            skip_reason = _plan_active_skip_reason(plan_type)
            if skip_reason:
                skip_detail = _plan_active_skip_detail(plan, active_tasks)
                logger.info("plan dispatch skipped due to active task: plan_id=%s detail=%s", int(plan["id"]), skip_detail)
                if skip_reason == ACTIVE_MONITOR_SKIP_REASON:
                    dispatched.append(
                        _keyword_monitor_skip_dispatch_result(
                            plan,
                            created_by="scheduler",
                            skip_detail=skip_detail,
                        )
                    )
                else:
                    dispatched.append(
                        _category_crawl_skip_dispatch_result(
                            plan,
                            created_by="scheduler",
                            skip_detail=skip_detail,
                        )
                    )
                continue
            continue
        plan_type = str(plan.get("plan_type") or "").strip().lower()
        if plan_type == "keyword_monitor" and _keyword_monitor_lock_active():
            logger.info("keyword monitor dispatch skipped due to active monitor: plan_id=%s", int(plan["id"]))
            dispatched.append(
                _keyword_monitor_skip_dispatch_result(
                    plan,
                    created_by="scheduler",
                    skip_detail="keyword monitor is already active",
                )
            )
            continue
        if plan_type in {"category_single", "category_ready_scan"}:
            skip_detail = _category_crawl_lock_detail(plan)
            if skip_detail:
                logger.info("category crawl dispatch skipped due to active crawler: plan_id=%s detail=%s", int(plan["id"]), skip_detail)
                dispatched.append(
                    _category_crawl_skip_dispatch_result(
                        plan,
                        created_by="scheduler",
                        skip_detail=skip_detail,
                    )
                )
                continue
        crawl_round = _ensure_open_round(store, plan)
        round_item = store.get_next_dispatchable_crawl_round_item(int(crawl_round["id"]))
        if round_item is None:
            updated_round = store.get_crawl_round(int(crawl_round["id"]))
            if updated_round is not None and updated_round.get("status") != "active":
                crawl_round = _ensure_open_round(store, plan)
                round_item = store.get_next_dispatchable_crawl_round_item(int(crawl_round["id"]))
        if round_item is None:
            continue
        task = store.create_task(
            task_type=str(plan["plan_type"]),
            payload=build_task_payload_for_round(plan, round_item, {**crawl_round.get("context", {}), "id": crawl_round["id"]}),
            created_by="scheduler",
            priority=20,
            plan_id=int(plan["id"]),
            round_id=int(crawl_round["id"]),
            round_item_id=int(round_item["id"]),
            display_name=build_task_display_name(plan, round_item),
        )
        store.mark_crawl_plan_dispatched(plan_id=int(plan["id"]), task_id=int(task["id"]))
        store.mark_crawl_round_dispatched(round_id=int(crawl_round["id"]), task_id=int(task["id"]))
        store.mark_crawl_round_item_dispatched(item_id=int(round_item["id"]), task_id=int(task["id"]))
        dispatched.append(task)
    return dispatched


def serialize_crawler_run(task: dict[str, Any], task_runs: list[dict[str, Any]]) -> dict[str, Any]:
    latest_run = task_runs[0] if task_runs else None
    return {
        "task_id": task["id"],
        "plan_id": task.get("plan_id"),
        "task_type": task.get("task_type"),
        "display_name": task.get("display_name") or PLAN_LABELS.get(task.get("task_type"), task.get("task_type")),
        "status": task.get("status"),
        "worker_type": task.get("worker_type"),
        "priority": task.get("priority"),
        "created_by": task.get("created_by"),
        "last_run_at": task.get("last_run_at"),
        "updated_at": task.get("updated_at"),
        "payload": task.get("payload") or {},
        "progress": task.get("progress") or {},
        "latest_run": latest_run,
        "run_count": len(task_runs),
    }


def list_crawler_runs(store: OpsStore, *, limit: int = 100) -> list[dict[str, Any]]:
    tasks = [
        item
        for item in store.list_tasks(limit=limit * 3)
        if str(item.get("task_type") or "") in SUPPORTED_PLAN_TYPES
    ][:limit]
    task_ids = [int(item["id"]) for item in tasks]
    run_map = {task_id: store.list_task_runs(task_id=task_id, limit=10) for task_id in task_ids}
    return [serialize_crawler_run(task, run_map.get(int(task["id"]), [])) for task in tasks]


def get_crawler_run_detail(store: OpsStore, task_id: int) -> dict[str, Any] | None:
    task = store.get_task(task_id)
    if task is None:
        return None
    if str(task.get("task_type") or "") not in SUPPORTED_PLAN_TYPES:
        return None
    return serialize_crawler_run(task, store.list_task_runs(task_id=task_id, limit=50))
