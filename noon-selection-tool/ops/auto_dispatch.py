from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any


AUTO_DISPATCH_ENTRY = "crawl_plans"
AUTO_DISPATCH_INTERVAL_SECONDS = 86400

CANONICAL_CATEGORY_PLAN_NAME = "daily-category-all-categories-x1000"
CANONICAL_KEYWORD_PLAN_NAME = "daily-keyword-pet-sports-x300"

CANONICAL_CATEGORY_PLAN_PAYLOAD: dict[str, Any] = {
    "categories": [
        "automotive",
        "baby",
        "beauty",
        "electronics",
        "fashion",
        "garden",
        "grocery",
        "home_kitchen",
        "office",
        "pets",
        "sports",
        "tools",
    ],
    "category_overrides": {},
    "default_product_count_per_leaf": 1000,
    "export_excel": False,
    "persist": True,
    "reason": "daily remote category scan all categories x1000",
    "subcategory_overrides": {},
}

CANONICAL_KEYWORD_PLAN_PAYLOAD: dict[str, Any] = {
    "amazon_count": 300,
    "monitor_config": "/app/runtime_data/keyword/control/keyword_monitor_pet_sports_12h.json",
    "monitor_report": False,
    "noon_count": 300,
    "persist": True,
    "reason": "daily keyword monitor pet+sports x300 per platform",
}

CANONICAL_AUTO_PLAN_SPECS: tuple[dict[str, Any], ...] = (
    {
        "family": "category",
        "plan_type": "category_ready_scan",
        "name": CANONICAL_CATEGORY_PLAN_NAME,
        "schedule_kind": "interval",
        "schedule_json": {"seconds": AUTO_DISPATCH_INTERVAL_SECONDS},
        "payload": CANONICAL_CATEGORY_PLAN_PAYLOAD,
        "aliases": (
            "remote-category-daily-all-categories-x1000",
            "alternating category sports-pets priority 500-200 postgres",
            "12h category ready scan x500",
        ),
    },
    {
        "family": "keyword",
        "plan_type": "keyword_monitor",
        "name": CANONICAL_KEYWORD_PLAN_NAME,
        "schedule_kind": "interval",
        "schedule_json": {"seconds": AUTO_DISPATCH_INTERVAL_SECONDS},
        "payload": CANONICAL_KEYWORD_PLAN_PAYLOAD,
        "aliases": (
            "alternating keyword pets-sports x300 postgres",
            "12h keyword pets sports ~4000",
        ),
    },
)

_LEGACY_AUTO_PLAN_NAMES = frozenset(
    name
    for spec in CANONICAL_AUTO_PLAN_SPECS
    for name in spec["aliases"]
)


def _utc_now(reference_time: datetime | None = None) -> datetime:
    now = reference_time or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalized_name(value: Any) -> str:
    return str(value or "").strip()


def _canonical_match_rank(plan: dict[str, Any], canonical_name: str) -> tuple[int, float]:
    activity_dt = _parse_dt(plan.get("last_dispatched_at")) or _parse_dt(plan.get("updated_at"))
    timestamp = activity_dt.timestamp() if activity_dt is not None else 0.0
    exact_rank = 0 if _normalized_name(plan.get("name")) == canonical_name else 1
    return (exact_rank, -timestamp)


def _scheduler_heartbeat_ok(worker_items: list[dict[str, Any]], stale_after_seconds: int = 1200) -> bool:
    for item in worker_items:
        worker_type = str(item.get("worker_type") or "").strip().lower()
        if worker_type != "scheduler":
            continue
        age = item.get("last_heartbeat_age_seconds")
        if age is None:
            return True
        try:
            if float(age) <= float(stale_after_seconds):
                return True
        except (TypeError, ValueError):
            continue
    return False


def canonical_auto_plan_specs() -> list[dict[str, Any]]:
    return [deepcopy(spec) for spec in CANONICAL_AUTO_PLAN_SPECS]


def is_canonical_auto_plan(plan: dict[str, Any]) -> bool:
    name = _normalized_name(plan.get("name"))
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    return any(
        name == spec["name"] and plan_type == str(spec["plan_type"]).strip().lower()
        for spec in CANONICAL_AUTO_PLAN_SPECS
    )


def is_legacy_auto_plan_candidate(plan: dict[str, Any]) -> bool:
    if is_canonical_auto_plan(plan):
        return False
    schedule_kind = str(plan.get("schedule_kind") or "").strip().lower()
    name = _normalized_name(plan.get("name"))
    return schedule_kind != "manual" or name in _LEGACY_AUTO_PLAN_NAMES


def auto_plan_family(plan: dict[str, Any]) -> str:
    name = _normalized_name(plan.get("name"))
    plan_type = str(plan.get("plan_type") or "").strip().lower()
    for spec in CANONICAL_AUTO_PLAN_SPECS:
        all_names = {spec["name"], *spec["aliases"]}
        if name in all_names and plan_type == str(spec["plan_type"]).strip().lower():
            return str(spec["family"])
    return ""


def annotate_crawl_plan(plan: dict[str, Any]) -> dict[str, Any]:
    annotated = dict(plan or {})
    family = auto_plan_family(annotated)
    annotated["auto_plan_family"] = family
    annotated["is_canonical_auto_plan"] = is_canonical_auto_plan(annotated)
    annotated["is_legacy_auto_plan_candidate"] = is_legacy_auto_plan_candidate(annotated)
    annotated["auto_dispatch_managed"] = bool(family)
    return annotated


def annotate_crawl_plans(plans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [annotate_crawl_plan(plan) for plan in plans]


def build_auto_dispatch_summary(
    plans: list[dict[str, Any]],
    *,
    worker_items: list[dict[str, Any]] | None = None,
    host_alternating_service_active: bool | None = None,
    scheduler_stale_after_seconds: int = 1200,
) -> dict[str, Any]:
    now = _utc_now()
    annotated_plans = annotate_crawl_plans(plans)
    scheduler_ok = _scheduler_heartbeat_ok(worker_items or [], stale_after_seconds=scheduler_stale_after_seconds)
    canonical_plans: list[dict[str, Any]] = []
    for spec in CANONICAL_AUTO_PLAN_SPECS:
        matches = [
            plan
            for plan in annotated_plans
            if str(plan.get("name") or "").strip() == spec["name"]
            and str(plan.get("plan_type") or "").strip().lower() == str(spec["plan_type"]).strip().lower()
        ]
        matches.sort(
            key=lambda item: _parse_dt(item.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        plan = matches[0] if matches else None
        next_run_at = _parse_dt((plan or {}).get("next_run_at"))
        canonical_plans.append(
            {
                "family": spec["family"],
                "name": spec["name"],
                "plan_type": spec["plan_type"],
                "exists": bool(plan),
                "plan_id": int(plan["id"]) if isinstance(plan, dict) and plan.get("id") is not None else None,
                "enabled": bool((plan or {}).get("enabled")),
                "schedule_kind": str((plan or {}).get("schedule_kind") or ""),
                "schedule_json": dict((plan or {}).get("schedule_json") or {}),
                "next_run_at": (plan or {}).get("next_run_at"),
                "is_due": bool(next_run_at and next_run_at <= now),
                "last_run_status": str((plan or {}).get("last_run_status") or ""),
                "last_run_task_id": (plan or {}).get("last_run_task_id"),
                "updated_at": (plan or {}).get("updated_at"),
                "payload": deepcopy((plan or {}).get("payload") or spec["payload"]),
            }
        )
    enabled_canonical_count = sum(1 for item in canonical_plans if item["enabled"])
    legacy_host_active = bool(host_alternating_service_active)
    return {
        "auto_dispatch_entry": AUTO_DISPATCH_ENTRY,
        "scheduler_heartbeat_ok": scheduler_ok,
        "canonical_auto_plans": canonical_plans,
        "canonical_auto_plan_count": len(canonical_plans),
        "enabled_canonical_auto_plan_count": enabled_canonical_count,
        "missing_canonical_families": [item["family"] for item in canonical_plans if not item["exists"]],
        "disabled_canonical_families": [item["family"] for item in canonical_plans if item["exists"] and not item["enabled"]],
        "legacy_host_alternating_service_active": legacy_host_active,
        "auto_dispatch_conflict": legacy_host_active and enabled_canonical_count > 0,
        "ready": scheduler_ok and enabled_canonical_count == len(canonical_plans) and not legacy_host_active,
    }


def sync_canonical_auto_plans(
    store,
    *,
    reference_time: datetime | None = None,
    force_due: bool = False,
) -> dict[str, Any]:
    now = _utc_now(reference_time).replace(microsecond=0)
    aligned_next_run_at = (now + timedelta(seconds=AUTO_DISPATCH_INTERVAL_SECONDS)).isoformat()
    force_due_at = now.isoformat()
    existing = store.list_crawl_plans(limit=500)
    matched_plan_ids: set[int] = set()
    canonical_results: list[dict[str, Any]] = []
    disabled_duplicate_ids: list[int] = []
    for spec in CANONICAL_AUTO_PLAN_SPECS:
        all_names = (spec["name"], *spec["aliases"])
        matches = [
            plan
            for plan in existing
            if str(plan.get("plan_type") or "").strip().lower() == str(spec["plan_type"]).strip().lower()
            and _normalized_name(plan.get("name")) in all_names
        ]
        matches.sort(key=lambda item: _canonical_match_rank(item, str(spec["name"])))
        reusable = matches[0] if matches else None
        if reusable is not None:
            plan = store.update_crawl_plan(
                int(reusable["id"]),
                name=spec["name"],
                enabled=True,
                schedule_kind=str(spec["schedule_kind"]),
                schedule_json=dict(spec["schedule_json"]),
                payload=deepcopy(spec["payload"]),
            )
        else:
            plan = store.create_crawl_plan(
                plan_type=str(spec["plan_type"]),
                name=spec["name"],
                created_by="auto_dispatch_repair",
                schedule_kind=str(spec["schedule_kind"]),
                schedule_json=dict(spec["schedule_json"]),
                payload=deepcopy(spec["payload"]),
                enabled=True,
            )
        if plan is None:
            continue
        selected_plan_id = int(plan["id"])
        matched_plan_ids.add(selected_plan_id)
        for duplicate in matches:
            duplicate_id = int(duplicate["id"])
            if duplicate_id == selected_plan_id:
                continue
            updated_duplicate = store.update_crawl_plan(
                duplicate_id,
                enabled=False,
                schedule_kind="manual",
                schedule_json={},
            )
            disabled_duplicate_ids.append(int((updated_duplicate or duplicate)["id"]))
        canonical_results.append(plan)

    refreshed_plans = store.list_crawl_plans(limit=500)
    disabled_legacy_ids: list[int] = []
    for plan in refreshed_plans:
        plan_id = int(plan["id"])
        if plan_id in matched_plan_ids:
            continue
        if not is_legacy_auto_plan_candidate(plan):
            continue
        updated = store.update_crawl_plan(plan_id, enabled=False)
        disabled_legacy_ids.append(int((updated or plan)["id"]))

    target_next_run_at = force_due_at if force_due else aligned_next_run_at
    aligned_plan_ids: list[int] = []
    for plan in canonical_results:
        plan_id = int(plan["id"])
        store.set_crawl_plan_next_run_at(plan_id, target_next_run_at)
        aligned_plan_ids.append(plan_id)

    final_plans = store.list_crawl_plans(limit=500)
    summary = build_auto_dispatch_summary(final_plans, worker_items=[], host_alternating_service_active=False)
    summary["repair_reference_time"] = now.isoformat()
    summary["aligned_next_run_at"] = aligned_next_run_at
    summary["force_due"] = bool(force_due)
    summary["forced_due_at"] = force_due_at if force_due else None
    summary["aligned_plan_ids"] = aligned_plan_ids
    summary["disabled_legacy_plan_ids"] = sorted(disabled_legacy_ids)
    summary["disabled_duplicate_plan_ids"] = sorted(disabled_duplicate_ids)
    return summary
