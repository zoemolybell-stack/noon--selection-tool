from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CATEGORY_FAILOVER_STATE_PATH = ROOT / "runtime_data" / "category" / "failover_state.json"

# Give the remote node a little less than 3 minutes so scheduler/watcher
# polling jitter still fits inside the user-facing <= 3 minute takeover goal.
REMOTE_HEARTBEAT_TIMEOUT_SECONDS = 150
REMOTE_FAILBACK_HEALTHY_SECONDS = 15 * 60
REMOTE_INFRA_FAILURE_WINDOW_SECONDS = 30 * 60
REMOTE_INFRA_FAILURE_THRESHOLD = 3

CATEGORY_FAILURE_CODES = (
    "node_unavailable",
    "chrome_unavailable",
    "db_tunnel_unavailable",
    "warehouse_sync_failed",
    "access_denied",
    "timeout",
    "page_parse_failure",
    "result_contract_mismatch",
)
CATEGORY_INFRA_FAILURE_CODES = {
    "node_unavailable",
    "chrome_unavailable",
    "db_tunnel_unavailable",
    "warehouse_sync_failed",
}
ROUND_ABORT_FAILURE_CATEGORY = "node_unavailable"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _parse_dt(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _json_load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


def _coalesce_reason_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def parse_category_failure_code(error_text: object) -> str:
    text = str(error_text or "").strip()
    if not text:
        return ""
    if ":" not in text:
        return ""
    prefix = text.split(":", 1)[0].strip().lower()
    return prefix if prefix in CATEGORY_FAILURE_CODES else ""


def classify_category_failure(
    *,
    error_text: str = "",
    result: dict[str, Any] | None = None,
) -> dict[str, str]:
    payload = dict(result or {})
    stdout_tail = payload.get("stdout_tail") if isinstance(payload.get("stdout_tail"), list) else []
    stderr_tail = payload.get("stderr_tail") if isinstance(payload.get("stderr_tail"), list) else []
    text_parts = [str(item) for item in stderr_tail + stdout_tail if str(item).strip()]
    if error_text:
        text_parts.append(str(error_text))
    normalized_text = "\n".join(text_parts)
    lower = normalized_text.lower()

    failure_category = parse_category_failure_code(error_text)
    if not failure_category:
        if "category_ready_scan followup warehouse sync failed" in lower or "warehouse_sync_failed" in lower:
            failure_category = "warehouse_sync_failed"
        elif "task_lease_lost" in lower or "lease_lost" in lower or "node unavailable" in lower:
            failure_category = "node_unavailable"
        elif "access denied" in lower or "errors.edgesuite.net" in lower or "akamai" in lower:
            failure_category = "access_denied"
        elif "page.goto timeout" in lower or "timeout" in lower or "timed out" in lower:
            failure_category = "timeout"
        elif (
            "psycopg" in lower
            or "connection refused" in lower
            or "network is unreachable" in lower
            or "server closed the connection unexpectedly" in lower
        ) and ("55432" in lower or "nas-db-tunnel" in lower or "db tunnel" in lower):
            failure_category = "db_tunnel_unavailable"
        elif "remote debugging" in lower or "json/version" in lower or "chrome unavailable" in lower or "browser cdp" in lower:
            failure_category = "chrome_unavailable"
        elif (
            "missing_result_file" in lower
            or "invalid_json" in lower
            or "incomplete_json" in lower
            or "result_contract_mismatch" in lower
        ):
            failure_category = "result_contract_mismatch"
        elif (
            "page_parse_failure" in lower
            or "parse failure" in lower
            or "selector_miss" in lower
            or "unable to parse" in lower
        ):
            failure_category = "page_parse_failure"
        else:
            failure_category = "node_unavailable"

    summary = _coalesce_reason_text(error_text)
    if not summary:
        summary = _coalesce_reason_text(stderr_tail[-1] if stderr_tail else "")
    if not summary:
        summary = _coalesce_reason_text(stdout_tail[-1] if stdout_tail else "")
    if not summary:
        summary = failure_category
    return {
        "failure_category": failure_category,
        "failure_summary": summary[:240],
        "last_error": f"{failure_category}: {summary[:240]}",
    }


def read_category_failover_state() -> dict[str, Any]:
    payload = _json_load(CATEGORY_FAILOVER_STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _heartbeat_age_seconds(worker: dict[str, Any], *, reference_time: datetime) -> int | None:
    heartbeat_at = _parse_dt(worker.get("heartbeat_at"))
    if heartbeat_at is None:
        return None
    return max(0, int((reference_time - heartbeat_at).total_seconds()))


def _recent_category_infra_failure_count(store, *, reference_time: datetime) -> int:
    cutoff = reference_time - timedelta(seconds=REMOTE_INFRA_FAILURE_WINDOW_SECONDS)
    count = 0
    for task in store.list_tasks(worker_type="category", limit=500):
        if str(task.get("status") or "").strip().lower() != "failed":
            continue
        updated_at = _parse_dt(task.get("updated_at"))
        if updated_at is None or updated_at < cutoff:
            continue
        code = parse_category_failure_code(task.get("last_error"))
        if code in CATEGORY_INFRA_FAILURE_CODES:
            count += 1
    return count


def evaluate_category_failover_state(
    store,
    *,
    reference_time: datetime | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    now = (reference_time or _utcnow()).astimezone(timezone.utc)
    previous = read_category_failover_state() if persist else {}
    remote_enabled = _is_truthy(os.getenv("NOON_REMOTE_CATEGORY_NODE_ENABLED"))
    workers = list(store.list_workers(max_age_seconds=None))
    remote_workers = []
    fallback_workers = []
    for worker in workers:
        details = worker.get("details") if isinstance(worker.get("details"), dict) else {}
        node_role = str(details.get("node_role") or worker.get("node_role") or "").strip().lower()
        if node_role == "remote_category":
            remote_workers.append(worker)
        elif node_role == "fallback_category":
            fallback_workers.append(worker)

    remote_workers.sort(key=lambda item: str(item.get("heartbeat_at") or ""), reverse=True)
    latest_remote = remote_workers[0] if remote_workers else None
    latest_remote_details = latest_remote.get("details") if isinstance(latest_remote, dict) and isinstance(latest_remote.get("details"), dict) else {}
    remote_heartbeat_age_seconds = _heartbeat_age_seconds(latest_remote or {}, reference_time=now)
    chrome_ready = latest_remote_details.get("chrome_ready")
    db_tunnel_ready = latest_remote_details.get("db_tunnel_ready")

    chrome_failure_streak = 0 if chrome_ready is not False else int(previous.get("chrome_failure_streak") or 0) + 1
    db_tunnel_failure_streak = 0 if db_tunnel_ready is not False else int(previous.get("db_tunnel_failure_streak") or 0) + 1
    recent_infra_failures = _recent_category_infra_failure_count(store, reference_time=now)

    remote_state = "disabled"
    remote_healthy = False
    if remote_enabled:
        if latest_remote is None or remote_heartbeat_age_seconds is None:
            remote_state = "missing"
        elif remote_heartbeat_age_seconds > REMOTE_HEARTBEAT_TIMEOUT_SECONDS:
            remote_state = "missing"
        elif chrome_failure_streak >= 2:
            remote_state = "unhealthy"
        elif db_tunnel_failure_streak >= 2:
            remote_state = "unhealthy"
        elif recent_infra_failures >= REMOTE_INFRA_FAILURE_THRESHOLD:
            remote_state = "unhealthy"
        else:
            health_state = str(latest_remote_details.get("health_state") or "").strip().lower()
            if health_state in {"unhealthy", "failed"}:
                remote_state = "unhealthy"
            else:
                remote_state = "healthy"
                remote_healthy = True

    fallback_running = any(worker.get("current_task_id") for worker in fallback_workers)
    previous_mode = str(previous.get("mode") or "").strip().lower()
    remote_healthy_since = _parse_dt(previous.get("remote_healthy_since"))
    last_failover_at = str(previous.get("last_failover_at") or "")
    last_failback_at = str(previous.get("last_failback_at") or "")

    if not remote_enabled:
        mode = "disabled"
        remote_healthy_since = None
    elif not remote_healthy:
        mode = "fallback_active"
        remote_healthy_since = None
        if previous_mode != "fallback_active":
            last_failover_at = now.isoformat()
    else:
        if previous_mode == "fallback_active":
            if remote_healthy_since is None:
                remote_healthy_since = now
            stable_seconds = max(0, int((now - remote_healthy_since).total_seconds()))
            if stable_seconds >= REMOTE_FAILBACK_HEALTHY_SECONDS and not fallback_running:
                mode = "remote_active"
                last_failback_at = now.isoformat()
            else:
                mode = "fallback_active"
        else:
            mode = "remote_active"
            if remote_healthy_since is None:
                remote_healthy_since = now

    payload = {
        "mode": mode,
        "updated_at": now.isoformat(),
        "remote_category_node_enabled": remote_enabled,
        "remote_category_node_state": remote_state,
        "remote_worker_heartbeat_present": latest_remote is not None,
        "remote_worker_heartbeat_age_seconds": remote_heartbeat_age_seconds,
        "remote_worker_name": str(latest_remote.get("worker_name") or "") if latest_remote else "",
        "remote_worker_host": str(latest_remote_details.get("node_host") or "") if latest_remote_details else "",
        "chrome_ready": chrome_ready,
        "db_tunnel_ready": db_tunnel_ready,
        "chrome_failure_streak": chrome_failure_streak,
        "db_tunnel_failure_streak": db_tunnel_failure_streak,
        "recent_category_infra_failures": recent_infra_failures,
        "fallback_worker_count": len(fallback_workers),
        "fallback_has_running_task": fallback_running,
        "fallback_should_accept_tasks": mode == "fallback_active",
        "nas_category_fallback_active": mode == "fallback_active",
        "category_failover_state": mode,
        "last_category_failover_at": last_failover_at,
        "last_category_failback_at": last_failback_at,
        "remote_healthy_since": remote_healthy_since.isoformat() if remote_healthy_since else "",
    }
    if persist:
        _atomic_write_json(CATEGORY_FAILOVER_STATE_PATH, payload)
    return payload


def _round_item_failure_category(round_item: dict[str, Any]) -> str:
    result_payload = round_item.get("result") if isinstance(round_item.get("result"), dict) else {}
    failure_category = str(result_payload.get("failure_category") or "").strip().lower()
    if failure_category in CATEGORY_FAILURE_CODES:
        return failure_category
    return parse_category_failure_code(round_item.get("last_error"))


def evaluate_category_round_guardrail(
    round_items: list[dict[str, Any]],
    *,
    failover_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sorted_items = sorted(
        (item for item in round_items if isinstance(item, dict)),
        key=lambda item: (int(item.get("item_order") or 0), int(item.get("id") or 0)),
    )
    pending_items = [item for item in sorted_items if str(item.get("status") or "").strip().lower() == "pending"]
    running_items = [item for item in sorted_items if str(item.get("status") or "").strip().lower() == "running"]
    if running_items or not pending_items:
        return {"abort": False, "reason": "", "failure_category": "", "details": {}}

    failover_payload = dict(failover_state or {})
    remote_enabled = bool(failover_payload.get("remote_category_node_enabled"))
    remote_state = str(failover_payload.get("remote_category_node_state") or "").strip().lower()
    fallback_should_accept = bool(failover_payload.get("fallback_should_accept_tasks"))
    fallback_worker_count = int(failover_payload.get("fallback_worker_count") or 0)
    if remote_enabled and remote_state in {"missing", "unhealthy"} and (not fallback_should_accept or fallback_worker_count <= 0):
        return {
            "abort": True,
            "reason": "remote_node_unhealthy_without_fallback",
            "failure_category": ROUND_ABORT_FAILURE_CATEGORY,
            "details": {
                "remote_category_node_state": remote_state or "missing",
                "fallback_worker_count": fallback_worker_count,
                "fallback_should_accept_tasks": fallback_should_accept,
            },
        }

    consecutive_infra_failures = 0
    last_failure_category = ""
    primary_failure_category = ""
    terminal_history = [
        item
        for item in sorted_items
        if str(item.get("status") or "").strip().lower() in {"completed", "failed", "skipped"}
    ]
    for item in reversed(terminal_history):
        status = str(item.get("status") or "").strip().lower()
        if status == "skipped":
            continue
        if status != "failed":
            break
        last_failure_category = _round_item_failure_category(item)
        if not primary_failure_category and last_failure_category:
            primary_failure_category = last_failure_category
        if last_failure_category not in CATEGORY_INFRA_FAILURE_CODES:
            break
        consecutive_infra_failures += 1
        if consecutive_infra_failures >= REMOTE_INFRA_FAILURE_THRESHOLD:
            return {
                "abort": True,
                "reason": "consecutive_infra_failures",
                "failure_category": primary_failure_category or last_failure_category or ROUND_ABORT_FAILURE_CATEGORY,
                "details": {
                    "consecutive_infra_failures": consecutive_infra_failures,
                    "failure_category": primary_failure_category or last_failure_category or ROUND_ABORT_FAILURE_CATEGORY,
                },
            }

    return {
        "abort": False,
        "reason": "",
        "failure_category": "",
        "details": {
            "consecutive_infra_failures": consecutive_infra_failures,
            "last_failure_category": last_failure_category,
        },
    }
