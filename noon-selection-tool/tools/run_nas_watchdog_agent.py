from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.auto_dispatch import AUTO_DISPATCH_ENTRY
from tools.keyword_runtime_health import build_quality_payload


BASE_URL = os.getenv("NOON_WATCHDOG_BASE_URL", "http://127.0.0.1:8865")
REPORT_ROOT = Path(os.getenv("NOON_REPORT_ROOT", "/volume1/docker/huihaokang-erp/shared/report/crawl"))
STATE_DIR = REPORT_ROOT / "state"
DAILY_DIR = REPORT_ROOT / "daily"
LATEST_JSON = STATE_DIR / "watchdog_latest.json"
LATEST_MD = STATE_DIR / "watchdog_latest.md"
RUNS_FILE = STATE_DIR / "watchdog_runs.jsonl"
HOST_ALTERNATING_SERVICE = os.getenv("NOON_WATCHDOG_HOST_SERVICE", "huihaokang-alternating-crawl.service")
REPORT_SERVICE = os.getenv("NOON_WATCHDOG_REPORT_SERVICE", "huihaokang-crawl-report.service")
REPORT_TIMER = os.getenv("NOON_WATCHDOG_REPORT_TIMER", "huihaokang-crawl-report.timer")
HOST_ALTERNATING_SERVICE_STATE = os.getenv("NOON_WATCHDOG_HOST_SERVICE_STATE", "").strip()
HOST_ALTERNATING_SERVICE_SUBSTATE = os.getenv("NOON_WATCHDOG_HOST_SERVICE_SUBSTATE", "").strip()
REPORT_SERVICE_STATE = os.getenv("NOON_WATCHDOG_REPORT_SERVICE_STATE", "").strip()
REPORT_SERVICE_SUBSTATE = os.getenv("NOON_WATCHDOG_REPORT_SERVICE_SUBSTATE", "").strip()
REPORT_TIMER_STATE = os.getenv("NOON_WATCHDOG_REPORT_TIMER_STATE", "").strip()
REPORT_TIMER_SUBSTATE = os.getenv("NOON_WATCHDOG_REPORT_TIMER_SUBSTATE", "").strip()
STALE_WORKER_SECONDS = int(os.getenv("NOON_WATCHDOG_STALE_WORKER_SECONDS", "1200"))
STALE_TASK_SECONDS = int(os.getenv("NOON_WATCHDOG_STALE_TASK_SECONDS", "1200"))
REPORT_MAX_AGE_HOURS = int(os.getenv("NOON_WATCHDOG_REPORT_MAX_AGE_HOURS", "26"))
REPORT_EXPECTATION_HOUR = int(os.getenv("NOON_WATCHDOG_REPORT_EXPECTATION_HOUR", "10"))
REPORT_EXPECTATION_MINUTE = int(os.getenv("NOON_WATCHDOG_REPORT_EXPECTATION_MINUTE", "15"))
REQUEST_RETRIES = int(os.getenv("NOON_WATCHDOG_REQUEST_RETRIES", "3"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("NOON_WATCHDOG_REQUEST_TIMEOUT_SECONDS", "8"))
REQUEST_BACKOFF_SECONDS = float(os.getenv("NOON_WATCHDOG_REQUEST_BACKOFF_SECONDS", "2"))
ACTIVE_TASK_STATUSES = {"queued", "pending", "running", "leased", "retrying"}
TERMINAL_TASK_STATUSES = {"completed", "failed", "cancelled", "skipped"}
KEYWORD_RUNTIME_ROOT = Path(os.getenv("NOON_KEYWORD_RUNTIME_DIR", "/volume1/docker/huihaokang-erp/runtime_data/keyword"))
KEYWORD_MONITOR_SUMMARY_PATH = os.getenv("NOON_KEYWORD_MONITOR_SUMMARY_PATH", "")
DIR_MODE = 0o755
FILE_MODE = 0o644


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def shanghai_now() -> datetime:
    return utc_now().astimezone(timezone(timedelta(hours=8)))


def parse_ts(value: Any) -> datetime | None:
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


def http_json(path: str, timeout: int = REQUEST_TIMEOUT_SECONDS, retries: int = REQUEST_RETRIES) -> dict[str, Any]:
    last_error: Exception | None = None
    attempts = max(1, retries)
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(f"{BASE_URL}{path}", method="GET")
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(REQUEST_BACKOFF_SECONDS * (attempt + 1))
    raise RuntimeError(f"request failed after {attempts} attempts: GET {path}: {last_error}") from last_error


def safe_http_json(path: str, timeout: int = REQUEST_TIMEOUT_SECONDS) -> tuple[dict[str, Any], str | None]:
    try:
        return http_json(path, timeout=timeout), None
    except Exception as exc:
        return {}, f"{type(exc).__name__}: {exc}"


def systemctl_state(unit: str, state_override: str = "", substate_override: str = "") -> dict[str, Any]:
    result = {
        "unit": unit,
        "active": False,
        "state": "unknown",
        "substate": "unknown",
        "error": None,
    }
    if state_override:
        state = state_override.strip() or "unknown"
        result["state"] = state
        result["active"] = state == "active"
        if substate_override:
            result["substate"] = substate_override.strip() or "unknown"
        return result
    try:
        state = subprocess.check_output(["systemctl", "is-active", unit], text=True).strip()
        result["state"] = state or "unknown"
        result["active"] = state == "active"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    try:
        show = subprocess.check_output(["systemctl", "show", unit, "-p", "SubState"], text=True).strip()
        if show.startswith("SubState="):
            result["substate"] = show.split("=", 1)[1] or "unknown"
    except Exception as exc:
        if result["error"] is None:
            result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    try:
        REPORT_ROOT.chmod(DIR_MODE)
    except OSError:
        pass
    for path in (STATE_DIR, DAILY_DIR):
        try:
            path.chmod(DIR_MODE)
        except OSError:
            pass


def write_readable_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    try:
        path.chmod(FILE_MODE)
    except OSError:
        pass


def append_readable_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    try:
        path.chmod(FILE_MODE)
    except OSError:
        pass


def _keyword_monitor_summary_candidates() -> list[Path]:
    candidates: list[Path] = []
    if KEYWORD_MONITOR_SUMMARY_PATH:
        candidates.append(Path(KEYWORD_MONITOR_SUMMARY_PATH))
    candidates.extend(
        [
            KEYWORD_RUNTIME_ROOT / "monitor" / "keyword_monitor_last_run.json",
            Path("/app/runtime_data/keyword/monitor/keyword_monitor_last_run.json"),
            Path("/volume1/docker/huihaokang-erp/runtime_data/keyword/monitor/keyword_monitor_last_run.json"),
            Path(__file__).resolve().parents[1] / "runtime_data" / "keyword" / "monitor" / "keyword_monitor_last_run.json",
        ]
    )
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def load_latest_keyword_monitor_summary() -> tuple[dict[str, Any], str | None]:
    for path in _keyword_monitor_summary_candidates():
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload, str(path)
    return {}, None


def normalize_keyword_quality_summary(payload: dict[str, Any]) -> dict[str, Any]:
    quality = payload.get("quality_summary") if isinstance(payload.get("quality_summary"), dict) else {}
    candidate = dict(quality or payload)
    normalized = build_quality_payload(
        candidate,
        live_batch_state=str(
            candidate.get("live_batch_state")
            or payload.get("live_batch_state")
            or payload.get("activity_state")
            or payload.get("monitor_state")
            or payload.get("status")
            or "idle"
        ),
        latest_terminal_quality_state=str(
            candidate.get("latest_terminal_batch_state")
            or candidate.get("latest_terminal_quality_state")
            or candidate.get("latest_quality_state")
            or payload.get("latest_terminal_batch_state")
            or payload.get("latest_terminal_quality_state")
            or payload.get("quality_state")
            or "unknown"
        ),
        operator_quality_state=str(
            candidate.get("operator_quality_state")
            or payload.get("operator_quality_state")
            or payload.get("quality_state")
            or "unknown"
        ),
    )
    normalized.update(
        {
            "state": candidate.get("state") or normalized.get("state") or "unknown",
            "crawl_state": candidate.get("crawl_state") or payload.get("crawl_state") or normalized.get("live_batch_state") or "unknown",
            "analysis_state": candidate.get("analysis_state") or payload.get("analysis_state") or "unknown",
            "platforms": dict(candidate.get("platforms") or payload.get("platforms") or {}),
            "signals": dict(candidate.get("signals") or payload.get("signals") or {}),
            "quality_flags": list(candidate.get("quality_flags") or payload.get("quality_flags") or []),
        }
    )
    return normalized


def load_latest_report_stamp() -> tuple[datetime | None, str | None]:
    latest_json = REPORT_ROOT / "latest.json"
    if latest_json.exists():
        try:
            payload = json.loads(latest_json.read_text(encoding="utf-8"))
            generated_at = parse_ts(payload.get("generated_at"))
            if generated_at:
                return generated_at, payload.get("release")
        except Exception:
            pass
    latest_md = REPORT_ROOT / "latest.md"
    if latest_md.exists():
        try:
            return datetime.fromtimestamp(latest_md.stat().st_mtime, tz=timezone.utc), None
        except Exception:
            pass
    return None, None


def _normalize_report_day_key(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return text
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m%d")
        except Exception:
            return None
    return None


def report_file_dates() -> dict[str, list[str]]:
    dates: dict[str, list[str]] = {}
    if not DAILY_DIR.exists():
        return dates
    for path in DAILY_DIR.glob("crawl_report_*.md"):
        day = None
        json_path = path.with_suffix(".json")
        if json_path.exists():
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    day = _normalize_report_day_key(payload.get("report_day_key") or payload.get("report_date"))
            except Exception:
                day = None
        if day is None:
            name = path.stem
            # crawl_report_YYYYMMDD_HHMM
            parts = name.split("_")
            if len(parts) < 4:
                continue
            day = parts[2]
            if len(day) != 8 or not day.isdigit():
                continue
        dates.setdefault(day, []).append(path.name)
    return dates


def collect_workers(system_health: dict[str, Any]) -> list[dict[str, Any]]:
    ops = system_health.get("ops") or {}
    workers = ops.get("workers") or []
    if isinstance(workers, list) and workers:
        return [item for item in workers if isinstance(item, dict)]
    fallback = http_json("/api/workers?limit=100")
    items = fallback.get("items") or []
    return [item for item in items if isinstance(item, dict)]


def collect_tasks() -> list[dict[str, Any]]:
    payload = http_json("/api/tasks?limit=500")
    items = payload.get("items") or []
    return [item for item in items if isinstance(item, dict)]


def task_is_stale(task: dict[str, Any], worker_map: dict[str, dict[str, Any]], now_utc: datetime) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    status = str(task.get("status") or "").strip().lower()
    if status not in ACTIVE_TASK_STATUSES:
        return False, reasons
    task_updated = parse_ts(task.get("updated_at")) or parse_ts(task.get("started_at")) or parse_ts(task.get("created_at"))
    lease_owner = str(task.get("lease_owner") or "").strip()
    worker = worker_map.get(lease_owner) if lease_owner else None
    if not lease_owner:
        reasons.append("missing_lease_owner")
    elif worker is None:
        reasons.append("missing_worker")
    else:
        worker_task_id = int(worker.get("current_task_id") or worker.get("task_id") or 0)
        if worker_task_id and worker_task_id != int(task.get("id") or 0):
            reasons.append("task_id_mismatch")
        heartbeat_at = parse_ts(worker.get("heartbeat_at")) or parse_ts(worker.get("updated_at"))
        if heartbeat_at and (now_utc - heartbeat_at).total_seconds() >= STALE_WORKER_SECONDS:
            reasons.append("stale_worker_heartbeat")
        worker_status = str(worker.get("status") or "").strip().lower()
        if worker_status not in {"running", "idle", "working", "active"}:
            reasons.append(f"worker_status={worker_status or 'unknown'}")
    if task_updated and (now_utc - task_updated).total_seconds() >= STALE_TASK_SECONDS:
        reasons.append("stale_task_update")
    return bool(reasons), reasons


def build_watchdog() -> tuple[str, dict[str, Any]]:
    ensure_dirs()
    report_now = shanghai_now()
    now_utc = utc_now()

    health, health_error = safe_http_json("/api/health", timeout=20)
    system_health, system_error = safe_http_json("/api/system/health", timeout=20)
    workers_error = None
    tasks_error = None
    workers = []
    tasks = []
    try:
        workers = collect_workers(system_health)
    except Exception as exc:
        workers = []
        workers_error = f"{type(exc).__name__}: {exc}"
    try:
        tasks = collect_tasks()
    except Exception as exc:
        tasks = []
        tasks_error = f"tasks_error: {type(exc).__name__}: {exc}"

    worker_map = {}
    for item in workers:
        name = str(item.get("worker_name") or "").strip()
        if name:
            worker_map[name] = item

    stale_workers = []
    for item in workers:
        heartbeat_at = parse_ts(item.get("heartbeat_at")) or parse_ts(item.get("updated_at"))
        if heartbeat_at and (now_utc - heartbeat_at).total_seconds() >= STALE_WORKER_SECONDS:
            stale_workers.append(
                {
                    "worker_name": item.get("worker_name"),
                    "worker_type": item.get("worker_type"),
                    "status": item.get("status"),
                    "task_id": item.get("task_id") or item.get("current_task_id"),
                    "heartbeat_at": item.get("heartbeat_at"),
                    "updated_at": item.get("updated_at"),
                }
            )

    stale_running_tasks = []
    active_tasks = [item for item in tasks if str(item.get("status") or "").strip().lower() in ACTIVE_TASK_STATUSES]
    for item in active_tasks:
        stale, reasons = task_is_stale(item, worker_map, now_utc)
        if stale:
            stale_running_tasks.append(
                {
                    "task_id": item.get("id"),
                    "task_type": item.get("task_type"),
                    "status": item.get("status"),
                    "display_name": item.get("display_name"),
                    "lease_owner": item.get("lease_owner"),
                    "worker_status": (worker_map.get(str(item.get("lease_owner") or "").strip()) or {}).get("status"),
                    "updated_at": item.get("updated_at"),
                    "reasons": reasons,
                }
            )

    system_active = systemctl_state(
        HOST_ALTERNATING_SERVICE,
        state_override=HOST_ALTERNATING_SERVICE_STATE,
        substate_override=HOST_ALTERNATING_SERVICE_SUBSTATE,
    )
    report_service = systemctl_state(
        REPORT_SERVICE,
        state_override=REPORT_SERVICE_STATE,
        substate_override=REPORT_SERVICE_SUBSTATE,
    )
    report_timer = systemctl_state(
        REPORT_TIMER,
        state_override=REPORT_TIMER_STATE,
        substate_override=REPORT_TIMER_SUBSTATE,
    )
    keyword_monitor_summary, keyword_monitor_summary_source = load_latest_keyword_monitor_summary()
    keyword_quality_summary = normalize_keyword_quality_summary(keyword_monitor_summary)
    runtime_keyword_quality = system_health.get("keyword_quality") or {}
    keyword_quality_state = str(
        runtime_keyword_quality.get("operator_quality_state")
        or runtime_keyword_quality.get("latest_terminal_batch_state")
        or runtime_keyword_quality.get("latest_terminal_quality_state")
        or runtime_keyword_quality.get("latest_quality_state")
        or keyword_quality_summary.get("state")
        or "unknown"
    ).strip().lower()
    shared_sync = system_health.get("shared_sync") or {}
    shared_sync_state = str(shared_sync.get("state") or shared_sync.get("status") or "queued").strip().lower()
    recent_sync_backlog: list[dict[str, Any]] = []
    for item in (system_health.get("ops") or {}).get("recent_runs") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("task_type") or "").strip().lower() != "warehouse_sync":
            continue
        result = item.get("result") or {}
        if not isinstance(result, dict):
            result = {}
        sync_state = result.get("sync_state") or {}
        if not isinstance(sync_state, dict):
            sync_state = {}
        skip_reason = str(result.get("skip_reason") or sync_state.get("skip_reason") or "").strip().lower()
        if skip_reason != "lock_active":
            continue
        recent_sync_backlog.append(
            {
                "task_id": item.get("task_id"),
                "run_id": item.get("id"),
                "status": item.get("status"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "skip_reason": skip_reason,
                "reason": result.get("reason") or item.get("display_name") or "",
            }
        )

    latest_report_at, latest_report_release = load_latest_report_stamp()
    latest_report_age_hours = None
    if latest_report_at:
        latest_report_age_hours = round((now_utc - latest_report_at).total_seconds() / 3600.0, 2)

    date_map = report_file_dates()
    shanghai_today = report_now.date()
    expected_dates = []
    missing_dates = []
    report_due_today = (
        report_now.hour > REPORT_EXPECTATION_HOUR
        or (report_now.hour == REPORT_EXPECTATION_HOUR and report_now.minute >= REPORT_EXPECTATION_MINUTE)
    )
    today_key = shanghai_today.strftime("%Y%m%d")
    if report_due_today and today_key not in date_map:
        missing_dates.append(today_key)
    for offset in range(1, 5):
        day = (shanghai_today - timedelta(days=offset)).strftime("%Y%m%d")
        expected_dates.append(day)
        if day not in date_map:
            missing_dates.append(day)

    report_gap = bool(missing_dates)
    report_stale = latest_report_age_hours is None or latest_report_age_hours > REPORT_MAX_AGE_HOURS
    historical_missing_dates = [item for item in missing_dates if item != today_key]
    current_report_missing = report_due_today and today_key in missing_dates
    current_report_stale = report_stale

    checks = {
        "api_health": {
            "ok": bool(health) and not health_error and str(health.get("status") or "").lower() == "ok",
            "error": health_error,
            "status": health.get("status"),
            "warehouse_db": health.get("warehouse_db"),
            "product_count": health.get("product_count"),
            "observation_count": health.get("observation_count"),
        },
        "system_health": {
            "ok": bool(system_health) and not system_error,
            "error": system_error,
            "worker_count": (system_health.get("ops") or {}).get("worker_count"),
        },
        "host_alternating_service": system_active,
        "report_service": report_service,
        "report_timer": report_timer,
        "report_freshness": {
            "ok": not report_gap and not report_stale,
            "latest_report_at": latest_report_at.isoformat() if latest_report_at else None,
            "latest_report_age_hours": latest_report_age_hours,
            "latest_report_release": latest_report_release,
            "report_due_today": report_due_today,
            "expected_dates": expected_dates,
            "available_dates": sorted(date_map.keys()),
            "missing_dates": missing_dates,
            "historical_missing_dates": historical_missing_dates,
            "current_report_missing": current_report_missing,
            "current_report_stale": current_report_stale,
        },
        "keyword_quality": {
            "ok": keyword_quality_state != "degraded",
            "state": keyword_quality_state,
            "runtime_summary": runtime_keyword_quality,
            "summary": keyword_quality_summary,
            "summary_source": keyword_monitor_summary_source,
        },
        "shared_sync": shared_sync,
        "auto_dispatch": system_health.get("auto_dispatch") or {},
        "sync_backlog": {
            "ok": not recent_sync_backlog and shared_sync_state != "skipped_due_to_active_lock",
            "state": shared_sync_state,
            "skip_reason": str(shared_sync.get("skip_reason") or "").strip().lower(),
            "recent_lock_active_runs": recent_sync_backlog,
        },
        "stale_workers": stale_workers,
        "stale_running_tasks": stale_running_tasks,
    }

    severity = "ok"
    issues = []
    auto_dispatch = checks["auto_dispatch"] if isinstance(checks["auto_dispatch"], dict) else {}
    canonical_auto_plans = list(auto_dispatch.get("canonical_auto_plans") or [])
    missing_canonical_families = list(auto_dispatch.get("missing_canonical_families") or [])
    disabled_canonical_families = list(auto_dispatch.get("disabled_canonical_families") or [])
    scheduler_heartbeat_ok = bool(auto_dispatch.get("scheduler_heartbeat_ok"))
    auto_dispatch_conflict = bool(auto_dispatch.get("auto_dispatch_conflict"))
    if not checks["api_health"]["ok"]:
        severity = "critical"
        issues.append({"check": "api_health", "severity": "critical", "message": checks["api_health"].get("error") or "health endpoint failed"})
    if not checks["system_health"]["ok"]:
        severity = "critical"
        issues.append({"check": "system_health", "severity": "critical", "message": checks["system_health"].get("error") or "system health endpoint failed"})
    if str(auto_dispatch.get("auto_dispatch_entry") or AUTO_DISPATCH_ENTRY) != AUTO_DISPATCH_ENTRY:
        severity = "critical"
        issues.append(
            {
                "check": "auto_dispatch_entry",
                "severity": "critical",
                "message": f"unexpected auto dispatch entry: {auto_dispatch.get('auto_dispatch_entry')}",
                "details": auto_dispatch,
            }
        )
    if not scheduler_heartbeat_ok:
        severity = "critical"
        issues.append(
            {
                "check": "scheduler_heartbeat",
                "severity": "critical",
                "message": "scheduler heartbeat missing for crawl_plans auto dispatch",
                "details": auto_dispatch,
            }
        )
    if missing_canonical_families:
        severity = "critical"
        issues.append(
            {
                "check": "canonical_auto_plans_missing",
                "severity": "critical",
                "message": f"missing canonical auto plans: {missing_canonical_families}",
                "details": canonical_auto_plans,
            }
        )
    if disabled_canonical_families:
        severity = "critical"
        issues.append(
            {
                "check": "canonical_auto_plans_disabled",
                "severity": "critical",
                "message": f"disabled canonical auto plans: {disabled_canonical_families}",
                "details": canonical_auto_plans,
            }
        )
    if auto_dispatch_conflict:
        severity = "warning" if severity == "ok" else severity
        issues.append(
            {
                "check": "auto_dispatch_conflict",
                "severity": "warning",
                "message": "legacy alternating service is still active while canonical auto plans are enabled",
                "details": {
                    "legacy_host_service": system_active,
                    "canonical_auto_plans": canonical_auto_plans,
                },
            }
        )
    if stale_workers:
        severity = "warning" if severity == "ok" else severity
        issues.append({"check": "stale_workers", "severity": "warning", "message": f"{len(stale_workers)} stale worker rows"})
    if stale_running_tasks:
        severity = "warning" if severity == "ok" else severity
        issues.append({"check": "stale_running_tasks", "severity": "warning", "message": f"{len(stale_running_tasks)} stale active tasks"})
    if historical_missing_dates:
        severity = "warning" if severity == "ok" else severity
        issues.append(
            {
                "check": "report_history_gap",
                "severity": "warning",
                "message": f"historical daily report gaps detected: {', '.join(historical_missing_dates)}",
                "details": {"historical_missing_dates": historical_missing_dates},
            }
        )
    if current_report_missing or current_report_stale:
        severity = "warning" if severity == "ok" else severity
        issues.append(
            {
                "check": "report_current_stale",
                "severity": "warning",
                "message": "current daily report chain is missing or stale",
                "details": {
                    "current_report_missing": current_report_missing,
                    "current_report_stale": current_report_stale,
                    "latest_report_age_hours": latest_report_age_hours,
                },
            }
        )
    if keyword_quality_state == "degraded":
        severity = "warning" if severity == "ok" else severity
        issues.append(
            {
                "check": "keyword_quality",
                "severity": "warning",
                "message": "keyword monitor quality_state=degraded",
                "details": {
                    "runtime_summary": runtime_keyword_quality,
                    "summary_source": keyword_monitor_summary_source,
                    "signals": keyword_quality_summary.get("signals") or {},
                    "quality_flags": keyword_quality_summary.get("quality_flags") or [],
                    "quality_reasons": keyword_quality_summary.get("quality_reasons") or [],
                    "quality_source_breakdown": keyword_quality_summary.get("quality_source_breakdown") or {},
                },
            }
        )
    if recent_sync_backlog or shared_sync_state == "skipped_due_to_active_lock":
        severity = "warning" if severity == "ok" else severity
        issues.append(
            {
                "check": "shared_sync_backlog",
                "severity": "warning",
                "message": "shared sync backlog / lock_active detected",
                "details": {
                    "shared_sync": shared_sync,
                    "recent_lock_active_runs": recent_sync_backlog,
                },
            }
        )

    current_alert_summary = {
        "state": severity,
        "checks": [str(item.get("check") or "").strip() for item in issues],
        "needs_attention": severity in {"warning", "critical"},
        "history_vs_current": {
            "historical_report_gap": bool(historical_missing_dates),
            "current_report_chain_problem": bool(current_report_missing or current_report_stale),
        },
        "counts": {
            "critical": sum(1 for item in issues if str(item.get("severity") or "").strip().lower() == "critical"),
            "warning": sum(1 for item in issues if str(item.get("severity") or "").strip().lower() == "warning"),
            "info": sum(1 for item in issues if str(item.get("severity") or "").strip().lower() == "info"),
        },
    }

    summary = {
        "generated_at": report_now.isoformat(),
        "status": severity,
        "release": latest_report_release,
        "issue_count": len(issues),
        "api_health_ok": checks["api_health"]["ok"],
        "system_health_ok": checks["system_health"]["ok"],
        "host_alternating_service_active": system_active["active"],
        "auto_dispatch_entry": str(auto_dispatch.get("auto_dispatch_entry") or AUTO_DISPATCH_ENTRY),
        "scheduler_heartbeat_ok": scheduler_heartbeat_ok,
        "canonical_auto_plans": canonical_auto_plans,
        "auto_dispatch_conflict": auto_dispatch_conflict,
        "stale_worker_count": len(stale_workers),
        "stale_running_task_count": len(stale_running_tasks),
        "report_missing_dates": missing_dates,
        "latest_report_age_hours": latest_report_age_hours,
        "historical_report_missing_dates": historical_missing_dates,
        "current_report_missing": current_report_missing,
        "current_report_stale": current_report_stale,
        "keyword_quality_state": keyword_quality_state,
        "keyword_runtime_operator_state": str(runtime_keyword_quality.get("operator_quality_state") or ""),
        "keyword_quality_summary_source": keyword_monitor_summary_source,
        "sync_backlog_count": len(recent_sync_backlog),
        "current_alert_summary": current_alert_summary,
    }

    markdown_lines = [
        "# NAS Watchdog Report",
        "",
        f"- Generated At: {report_now.strftime('%Y-%m-%d %H:%M:%S')} Asia/Shanghai",
        f"- Status: `{severity}`",
        f"- Auto Dispatch Entry: `{auto_dispatch.get('auto_dispatch_entry') or AUTO_DISPATCH_ENTRY}`",
        f"- Scheduler Heartbeat: `{'ok' if scheduler_heartbeat_ok else 'missing'}`",
        f"- Legacy Host Alternating Service: `{HOST_ALTERNATING_SERVICE}` -> `{system_active.get('state')}`",
        f"- API Health: `{checks['api_health']['status'] or 'n/a'}`",
        f"- System Health: `{'ok' if checks['system_health']['ok'] else 'error'}`",
        f"- Report Freshness: `{'ok' if not report_gap and not report_stale else 'stale/missing'}`",
        "",
        "## Issues",
    ]
    if issues:
        for item in issues:
            markdown_lines.append(f"- `{item['check']}`: {item['message']}")
    else:
        markdown_lines.append("- none")
    markdown_lines.extend(
        [
            "",
            "## Stale Workers",
        ]
    )
    if stale_workers:
        for item in stale_workers:
            markdown_lines.append(
                f"- {item.get('worker_name')} | type={item.get('worker_type')} | status={item.get('status')} | task_id={item.get('task_id')} | heartbeat_at={item.get('heartbeat_at')}"
            )
    else:
        markdown_lines.append("- none")
    markdown_lines.extend(
        [
            "",
            "## Stale Running Tasks",
        ]
    )
    if stale_running_tasks:
        for item in stale_running_tasks:
            markdown_lines.append(
                f"- task_id={item.get('task_id')} | type={item.get('task_type')} | status={item.get('status')} | lease_owner={item.get('lease_owner')} | reasons={','.join(item.get('reasons') or [])}"
            )
    else:
        markdown_lines.append("- none")
    markdown_lines.extend(
        [
            "",
            "## Report Freshness",
            f"- latest_report_at: `{checks['report_freshness']['latest_report_at']}`",
            f"- latest_report_age_hours: `{checks['report_freshness']['latest_report_age_hours']}`",
            f"- missing_dates: `{checks['report_freshness']['missing_dates']}`",
            f"- historical_missing_dates: `{checks['report_freshness']['historical_missing_dates']}`",
            f"- current_report_missing: `{checks['report_freshness']['current_report_missing']}`",
            f"- current_report_stale: `{checks['report_freshness']['current_report_stale']}`",
            "",
            "## Keyword Quality",
            f"- keyword_quality_state: `{keyword_quality_state}`",
            f"- keyword_runtime_operator_state: `{runtime_keyword_quality.get('operator_quality_state') or ''}`",
            f"- quality_summary_source: `{keyword_monitor_summary_source or 'n/a'}`",
            f"- quality_flags: `{keyword_quality_summary.get('quality_flags') or []}`",
            f"- quality_reasons: `{keyword_quality_summary.get('quality_reasons') or []}`",
            f"- quality_evidence: `{(keyword_quality_summary.get('quality_evidence') or [])[:6]}`",
            f"- quality_source_breakdown: `{keyword_quality_summary.get('quality_source_breakdown') or {}}`",
            f"- signals: `{keyword_quality_summary.get('signals') or {}}`",
            f"- recent_sync_backlog: `{recent_sync_backlog}`",
            "",
            "## Auto Dispatch",
            f"- entry: `{auto_dispatch.get('auto_dispatch_entry') or AUTO_DISPATCH_ENTRY}`",
            f"- scheduler_heartbeat_ok: `{scheduler_heartbeat_ok}`",
            f"- auto_dispatch_conflict: `{auto_dispatch_conflict}`",
            f"- canonical_auto_plans: `{canonical_auto_plans}`",
            "",
            "## Raw Status",
            "```json",
            json.dumps(
                {
                    "summary": summary,
                    "checks": checks,
                    "issues": issues,
                },
                ensure_ascii=False,
                indent=2,
            ),
            "```",
            "",
        ]
    )
    markdown = "\n".join(markdown_lines)

    payload = {
        "summary": summary,
        "checks": checks,
        "issues": issues,
        "health": health,
        "system_health": system_health,
        "host_service": system_active,
        "report_service": report_service,
        "report_timer": report_timer,
        "stale_workers": stale_workers,
        "stale_running_tasks": stale_running_tasks,
        "tasks_sample": active_tasks[:20],
        "workers_sample": workers[:20],
        "report_freshness": checks["report_freshness"],
        "current_alert_summary": current_alert_summary,
        "request_errors": {
            "api_health": health_error,
            "system_health": system_error,
            "workers": workers_error,
            "tasks": tasks_error,
        },
    }
    return markdown, payload


def write_outputs(markdown: str, payload: dict[str, Any]) -> Path:
    ensure_dirs()
    report_now = shanghai_now()
    stem = f"watchdog_{report_now.strftime('%Y%m%d_%H%M')}"
    daily_md = DAILY_DIR / f"{stem}.md"
    daily_json = DAILY_DIR / f"{stem}.json"
    latest_md = LATEST_MD
    latest_json = LATEST_JSON
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    write_readable_text(latest_md, markdown)
    write_readable_text(latest_json, payload_json)
    write_readable_text(daily_md, markdown)
    write_readable_text(daily_json, payload_json)
    append_readable_jsonl(RUNS_FILE, payload["summary"])
    return daily_md


def main() -> int:
    markdown, payload = build_watchdog()
    out_path = write_outputs(markdown, payload)
    print(out_path)
    print(json.dumps(payload["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
