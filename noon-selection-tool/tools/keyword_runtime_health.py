from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.keyword_quality_summary import summarize_recent_keyword_runs
from config.settings import Settings
from scrapers.base_scraper import build_keyword_result_stem


DEFAULT_LOG_PREVIEW_LINES = 8
ACTIVE_LOG_WINDOW_SECONDS = 300
DEFAULT_RESULT_SAMPLE_SIZE = 3
DEFAULT_RUNTIME_CODE_FILES = (
    ROOT / "run_keyword_monitor.py",
    ROOT / "keyword_main.py",
    ROOT / "scrapers" / "base_scraper.py",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {"_error": "invalid_json", "_path": str(path)}


def _parse_iso_datetime(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _seconds_since(raw_value: Any) -> int | None:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return None
    return max(0, int((datetime.now() - parsed).total_seconds()))


def _seconds_since_epoch(timestamp: float | int | None) -> int | None:
    if timestamp is None:
        return None
    try:
        return max(0, int(datetime.now().timestamp() - float(timestamp)))
    except (TypeError, ValueError):
        return None


def _datetime_to_iso(value: datetime | None) -> str:
    if not value:
        return ""
    return value.replace(microsecond=0).isoformat()


def _pid_exists(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False

    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = (result.stdout or "").strip()
        if not output or output.startswith("INFO:"):
            return False
        return f'"{pid}"' in output or f",{pid}," in output

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _tail_lines(path: Path, *, limit: int = DEFAULT_LOG_PREVIEW_LINES) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    return lines[-max(limit, 0) :]


def _collect_table_counts(conn: sqlite3.Connection, tables: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        counts[table] = int(row["count"] if row and row["count"] is not None else 0)
    return counts


def collect_runtime_db_payload(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"db_path": str(db_path), "exists": False}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        stats = _collect_table_counts(
            conn,
            ("keywords", "keyword_edges", "crawl_observations", "keyword_runs", "keyword_metrics_snapshots"),
        )
        recent_runs = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, run_type, trigger_mode, seed_keyword, snapshot_id, status, keyword_count, started_at, finished_at
                FROM keyword_runs
                ORDER BY id DESC
                LIMIT 10
                """
            ).fetchall()
        ]
        return {
            "db_path": str(db_path),
            "exists": True,
            "stats": stats,
            "recent_runs": recent_runs,
        }
    finally:
        conn.close()


def collect_lock_payload(lock_path: Path) -> dict[str, Any]:
    lock_payload = _read_json(lock_path)
    pid = int(lock_payload.get("pid") or 0) if lock_payload else 0
    pid_running = _pid_exists(pid)
    started_at = lock_payload.get("started_at")
    updated_at = lock_payload.get("updated_at") or started_at
    age_seconds = _seconds_since(updated_at)

    monitor_state = "idle"
    if lock_path.exists() and pid_running:
        monitor_state = "running"
    elif lock_path.exists():
        monitor_state = "stale_lock"

    return {
        "lock_path": str(lock_path),
        "lock_exists": lock_path.exists(),
        "pid": pid or None,
        "pid_running": pid_running,
        "started_at": started_at or "",
        "updated_at": updated_at or "",
        "age_seconds": age_seconds,
        "snapshot_id": lock_payload.get("snapshot_id") or "",
        "state": monitor_state,
        "status": lock_payload.get("status") or "",
        "current_stage": lock_payload.get("current_stage") or "",
        "stage_note": lock_payload.get("stage_note") or "",
        "raw": lock_payload,
    }


def collect_summary_payload(summary_path: Path) -> dict[str, Any]:
    payload = _read_json(summary_path)
    finished_at = payload.get("finished_at")
    quality_summary = payload.get("quality_summary") if isinstance(payload.get("quality_summary"), dict) else {}
    quality_state = str(payload.get("quality_state") or quality_summary.get("state") or "").strip().lower()
    if quality_state not in {"full", "partial", "degraded"}:
        quality_state = "unknown"
    quality_reasons = normalize_quality_reason_tokens(
        list(payload.get("quality_reasons") or [])
        + list(quality_summary.get("quality_reasons") or [])
        + list(quality_summary.get("quality_evidence") or [])
        + list(quality_summary.get("quality_flags") or [])
    )
    quality_flags = normalize_quality_reason_tokens(
        list(payload.get("quality_flags") or []) + list(quality_summary.get("quality_flags") or [])
    )
    quality_evidence = normalize_quality_reason_tokens(
        list(payload.get("quality_evidence") or [])
        + list(quality_summary.get("quality_evidence") or [])
        + list(quality_summary.get("quality_flags") or [])
    )
    if not quality_evidence:
        quality_evidence = quality_reasons[:]
    quality_source_breakdown = payload.get("quality_source_breakdown")
    if not isinstance(quality_source_breakdown, dict):
        quality_source_breakdown = quality_summary.get("quality_source_breakdown")
    if not isinstance(quality_source_breakdown, dict):
        quality_source_breakdown = {}
    quality_source_breakdown = _normalize_quality_source_breakdown(quality_source_breakdown)
    return {
        "summary_path": str(summary_path),
        "exists": summary_path.exists(),
        "status": payload.get("status") or "",
        "crawl_status": payload.get("crawl_status") or "",
        "snapshot_id": payload.get("snapshot_id") or "",
        "started_at": payload.get("started_at") or "",
        "updated_at": payload.get("updated_at") or "",
        "finished_at": finished_at or "",
        "age_seconds": _seconds_since(finished_at),
        "current_stage": payload.get("current_stage") or "",
        "stage_note": payload.get("stage_note") or "",
        "expanded_keyword_count": int(payload.get("expanded_keyword_count") or 0),
        "expanded_filtered_count": int(payload.get("expanded_filtered_count") or 0),
        "crawled_keyword_count": int(payload.get("crawled_keyword_count") or 0),
        "persisted_product_count": int(payload.get("persisted_product_count") or 0),
        "analyzed_keyword_count": int(payload.get("analyzed_keyword_count") or 0),
        "errors_count": len(payload.get("errors") or []),
        "warehouse_sync_status": str((payload.get("warehouse_sync") or {}).get("status") or ""),
        "quality_state": quality_state,
        "quality_reasons": quality_reasons,
        "quality_flags": quality_flags,
        "quality_evidence": quality_evidence,
        "quality_source_breakdown": quality_source_breakdown,
        "raw": payload,
    }


def collect_batch_logs(batch_log_dir: Path, *, preview_lines: int = DEFAULT_LOG_PREVIEW_LINES) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    latest_previews: list[dict[str, Any]] = []
    if not batch_log_dir.exists():
        return {"batch_log_dir": str(batch_log_dir), "exists": False, "items": items, "latest_previews": latest_previews}

    log_files = sorted(batch_log_dir.glob("*.log"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in log_files[:10]:
        stat = path.stat()
        payload = {
            "name": path.name,
            "path": str(path),
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        }
        items.append(payload)

    for path in log_files[:2]:
        latest_previews.append(
            {
                "name": path.name,
                "path": str(path),
                "tail": _tail_lines(path, limit=preview_lines),
            }
        )

    latest_log = log_files[0] if log_files else None
    latest_log_mtime = latest_log.stat().st_mtime if latest_log else None

    return {
        "batch_log_dir": str(batch_log_dir),
        "exists": True,
        "items": items,
        "latest_previews": latest_previews,
        "latest_log_path": str(latest_log) if latest_log else "",
        "latest_log_age_seconds": _seconds_since_epoch(latest_log_mtime),
    }


def collect_warehouse_payload(warehouse_db_path: Path) -> dict[str, Any]:
    if not warehouse_db_path.exists():
        return {"warehouse_db_path": str(warehouse_db_path), "exists": False}

    conn = sqlite3.connect(warehouse_db_path)
    conn.row_factory = sqlite3.Row
    try:
        table_counts = _collect_table_counts(
            conn,
            ("product_identity", "observation_events", "product_keyword_membership", "keyword_catalog", "keyword_runs_log", "keyword_metric_snapshots"),
        )
        source_breakdown = [
            dict(row)
            for row in conn.execute(
                """
                SELECT source_scope, COUNT(*) AS source_db_count, MAX(imported_at) AS last_imported_at
                FROM source_databases
                GROUP BY source_scope
                ORDER BY source_scope ASC
                """
            ).fetchall()
        ]
        keyword_import = conn.execute(
            """
            SELECT source_label, imported_at, source_keyword_count, source_observation_count
            FROM source_databases
            WHERE source_scope = 'keyword_stage'
            ORDER BY imported_at DESC, source_label DESC
            LIMIT 1
            """
        ).fetchone()
        return {
            "warehouse_db_path": str(warehouse_db_path),
            "exists": True,
            "table_counts": table_counts,
            "source_breakdown": source_breakdown,
            "latest_keyword_import": dict(keyword_import) if keyword_import else {},
        }
    finally:
        conn.close()


def build_warehouse_lag_payload(runtime_payload: dict[str, Any], warehouse_payload: dict[str, Any]) -> dict[str, Any]:
    if not runtime_payload.get("exists") or not warehouse_payload.get("exists"):
        return {
            "state": "unavailable",
            "keyword_gap": None,
            "observation_gap": None,
            "import_age_seconds": None,
        }

    latest_keyword_import = warehouse_payload.get("latest_keyword_import") or {}
    runtime_stats = runtime_payload.get("stats") or {}
    runtime_keyword_count = int(runtime_stats.get("keywords") or 0)
    runtime_observation_count = int(runtime_stats.get("crawl_observations") or 0)
    imported_keyword_count = int(latest_keyword_import.get("source_keyword_count") or 0)
    imported_observation_count = int(latest_keyword_import.get("source_observation_count") or 0)
    import_age_seconds = _seconds_since(latest_keyword_import.get("imported_at"))
    keyword_gap = runtime_keyword_count - imported_keyword_count
    observation_gap = runtime_observation_count - imported_observation_count

    state = "up_to_date"
    if not latest_keyword_import:
        state = "missing_keyword_import"
    elif keyword_gap > 0 or observation_gap > 0:
        state = "lagging"

    return {
        "state": state,
        "runtime_keyword_count": runtime_keyword_count,
        "runtime_observation_count": runtime_observation_count,
        "imported_keyword_count": imported_keyword_count,
        "imported_observation_count": imported_observation_count,
        "keyword_gap": keyword_gap,
        "observation_gap": observation_gap,
        "imported_at": latest_keyword_import.get("imported_at") or "",
        "import_age_seconds": import_age_seconds,
        "source_label": latest_keyword_import.get("source_label") or "",
    }


def collect_runtime_code_payload(
    *,
    runtime_code_files: tuple[Path, ...],
    process_started_at: Any,
    monitor_state: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    newest_item: dict[str, Any] | None = None
    process_started_dt = _parse_iso_datetime(process_started_at)

    for path in runtime_code_files:
        exists = path.exists()
        modified_dt = datetime.fromtimestamp(path.stat().st_mtime) if exists else None
        item = {
            "path": str(path),
            "exists": exists,
            "last_modified_at": _datetime_to_iso(modified_dt),
            "modified_after_process_start": bool(
                exists and process_started_dt and modified_dt and modified_dt > process_started_dt
            ),
        }
        items.append(item)
        if not exists or not modified_dt:
            continue
        if newest_item is None or modified_dt > _parse_iso_datetime(newest_item.get("last_modified_at")):
            newest_item = item

    state = "no_live_process"
    needs_restart_to_apply_code = False
    if monitor_state == "stale_lock":
        state = "stale_lock"
    elif monitor_state == "running":
        if not process_started_dt:
            state = "unknown_process_start"
        elif any(item["modified_after_process_start"] for item in items):
            state = "running_process_predates_disk_code"
            needs_restart_to_apply_code = True
        else:
            state = "running_process_matches_disk_code"

    return {
        "state": state,
        "process_started_at": process_started_at or "",
        "needs_restart_to_apply_code": needs_restart_to_apply_code,
        "tracked_files": items,
        "newest_runtime_file": newest_item or {},
    }


def _sample_result_file_payload(
    *,
    path: Path,
    platform: str,
    process_started_at: Any,
) -> dict[str, Any]:
    raw = _read_json(path)
    keyword = str(raw.get("keyword") or "").strip()
    meta = raw.get("_meta") if isinstance(raw, dict) else None
    products = raw.get("products") if isinstance(raw, dict) else None
    payload_valid = bool(
        isinstance(raw, dict)
        and keyword
        and isinstance(products, list)
        and isinstance(meta, dict)
        and str(meta.get("platform") or "").strip().lower() == platform.lower()
        and str(meta.get("keyword") or "").strip() == keyword
    )
    expected_name = f"{build_keyword_result_stem(keyword)}.json" if keyword else ""
    modified_after_process_start = False
    process_started_dt = _parse_iso_datetime(process_started_at)
    if process_started_dt and path.exists():
        modified_dt = datetime.fromtimestamp(path.stat().st_mtime)
        modified_after_process_start = modified_dt > process_started_dt
    return {
        "name": path.name,
        "path": str(path),
        "keyword": keyword,
        "payload_valid": payload_valid,
        "expected_name": expected_name,
        "matches_current_filename": bool(expected_name and path.name == expected_name),
        "modified_after_process_start": modified_after_process_start,
        "has_meta": isinstance(meta, dict),
    }


def collect_result_file_payload(
    *,
    snapshot_root: Path,
    snapshot_id: str,
    process_started_at: Any,
    sample_size: int = DEFAULT_RESULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    if not snapshot_id:
        return {
            "snapshot_root": str(snapshot_root),
            "snapshot_id": "",
            "exists": False,
            "state": "missing_snapshot_id",
            "platforms": {},
        }

    snapshot_dir = snapshot_root / snapshot_id
    if not snapshot_dir.exists():
        return {
            "snapshot_root": str(snapshot_root),
            "snapshot_id": snapshot_id,
            "exists": False,
            "state": "missing_snapshot_dir",
            "platforms": {},
        }

    platforms: dict[str, Any] = {}
    aggregate_state = "current_semantics"

    for platform_dir in sorted(path for path in snapshot_dir.iterdir() if path.is_dir()):
        result_files = sorted(platform_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
        sampled = [
            _sample_result_file_payload(
                path=path,
                platform=platform_dir.name,
                process_started_at=process_started_at,
            )
            for path in result_files[: max(sample_size, 0)]
        ]
        platform_state = "no_files"
        if sampled:
            if any(not item["payload_valid"] for item in sampled):
                platform_state = "invalid_payloads_detected"
            elif all(item["matches_current_filename"] for item in sampled):
                platform_state = "current_semantics"
            else:
                platform_state = "legacy_filename_semantics"

        if platform_state == "invalid_payloads_detected":
            aggregate_state = "invalid_payloads_detected"
        elif platform_state == "legacy_filename_semantics" and aggregate_state == "current_semantics":
            aggregate_state = "legacy_filename_semantics"

        platforms[platform_dir.name] = {
            "state": platform_state,
            "sampled_file_count": len(sampled),
            "valid_payload_file_count": sum(1 for item in sampled if item["payload_valid"]),
            "current_filename_match_count": sum(1 for item in sampled if item["matches_current_filename"]),
            "files_modified_after_process_start": sum(1 for item in sampled if item["modified_after_process_start"]),
            "sampled_files": sampled,
        }

    if not platforms:
        aggregate_state = "no_platform_dirs"

    return {
        "snapshot_root": str(snapshot_root),
        "snapshot_id": snapshot_id,
        "exists": True,
        "state": aggregate_state,
        "platforms": platforms,
    }


def build_restart_verification_payload(
    *,
    monitor_state: str,
    lock_payload: dict[str, Any],
    summary_payload: dict[str, Any],
    runtime_code: dict[str, Any],
    result_files: dict[str, Any],
) -> dict[str, Any]:
    lock_checkpoint_present = bool(
        lock_payload.get("updated_at") and lock_payload.get("current_stage") and lock_payload.get("status")
    )
    summary_checkpoint_present = bool(
        summary_payload.get("updated_at") and summary_payload.get("current_stage")
    )
    code_alignment_state = str(runtime_code.get("state") or "")
    result_file_state = str(result_files.get("state") or "")

    state = "idle"
    if monitor_state == "running":
        if code_alignment_state == "running_process_predates_disk_code":
            state = "restart_required_after_completion"
        elif not lock_checkpoint_present:
            state = "running_without_checkpoint_semantics"
        elif result_file_state in {"legacy_filename_semantics", "invalid_payloads_detected"}:
            state = "running_result_semantics_not_verified"
        else:
            state = "running_semantics_verified"
    elif monitor_state == "idle_with_summary":
        if summary_checkpoint_present and code_alignment_state in {"no_live_process", "running_process_matches_disk_code"}:
            state = "restart_cycle_ready"
        else:
            state = "await_next_restart_window"
    elif monitor_state == "stale_lock":
        state = "stale_lock"

    return {
        "state": state,
        "lock_checkpoint_present": lock_checkpoint_present,
        "summary_checkpoint_present": summary_checkpoint_present,
        "code_alignment_state": code_alignment_state,
        "result_file_state": result_file_state,
    }


def build_operator_hint(
    *,
    monitor_state: str,
    activity_state: str,
    warehouse_lag: dict[str, Any],
    runtime_code: dict[str, Any],
    quality_payload: dict[str, Any],
) -> dict[str, str]:
    lag_state = str(warehouse_lag.get("state") or "")
    runtime_code_state = str(runtime_code.get("state") or "")
    quality_state = str(quality_payload.get("state") or "")
    if quality_state == "degraded":
        return {
            "level": "warning",
            "code": "keyword_quality_degraded",
            "message": "keyword runtime completed in a degraded state; inspect platform fallbacks and missing dependencies before trusting the result",
        }
    if quality_state == "partial":
        return {
            "level": "info",
            "code": "keyword_quality_partial",
            "message": "keyword runtime completed with partial quality; verify platform-level evidence before using the result as ground truth",
        }
    if activity_state == "running_active_logs":
        if runtime_code_state == "running_process_predates_disk_code":
            return {
                "level": "info",
                "code": "active_batch_restart_after_completion",
                "message": "live keyword batch predates the latest runtime code on disk; let it finish, then restart the producer before relying on the new hardening",
            }
        if lag_state == "lagging":
            return {
                "level": "info",
                "code": "active_batch_warehouse_lag_expected",
                "message": "live keyword batch is active; do not restart mid-run, warehouse lag is expected until the next sync point",
            }
        return {
            "level": "info",
            "code": "active_batch_continue",
            "message": "live keyword batch is active; no manual intervention is suggested",
        }
    if activity_state == "running_quiet_logs":
        return {
            "level": "warning",
            "code": "running_without_recent_logs",
            "message": "monitor lock is active but batch logs are quiet; inspect if this persists",
        }
    if monitor_state == "stale_lock":
        return {
            "level": "warning",
            "code": "stale_lock_detected",
            "message": "monitor lock exists without a live process; cleanup or restart may be required",
        }
    if lag_state == "lagging":
        return {
            "level": "info",
            "code": "warehouse_lagging",
            "message": "runtime keyword data is ahead of warehouse; wait for the next warehouse sync if a live batch is still running",
        }
    return {
        "level": "info",
        "code": "healthy_idle",
        "message": "no immediate operator action is suggested",
    }


def _normalize_quality_state(value: Any) -> str:
    state = str(value or "").strip().lower()
    return state if state in {"full", "partial", "degraded"} else "unknown"


def _normalize_quality_token(value: Any) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    lowered = token.lower()
    if "beautifulsoup4_unavailable" in lowered:
        return "runtime_import_error" if bs4_available_in_current_runtime() else "dependency_missing"
    if lowered.startswith("dependency_missing:"):
        return "dependency_missing"
    if lowered.startswith("runtime_import_error:"):
        return "runtime_import_error"
    if "google_trends_missing" in lowered:
        return "signal_missing:google_trends"
    if "amazon_bsr_missing" in lowered:
        return "signal_missing:amazon_bsr"
    if lowered.startswith("missing_result_file:") or lowered.startswith("expected_result_file:") or "incomplete_json" in lowered or "invalid_json" in lowered:
        return "result_contract_mismatch"
    if "captcha" in lowered or "robot" in lowered or "service unavailable" in lowered or "temporarily unavailable" in lowered or "something went wrong" in lowered or "we're sorry" in lowered:
        return "external_site_error"
    if lowered.startswith("external_site_error:"):
        return "external_site_error"
    if "timeout" in lowered or "timed out" in lowered or "net::err" in lowered:
        return "timeout"
    if lowered.startswith("page_recognition_failed:"):
        return "page_recognition_failed"
    if "fallback_misfire" in lowered:
        return "fallback_misfire"
    if "analysis_empty" in lowered:
        return "analysis_empty"
    if "failed_keywords" in lowered:
        return "crawl_partial:failed_keywords"
    if "zero_results" in lowered:
        return "crawl_zero_results"
    if "crawl_status_failed" in lowered or lowered == "run_failed":
        return "run_failed"
    if "monitor_errors_present" in lowered:
        return "monitor_errors_present"
    if "running_without_recent_logs" in lowered:
        return "running_without_recent_logs"
    if "partial" in lowered:
        return "partial_results"
    if "stale_lock" in lowered:
        return "stale_lock_detected"
    if "warehouse_lagging" in lowered:
        return "warehouse_lagging"
    return token


def normalize_quality_reason_token(value: Any) -> str:
    return _normalize_quality_token(value)


def normalize_quality_reason_tokens(values: list[Any]) -> list[str]:
    return _dedupe_nonempty(values)


def bs4_available_in_current_runtime() -> bool:
    try:
        import importlib.util

        return importlib.util.find_spec("bs4") is not None
    except Exception:
        return False


def _dedupe_nonempty(values: list[Any]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        text = _normalize_quality_token(raw_value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _normalize_quality_source_payload(source_name: str, raw_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw_payload or {})
    status = str(payload.get("status") or "").strip().lower()
    reason_codes = normalize_quality_reason_tokens(list(payload.get("reason_codes") or []))
    evidence = normalize_quality_reason_tokens(list(payload.get("evidence") or []))
    primary_reason = normalize_quality_reason_token(payload.get("primary_reason") or (reason_codes[0] if reason_codes else ""))
    if not reason_codes and evidence:
        reason_codes = normalize_quality_reason_tokens(evidence)
    if not reason_codes and primary_reason:
        reason_codes = [primary_reason]
    normalized_reason_codes = normalize_quality_reason_tokens(reason_codes + evidence)
    normalized_evidence = normalize_quality_reason_tokens(evidence + reason_codes)
    state = _normalize_quality_state(payload.get("state"))
    if state == "unknown":
        if any(
            code in {
                "dependency_missing",
                "runtime_import_error",
                "amazon_parse_failure",
                "amazon_upstream_blocked",
                "result_contract_mismatch",
                "runtime_error",
                "external_site_error",
                "timeout",
                "page_recognition_failed",
                "fallback_misfire",
            }
            for code in normalized_reason_codes
        ):
            state = "degraded"
        elif status in {"partial", "zero_results"} or any(code in {"zero_results", "partial_results"} for code in normalized_reason_codes):
            state = "partial"
        elif status == "completed":
            state = "full"
    if not primary_reason:
        primary_reason = normalized_reason_codes[0] if normalized_reason_codes else ""
    normalized = dict(payload)
    normalized.update(
        {
            "source": source_name,
            "state": state,
            "status": status or str(payload.get("status") or ""),
            "reason_codes": normalized_reason_codes,
            "primary_reason": primary_reason,
            "evidence": normalized_evidence,
        }
    )
    return normalized


def _normalize_quality_source_breakdown(source_breakdown: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for source_name, raw_payload in (source_breakdown or {}).items():
        if isinstance(raw_payload, dict):
            normalized[str(source_name)] = _normalize_quality_source_payload(str(source_name), raw_payload)
    return normalized


def _infer_live_batch_state(summary_payload: dict[str, Any], fallback: str | None = None) -> str:
    candidate = str(
        summary_payload.get("live_batch_state")
        or summary_payload.get("activity_state")
        or summary_payload.get("monitor_state")
        or summary_payload.get("status")
        or fallback
        or ""
    ).strip().lower()
    if candidate in {"running", "running_active_logs", "running_quiet_logs", "running_no_logs"}:
        return "running"
    if candidate in {"idle", "idle_with_summary", "idle_with_terminal_summary"}:
        return "idle"
    if candidate == "stale_lock":
        return "stale_lock"
    if candidate in {"partial", "completed", "failed", "queued", "leased", "retrying"}:
        return "running" if candidate in {"queued", "leased", "retrying"} else "terminal"
    return "unknown"


def build_quality_payload(
    summary_payload: dict[str, Any],
    *,
    live_batch_state: str | None = None,
    latest_terminal_quality_state: str | None = None,
    operator_quality_state: str | None = None,
) -> dict[str, Any]:
    quality_summary = summary_payload.get("quality_summary") if isinstance(summary_payload.get("quality_summary"), dict) else {}
    raw_reasons = (
        list(summary_payload.get("quality_reasons") or [])
        + list(summary_payload.get("operator_quality_reasons") or [])
        + list(summary_payload.get("latest_terminal_quality_reasons") or [])
        + list(summary_payload.get("latest_quality_reasons") or [])
        + list(quality_summary.get("quality_reasons") or [])
        + list(quality_summary.get("quality_flags") or [])
        + list(quality_summary.get("quality_evidence") or [])
    )
    raw_flags = (
        list(summary_payload.get("quality_flags") or [])
        + list(summary_payload.get("operator_quality_flags") or [])
        + list(quality_summary.get("quality_flags") or [])
    )
    raw_evidence = (
        list(summary_payload.get("evidence") or [])
        + list(summary_payload.get("quality_evidence") or [])
        + list(summary_payload.get("operator_quality_evidence") or [])
        + list(summary_payload.get("latest_terminal_quality_evidence") or [])
        + list(summary_payload.get("latest_quality_evidence") or [])
        + list(quality_summary.get("quality_evidence") or [])
        + list(quality_summary.get("quality_flags") or [])
    )
    source_breakdown = summary_payload.get("quality_source_breakdown")
    if not isinstance(source_breakdown, dict):
        source_breakdown = summary_payload.get("operator_quality_source_breakdown")
    if not isinstance(source_breakdown, dict):
        source_breakdown = summary_payload.get("latest_terminal_quality_source_breakdown")
    if not isinstance(source_breakdown, dict):
        source_breakdown = summary_payload.get("latest_quality_source_breakdown")
    if not isinstance(source_breakdown, dict):
        source_breakdown = quality_summary.get("quality_source_breakdown")
    if not isinstance(source_breakdown, dict):
        source_breakdown = {}
    source_breakdown = _normalize_quality_source_breakdown(source_breakdown)

    normalized_reasons = _dedupe_nonempty(raw_reasons + raw_flags + raw_evidence)
    normalized_evidence = _dedupe_nonempty(raw_evidence + raw_reasons + raw_flags)

    if not normalized_reasons:
        normalized_reasons = _dedupe_nonempty(
            list((quality_summary.get("quality_flags") or []))
            + list((quality_summary.get("quality_evidence") or []))
        )
    if not normalized_evidence:
        normalized_evidence = normalized_reasons[:]

    quality_state = _normalize_quality_state(summary_payload.get("quality_state") or quality_summary.get("state"))
    if quality_state == "unknown":
        quality_state = _normalize_quality_state(summary_payload.get("status"))

    inferred_live_batch_state = _infer_live_batch_state(summary_payload, fallback=live_batch_state)
    if live_batch_state:
        inferred_live_batch_state = _infer_live_batch_state({"live_batch_state": live_batch_state, "status": live_batch_state}, fallback=live_batch_state)

    if not latest_terminal_quality_state:
        terminal_state = _normalize_quality_state(
            summary_payload.get("latest_terminal_batch_state")
            or summary_payload.get("latest_terminal_quality_state")
            or summary_payload.get("terminal_quality_state")
        )
        if terminal_state == "unknown":
            terminal_state = _normalize_quality_state(quality_summary.get("state"))
        latest_terminal_quality_state = terminal_state
    else:
        latest_terminal_quality_state = _normalize_quality_state(latest_terminal_quality_state)

    if not operator_quality_state:
        operator_quality_state = _normalize_quality_state(
            summary_payload.get("operator_quality_state") or quality_summary.get("operator_quality_state") or quality_state
        )
    else:
        operator_quality_state = _normalize_quality_state(operator_quality_state)

    bs4_unavailable = any(token in {"dependency_missing", "runtime_import_error"} for token in normalized_reasons + normalized_evidence)
    google_trends_missing = any(token == "signal_missing:google_trends" for token in normalized_reasons + normalized_evidence)
    amazon_bsr_missing = any(token == "signal_missing:amazon_bsr" for token in normalized_reasons + normalized_evidence)

    evidence = _dedupe_nonempty(normalized_evidence or normalized_reasons)
    if inferred_live_batch_state == "stale_lock" and "stale_lock_detected" not in evidence:
        evidence.append("stale_lock_detected")

    if operator_quality_state == "unknown":
        operator_quality_state = latest_terminal_quality_state if latest_terminal_quality_state != "unknown" else quality_state
    if operator_quality_state == "unknown":
        operator_quality_state = inferred_live_batch_state if inferred_live_batch_state in {"running", "stale_lock"} else "unknown"

    result = {
        "state": operator_quality_state,
        "live_batch_state": inferred_live_batch_state,
        "latest_terminal_batch_state": latest_terminal_quality_state,
        "latest_terminal_quality_state": latest_terminal_quality_state,
        "operator_quality_state": operator_quality_state,
        "reasons": normalized_reasons,
        "quality_reasons": normalized_reasons,
        "evidence": evidence,
        "quality_evidence": evidence,
        "quality_source_breakdown": source_breakdown,
        "source_breakdown": source_breakdown,
        "beautifulsoup4_unavailable": bs4_unavailable,
        "google_trends_missing": google_trends_missing,
        "amazon_bsr_missing": amazon_bsr_missing,
    }
    result["latest_quality_state"] = latest_terminal_quality_state
    result["latest_quality_reasons"] = list(summary_payload.get("latest_quality_reasons") or summary_payload.get("latest_terminal_quality_reasons") or normalized_reasons)
    result["latest_quality_evidence"] = list(summary_payload.get("latest_quality_evidence") or summary_payload.get("latest_terminal_quality_evidence") or normalized_evidence)
    result["latest_quality_source_breakdown"] = _normalize_quality_source_breakdown(
        dict(
            summary_payload.get("latest_quality_source_breakdown")
            or summary_payload.get("latest_terminal_quality_source_breakdown")
            or source_breakdown
        )
    )
    result["operator_quality_reasons"] = list(summary_payload.get("operator_quality_reasons") or normalized_reasons)
    result["operator_quality_evidence"] = list(summary_payload.get("operator_quality_evidence") or normalized_evidence)
    result["operator_quality_source_breakdown"] = _normalize_quality_source_breakdown(
        dict(summary_payload.get("operator_quality_source_breakdown") or source_breakdown)
    )
    result["quality_state_breakdown"] = dict(summary_payload.get("quality_state_breakdown") or quality_summary.get("quality_state_breakdown") or {})
    result["quality_status_summary"] = str(summary_payload.get("quality_status_summary") or quality_summary.get("quality_status_summary") or "")
    return result


def build_keyword_quality_truth_model(recent_runs: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize_recent_keyword_runs(recent_runs)
    truth = dict(summary)
    live_source_breakdown = _normalize_quality_source_breakdown(
        dict(summary.get("live_quality_source_breakdown") or {})
    )
    operator_source_breakdown = _normalize_quality_source_breakdown(
        dict(summary.get("operator_quality_source_breakdown") or live_source_breakdown)
    )
    latest_terminal_source_breakdown = _normalize_quality_source_breakdown(
        dict(summary.get("latest_terminal_quality_source_breakdown") or {})
    )
    latest_source_breakdown = _normalize_quality_source_breakdown(
        dict(
            summary.get("latest_quality_source_breakdown")
            or latest_terminal_source_breakdown
            or operator_source_breakdown
            or live_source_breakdown
        )
    )
    quality_source_breakdown = dict(
        operator_source_breakdown or latest_source_breakdown or live_source_breakdown
    )

    quality_reasons = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_reasons") or [])
        + list(summary.get("latest_terminal_quality_reasons") or [])
        + list(summary.get("latest_quality_reasons") or [])
        + list(quality_source_breakdown.get("crawl", {}).get("reason_codes") or [])
        + list(quality_source_breakdown.get("amazon", {}).get("reason_codes") or [])
        + list(quality_source_breakdown.get("noon", {}).get("reason_codes") or [])
    )
    evidence = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_evidence") or [])
        + list(summary.get("latest_terminal_quality_evidence") or [])
        + list(summary.get("latest_quality_evidence") or [])
        + list(quality_source_breakdown.get("crawl", {}).get("evidence") or [])
        + list(quality_source_breakdown.get("amazon", {}).get("evidence") or [])
        + list(quality_source_breakdown.get("noon", {}).get("evidence") or [])
        + quality_reasons
    )

    truth["live_quality_source_breakdown"] = live_source_breakdown
    truth["operator_quality_source_breakdown"] = operator_source_breakdown
    truth["latest_terminal_quality_source_breakdown"] = latest_terminal_source_breakdown
    truth["latest_quality_source_breakdown"] = latest_source_breakdown
    truth["quality_source_breakdown"] = quality_source_breakdown
    truth["quality_reasons"] = quality_reasons
    truth["quality_evidence"] = evidence
    truth["evidence"] = evidence
    truth["latest_terminal_batch_state"] = str(
        summary.get("latest_terminal_batch_state")
        or summary.get("latest_terminal_quality_state")
        or "unknown"
    )
    truth["latest_quality_state"] = str(
        summary.get("operator_quality_state")
        or summary.get("latest_terminal_quality_state")
        or summary.get("latest_quality_state")
        or "unknown"
    )
    truth["latest_quality_reasons"] = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_reasons") or [])
        + list(summary.get("latest_quality_reasons") or [])
        + list(summary.get("latest_terminal_quality_reasons") or [])
        + quality_reasons
    )
    truth["latest_quality_evidence"] = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_evidence") or [])
        + list(summary.get("latest_quality_evidence") or [])
        + list(summary.get("latest_terminal_quality_evidence") or [])
        + evidence
    )
    truth["operator_quality_reasons"] = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_reasons") or []) + quality_reasons
    )
    truth["operator_quality_evidence"] = normalize_quality_reason_tokens(
        list(summary.get("operator_quality_evidence") or []) + evidence
    )
    truth["quality_state_breakdown"] = dict(summary.get("quality_state_breakdown") or {})
    truth["quality_status_summary"] = str(summary.get("quality_status_summary") or "")
    truth["recent_runs"] = list(summary.get("recent_runs") or [])
    truth["quality_summary"] = dict(summary)
    return truth


def build_health_payload(
    *,
    db_path: Path,
    monitor_dir: Path,
    warehouse_db_path: Path,
    preview_lines: int = DEFAULT_LOG_PREVIEW_LINES,
    runtime_code_files: tuple[Path, ...] = DEFAULT_RUNTIME_CODE_FILES,
    result_sample_size: int = DEFAULT_RESULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    lock_path = monitor_dir / "keyword_monitor.lock"
    summary_path = monitor_dir / "keyword_monitor_last_run.json"
    batch_log_dir = monitor_dir / "batch_logs"

    runtime_payload = collect_runtime_db_payload(db_path)
    lock_payload = collect_lock_payload(lock_path)
    summary_payload = collect_summary_payload(summary_path)
    batch_logs_payload = collect_batch_logs(batch_log_dir, preview_lines=preview_lines)
    warehouse_payload = collect_warehouse_payload(warehouse_db_path)
    warehouse_lag = build_warehouse_lag_payload(runtime_payload, warehouse_payload)

    monitor_state = lock_payload["state"]
    if monitor_state == "idle" and summary_payload.get("exists"):
        monitor_state = "idle_with_summary"

    activity_state = "unknown"
    latest_log_age_seconds = batch_logs_payload.get("latest_log_age_seconds")
    if monitor_state == "running":
        if latest_log_age_seconds is None:
            activity_state = "running_no_logs"
        elif latest_log_age_seconds <= ACTIVE_LOG_WINDOW_SECONDS:
            activity_state = "running_active_logs"
        else:
            activity_state = "running_quiet_logs"
    elif monitor_state == "stale_lock":
        activity_state = "stale_lock"
    elif monitor_state == "idle_with_summary":
        activity_state = "idle_with_summary"
    else:
        activity_state = "idle"

    quality_payload = build_quality_payload(
        summary_payload,
        live_batch_state=activity_state,
        latest_terminal_quality_state=_normalize_quality_state(
            summary_payload.get("quality_state") or (summary_payload.get("quality_summary") or {}).get("state")
        ),
    )

    runtime_code = collect_runtime_code_payload(
        runtime_code_files=runtime_code_files,
        process_started_at=lock_payload.get("started_at"),
        monitor_state=monitor_state,
    )
    result_files = collect_result_file_payload(
        snapshot_root=monitor_dir.parent / "snapshots",
        snapshot_id=str(lock_payload.get("snapshot_id") or summary_payload.get("snapshot_id") or ""),
        process_started_at=lock_payload.get("started_at"),
        sample_size=result_sample_size,
    )
    restart_verification = build_restart_verification_payload(
        monitor_state=monitor_state,
        lock_payload=lock_payload,
        summary_payload=summary_payload,
        runtime_code=runtime_code,
        result_files=result_files,
    )

    operator_hint = build_operator_hint(
        monitor_state=monitor_state,
        activity_state=activity_state,
        warehouse_lag=warehouse_lag,
        runtime_code=runtime_code,
        quality_payload=quality_payload,
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "monitor_state": monitor_state,
        "activity_state": activity_state,
        "runtime": runtime_payload,
        "lock": lock_payload,
        "last_summary": summary_payload,
        "batch_logs": batch_logs_payload,
        "warehouse": warehouse_payload,
        "warehouse_lag": warehouse_lag,
        "quality": quality_payload,
        "runtime_code": runtime_code,
        "result_files": result_files,
        "restart_verification": restart_verification,
        "operator_hint": operator_hint,
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    settings = Settings()
    settings.set_runtime_scope("keyword")
    payload = build_health_payload(
        db_path=settings.product_store_db_path,
        monitor_dir=settings.data_dir / "monitor",
        warehouse_db_path=settings.shared_data_dir / "analytics" / "warehouse.db",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
