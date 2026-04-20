from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from config.settings import Settings


def _atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    try:
        temp_path.write_text(content, encoding=encoding)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)
    return path


def _atomic_write_json(path: Path, payload: object) -> Path:
    return _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_load_json_file(
    path: Path,
    *,
    expected_type: type | tuple[type, ...] | None = None,
) -> tuple[object | None, str]:
    if not path.exists():
        return None, f"missing_file:{path.name}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"invalid_json:{path.name}:{exc}"
    if expected_type is not None and not isinstance(payload, expected_type):
        expected_name = (
            ",".join(item.__name__ for item in expected_type)
            if isinstance(expected_type, tuple)
            else expected_type.__name__
        )
        return None, f"invalid_json_type:{path.name}:expected={expected_name}"
    return payload, ""


def load_monitor_profile(settings: Settings, args: argparse.Namespace, *, logger: logging.Logger | None = None) -> dict:
    default_baseline = settings.project_root / "config" / "keyword_baseline_seeds.txt"
    profile = {
        "baseline_file": str(default_baseline) if default_baseline.exists() else args.baseline_file,
        "tracked_priority": 30,
        "expand_limit": args.expand_limit if args.expand_limit is not None else 10,
        "expand_stale_hours": getattr(args, "expand_stale_hours", None) if getattr(args, "expand_stale_hours", None) is not None else 72,
        "expand_source_types": getattr(args, "expand_source_types", None) or ["baseline", "manual", "generated", "tracked"],
        "expand_platforms": args.expand_platforms or ["noon", "amazon"],
        "crawl_platforms": args.platforms or ["noon", "amazon"],
        "crawl_stale_hours": args.stale_hours if args.stale_hours is not None else 24,
        "crawl_limit": args.limit if args.limit is not None else 200,
        "monitor_report": bool(args.monitor_report),
    }

    config_path = Path(args.monitor_config) if args.monitor_config else (settings.project_root / "config" / "keyword_monitor_defaults.json")
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                profile.update({k: v for k, v in loaded.items() if v not in (None, "")})
        except Exception as exc:
            if logger:
                logger.warning("monitor 配置读取失败，使用默认参数: %s", exc)

    if args.baseline_file:
        profile["baseline_file"] = args.baseline_file
    if args.expand_limit is not None:
        profile["expand_limit"] = args.expand_limit
    if getattr(args, "expand_stale_hours", None) is not None:
        profile["expand_stale_hours"] = args.expand_stale_hours
    if getattr(args, "expand_source_types", None):
        profile["expand_source_types"] = args.expand_source_types
    if args.expand_platforms:
        profile["expand_platforms"] = args.expand_platforms
    if args.platforms:
        profile["crawl_platforms"] = args.platforms
    if args.stale_hours is not None:
        profile["crawl_stale_hours"] = args.stale_hours
    if args.limit is not None:
        profile["crawl_limit"] = args.limit
    if args.monitor_report:
        profile["monitor_report"] = True
    return profile


def monitor_dir(settings: Settings) -> Path:
    path = settings.data_dir / "monitor"
    path.mkdir(parents=True, exist_ok=True)
    return path


def monitor_lock_path(settings: Settings, *, lock_filename: str) -> Path:
    return monitor_dir(settings) / lock_filename


def monitor_summary_path(settings: Settings, *, summary_filename: str) -> Path:
    return monitor_dir(settings) / summary_filename


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _pid_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_monitor_lock(settings: Settings, snapshot_id: str, *, lock_filename: str) -> dict[str, object]:
    lock_path = monitor_lock_path(settings, lock_filename=lock_filename)
    if lock_path.exists():
        existing_payload, _ = _safe_load_json_file(lock_path, expected_type=dict)
        existing = existing_payload if isinstance(existing_payload, dict) else {}
        existing_pid = _safe_int(existing.get("pid"), 0)
        if _pid_is_alive(existing_pid):
            raise SystemExit(
                f"keyword monitor is already running: pid={existing_pid}, snapshot={existing.get('snapshot_id')}"
            )
        lock_path.unlink(missing_ok=True)

    payload = {
        "pid": os.getpid(),
        "snapshot_id": snapshot_id,
        "started_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status": "running",
        "current_stage": "starting",
    }
    _atomic_write_json(lock_path, payload)
    return payload


def release_monitor_lock(settings: Settings, *, lock_filename: str) -> None:
    monitor_lock_path(settings, lock_filename=lock_filename).unlink(missing_ok=True)


def refresh_monitor_lock(
    settings: Settings,
    lock_payload: dict[str, object],
    *,
    lock_filename: str,
    **updates,
) -> dict[str, object]:
    payload = dict(lock_payload or {})
    payload.update({key: value for key, value in updates.items() if value is not None})
    payload["updated_at"] = datetime.now().isoformat()
    _atomic_write_json(monitor_lock_path(settings, lock_filename=lock_filename), payload)
    return payload


def write_monitor_summary(settings: Settings, summary: dict[str, object], *, summary_filename: str) -> Path:
    payload = dict(summary)
    payload["updated_at"] = datetime.now().isoformat()
    return _atomic_write_json(monitor_summary_path(settings, summary_filename=summary_filename), payload)


def checkpoint_monitor_state(
    settings: Settings,
    summary: dict[str, object],
    lock_payload: dict[str, object],
    *,
    stage: str,
    lock_filename: str,
    summary_filename: str,
    note: str = "",
) -> dict[str, object]:
    summary["current_stage"] = stage
    if note:
        summary["stage_note"] = note
    elif "stage_note" in summary:
        summary["stage_note"] = ""
    refreshed_lock = refresh_monitor_lock(
        settings,
        lock_payload,
        lock_filename=lock_filename,
        status=str(summary.get("status") or "running"),
        current_stage=stage,
        stage_note=note,
        expanded_seed_count=int(summary.get("expanded_seed_count") or 0),
        expanded_keyword_count=int(summary.get("expanded_keyword_count") or 0),
        crawled_keyword_count=int(summary.get("crawled_keyword_count") or 0),
        persisted_product_count=int(summary.get("persisted_product_count") or 0),
        analyzed_keyword_count=int(summary.get("analyzed_keyword_count") or 0),
    )
    summary["lock_payload"] = refreshed_lock
    write_monitor_summary(settings, summary, summary_filename=summary_filename)
    return refreshed_lock


def official_keyword_db_path(settings: Settings) -> Path:
    return settings.project_root / "runtime_data" / "keyword" / "product_store.db"


def sync_warehouse(
    settings: Settings,
    *,
    reason: str,
    wait_for_lock: bool = True,
    actor: str = "keyword_window",
    lock_wait_seconds: int = 180,
    lock_retry_seconds: int = 5,
    logger: logging.Logger | None = None,
) -> dict[str, object]:
    current_db = settings.product_store_db_ref
    warehouse_db = settings.warehouse_db_ref
    command = [
        sys.executable,
        str(settings.project_root / "run_shared_warehouse_sync.py"),
        "--actor",
        actor,
        "--reason",
        reason,
        "--trigger-db",
        current_db,
        "--warehouse-db",
        warehouse_db,
    ]
    deadline = time.time() + (lock_wait_seconds if wait_for_lock else 0)
    while True:
        completed = subprocess.run(
            command,
            cwd=str(settings.project_root),
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
            if line.startswith("WAREHOUSE_SYNC_RESULT="):
                try:
                    result_payload = json.loads(line[len("WAREHOUSE_SYNC_RESULT="):])
                except json.JSONDecodeError:
                    result_payload = None
                break
        if completed.returncode != 0:
            detail = "\n".join(log_lines[-20:])
            payload = {
                "status": str((result_payload or {}).get("status") or "failed"),
                "reason": str((result_payload or {}).get("reason") or reason),
                "keyword_db": str(current_db),
                "warehouse_db": str(warehouse_db),
                "command": command,
                "log_tail": log_lines[-20:],
                "returncode": completed.returncode,
                "error": detail or str(completed.returncode),
                "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
                "sync_state": (result_payload or {}).get("sync_state") or {},
            }
            if logger:
                logger.error("keyword warehouse sync failed (%s): %s", reason, payload["error"])
            return payload

        payload = {
            "status": str((result_payload or {}).get("status") or "completed"),
            "reason": str((result_payload or {}).get("reason") or reason),
            "keyword_db": str(current_db),
            "warehouse_db": str(warehouse_db),
            "command": command,
            "log_tail": log_lines[-10:],
            "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
            "sync_state": (result_payload or {}).get("sync_state") or {},
        }
        if payload["status"] == "skipped" and str(payload.get("skip_reason") or "") == "lock_active":
            if wait_for_lock and time.time() < deadline:
                time.sleep(max(lock_retry_seconds, 1))
                continue
            if logger:
                logger.info("skip warehouse sync: shared lock active -> %s", payload.get("skip_reason") or "unknown")
            return payload
        if logger:
            logger.info("keyword warehouse synced: %s", warehouse_db)
        return payload
