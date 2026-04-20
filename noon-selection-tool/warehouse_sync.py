from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_SYNC_DIR = ROOT / "data" / "analytics"
DEFAULT_SYNC_LOCK_PATH = DEFAULT_SYNC_DIR / "warehouse_sync.lock"
DEFAULT_SYNC_STATE_PATH = DEFAULT_SYNC_DIR / "warehouse_sync_status.json"
SYNC_STATE_SCHEMA_VERSION = "20260329a"
DEFAULT_SYNC_LEASE_SECONDS = 15 * 60


def _now_iso() -> str:
    return datetime.now().isoformat()


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    os.replace(temp_path, path)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _safe_load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalize_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"running", "completed", "failed", "skipped", "idle"}:
        return normalized
    if normalized == "success":
        return "completed"
    return "unknown"


def _build_state_payload(
    *,
    status: str,
    actor: str = "",
    reason: str = "",
    warehouse_db: str = "",
    trigger_db: str = "",
    requested_at: str = "",
    started_at: str = "",
    finished_at: str = "",
    updated_at: str = "",
    lock_token: str = "",
    skip_reason: str = "",
    error: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SYNC_STATE_SCHEMA_VERSION,
        "status": _normalize_status(status),
        "actor": actor or "",
        "reason": reason or "",
        "warehouse_db": warehouse_db or "",
        "trigger_db": trigger_db or "",
        "requested_at": requested_at or "",
        "started_at": started_at or "",
        "finished_at": finished_at or "",
        "updated_at": _now_iso() if updated_at is None else updated_at,
        "lock_token": lock_token or "",
        "skip_reason": skip_reason or "",
        "error": error or "",
        "metadata": metadata or {},
    }


def read_sync_state(state_path: Path = DEFAULT_SYNC_STATE_PATH) -> dict[str, Any]:
    if not state_path.exists():
        return _build_state_payload(status="idle", updated_at="")
    payload = _safe_load_json(state_path)
    if not payload:
        return _build_state_payload(status="unknown", error="invalid_sync_state_file")
    payload["status"] = _normalize_status(payload.get("status", "unknown"))
    payload.setdefault("schema_version", SYNC_STATE_SCHEMA_VERSION)
    payload.setdefault("metadata", {})
    payload.setdefault("updated_at", "")
    return payload


def read_sync_lock(lock_path: Path = DEFAULT_SYNC_LOCK_PATH) -> dict[str, Any]:
    if not lock_path.exists():
        return {}
    payload = _safe_load_json(lock_path)
    return payload if isinstance(payload, dict) else {}


def _is_lock_active(lock_payload: dict[str, Any], *, now: datetime) -> bool:
    expires_at = str(lock_payload.get("expires_at") or "").strip()
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now
    except ValueError:
        return False


def acquire_sync_lock(
    *,
    actor: str,
    reason: str,
    warehouse_db: str,
    trigger_db: str = "",
    lock_path: Path = DEFAULT_SYNC_LOCK_PATH,
    state_path: Path = DEFAULT_SYNC_STATE_PATH,
    lease_seconds: int = DEFAULT_SYNC_LEASE_SECONDS,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = datetime.now()
    current_lock = read_sync_lock(lock_path)
    if current_lock and _is_lock_active(current_lock, now=now):
        return _build_state_payload(
            status="skipped",
            actor=actor,
            reason=reason,
            warehouse_db=warehouse_db,
            trigger_db=trigger_db,
            skip_reason="lock_active",
            metadata={
                "active_lock": current_lock,
                **(metadata or {}),
            },
        )

    lock_token = uuid.uuid4().hex
    requested_at = now.isoformat()
    lock_payload = {
        "schema_version": SYNC_STATE_SCHEMA_VERSION,
        "lock_token": lock_token,
        "actor": actor,
        "reason": reason,
        "warehouse_db": warehouse_db,
        "trigger_db": trigger_db,
        "pid": os.getpid(),
        "requested_at": requested_at,
        "updated_at": requested_at,
        "expires_at": (now + timedelta(seconds=max(int(lease_seconds), 1))).isoformat(),
        "metadata": metadata or {},
    }
    _atomic_write_json(lock_path, lock_payload)

    state_payload = _build_state_payload(
        status="running",
        actor=actor,
        reason=reason,
        warehouse_db=warehouse_db,
        trigger_db=trigger_db,
        requested_at=requested_at,
        started_at=requested_at,
        updated_at=requested_at,
        lock_token=lock_token,
        metadata=metadata,
    )
    _atomic_write_json(state_path, state_payload)
    return state_payload


def finalize_sync_state(
    *,
    lock_token: str,
    final_status: str,
    actor: str,
    reason: str,
    warehouse_db: str,
    trigger_db: str = "",
    state_path: Path = DEFAULT_SYNC_STATE_PATH,
    lock_path: Path = DEFAULT_SYNC_LOCK_PATH,
    skip_reason: str = "",
    error: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_lock = read_sync_lock(lock_path)
    requested_at = ""
    started_at = ""
    if current_lock and str(current_lock.get("lock_token") or "") == str(lock_token or ""):
        requested_at = str(current_lock.get("requested_at") or "")
        started_at = str(current_lock.get("requested_at") or "")
        lock_path.unlink(missing_ok=True)

    finished_at = _now_iso()
    payload = _build_state_payload(
        status=final_status,
        actor=actor,
        reason=reason,
        warehouse_db=warehouse_db,
        trigger_db=trigger_db,
        requested_at=requested_at,
        started_at=started_at,
        finished_at=finished_at,
        updated_at=finished_at,
        lock_token=str(lock_token or ""),
        skip_reason=skip_reason,
        error=error,
        metadata=metadata,
    )
    _atomic_write_json(state_path, payload)
    return payload
