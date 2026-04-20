from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ops.task_store import OpsStore


STAGE_ORDER = [
    ("runtime_collecting", "runtime collecting"),
    ("stage_persisted", "stage persisted"),
    ("warehouse_syncing", "warehouse syncing"),
    ("partial_visible", "partial visible"),
    ("web_visible", "web visible"),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_progress(
    stage_key: str,
    *,
    message: str = "",
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    completed: bool = False,
) -> dict[str, Any]:
    stage_names = [item[0] for item in STAGE_ORDER]
    current_index = stage_names.index(stage_key) if stage_key in stage_names else 0
    stages = []
    for index, (key, label) in enumerate(STAGE_ORDER):
        status = "pending"
        if index < current_index:
            status = "completed"
        elif index == current_index:
            status = "completed" if completed else "active"
        stages.append({"key": key, "label": label, "status": status})
    return {
        "stage": stage_key,
        "message": message,
        "updated_at": _now_iso(),
        "metrics": dict(metrics or {}),
        "details": dict(details or {}),
        "stages": stages,
    }


@dataclass
class TaskProgressReporter:
    task_id: int

    def update(
        self,
        stage_key: str,
        *,
        message: str = "",
        metrics: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
        completed: bool = False,
    ) -> dict[str, Any] | None:
        payload = build_progress(
            stage_key,
            message=message,
            metrics=metrics,
            details=details,
            completed=completed,
        )
        store = OpsStore()
        try:
            return store.update_task_progress(task_id=int(self.task_id), progress=payload)
        finally:
            store.close()


def get_task_progress_reporter_from_env() -> TaskProgressReporter | None:
    import os

    raw_task_id = str(os.getenv("NOON_OPS_TASK_ID") or "").strip()
    if not raw_task_id:
        return None
    try:
        task_id = int(raw_task_id)
    except ValueError:
        return None
    return TaskProgressReporter(task_id=task_id)
