import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


BASE_URL = os.getenv("NOON_RUNNER_BASE_URL", "http://127.0.0.1:8865")
POLL_SECONDS = int(os.getenv("NOON_RUNNER_POLL_SECONDS", "20"))
SYNC_TIMEOUT_SECONDS = int(os.getenv("NOON_RUNNER_SYNC_TIMEOUT_SECONDS", str(20 * 60)))
SLOT_SECONDS = int(os.getenv("NOON_RUNNER_SLOT_SECONDS", str(4 * 60 * 60)))
EMPTY_PLAN_SLEEP_SECONDS = int(os.getenv("NOON_RUNNER_EMPTY_PLAN_SLEEP_SECONDS", str(max(POLL_SECONDS, 60))))
REPORT_ROOT = Path(os.getenv("NOON_REPORT_ROOT", "/volume1/docker/huihaokang-erp/shared/report/crawl"))
STATE_DIR = REPORT_ROOT / "state"
STATE_FILE = STATE_DIR / "runner_state.json"
METRICS_FILE = STATE_DIR / "metrics_snapshots.jsonl"
RUNNER_NAME = "alternating_crawl_service"
TERMINAL = {"completed", "failed", "cancelled", "skipped"}
ACTIVE = {"queued", "pending", "running", "leased", "retrying"}
CANONICAL_AUTO_DISPATCH_ENTRY = "crawl_plans"
LEGACY_SERVICE_ENV = "NOON_ENABLE_LEGACY_ALTERNATING_SERVICE"
DISABLED_HEARTBEAT_SECONDS = int(os.getenv("NOON_RUNNER_DISABLED_HEARTBEAT_SECONDS", "300"))
LEGACY_SERVICE_NAME = os.getenv("NOON_LEGACY_ALTERNATING_SERVICE_NAME", "huihaokang-alternating-crawl.service")

CATEGORY_PLAN_NAME = "alternating category sports-pets priority 500-200 postgres"
KEYWORD_PLAN_NAME = "alternating keyword pets-sports x300 postgres"

PHASES = [
    {
        "key": "category",
        "plan_type": "category_ready_scan",
        "name": CATEGORY_PLAN_NAME,
        "payload": {
            "categories": [
                "sports",
                "pets",
                "automotive",
                "baby",
                "beauty",
                "electronics",
                "fashion",
                "garden",
                "grocery",
                "home_kitchen",
                "office",
                "tools",
            ],
            "category_overrides": {
                "sports": 500,
                "pets": 500,
            },
            "default_product_count_per_leaf": 200,
            "export_excel": False,
            "persist": True,
            "reason": "24h alternating crawl - sports and pets priority at 500 per leaf, others at 200",
        },
        "sync_reason": "alternating-runner category phase",
    },
    {
        "key": "keyword",
        "plan_type": "keyword_monitor",
        "name": KEYWORD_PLAN_NAME,
        "payload": {
            "amazon_count": 300,
            "monitor_config": "/app/runtime_data/keyword/control/keyword_monitor_pet_sports_12h.json",
            "monitor_report": False,
            "noon_count": 300,
            "persist": True,
            "reason": "24h alternating crawl - sports and pets keyword priority at 300 per platform",
        },
        "sync_reason": "alternating-runner keyword phase",
    },
]

SELF_SYNCING_TASK_TYPES = {"category_ready_scan", "keyword_monitor"}


class RunnerApiError(RuntimeError):
    def __init__(self, method: str, path: str, status_code: int, detail: str) -> None:
        self.method = method
        self.path = path
        self.status_code = int(status_code)
        self.detail = detail
        super().__init__(f"{method} {path} failed: {status_code} {detail}")


class NoDispatchablePlanError(RunnerApiError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def legacy_service_enabled() -> bool:
    return str(os.getenv(LEGACY_SERVICE_ENV) or "").strip().lower() in {"1", "true", "yes", "on"}


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def log(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}", flush=True)


def request_json(method: str, path: str, payload: dict | None = None, timeout: int = 30) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "ignore")
        raise RunnerApiError(method, path, exc.code, detail) from exc


def list_tasks(limit: int = 500) -> list[dict]:
    payload = request_json("GET", f"/api/tasks?limit={limit}")
    return payload.get("items") or []


def list_workers() -> list[dict]:
    payload = request_json("GET", "/api/workers?limit=100")
    return payload.get("items") or []


def get_task(task_id: int) -> dict | None:
    for item in list_tasks():
        if int(item.get("id") or 0) == int(task_id):
            return item
    return None


def list_plans() -> list[dict]:
    payload = request_json("GET", "/api/crawler/plans?limit=200")
    return payload.get("items") or []


def patch_plan(plan_id: int, name: str, payload: dict) -> None:
    request_json(
        "PATCH",
        f"/api/crawler/plans/{plan_id}",
        {
            "name": name,
            "payload": payload,
            "enabled": False,
            "schedule_kind": "manual",
            "schedule_json": {},
        },
    )


def ensure_plan(plan_type: str, name: str, payload: dict) -> int:
    for item in list_plans():
        if str(item.get("name") or "").strip() == name:
            patch_plan(int(item["id"]), name, payload)
            return int(item["id"])
    created = request_json(
        "POST",
        "/api/crawler/plans",
        {
            "plan_type": plan_type,
            "name": name,
            "created_by": RUNNER_NAME,
            "enabled": False,
            "schedule_kind": "manual",
            "schedule_json": {},
            "payload": payload,
        },
    )
    return int(created["id"])


def launch_plan(plan_id: int) -> int:
    path = f"/api/crawler/plans/{plan_id}/launch"
    try:
        task = request_json("POST", path)
    except RunnerApiError as exc:
        normalized = str(exc.detail or "").lower()
        if exc.status_code == 404 and "no dispatchable round items for plan" in normalized:
            raise NoDispatchablePlanError(exc.method, exc.path, exc.status_code, exc.detail) from exc
        raise
    return int(task["id"])


def cancel_task(task_id: int) -> None:
    request_json("POST", f"/api/tasks/{task_id}/cancel")


def create_sync_task(reason: str) -> int:
    task = request_json(
        "POST",
        "/api/tasks",
        {
            "task_type": "warehouse_sync",
            "payload": {"reason": reason},
            "created_by": RUNNER_NAME,
            "priority": 90,
            "schedule_type": "manual",
            "worker_type": "sync",
        },
    )
    return int(task["id"])


def append_snapshot(event: str, **extra: object) -> None:
    ensure_dirs()
    try:
        health = request_json("GET", "/api/health")
    except Exception as exc:
        health = {
            "status": "unavailable",
            "warehouse_db": "",
            "product_count": None,
            "observation_count": None,
            "snapshot_error": str(exc),
        }
    payload = {
        "ts": utc_now_iso(),
        "event": event,
        "status": health.get("status"),
        "warehouse_db": health.get("warehouse_db"),
        "product_count": health.get("product_count"),
        "observation_count": health.get("observation_count"),
    }
    payload.update(extra)
    with METRICS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _maybe_disable_service_boot() -> dict:
    if os.name == "nt":
        return {"status": "skipped", "reason": "windows"}
    geteuid = getattr(os, "geteuid", None)
    if not callable(geteuid) or int(geteuid()) != 0:
        return {"status": "skipped", "reason": "not_root"}
    try:
        proc = subprocess.run(  # type: ignore[name-defined]
            ["systemctl", "disable", LEGACY_SERVICE_NAME],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except Exception as exc:
        return {"status": "failed", "reason": str(exc)}
    return {
        "status": "completed" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "stderr": (proc.stderr or "").strip(),
        "stdout": (proc.stdout or "").strip(),
    }


def run_disabled_loop() -> None:
    ensure_dirs()
    disable_result = _maybe_disable_service_boot()
    disabled_state = {
        "mode": "disabled",
        "runner_name": RUNNER_NAME,
        "auto_dispatch_entry": CANONICAL_AUTO_DISPATCH_ENTRY,
        "updated_at": utc_now_iso(),
    }
    try:
        save_state(disabled_state)
    except Exception:
        pass
    try:
        append_snapshot(
            "legacy_disabled",
            runner_name=RUNNER_NAME,
            auto_dispatch_entry=CANONICAL_AUTO_DISPATCH_ENTRY,
            disable_result=disable_result,
        )
    except Exception:
        pass
    log("legacy alternating service disabled; crawl_plans + scheduler is canonical")
    while True:
        time.sleep(max(POLL_SECONDS, DISABLED_HEARTBEAT_SECONDS))
        try:
            save_state(disabled_state)
        except Exception:
            pass
        try:
            append_snapshot(
                "legacy_disabled_heartbeat",
                runner_name=RUNNER_NAME,
                auto_dispatch_entry=CANONICAL_AUTO_DISPATCH_ENTRY,
            )
        except Exception:
            pass


def load_state() -> dict:
    ensure_dirs()
    if not STATE_FILE.exists():
        return {"next_phase_index": 0, "cycle_count": 0, "updated_at": utc_now_iso()}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"next_phase_index": 0, "cycle_count": 0, "updated_at": utc_now_iso()}


def save_state(state: dict) -> None:
    ensure_dirs()
    state["updated_at"] = utc_now_iso()
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def find_active_plan_task(plan_id: int) -> dict | None:
    workers = list_workers()
    live_worker_map = {
        str(item.get("worker_name") or ""): item
        for item in workers
        if str(item.get("worker_name") or "").strip()
    }
    for item in list_tasks():
        if int(item.get("plan_id") or 0) != int(plan_id):
            continue
        status = str(item.get("status") or "").strip().lower()
        if status not in ACTIVE:
            continue
        lease_owner = str(item.get("lease_owner") or "").strip()
        if status in {"running", "leased", "retrying"} and lease_owner:
            worker = live_worker_map.get(lease_owner)
            worker_task_id = int(((worker or {}).get("current_task_id") or 0))
            if not worker or worker_task_id != int(item.get("id") or 0):
                log(f"plan {plan_id} found zombie active task {item.get('id')} lease_owner={lease_owner}; cancelling")
                try:
                    cancel_task(int(item["id"]))
                except Exception as exc:
                    log(f"failed to cancel zombie task {item.get('id')}: {exc}")
                continue
            return item
    return None


def wait_for_terminal(task_id: int, slot_deadline: float) -> dict:
    cancelled = False
    while True:
        task = get_task(task_id)
        if not task:
            return {"id": task_id, "status": "missing"}
        status = str(task.get("status") or "").strip().lower()
        if status in TERMINAL:
            return task
        if (not cancelled) and time.time() >= slot_deadline:
            log(f"slot deadline reached, cancelling task {task_id}")
            try:
                cancel_task(task_id)
            except Exception as exc:
                log(f"cancel request failed for task {task_id}: {exc}")
            cancelled = True
        time.sleep(POLL_SECONDS)


def wait_sync(sync_task_id: int) -> dict:
    deadline = time.time() + SYNC_TIMEOUT_SECONDS
    last_seen = None
    while time.time() < deadline:
        task = get_task(sync_task_id)
        if task:
            last_seen = task
            status = str(task.get("status") or "").strip().lower()
            if status in TERMINAL:
                return task
        time.sleep(5)
    return last_seen or {"id": sync_task_id, "status": "missing"}


def maybe_run_phase_sync(slot_number: int, phase: dict, task_id: int, terminal: dict) -> None:
    status = str(terminal.get("status") or "").strip().lower()
    task_type = str(terminal.get("task_type") or phase.get("plan_type") or "").strip().lower()
    should_run_sync = True
    if status == "completed" and task_type in SELF_SYNCING_TASK_TYPES:
        should_run_sync = False
        log(f"slot {slot_number} skipping duplicate sync after completed {task_type} task {task_id}")
        append_snapshot(
            "sync_skipped",
            slot_number=slot_number,
            phase=phase["key"],
            plan_id=terminal.get("plan_id") or "",
            task_id=task_id,
            task_status=status,
            reason="self_syncing_task_completed",
        )
    if not should_run_sync:
        return

    sync_task_id = create_sync_task(f"{phase['sync_reason']} slot {slot_number}")
    log(f"slot {slot_number} created sync task {sync_task_id}")
    sync_terminal = wait_sync(sync_task_id)
    sync_status = str(sync_terminal.get("status") or "").strip().lower()
    log(f"slot {slot_number} sync task {sync_task_id} terminal={sync_status}")
    append_snapshot(
        "sync_terminal",
        slot_number=slot_number,
        phase=phase["key"],
        task_id=sync_task_id,
        task_status=sync_status,
    )


def run_phase(slot_number: int, phase_index: int, phase: dict) -> None:
    plan_id = ensure_plan(phase["plan_type"], phase["name"], phase["payload"])
    slot_deadline = time.time() + SLOT_SECONDS
    log(f"slot {slot_number} start: {phase['key']} plan={plan_id}")
    append_snapshot("slot_started", slot_number=slot_number, phase=phase["key"], plan_id=plan_id)
    active = find_active_plan_task(plan_id)
    if active:
        task_id = int(active["id"])
        log(f"slot {slot_number} reusing active task {task_id} for {phase['key']}")
    else:
        try:
            task_id = launch_plan(plan_id)
        except NoDispatchablePlanError as exc:
            reason = "no_dispatchable_round_items"
            log(f"slot {slot_number} no dispatchable items for {phase['key']} plan {plan_id}; skipping phase")
            append_snapshot(
                "phase_skipped",
                slot_number=slot_number,
                phase=phase["key"],
                plan_id=plan_id,
                reason=reason,
                detail=exc.detail,
            )
            time.sleep(EMPTY_PLAN_SLEEP_SECONDS)
            log(f"slot {slot_number} end: {phase['key']} skipped")
            append_snapshot(
                "slot_ended",
                slot_number=slot_number,
                phase=phase["key"],
                plan_id=plan_id,
                skipped=True,
                reason=reason,
            )
            return
        log(f"slot {slot_number} launched {phase['key']} task {task_id} run=1")
    terminal = wait_for_terminal(task_id, slot_deadline)
    status = str(terminal.get("status") or "").strip().lower()
    log(f"slot {slot_number} task {task_id} terminal={status}")
    append_snapshot(
        "task_terminal",
        slot_number=slot_number,
        phase=phase["key"],
        plan_id=plan_id,
        task_id=task_id,
        task_status=status,
    )
    maybe_run_phase_sync(slot_number, phase, task_id, terminal)
    log(f"slot {slot_number} end: {phase['key']}")
    append_snapshot("slot_ended", slot_number=slot_number, phase=phase["key"], plan_id=plan_id)


def main() -> None:
    ensure_dirs()
    if not legacy_service_enabled():
        run_disabled_loop()
        return
    state = load_state()
    append_snapshot("runner_started", next_phase_index=state.get("next_phase_index", 0))
    log("alternating crawl service started")
    slot_number = int(state.get("slot_number") or 0)
    while True:
        phase_index = int(state.get("next_phase_index") or 0) % len(PHASES)
        slot_number += 1
        state["next_phase_index"] = phase_index
        state["slot_number"] = slot_number
        save_state(state)
        run_phase(slot_number, phase_index, PHASES[phase_index])
        next_phase = (phase_index + 1) % len(PHASES)
        state["next_phase_index"] = next_phase
        state["slot_number"] = slot_number
        if next_phase == 0:
            state["cycle_count"] = int(state.get("cycle_count") or 0) + 1
        save_state(state)


if __name__ == "__main__":
    main()
