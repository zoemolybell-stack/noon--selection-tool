import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


BASE_URL = "http://127.0.0.1:8865"
CATEGORY_PLAN_NAME = "formal category x200 isolated"
KEYWORD_PLAN_NAME = "formal keyword pets sports x20 isolated"
CATEGORY_WINDOW_SECONDS = 60 * 60
KEYWORD_WINDOW_SECONDS = 60 * 60
POLL_SECONDS = 20
SYNC_TIMEOUT_SECONDS = 20 * 60
RUNNER_NAME = "formal_crawl_window_2h"
SELF_SYNCING_TASK_TYPES = {"category_ready_scan", "keyword_monitor"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"[{_now()}] {message}", flush=True)


def request_json(method: str, path: str, payload: dict | None = None) -> dict:
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
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "ignore")
        raise RuntimeError(f"{method} {path} failed: {exc.code} {detail}") from exc


def list_tasks(status: str = "", limit: int = 200) -> list[dict]:
    suffix = f"?limit={limit}"
    if status:
        suffix += f"&status={status}"
    payload = request_json("GET", f"/api/tasks{suffix}")
    return payload.get("items") or []


def get_task(task_id: int) -> dict | None:
    for item in list_tasks(limit=500):
        if int(item.get("id") or 0) == int(task_id):
            return item
    return None


def cancel_task(task_id: int) -> dict:
    return request_json("POST", f"/api/tasks/{task_id}/cancel")


def create_sync_task(reason: str) -> int:
    payload = {
        "task_type": "warehouse_sync",
        "payload": {"reason": reason},
        "created_by": RUNNER_NAME,
        "priority": 100,
        "schedule_type": "manual",
        "worker_type": "sync",
    }
    task = request_json("POST", "/api/tasks", payload)
    return int(task["id"])


def wait_for_terminal(task_id: int, timeout_seconds: int) -> dict | None:
    deadline = time.time() + timeout_seconds
    last_seen = None
    while time.time() < deadline:
        task = get_task(task_id)
        if task:
            last_seen = task
            status = str(task.get("status") or "").strip().lower()
            if status in {"completed", "failed", "cancelled", "skipped"}:
                return task
        time.sleep(POLL_SECONDS)
    return last_seen


def should_run_followup_sync(task: dict | None) -> bool:
    if not task:
        return True
    status = str(task.get("status") or "").strip().lower()
    task_type = str(task.get("task_type") or "").strip().lower()
    return not (status == "completed" and task_type in SELF_SYNCING_TASK_TYPES)


def patch_plan(plan_id: int, payload: dict, name: str) -> None:
    request_json(
        "PATCH",
        f"/api/crawler/plans/{plan_id}",
        {
            "name": name,
            "payload": payload,
        },
    )


def list_plans() -> list[dict]:
    payload = request_json("GET", "/api/crawler/plans?limit=200")
    return payload.get("items") or []


def ensure_plan(plan_type: str, name: str, payload: dict) -> int:
    for item in list_plans():
        if str(item.get("name") or "").strip() == name:
            patch_plan(int(item["id"]), payload, name)
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
    task = request_json("POST", f"/api/crawler/plans/{plan_id}/launch")
    return int(task["id"])


def run_phase(plan_id: int, label: str, window_seconds: int) -> list[int]:
    phase_deadline = time.time() + window_seconds
    launched: list[int] = []
    while time.time() < phase_deadline:
        task_id = launch_plan(plan_id)
        launched.append(task_id)
        log(f"{label}: launched task {task_id}")
        while time.time() < phase_deadline:
            task = get_task(task_id)
            if not task:
                time.sleep(POLL_SECONDS)
                continue
            status = str(task.get("status") or "").strip().lower()
            if status in {"completed", "failed", "cancelled", "skipped"}:
                log(f"{label}: task {task_id} finished with status={status}")
                break
            time.sleep(POLL_SECONDS)
        else:
            cancel_task(task_id)
            log(f"{label}: task {task_id} cancelled at window boundary")
        remaining = phase_deadline - time.time()
        if remaining <= POLL_SECONDS:
            break
    return launched


def main() -> None:
    log("formal crawl window started")
    category_plan_id = ensure_plan(
        "category_ready_scan",
        CATEGORY_PLAN_NAME,
        {
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
            "default_product_count_per_leaf": 200,
            "export_excel": False,
            "persist": True,
            "reason": "2h formal crawl window - low intensity category",
        },
    )
    keyword_plan_id = ensure_plan(
        "keyword_monitor",
        KEYWORD_PLAN_NAME,
        {
            "amazon_count": 20,
            "monitor_config": "/app/runtime_data/keyword/control/keyword_monitor_pet_sports_12h.json",
            "monitor_report": False,
            "noon_count": 20,
            "persist": True,
            "reason": "2h formal crawl window - low intensity keyword",
        },
    )
    category_tasks = run_phase(category_plan_id, "category", CATEGORY_WINDOW_SECONDS)
    category_terminal = get_task(category_tasks[-1]) if category_tasks else None
    if should_run_followup_sync(category_terminal):
        sync_task_id = create_sync_task("formal-crawl-window category phase")
        log(f"category: sync task {sync_task_id} created")
        wait_for_terminal(sync_task_id, SYNC_TIMEOUT_SECONDS)
    else:
        log("category: skipped duplicate sync after completed self-syncing task")
    keyword_tasks = run_phase(keyword_plan_id, "keyword", KEYWORD_WINDOW_SECONDS)
    keyword_terminal = get_task(keyword_tasks[-1]) if keyword_tasks else None
    if should_run_followup_sync(keyword_terminal):
        sync_task_id = create_sync_task("formal-crawl-window keyword phase")
        log(f"keyword: sync task {sync_task_id} created")
        wait_for_terminal(sync_task_id, SYNC_TIMEOUT_SECONDS)
    else:
        log("keyword: skipped duplicate sync after completed self-syncing task")
    log(
        json.dumps(
            {
                "status": "completed",
                "category_tasks": category_tasks,
                "keyword_tasks": keyword_tasks,
                "finished_at": _now(),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
