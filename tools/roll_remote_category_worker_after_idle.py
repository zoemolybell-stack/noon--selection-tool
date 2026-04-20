from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request as urllib_request


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_URL = "http://192.168.100.20:8865"
DEFAULT_COMPOSE_FILE = ROOT / "docker-compose.category-node.yml"
DEFAULT_ENV_FILE = ROOT / ".env.category-node"
DEFAULT_POLL_SECONDS = 60
DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Wait for the current remote category task to finish, then rebuild and roll category-worker only.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--compose-file", default=str(DEFAULT_COMPOSE_FILE))
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--target-task-id", type=int, default=0)
    parser.add_argument("--log-dir", default=str(ROOT / "logs" / "remote_category_rollout"))
    return parser


class RunLog:
    def __init__(self, root: Path) -> None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = root / f"roll_{stamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.text_path = self.run_dir / "events.log"
        self.json_path = self.run_dir / "result.json"
        self.payload: dict[str, Any] = {"started_at": utcnow_iso(), "events": []}

    def event(self, message: str, **details: Any) -> None:
        item: dict[str, Any] = {"timestamp": utcnow_iso(), "message": message}
        if details:
            item["details"] = details
        self.payload["events"].append(item)
        line = f"[{item['timestamp']}] {message}"
        if details:
            line += " | " + json.dumps(details, ensure_ascii=False, sort_keys=True)
        print(line, flush=True)
        with self.text_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        self.flush()

    def flush(self) -> None:
        self.json_path.write_text(json.dumps(self.payload, ensure_ascii=False, indent=2), encoding="utf-8")


def http_json(url: str, *, timeout: int = 15) -> dict[str, Any]:
    request = urllib_request.Request(url, method="GET")
    with urllib_request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8")
    loaded = json.loads(text or "{}")
    return loaded if isinstance(loaded, dict) else {}


def task_is_active(task: dict[str, Any]) -> bool:
    return str(task.get("status") or "").strip().lower() in {"running", "leased", "pending", "retrying"}


def remote_category_running_task(base_url: str, target_task_id: int = 0) -> dict[str, Any] | None:
    payload = http_json(f"{base_url.rstrip('/')}/api/tasks?worker_type=category&status=running&limit=50")
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        lease_owner = str(item.get("lease_owner") or "").strip()
        node_role = str(((item.get("payload") or {}).get("node_role")) or "").strip().lower()
        worker_type = str(item.get("worker_type") or "").strip().lower()
        if worker_type != "category":
            continue
        if target_task_id and int(item.get("id") or 0) != target_task_id:
            continue
        if node_role == "remote_category" or lease_owner.startswith("remote-category-"):
            return item
    return None


def wait_for_boundary(*, base_url: str, log: RunLog, poll_seconds: int, timeout_seconds: int, target_task_id: int) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        task = remote_category_running_task(base_url, target_task_id=target_task_id)
        if task is None:
            return {"ready": True, "reason": "no_remote_category_task_running", "at": utcnow_iso()}
        progress = task.get("progress") if isinstance(task.get("progress"), dict) else {}
        log.event(
            "waiting for task boundary",
            task_id=int(task.get("id") or 0),
            status=str(task.get("status") or ""),
            stage=str(progress.get("stage") or ""),
            message=str(progress.get("message") or ""),
            lease_owner=str(task.get("lease_owner") or ""),
            updated_at=str(task.get("updated_at") or ""),
        )
        if not task_is_active(task):
            return {"ready": True, "reason": "target_task_terminal", "task_id": int(task.get("id") or 0), "at": utcnow_iso()}
        time.sleep(max(10, poll_seconds))
    return {"ready": False, "reason": "timeout", "at": utcnow_iso()}


def run_command(command: list[str], *, cwd: Path, log: RunLog, label: str) -> dict[str, Any]:
    log.event("running command", label=label, command=command)
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    result = {
        "command": command,
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout.splitlines()[-40:],
        "stderr_tail": proc.stderr.splitlines()[-40:],
    }
    log.event("command finished", label=label, result=result)
    return result


def verify_restart(*, base_url: str, log: RunLog) -> dict[str, Any]:
    ps_result = run_command(
        ["docker", "ps", "--format", "table {{.Names}}\\t{{.Status}}\\t{{.Ports}}"],
        cwd=ROOT,
        log=log,
        label="docker ps",
    )
    status = http_json(f"{base_url.rstrip('/')}/api/system/health", timeout=20)
    workers_payload = http_json(f"{base_url.rstrip('/')}/api/workers?limit=20", timeout=20)
    workers = workers_payload.get("items") if isinstance(workers_payload.get("items"), list) else []
    remote_worker = next(
        (
            item
            for item in workers
            if isinstance(item, dict)
            and str(item.get("worker_type") or "").strip().lower() == "category"
            and str(item.get("node_role") or ((item.get("details") or {}).get("node_role")) or "").strip().lower()
            == "remote_category"
        ),
        None,
    )
    return {
        "docker_ps": ps_result,
        "system_health_ok": str(status.get("status") or "").strip().lower() == "ok",
        "remote_worker_present": remote_worker is not None,
        "remote_worker": remote_worker,
    }


def main() -> int:
    args = build_parser().parse_args()
    log = RunLog(Path(args.log_dir))
    log.payload["input"] = vars(args)
    log.flush()

    boundary = wait_for_boundary(
        base_url=args.base_url,
        log=log,
        poll_seconds=int(args.poll_seconds),
        timeout_seconds=int(args.timeout_seconds),
        target_task_id=int(args.target_task_id or 0),
    )
    log.payload["boundary"] = boundary
    log.flush()
    if not boundary.get("ready"):
        log.event("boundary wait timed out; no restart executed")
        return 2

    build_result = run_command(
        ["docker", "compose", "-f", args.compose_file, "--env-file", args.env_file, "build", "category-worker"],
        cwd=ROOT,
        log=log,
        label="build category-worker",
    )
    log.payload["build_result"] = build_result
    log.flush()

    up_result = run_command(
        [
            "docker",
            "compose",
            "-f",
            args.compose_file,
            "--env-file",
            args.env_file,
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "category-worker",
        ],
        cwd=ROOT,
        log=log,
        label="roll category-worker",
    )
    log.payload["roll_result"] = up_result
    log.flush()

    verify = verify_restart(base_url=args.base_url, log=log)
    log.payload["verify"] = verify
    log.payload["finished_at"] = utcnow_iso()
    log.flush()
    log.event("rollout verification completed", verify=verify)

    ok = (
        int(build_result.get("returncode") or 1) == 0
        and int(up_result.get("returncode") or 1) == 0
        and bool(verify.get("system_health_ok"))
        and bool(verify.get("remote_worker_present"))
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
