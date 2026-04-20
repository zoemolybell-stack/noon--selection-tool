from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

import paramiko


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_URL = "http://192.168.100.20:8865"
DEFAULT_NAS_HOST = "192.168.100.20"
DEFAULT_NAS_USER = "13799212678"
DEFAULT_NAS_KEY = ROOT / "ssh" / "id_ed25519"
DEFAULT_LOCAL_CATEGORY_CONTAINER = "huihaokang-remote-category-worker"
DEFAULT_REMOTE_KEYWORD_CONTAINER = "huihaokang-keyword-worker"
DEFAULT_TIMEOUT_SECONDS = 480
DEFAULT_POLL_SECONDS = 15
DEFAULT_HEARTBEAT_MAX_AGE_SECONDS = 180
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().isoformat()


def parse_dt(value: Any) -> datetime | None:
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


def heartbeat_age_seconds(worker: dict[str, Any]) -> int | None:
    heartbeat_at = parse_dt(worker.get("heartbeat_at"))
    if heartbeat_at is None:
        return None
    return max(0, int((utcnow() - heartbeat_at).total_seconds()))


class RunLog:
    def __init__(self, root: Path) -> None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = root / f"self_heal_check_{stamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.json_path = self.run_dir / "result.json"
        self.text_path = self.run_dir / "events.log"
        self.payload: dict[str, Any] = {
            "started_at": utcnow_iso(),
            "events": [],
        }

    def event(self, message: str, **details: Any) -> None:
        item = {
            "timestamp": utcnow_iso(),
            "message": message,
        }
        if details:
            item["details"] = details
        self.payload["events"].append(item)
        line = f"[{item['timestamp']}] {message}"
        if details:
            line += " | " + json.dumps(details, ensure_ascii=False, sort_keys=True)
        with self.text_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print(line, flush=True)
        self.flush()

    def flush(self) -> None:
        self.json_path.write_text(json.dumps(self.payload, ensure_ascii=False, indent=2), encoding="utf-8")


def http_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None, timeout: int = 12) -> dict[str, Any]:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib_request.Request(url, data=body, method=method, headers=headers)
    with urllib_request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8")
    loaded = json.loads(text or "{}")
    return loaded if isinstance(loaded, dict) else {}


def docker_local(command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return {
        "command": command,
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def local_container_running(container_name: str) -> dict[str, Any]:
    result = docker_local(["docker", "inspect", "--format", "{{.State.Running}}", container_name])
    return {
        "exists": result["returncode"] == 0,
        "running": result["returncode"] == 0 and str(result["stdout"]).strip().lower() == "true",
        "inspect": result,
    }


def kill_local_container(container_name: str) -> dict[str, Any]:
    return docker_local(["docker", "kill", container_name])


def run_local_ensure(log: RunLog) -> dict[str, Any]:
    manage_script = ROOT / "tools" / "manage_remote_category_runtime.ps1"
    proc = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-WindowStyle",
            "Hidden",
            "-File",
            str(manage_script),
            "-Action",
            "ensure",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        creationflags=CREATE_NO_WINDOW,
    )
    result = {
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout.splitlines()[-20:],
        "stderr_tail": proc.stderr.splitlines()[-20:],
    }
    log.event("local ensure executed", result=result)
    return result


class NasSsh:
    def __init__(self, host: str, username: str, key_path: Path) -> None:
        self.host = host
        self.username = username
        self.key_path = key_path
        self.client: paramiko.SSHClient | None = None

    def __enter__(self) -> "NasSsh":
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.Ed25519Key.from_private_key_file(str(self.key_path))
        client.connect(
            self.host,
            username=self.username,
            pkey=key,
            timeout=10,
            banner_timeout=10,
            auth_timeout=10,
        )
        self.client = client
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def exec(self, command: str) -> dict[str, Any]:
        if self.client is None:
            raise RuntimeError("SSH client not connected")
        stdin, stdout, stderr = self.client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        return {
            "command": command,
            "returncode": int(exit_code),
            "stdout": stdout.read().decode("utf-8", errors="replace").strip(),
            "stderr": stderr.read().decode("utf-8", errors="replace").strip(),
        }

    def remote_container_running(self, container_name: str) -> dict[str, Any]:
        result = self.exec(f"docker inspect --format '{{{{.State.Running}}}}' {container_name}")
        return {
            "exists": result["returncode"] == 0,
            "running": result["returncode"] == 0 and str(result["stdout"]).strip().lower() == "true",
            "inspect": result,
        }

    def remote_container_control_supported(self, container_name: str) -> dict[str, Any]:
        inspect_result = self.exec(f"docker inspect --format '{{{{.State.Running}}}}' {container_name}")
        stderr = str(inspect_result.get("stderr") or "").lower()
        stdout = str(inspect_result.get("stdout") or "").strip().lower()
        supported = inspect_result["returncode"] == 0
        if "permission denied" in stderr or "a password is required" in stderr:
            supported = False
        return {
            "supported": supported,
            "reason": "docker_access_denied" if not supported else "ok",
            "probe": inspect_result,
            "running": supported and stdout == "true",
        }

    def kill_remote_container(self, container_name: str) -> dict[str, Any]:
        return self.exec(f"docker kill {container_name}")

    def start_remote_container(self, container_name: str) -> dict[str, Any]:
        return self.exec(f"docker start {container_name}")


def find_worker(workers: list[dict[str, Any]], *, worker_type: str = "", node_role: str = "") -> dict[str, Any]:
    for item in workers:
        if worker_type and str(item.get("worker_type") or "").strip().lower() != worker_type:
            continue
        role = str(item.get("node_role") or ((item.get("details") or {}).get("node_role")) or "").strip().lower()
        if node_role and role != node_role:
            continue
        return item
    return {}


def summarize_worker(worker: dict[str, Any]) -> dict[str, Any]:
    if not worker:
        return {}
    details = worker.get("details") if isinstance(worker.get("details"), dict) else {}
    return {
        "worker_name": str(worker.get("worker_name") or ""),
        "worker_type": str(worker.get("worker_type") or ""),
        "status": str(worker.get("status") or ""),
        "current_task_id": int(worker.get("current_task_id") or worker.get("task_id") or 0),
        "heartbeat_at": str(worker.get("heartbeat_at") or ""),
        "heartbeat_age_seconds": heartbeat_age_seconds(worker),
        "node_role": str(worker.get("node_role") or details.get("node_role") or ""),
        "node_host": str(worker.get("node_host") or details.get("node_host") or ""),
    }


def fetch_snapshot(base_url: str, nas: NasSsh, local_category_container: str, remote_keyword_container: str) -> dict[str, Any]:
    workers_payload = http_json(f"{base_url.rstrip('/')}/api/workers?limit=50")
    tasks_payload = http_json(f"{base_url.rstrip('/')}/api/tasks?limit=50")
    workers = workers_payload.get("items") if isinstance(workers_payload.get("items"), list) else []
    tasks = tasks_payload.get("items") if isinstance(tasks_payload.get("items"), list) else []
    keyword_worker = find_worker(workers, worker_type="keyword")
    remote_category_worker = find_worker(workers, worker_type="category", node_role="remote_category")
    remote_keyword_probe = nas.remote_container_control_supported(remote_keyword_container)
    return {
        "timestamp": utcnow_iso(),
        "keyword_worker": summarize_worker(keyword_worker),
        "remote_category_worker": summarize_worker(remote_category_worker),
        "running_tasks": [
            {
                "id": int(item.get("id") or 0),
                "task_type": str(item.get("task_type") or ""),
                "worker_type": str(item.get("worker_type") or ""),
                "status": str(item.get("status") or ""),
                "display_name": str(item.get("display_name") or ""),
                "lease_owner": str(item.get("lease_owner") or ""),
            }
            for item in tasks
            if str(item.get("status") or "").strip().lower() in {"running", "leased", "pending", "retrying"}
        ],
        "local_category_container": local_container_running(local_category_container),
        "remote_keyword_container": {
            "supported": remote_keyword_probe["supported"],
            "reason": remote_keyword_probe["reason"],
            "running": remote_keyword_probe["running"],
            "inspect": remote_keyword_probe["probe"],
        },
    }


def worker_healthy(worker: dict[str, Any], max_age_seconds: int) -> bool:
    if not worker:
        return False
    age = worker.get("heartbeat_age_seconds")
    if age is None:
        return False
    return int(age) <= max_age_seconds


def recovered(snapshot: dict[str, Any], heartbeat_max_age_seconds: int) -> bool:
    remote_keyword = snapshot.get("remote_keyword_container", {})
    keyword_worker_ok = worker_healthy(snapshot.get("keyword_worker") or {}, heartbeat_max_age_seconds)
    remote_keyword_ok = keyword_worker_ok
    if remote_keyword.get("supported"):
        remote_keyword_ok = remote_keyword_ok and bool(remote_keyword.get("running"))
    return bool(
        snapshot.get("local_category_container", {}).get("running")
        and remote_keyword_ok
        and worker_healthy(snapshot.get("remote_category_worker") or {}, heartbeat_max_age_seconds)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify local category + NAS keyword crawler self-heal after simulated worker crashes.")
    parser.add_argument("--nas-base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--nas-host", default=DEFAULT_NAS_HOST)
    parser.add_argument("--nas-user", default=DEFAULT_NAS_USER)
    parser.add_argument("--nas-key", default=str(DEFAULT_NAS_KEY))
    parser.add_argument("--local-category-container", default=DEFAULT_LOCAL_CATEGORY_CONTAINER)
    parser.add_argument("--remote-keyword-container", default=DEFAULT_REMOTE_KEYWORD_CONTAINER)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--heartbeat-max-age-seconds", type=int, default=DEFAULT_HEARTBEAT_MAX_AGE_SECONDS)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    log = RunLog(ROOT / "logs" / "self_heal_checks")
    log.payload["input"] = vars(args)
    log.flush()

    try:
        with NasSsh(args.nas_host, args.nas_user, Path(args.nas_key)) as nas:
            before = fetch_snapshot(
                args.nas_base_url,
                nas,
                args.local_category_container,
                args.remote_keyword_container,
            )
            log.payload["before"] = before
            log.event("captured baseline snapshot", snapshot=before)

            if args.dry_run:
                log.payload["auto_recovered"] = None
                log.payload["finished_at"] = utcnow_iso()
                log.event("dry run completed")
                return 0

            local_kill = kill_local_container(args.local_category_container)
            keyword_control = before.get("remote_keyword_container", {})
            if keyword_control.get("supported"):
                remote_kill = nas.kill_remote_container(args.remote_keyword_container)
            else:
                remote_kill = {
                    "command": f"docker kill {args.remote_keyword_container}",
                    "returncode": None,
                    "stdout": "",
                    "stderr": str(keyword_control.get("inspect", {}).get("stderr") or ""),
                    "skipped": True,
                    "reason": str(keyword_control.get("reason") or "docker_access_denied"),
                }
            log.payload["local_kill"] = local_kill
            log.payload["remote_kill"] = remote_kill
            log.event("simulated crawler worker crash", local_kill=local_kill, remote_kill=remote_kill)

            deadline = time.time() + max(30, args.timeout_seconds)
            recovered_snapshot: dict[str, Any] | None = None
            while time.time() < deadline:
                time.sleep(max(5, args.poll_seconds))
                current = fetch_snapshot(
                    args.nas_base_url,
                    nas,
                    args.local_category_container,
                    args.remote_keyword_container,
                )
                log.event("polled recovery state", snapshot=current)
                if recovered(current, args.heartbeat_max_age_seconds):
                    recovered_snapshot = current
                    break

            log.payload["auto_recovered"] = recovered_snapshot is not None
            if recovered_snapshot is not None:
                log.payload["after"] = recovered_snapshot
                log.payload["finished_at"] = utcnow_iso()
                log.event("auto recovery verified", snapshot=recovered_snapshot)
                return 0

            log.event("auto recovery not observed before timeout; running safety remediation")
            local_ensure = run_local_ensure(log)
            if before.get("remote_keyword_container", {}).get("supported"):
                remote_start = nas.start_remote_container(args.remote_keyword_container)
            else:
                remote_start = {
                    "command": f"docker start {args.remote_keyword_container}",
                    "returncode": None,
                    "stdout": "",
                    "stderr": "",
                    "skipped": True,
                    "reason": "docker_access_denied",
                }
            final_snapshot = fetch_snapshot(
                args.nas_base_url,
                nas,
                args.local_category_container,
                args.remote_keyword_container,
            )
            log.payload["remediation"] = {
                "local_ensure": local_ensure,
                "remote_start": remote_start,
            }
            log.payload["after"] = final_snapshot
            log.payload["finished_at"] = utcnow_iso()
            log.event("safety remediation completed", snapshot=final_snapshot, remote_start=remote_start)
            return 1
    except Exception as exc:
        log.payload["finished_at"] = utcnow_iso()
        log.payload["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        log.event("self-heal verification failed", error=log.payload["error"])
        return 2
    finally:
        log.flush()


if __name__ == "__main__":
    raise SystemExit(main())
