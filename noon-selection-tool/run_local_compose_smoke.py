from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def required_files() -> list[Path]:
    return [
        ROOT / "Dockerfile",
        ROOT / "docker-compose.yml",
        ROOT / ".env.nas.example",
        ROOT / "run_task_scheduler.py",
        ROOT / "task_cli.py",
        ROOT / "web_beta" / "app.py",
    ]


def file_checks() -> dict:
    checks = {}
    for path in required_files():
        checks[str(path.relative_to(ROOT)).replace("\\", "/")] = path.exists()
    return checks


def docker_available() -> bool:
    return shutil.which("docker") is not None


@contextmanager
def materialize_compose_env(env_file: Path | None):
    target = ROOT / ".env.nas"
    if env_file is None:
        if not target.exists():
            raise FileNotFoundError("Compose smoke requires an env file. Pass --env-file or create .env.nas.")
        yield target
        return

    source = env_file.resolve()
    if not source.exists():
        raise FileNotFoundError(f"Env file not found: {source}")

    if source == target.resolve():
        yield target
        return

    backup = target.read_text(encoding="utf-8") if target.exists() else None
    shutil.copyfile(source, target)
    try:
        yield target
    finally:
        if backup is None:
            try:
                target.unlink()
            except FileNotFoundError:
                pass
        else:
            target.write_text(backup, encoding="utf-8")


def run_compose_config(env_file: Path | None) -> dict:
    command = ["docker", "compose"]
    with materialize_compose_env(env_file) as compose_env_file:
        env_values = {}
        for raw_line in compose_env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_values[key.strip()] = value.strip()
        command.extend(["--project-name", env_values.get("COMPOSE_PROJECT_NAME", "huihaokang-stable")])
        command.extend(["--env-file", str(compose_env_file)])
        if env_values.get("TUNNEL_TOKEN", "").strip():
            command.extend(["--profile", "tunnel"])
        command.extend(["-f", str(ROOT / "docker-compose.yml"), "config"])
        proc = subprocess.run(
            command,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
    return {
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout.splitlines()[-40:],
        "stderr_tail": proc.stderr.splitlines()[-40:],
        "command": command,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local Docker Compose smoke checks for NAS stable.")
    parser.add_argument("--env-file", default="", help="Optional env file. Falls back to .env.nas if present.")
    args = parser.parse_args()

    checks = file_checks()
    result = {
        "status": "unknown",
        "root": str(ROOT),
        "required_files": checks,
        "docker_available": docker_available(),
    }

    if not all(checks.values()):
        result["status"] = "failed"
        result["reason"] = "missing_required_files"
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    env_file = None
    if args.env_file:
        env_file = Path(args.env_file).resolve()
    else:
        candidate = ROOT / ".env.nas"
        if candidate.exists():
            env_file = candidate

    result["env_file"] = str(env_file) if env_file else str((ROOT / ".env.nas")) if (ROOT / ".env.nas").exists() else ""

    if not result["docker_available"]:
        result["status"] = "skipped"
        result["reason"] = "docker_not_installed"
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    try:
        config_result = run_compose_config(env_file)
    except FileNotFoundError as exc:
        result["status"] = "failed"
        result["reason"] = "env_file_missing"
        result["error"] = str(exc)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1
    result["compose_config"] = config_result
    if config_result["returncode"] != 0:
        result["status"] = "failed"
        result["reason"] = "compose_config_failed"
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    result["status"] = "completed"
    result["reason"] = "compose_config_ok"
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
