from __future__ import annotations

import argparse
import atexit
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_REMOTE_PORT = int(os.getenv("NOON_REMOTE_ARTIFACT_SSH_PORT") or "22")
DEFAULT_REMOTE_USER = str(os.getenv("NOON_REMOTE_ARTIFACT_SSH_USER") or "").strip()
DEFAULT_REMOTE_HOST = str(os.getenv("NOON_REMOTE_ARTIFACT_SSH_HOST") or "").strip()
DEFAULT_IDENTITY_FILE = str(os.getenv("NOON_REMOTE_ARTIFACT_SSH_IDENTITY_FILE") or "").strip()
DEFAULT_REMOTE_BATCH_ROOT = str(
    os.getenv("NOON_REMOTE_CATEGORY_BATCH_ROOT")
    or "/volume1/docker/huihaokang-erp/shared/data/batch_scans"
).strip()
DEFAULT_SYNC_MODE = str(os.getenv("NOON_CATEGORY_ARTIFACT_SYNC_MODE") or "").strip().lower()


def artifact_sync_mode() -> str:
    return DEFAULT_SYNC_MODE or "disabled"


def artifact_sync_enabled() -> bool:
    return artifact_sync_mode() == "ssh_push"


def _ssh_target(user: str, host: str) -> str:
    return f"{user}@{host}" if user else host


def _ssh_base_command(*, port: int, identity_file: str = "", known_hosts_file: str = "") -> list[str]:
    command = ["ssh", "-p", str(int(port))]
    if identity_file.strip():
        command.extend(["-i", identity_file.strip()])
    command.extend(["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"])
    if known_hosts_file.strip():
        command.extend(["-o", f"UserKnownHostsFile={known_hosts_file.strip()}"])
    return command


def _scp_base_command(*, port: int, identity_file: str = "", known_hosts_file: str = "") -> list[str]:
    # UGREEN SSH exposes volume paths reliably through legacy SCP mode, not SFTP mode.
    command = ["scp", "-O", "-P", str(int(port))]
    if identity_file.strip():
        command.extend(["-i", identity_file.strip()])
    command.extend(["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"])
    if known_hosts_file.strip():
        command.extend(["-o", f"UserKnownHostsFile={known_hosts_file.strip()}"])
    return command


def _prepare_identity_file(identity_file: str) -> str:
    source = str(identity_file or "").strip()
    if not source:
        return ""
    source_path = Path(source)
    if not source_path.exists():
        return source
    try:
        current_mode = source_path.stat().st_mode & 0o777
    except OSError:
        return source
    if current_mode <= 0o600:
        return source

    temp_dir = Path(tempfile.mkdtemp(prefix="runtime-category-ssh-"))
    temp_identity = temp_dir / source_path.name
    shutil.copyfile(source_path, temp_identity)
    os.chmod(temp_identity, 0o600)
    atexit.register(shutil.rmtree, temp_dir, ignore_errors=True)
    return str(temp_identity)


def _prepare_known_hosts_file() -> str:
    temp_dir = Path(tempfile.mkdtemp(prefix="runtime-category-known-hosts-"))
    known_hosts = temp_dir / "known_hosts"
    known_hosts.touch(exist_ok=True)
    os.chmod(known_hosts, 0o600)
    atexit.register(shutil.rmtree, temp_dir, ignore_errors=True)
    return str(known_hosts)


def build_runtime_category_artifact_metadata(
    *,
    snapshot_id: str,
    runtime_map_path: Path,
    batch_dir: Path,
    source_node: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": str(snapshot_id).strip(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_map_name": runtime_map_path.name,
        "batch_dir": str(batch_dir),
        "source_node": str(source_node).strip() or "local",
    }


def push_runtime_category_artifacts(
    *,
    runtime_map_path: Path,
    snapshot_id: str,
    source_node: str,
    batch_dir: Path | None = None,
    remote_batch_root: str = "",
    remote_host: str = "",
    remote_user: str = "",
    remote_port: int | None = None,
    identity_file: str = "",
) -> dict[str, Any]:
    runtime_map = Path(runtime_map_path).resolve()
    if not runtime_map.exists():
        return {
            "status": "skipped",
            "reason": "runtime_map_missing",
            "runtime_map_path": str(runtime_map),
        }

    resolved_remote_host = str(remote_host or DEFAULT_REMOTE_HOST).strip()
    resolved_remote_user = str(remote_user or DEFAULT_REMOTE_USER).strip()
    resolved_remote_port = int(remote_port or DEFAULT_REMOTE_PORT)
    resolved_identity = _prepare_identity_file(str(identity_file or DEFAULT_IDENTITY_FILE).strip())
    resolved_known_hosts = _prepare_known_hosts_file()
    resolved_remote_root = str(remote_batch_root or DEFAULT_REMOTE_BATCH_ROOT).strip()
    if not resolved_remote_host or not resolved_remote_root:
        return {
            "status": "skipped",
            "reason": "artifact_sync_not_configured",
            "runtime_map_path": str(runtime_map),
        }

    resolved_batch_dir = Path(batch_dir or runtime_map.parent).resolve()
    remote_dir = f"{resolved_remote_root.rstrip('/')}/{str(snapshot_id).strip()}"
    metadata = build_runtime_category_artifact_metadata(
        snapshot_id=snapshot_id,
        runtime_map_path=runtime_map,
        batch_dir=resolved_batch_dir,
        source_node=source_node,
    )
    ssh_target = _ssh_target(resolved_remote_user, resolved_remote_host)
    mkdir_command = _ssh_base_command(
        port=resolved_remote_port,
        identity_file=resolved_identity,
        known_hosts_file=resolved_known_hosts,
    ) + [
        ssh_target,
        f"mkdir -p '{remote_dir}'",
    ]
    mkdir_proc = subprocess.run(
        mkdir_command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if mkdir_proc.returncode != 0:
        return {
            "status": "failed",
            "reason": "remote_mkdir_failed",
            "runtime_map_path": str(runtime_map),
            "remote_dir": remote_dir,
            "command": mkdir_command,
            "error": (mkdir_proc.stderr or mkdir_proc.stdout).strip(),
        }

    metadata_name = "runtime_category_map.meta.json"
    with tempfile.TemporaryDirectory() as temp_dir:
        metadata_path = Path(temp_dir) / metadata_name
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        transferred_files = []
        for local_path, remote_name in ((runtime_map, "runtime_category_map.json"), (metadata_path, metadata_name)):
            scp_command = _scp_base_command(
                port=resolved_remote_port,
                identity_file=resolved_identity,
                known_hosts_file=resolved_known_hosts,
            ) + [
                str(local_path),
                f"{ssh_target}:{remote_dir}/{remote_name}",
            ]
            proc = subprocess.run(
                scp_command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            if proc.returncode != 0:
                return {
                    "status": "failed",
                    "reason": "artifact_copy_failed",
                    "runtime_map_path": str(runtime_map),
                    "remote_dir": remote_dir,
                    "command": scp_command,
                    "error": (proc.stderr or proc.stdout).strip(),
                    "transferred_files": transferred_files,
                }
            transferred_files.append(remote_name)

    return {
        "status": "completed",
        "reason": "artifact_sync_completed",
        "runtime_map_path": str(runtime_map),
        "remote_dir": remote_dir,
        "transferred_files": transferred_files,
        "metadata": metadata,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push minimal runtime category artifacts to NAS shared storage over SSH/SCP.")
    parser.add_argument("--runtime-map", required=True, help="Path to runtime_category_map.json")
    parser.add_argument("--snapshot-id", required=True, help="Category batch snapshot id / output dir name")
    parser.add_argument("--source-node", default="", help="Source node label for metadata")
    parser.add_argument("--batch-dir", default="", help="Local batch directory for metadata")
    parser.add_argument("--remote-batch-root", default="", help="Remote NAS batch_scans root")
    parser.add_argument("--remote-host", default="", help="Remote SSH host")
    parser.add_argument("--remote-user", default="", help="Remote SSH user")
    parser.add_argument("--remote-port", type=int, default=0, help="Remote SSH port")
    parser.add_argument("--identity-file", default="", help="SSH identity file")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = push_runtime_category_artifacts(
        runtime_map_path=Path(args.runtime_map),
        snapshot_id=args.snapshot_id,
        source_node=str(args.source_node or "").strip() or (os.getenv("NOON_WORKER_NODE_HOST") or os.getenv("COMPUTERNAME") or "local"),
        batch_dir=Path(args.batch_dir) if str(args.batch_dir or "").strip() else None,
        remote_batch_root=args.remote_batch_root,
        remote_host=args.remote_host,
        remote_user=args.remote_user,
        remote_port=args.remote_port or None,
        identity_file=args.identity_file,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"completed", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
