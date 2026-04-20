from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "dist" / "releases"

EXCLUDED_DIRS = {
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    ".playwright-cli",
    ".tmp",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    "data",
    "runtime_data",
    "browser_profiles",
    "logs",
    "dist",
    "test-results",
}

EXCLUDED_FILES = {
    ".env",
    ".env.nas",
    "explore_category_filter.py",
    "explore_category_filter2.py",
    "run_local_compose_smoke.py",
    "run_ops_postgres_smoke.py",
    "run_product_store_postgres_smoke.py",
    "run_warehouse_postgres_smoke.py",
}

EXCLUDED_SUFFIXES = {
    ".db",
    ".sqlite",
    ".sqlite3",
    ".log",
    ".tmp",
    ".pyc",
}


def build_version(explicit: str | None) -> str:
    if explicit:
        return explicit.strip()
    return datetime.now().strftime("huihaokang-nas-%Y%m%d-%H%M%S")


def should_exclude(path: Path) -> bool:
    relative_parts = path.relative_to(ROOT).parts
    if relative_parts and relative_parts[0] == "output":
        if len(relative_parts) > 1 and relative_parts[1] == "playwright":
            return True
        if path.suffix.lower() not in {".py"} and path.name != "__init__.py":
            return True
    if any(part in EXCLUDED_DIRS for part in relative_parts):
        return True
    if path.name in EXCLUDED_FILES:
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    return False


def collect_release_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if should_exclude(path):
            continue
        files.append(path)
    return sorted(files)


def build_manifest(version: str, files: list[Path], archive_name: str) -> dict:
    created_at = datetime.now().astimezone().isoformat()
    relative_files = [str(path.relative_to(ROOT)).replace("\\", "/") for path in files]
    return {
        "release_version": version,
        "created_at": created_at,
        "release_type": "nas-stable-manual",
        "archive_name": archive_name,
        "environment_model": {
            "local_beta": "development and validation only",
            "nas_stable": "retained-data production environment with in-place SQLite to Postgres cutover",
        },
        "included_file_count": len(relative_files),
        "included_files": relative_files,
        "required_docs": [
            "docs/PROJECT_WHITEPAPER.md",
            "docs/DEV_HANDOFF.md",
            "docs/DEV_COLLAB_LOG.md",
            "docs/NAS_DEPLOYMENT_RUNBOOK.md",
            "docs/NAS_RELEASE_ROLLBACK.md",
        ],
        "notes": [
            "This bundle excludes local data/runtime/browser profile directories.",
            "NAS stable keeps its existing data volumes and migrates NAS SQLite data to NAS Postgres in place.",
            "Do not copy local beta SQLite databases into NAS stable.",
            "SQLite files on NAS remain migration and rollback sources until cutover is accepted.",
        ],
    }


def write_release_bundle(output_dir: Path, version: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / f"{version}.zip"
    manifest_path = output_dir / f"{version}.manifest.json"

    if archive_path.exists():
        archive_path.unlink()
    if manifest_path.exists():
        manifest_path.unlink()

    files = collect_release_files()
    manifest = build_manifest(version, files, archive_path.name)

    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as bundle:
        for file_path in files:
            arcname = str(file_path.relative_to(ROOT)).replace("\\", "/")
            bundle.write(file_path, arcname)
        bundle.writestr("release-manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return archive_path, manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a NAS-stable manual release bundle.")
    parser.add_argument("--version", default="", help="Explicit release version tag.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT), help="Directory for release bundle output.")
    parser.add_argument("--clean-output", action="store_true", help="Delete output dir before building.")
    args = parser.parse_args()

    version = build_version(args.version or None)
    output_dir = Path(args.output_dir).resolve()
    if args.clean_output and output_dir.exists():
        shutil.rmtree(output_dir)

    archive_path, manifest_path = write_release_bundle(output_dir, version)
    result = {
        "status": "completed",
        "version": version,
        "archive": str(archive_path),
        "manifest": str(manifest_path),
        "output_dir": str(output_dir),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
