from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from uuid import uuid4

from config.settings import Settings
from keyword_runtime_contract import result_file_path


def keyword_pool_path(settings: Settings) -> Path:
    return settings.data_dir / "processed" / "keywords.json"


def keyword_snapshot_path(settings: Settings) -> Path:
    return settings.snapshot_dir / "keywords.json"


def atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    try:
        temp_path.write_text(content, encoding=encoding)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)
    return path


def atomic_write_json(path: Path, payload: object) -> Path:
    return atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def atomic_write_lines(path: Path, lines: list[str]) -> Path:
    return atomic_write_text(path, "\n".join(lines) + "\n", encoding="utf-8")


def safe_load_json_file(
    path: Path,
    *,
    expected_type: type | tuple[type, ...] | None = None,
    required_keys: list[str] | tuple[str, ...] | None = None,
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

    if required_keys:
        if not isinstance(payload, dict):
            return None, f"incomplete_json:{path.name}:not_object"
        missing_keys = [key for key in required_keys if key not in payload]
        if missing_keys:
            return None, f"incomplete_json:{path.name}:missing={','.join(missing_keys)}"

    return payload, ""


def load_keyword_list_payload(path: Path) -> tuple[list[str], str]:
    payload, error = safe_load_json_file(path, expected_type=list)
    if not isinstance(payload, list):
        return [], error
    return sorted({str(item).strip().lower() for item in payload if str(item).strip()}), ""


def read_keyword_pool(settings: Settings) -> list[str]:
    keywords, _ = load_keyword_list_payload(keyword_pool_path(settings))
    return keywords


def write_keyword_pool(settings: Settings, keywords: list[str]) -> Path:
    normalized = sorted({str(keyword or "").strip().lower() for keyword in keywords if str(keyword or "").strip()})
    path = keyword_pool_path(settings)
    return atomic_write_json(path, normalized)


def collect_manual_keywords(args: argparse.Namespace) -> list[str]:
    keywords: list[str] = []
    if getattr(args, "keyword", None):
        keywords.extend(part.strip() for part in str(args.keyword).split(","))

    if getattr(args, "keywords_file", None):
        file_path = Path(args.keywords_file)
        if not file_path.exists():
            raise SystemExit(f"关键词文件不存在: {file_path}")
        if file_path.suffix.lower() == ".json":
            payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
            if isinstance(payload, list):
                keywords.extend(str(item).strip() for item in payload)
        else:
            for line in file_path.read_text(encoding="utf-8-sig").splitlines():
                clean = line.strip()
                if clean and not clean.startswith("#"):
                    keywords.append(clean)

    normalized = sorted({kw.strip().lower() for kw in keywords if kw and kw.strip()})
    if getattr(args, "limit", None) is not None:
        normalized = normalized[: args.limit]
    return normalized


def load_keywords_from_file(file_path: str | Path) -> list[str]:
    path = Path(file_path)
    if not path.exists():
        raise SystemExit(f"关键词文件不存在: {path}")

    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, list):
            return sorted({str(item).strip().lower() for item in payload if str(item).strip()})
        return []

    keywords = [
        line.strip().lower()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    return sorted(set(keywords))


def load_resume_keywords(settings: Settings) -> tuple[list[str], dict[str, object]]:
    candidates = [
        ("snapshot", keyword_snapshot_path(settings)),
        ("pool", keyword_pool_path(settings)),
    ]
    details: list[dict[str, str]] = []

    for source, path in candidates:
        keywords, error = load_keyword_list_payload(path)
        details.append(
            {
                "source": source,
                "path": str(path),
                "status": "completed" if keywords else "invalid",
                "error": error,
            }
        )
        if keywords:
            return keywords, {"source": source, "path": str(path), "candidates": details}

    return [], {"source": "generated", "path": "", "candidates": details}


def filter_resume_completed_keywords(
    settings: Settings,
    keywords: list[str],
    platforms: list[str],
) -> tuple[list[str], dict[str, object]]:
    remaining: list[str] = []
    completed: list[str] = []
    for keyword in keywords:
        has_all_platform_results = all(result_file_path(settings, platform, keyword).exists() for platform in platforms)
        if has_all_platform_results:
            completed.append(keyword)
        else:
            remaining.append(keyword)
    return remaining, {
        "completed_keyword_count": len(completed),
        "completed_keywords_preview": completed[:20],
        "remaining_keyword_count": len(remaining),
    }
