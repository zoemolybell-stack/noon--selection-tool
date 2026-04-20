from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
RUNTIME_DATA_ROOT = Path(
    os.getenv("NOON_RUNTIME_DATA_ROOT")
    or os.getenv("NOON_HOST_RUNTIME_DATA_DIR")
    or ("/app/runtime_data" if Path("/app/runtime_data").exists() else str(ROOT / "runtime_data"))
)
KEYWORD_CONTROL_DIR = RUNTIME_DATA_ROOT / "crawler_control" / "keyword_controls"
KEYWORD_CONTROL_DIR.mkdir(parents=True, exist_ok=True)

KEYWORD_CONTROL_SCOPES = ("baseline", "generated", "tracked", "manual")
KEYWORD_CONTROL_MATCH_MODES = ("exact", "contains")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json_load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)
    return path


def _atomic_write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)
    return path


def normalize_keyword_token(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_keyword_list(values: list[object] | tuple[object, ...] | set[object] | None) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values or []:
        keyword = normalize_keyword_token(value)
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        items.append(keyword)
    return items


def normalize_blocked_sources(values: list[object] | tuple[object, ...] | set[object] | None) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values or []:
        token = normalize_keyword_token(value)
        if token not in KEYWORD_CONTROL_SCOPES or token in seen:
            continue
        seen.add(token)
        items.append(token)
    return items


def normalize_match_mode(value: object, default: str = "exact") -> str:
    token = normalize_keyword_token(value)
    if token in KEYWORD_CONTROL_MATCH_MODES:
        return token
    return default


def resolve_monitor_config_path(raw_path: str) -> Path:
    candidate = Path(str(raw_path or "").strip())
    if candidate.is_absolute():
        return candidate
    relative_to_config = (CONFIG_DIR / candidate).resolve()
    if relative_to_config.exists():
        return relative_to_config
    return (ROOT / candidate).resolve()


def load_monitor_config_payload(monitor_config: str) -> tuple[Path, dict[str, Any]]:
    config_path = (
        resolve_monitor_config_path(monitor_config)
        if str(monitor_config or "").strip()
        else (CONFIG_DIR / "keyword_monitor_defaults.json").resolve()
    )
    payload = _safe_json_load(config_path, {})
    if not isinstance(payload, dict):
        payload = {}
    return config_path, payload


def resolve_baseline_file_path(
    *,
    monitor_config: str,
    baseline_file_override: str = "",
) -> Path:
    config_path, payload = load_monitor_config_payload(monitor_config)
    baseline_file_raw = str(baseline_file_override or payload.get("baseline_file") or "").strip()
    if baseline_file_raw:
        baseline_path = Path(baseline_file_raw)
        if not baseline_path.is_absolute():
            baseline_path = (config_path.parent / baseline_path).resolve()
    else:
        baseline_path = (CONFIG_DIR / "keyword_baseline_seeds.txt").resolve()
    return baseline_path


def _load_keyword_list(path: Path) -> list[str]:
    if not path.exists():
        return []
    items: list[str] = []
    seen: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        keyword = normalize_keyword_token(raw_line)
        if not keyword or keyword.startswith("#") or keyword in seen:
            continue
        seen.add(keyword)
        items.append(keyword)
    return items


def _extract_keyword_token(raw_line: object) -> str:
    keyword = normalize_keyword_token(raw_line)
    if not keyword or keyword.startswith("#"):
        return ""
    return keyword


def _apply_legacy_baseline_overrides(source_keywords: list[str], state: Mapping[str, Any] | None) -> list[str]:
    ordered_keywords = normalize_keyword_list(source_keywords)
    seen = set(ordered_keywords)
    for keyword in normalize_keyword_list((state or {}).get("baseline_additions") or []):
        if keyword in seen:
            continue
        ordered_keywords.append(keyword)
        seen.add(keyword)
    removals = set(normalize_keyword_list((state or {}).get("baseline_removals") or []))
    if removals:
        ordered_keywords = [keyword for keyword in ordered_keywords if keyword not in removals]
    return ordered_keywords


def _rewrite_baseline_keyword_file(
    path: Path,
    *,
    add_keywords: list[object] | tuple[object, ...] | set[object] | None = None,
    remove_keywords: list[object] | tuple[object, ...] | set[object] | None = None,
) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    additions = normalize_keyword_list(add_keywords)
    removals = set(normalize_keyword_list(remove_keywords))

    rewritten_lines: list[str] = []
    seen_keywords: set[str] = set()
    for raw_line in lines:
        keyword = _extract_keyword_token(raw_line)
        if not keyword:
            rewritten_lines.append(str(raw_line).rstrip("\r\n"))
            continue
        if keyword in removals or keyword in seen_keywords:
            continue
        rewritten_lines.append(keyword)
        seen_keywords.add(keyword)

    appended_keywords = [keyword for keyword in additions if keyword not in removals and keyword not in seen_keywords]
    if appended_keywords:
        if rewritten_lines and str(rewritten_lines[-1]).strip():
            rewritten_lines.append("")
        rewritten_lines.extend(appended_keywords)

    content = "\n".join(rewritten_lines)
    if rewritten_lines:
        content += "\n"
    _atomic_write_text(path, content)
    return _load_keyword_list(path)


def _baseline_file_is_writable(path: Path) -> bool:
    target_dir = path if path.is_dir() else path.parent
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return os.access(path, os.W_OK)
        return os.access(target_dir, os.W_OK)
    except Exception:
        return False


def _state_file_path(monitor_config: str, baseline_file_override: str = "") -> Path:
    config_path = (
        resolve_monitor_config_path(monitor_config)
        if str(monitor_config or "").strip()
        else resolve_baseline_file_path(monitor_config="", baseline_file_override=baseline_file_override)
    )
    fingerprint = hashlib.sha1(str(config_path).lower().encode("utf-8")).hexdigest()[:10]
    stem = config_path.stem or "keyword-monitor"
    return KEYWORD_CONTROL_DIR / f"{stem}-{fingerprint}.json"


def _default_blocked_sources(values: list[object] | tuple[object, ...] | set[object] | None) -> list[str]:
    normalized = normalize_blocked_sources(values)
    return normalized or list(KEYWORD_CONTROL_SCOPES)


def _rule_id(keyword: str, match_mode: str) -> str:
    return f"{match_mode}:{keyword}"


def _normalize_rule_items(
    items: list[object] | tuple[object, ...] | set[object] | None,
    *,
    default_match_mode: str,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_item in items or []:
        if not isinstance(raw_item, Mapping):
            continue
        keyword = normalize_keyword_token(raw_item.get("keyword") or raw_item.get("root") or raw_item.get("value"))
        match_mode = normalize_match_mode(raw_item.get("match_mode"), default_match_mode)
        blocked_sources = _default_blocked_sources(raw_item.get("blocked_sources") or [])
        if not keyword or not blocked_sources:
            continue
        rule_key = (keyword, match_mode)
        existing = merged.get(rule_key)
        reason = str(raw_item.get("reason") or "").strip()
        updated_at = str(raw_item.get("updated_at") or "").strip() or _now_iso()
        if existing:
            existing["blocked_sources"] = normalize_blocked_sources(
                [*(existing.get("blocked_sources") or []), *blocked_sources]
            )
            if reason:
                existing["reason"] = reason
            existing["updated_at"] = updated_at or existing["updated_at"]
            continue
        merged[rule_key] = {
            "id": str(raw_item.get("id") or _rule_id(keyword, match_mode)).strip() or _rule_id(keyword, match_mode),
            "keyword": keyword,
            "match_mode": match_mode,
            "blocked_sources": blocked_sources,
            "reason": reason,
            "updated_at": updated_at,
        }
    return sorted(
        merged.values(),
        key=lambda item: (
            item.get("match_mode") != "exact",
            str(item.get("keyword") or ""),
        ),
    )


def _load_state(monitor_config: str, baseline_file_override: str = "") -> dict[str, Any]:
    path = _state_file_path(monitor_config, baseline_file_override)
    payload = _safe_json_load(path, {})
    if not isinstance(payload, dict):
        payload = {}
    disabled_keywords = _normalize_rule_items(payload.get("disabled_keywords") or [], default_match_mode="exact")
    legacy_exclusions = _normalize_rule_items(payload.get("exclusions") or [], default_match_mode="exact")
    if not disabled_keywords and legacy_exclusions:
        disabled_keywords = legacy_exclusions
    blocked_roots = _normalize_rule_items(payload.get("blocked_roots") or [], default_match_mode="contains")
    return {
        "version": int(payload.get("version") or 2),
        "monitor_config": str(payload.get("monitor_config") or monitor_config or "").strip(),
        "monitor_config_path": str(payload.get("monitor_config_path") or ""),
        "baseline_additions": normalize_keyword_list(payload.get("baseline_additions") or []),
        "baseline_removals": normalize_keyword_list(payload.get("baseline_removals") or []),
        "disabled_keywords": disabled_keywords,
        "blocked_roots": blocked_roots,
        "updated_at": str(payload.get("updated_at") or ""),
    }


def _save_state(monitor_config: str, state: Mapping[str, Any], baseline_file_override: str = "") -> Path:
    config_path = resolve_monitor_config_path(monitor_config) if str(monitor_config or "").strip() else None
    disabled_keywords = _normalize_rule_items(state.get("disabled_keywords") or [], default_match_mode="exact")
    blocked_roots = _normalize_rule_items(state.get("blocked_roots") or [], default_match_mode="contains")
    payload = {
        "version": 2,
        "monitor_config": str(monitor_config or "").strip(),
        "monitor_config_path": str(config_path) if config_path else "",
        "baseline_additions": normalize_keyword_list(state.get("baseline_additions") or []),
        "baseline_removals": normalize_keyword_list(state.get("baseline_removals") or []),
        "disabled_keywords": disabled_keywords,
        "blocked_roots": blocked_roots,
        "exclusions": disabled_keywords,
        "updated_at": _now_iso(),
    }
    return _atomic_write_json(_state_file_path(monitor_config, baseline_file_override), payload)


def get_effective_baseline_keywords(monitor_config: str, baseline_file_override: str = "") -> list[str]:
    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    state = _load_state(monitor_config, baseline_file_override)
    return _apply_legacy_baseline_overrides(_load_keyword_list(baseline_path), state)


def get_keyword_control_state(monitor_config: str, baseline_file_override: str = "") -> dict[str, Any]:
    config_path, config_payload = load_monitor_config_payload(monitor_config)
    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    source_keywords = _load_keyword_list(baseline_path)
    state = _load_state(monitor_config, baseline_file_override)
    baseline_additions = set(normalize_keyword_list(state.get("baseline_additions") or []))
    baseline_removals = set(normalize_keyword_list(state.get("baseline_removals") or []))
    effective_baseline = _apply_legacy_baseline_overrides(source_keywords, state)
    disabled_keywords = _merge_disabled_keyword_rules(state.get("disabled_keywords") or [], fallback_updated_at=str(state.get("updated_at") or ""))
    blocked_roots = _merge_blocked_root_rules(state.get("blocked_roots") or [], fallback_updated_at=str(state.get("updated_at") or ""))
    exclusions = _build_legacy_exclusions_view(disabled_keywords)

    baseline_items: list[dict[str, Any]] = []
    source_keyword_set = set(source_keywords)
    for keyword in effective_baseline:
        baseline_items.append(
            {
                "keyword": keyword,
                "source_scope": "baseline",
                "origin": "baseline_addition" if keyword in baseline_additions and keyword not in source_keyword_set else "baseline_file",
                "created_at": "",
            }
        )

    return {
        "monitor_config": str(monitor_config or "").strip() or config_path.name,
        "monitor_label": config_path.stem.replace("_", " "),
        "monitor_config_path": str(config_path),
        "baseline_file": str(baseline_path),
        "baseline_file_exists": baseline_path.exists(),
        "baseline_file_writable": _baseline_file_is_writable(baseline_path),
        "baseline_storage_mode": "baseline_file",
        "legacy_baseline_overlay_count": len(baseline_additions) + len(baseline_removals),
        "baseline_keywords": baseline_items,
        "baseline_file_keywords": source_keywords,
        "baseline_additions": sorted(baseline_additions),
        "baseline_removals": sorted(baseline_removals),
        "disabled_keywords": disabled_keywords,
        "disabled_keyword_count": len(disabled_keywords),
        "blocked_roots": blocked_roots,
        "blocked_root_count": len(blocked_roots),
        "exclusions": exclusions,
        "exclusion_rules": exclusions,
        "rules": exclusions,
        "available_sources": list(KEYWORD_CONTROL_SCOPES),
        "available_root_match_modes": list(KEYWORD_ROOT_MATCH_MODES),
        "expand_source_types": [
            normalize_keyword_token(item)
            for item in (config_payload.get("expand_source_types") or [])
            if normalize_keyword_token(item) in KEYWORD_CONTROL_SCOPES
        ],
        "updated_at": str(state.get("updated_at") or ""),
    }


def update_monitor_baseline_keywords(
    monitor_config: str,
    *,
    keywords: list[object],
    mode: str,
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keywords = normalize_keyword_list(keywords)
    if not normalized_keywords:
        raise ValueError("keywords is required")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in {"add", "remove"}:
        raise ValueError("mode must be add or remove")

    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    state = _load_state(monitor_config, baseline_file_override)
    effective_baseline = _apply_legacy_baseline_overrides(_load_keyword_list(baseline_path), state)
    if normalized_mode == "add":
        _rewrite_baseline_keyword_file(baseline_path, add_keywords=[*effective_baseline, *normalized_keywords])
    else:
        _rewrite_baseline_keyword_file(baseline_path, add_keywords=effective_baseline, remove_keywords=normalized_keywords)

    _save_state(
        monitor_config,
        {
            **state,
            "baseline_additions": [],
            "baseline_removals": [],
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def update_monitor_disabled_keyword_rules(
    monitor_config: str,
    *,
    keywords: list[object],
    blocked_sources: list[object] | None,
    reason: str = "",
    mode: str = "disable",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keywords = normalize_keyword_list(keywords)
    if not normalized_keywords:
        raise ValueError("keywords is required")
    normalized_mode = normalize_keyword_token(mode)
    if normalized_mode not in {"disable", "restore", "upsert", "delete"}:
        raise ValueError("mode must be disable, restore, upsert, or delete")

    is_upsert = normalized_mode in {"disable", "upsert"}
    normalized_sources = normalize_blocked_sources(blocked_sources or [])
    if is_upsert and not normalized_sources:
        raise ValueError("blocked_sources is required")

    state = _load_state(monitor_config, baseline_file_override)
    disabled_keywords = _merge_disabled_keyword_rules(
        state.get("disabled_keywords") or [],
        fallback_updated_at=str(state.get("updated_at") or ""),
    )
    rules_by_keyword = {item["keyword"]: dict(item) for item in disabled_keywords}
    ordered_keywords = [item["keyword"] for item in disabled_keywords]
    normalized_reason = str(reason or "").strip()

    for keyword in normalized_keywords:
        existing = rules_by_keyword.get(keyword)
        if is_upsert:
            if existing is None:
                rules_by_keyword[keyword] = {
                    "id": keyword,
                    "keyword": keyword,
                    "blocked_sources": normalized_sources,
                    "reason": normalized_reason,
                    "updated_at": _now_iso(),
                    "match_mode": "exact",
                }
                ordered_keywords.append(keyword)
                continue
            existing["blocked_sources"] = normalize_blocked_sources(
                [*(existing.get("blocked_sources") or []), *normalized_sources]
            )
            if normalized_reason:
                existing["reason"] = normalized_reason
            existing["updated_at"] = _now_iso()
            existing["match_mode"] = "exact"
            continue

        if existing is None:
            continue
        if normalized_sources:
            next_sources = [
                item
                for item in normalize_blocked_sources(existing.get("blocked_sources") or [])
                if item not in normalized_sources
            ]
            if next_sources:
                existing["blocked_sources"] = next_sources
                existing["updated_at"] = _now_iso()
            else:
                rules_by_keyword.pop(keyword, None)
        else:
            rules_by_keyword.pop(keyword, None)

    next_rules = [rules_by_keyword[keyword] for keyword in ordered_keywords if keyword in rules_by_keyword]
    _save_state(
        monitor_config,
        {
            **state,
            "disabled_keywords": next_rules,
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def update_monitor_blocked_root_rules(
    monitor_config: str,
    *,
    root_keywords: list[object],
    blocked_sources: list[object] | None,
    reason: str = "",
    match_mode: str = "exact",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_roots = normalize_keyword_list(root_keywords)
    if not normalized_roots:
        raise ValueError("root_keywords is required")
    normalized_mode = normalize_keyword_token(mode)
    if normalized_mode not in {"upsert", "delete", "add", "remove"}:
        raise ValueError("mode must be upsert, delete, add, or remove")
    normalized_match_mode = normalize_root_match_mode(match_mode, default="")
    if not normalized_match_mode:
        raise ValueError("match_mode must be exact or contains")

    is_upsert = normalized_mode in {"upsert", "add"}
    normalized_sources = normalize_blocked_sources(blocked_sources or [])
    if is_upsert and not normalized_sources:
        raise ValueError("blocked_sources is required")

    state = _load_state(monitor_config, baseline_file_override)
    blocked_roots = _merge_blocked_root_rules(
        state.get("blocked_roots") or [],
        fallback_updated_at=str(state.get("updated_at") or ""),
    )
    rules_by_key = {
        (item["match_mode"], item["root_keyword"]): dict(item)
        for item in blocked_roots
    }
    ordered_keys = [(item["match_mode"], item["root_keyword"]) for item in blocked_roots]
    normalized_reason = str(reason or "").strip()

    for root_keyword in normalized_roots:
        key = (normalized_match_mode, root_keyword)
        existing = rules_by_key.get(key)
        if is_upsert:
            if existing is None:
                rules_by_key[key] = {
                    "id": f"{normalized_match_mode}:{root_keyword}",
                    "root_keyword": root_keyword,
                    "match_mode": normalized_match_mode,
                    "blocked_sources": normalized_sources,
                    "reason": normalized_reason,
                    "updated_at": _now_iso(),
                }
                ordered_keys.append(key)
                continue
            existing["blocked_sources"] = normalize_blocked_sources(
                [*(existing.get("blocked_sources") or []), *normalized_sources]
            )
            if normalized_reason:
                existing["reason"] = normalized_reason
            existing["updated_at"] = _now_iso()
            continue

        if existing is None:
            continue
        if normalized_sources:
            next_sources = [
                item
                for item in normalize_blocked_sources(existing.get("blocked_sources") or [])
                if item not in normalized_sources
            ]
            if next_sources:
                existing["blocked_sources"] = next_sources
                existing["updated_at"] = _now_iso()
            else:
                rules_by_key.pop(key, None)
        else:
            rules_by_key.pop(key, None)

    next_rules = [rules_by_key[key] for key in ordered_keys if key in rules_by_key]
    _save_state(
        monitor_config,
        {
            **state,
            "blocked_roots": next_rules,
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def update_monitor_exclusion_rule(
    monitor_config: str,
    *,
    keyword: str,
    blocked_sources: list[object] | None,
    reason: str = "",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keyword = normalize_keyword_token(keyword)
    if not normalized_keyword:
        raise ValueError("keyword is required")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in {"upsert", "delete"}:
        raise ValueError("mode must be upsert or delete")
    return update_monitor_disabled_keyword_rules(
        monitor_config,
        keywords=[normalized_keyword],
        blocked_sources=blocked_sources,
        reason=reason,
        mode="disable" if normalized_mode == "upsert" else "restore",
        baseline_file_override=baseline_file_override,
    )


def build_exclusion_lookup(state: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    disabled_keywords = _merge_disabled_keyword_rules(
        (state or {}).get("disabled_keywords") or [],
        (state or {}).get("exclusions") or [],
        fallback_updated_at=str((state or {}).get("updated_at") or ""),
    )
    for item in disabled_keywords:
        keyword = normalize_keyword_token(item.get("keyword"))
        blocked_sources = normalize_blocked_sources(item.get("blocked_sources") or [])
        if not keyword or not blocked_sources:
            continue
        lookup[keyword] = {
            "id": str(item.get("id") or keyword).strip() or keyword,
            "keyword": keyword,
            "blocked_sources": blocked_sources,
            "reason": str(item.get("reason") or "").strip(),
            "updated_at": str(item.get("updated_at") or "").strip(),
            "match_mode": "exact",
        }
    return lookup


def build_keyword_control_lookup(state: Mapping[str, Any] | None) -> dict[str, Any]:
    disabled_keywords = build_exclusion_lookup(state)
    blocked_root_rules = _merge_blocked_root_rules(
        (state or {}).get("blocked_roots") or [],
        fallback_updated_at=str((state or {}).get("updated_at") or ""),
    )
    exact_roots: list[dict[str, Any]] = []
    contains_roots: list[dict[str, Any]] = []
    for item in blocked_root_rules:
        if item.get("match_mode") == "contains":
            contains_roots.append(item)
        else:
            exact_roots.append(item)
    return {
        "disabled_keywords": disabled_keywords,
        "blocked_roots": {
            "exact": exact_roots,
            "contains": contains_roots,
        },
    }


def _row_metadata(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        return {}
    metadata = row.get("metadata")
    if isinstance(metadata, Mapping):
        return dict(metadata)
    metadata_json = row.get("metadata_json")
    if isinstance(metadata_json, str) and metadata_json.strip():
        try:
            parsed = json.loads(metadata_json)
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def _extract_root_candidates(row: Mapping[str, Any] | None = None, keyword: object = "") -> list[str]:
    metadata = _row_metadata(row)
    return normalize_keyword_list(
        [
            metadata.get("root_seed_keyword"),
            metadata.get("seed_keyword"),
            (row or {}).get("display_keyword") if isinstance(row, Mapping) else "",
            (row or {}).get("keyword") if isinstance(row, Mapping) else "",
            keyword,
        ]
    )


def classify_keyword_source_scopes(row: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(row, Mapping):
        return []
    metadata = _row_metadata(row)
    seen: set[str] = set()
    items: list[str] = []
    for candidate in (
        row.get("source_type"),
        row.get("tracking_mode"),
        metadata.get("registration_source"),
        metadata.get("parent_source_type"),
        metadata.get("source_scope"),
        metadata.get("tracking_mode"),
    ):
        token = normalize_keyword_token(candidate)
        if token in KEYWORD_CONTROL_SCOPES and token not in seen:
            seen.add(token)
            items.append(token)
    return items


def _resolve_keyword_control_lookup(control_lookup: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(control_lookup, Mapping):
        return {}, {"exact": [], "contains": []}
    if "disabled_keywords" in control_lookup or "blocked_roots" in control_lookup:
        disabled_keywords = control_lookup.get("disabled_keywords")
        blocked_roots = control_lookup.get("blocked_roots")
        if not isinstance(disabled_keywords, Mapping):
            disabled_keywords = {}
        if not isinstance(blocked_roots, Mapping):
            blocked_roots = {"exact": [], "contains": []}
        return disabled_keywords, blocked_roots
    return control_lookup, {"exact": [], "contains": []}


def match_keyword_control_rule(
    keyword: str,
    source_scopes: list[object],
    control_lookup: Mapping[str, Any] | None,
    *,
    root_keywords: list[object] | tuple[object, ...] | set[object] | None = None,
) -> dict[str, Any] | None:
    normalized_keyword = normalize_keyword_token(keyword)
    if not normalized_keyword or not control_lookup:
        return None
    active_sources = set(normalize_blocked_sources(source_scopes or []))
    if not active_sources:
        return None

    disabled_lookup, blocked_root_lookup = _resolve_keyword_control_lookup(control_lookup)
    exact_rule = disabled_lookup.get(normalized_keyword) if isinstance(disabled_lookup, Mapping) else None
    if isinstance(exact_rule, Mapping):
        blocked_sources = set(normalize_blocked_sources(exact_rule.get("blocked_sources") or []))
        matched_sources = sorted(blocked_sources.intersection(active_sources))
        if matched_sources:
            return {
                "rule_type": "disabled_keyword",
                "keyword": normalized_keyword,
                "matched_value": normalized_keyword,
                "blocked_sources": matched_sources,
                "reason": str(exact_rule.get("reason") or "").strip(),
                "updated_at": str(exact_rule.get("updated_at") or "").strip(),
                "match_mode": "exact",
            }

    normalized_roots = normalize_keyword_list(root_keywords or [])
    if not normalized_roots:
        normalized_roots = [normalized_keyword]

    exact_roots = blocked_root_lookup.get("exact") if isinstance(blocked_root_lookup, Mapping) else []
    for rule in exact_roots or []:
        if not isinstance(rule, Mapping):
            continue
        blocked_sources = set(normalize_blocked_sources(rule.get("blocked_sources") or []))
        matched_sources = sorted(blocked_sources.intersection(active_sources))
        if not matched_sources:
            continue
        root_keyword = normalize_keyword_token(rule.get("root_keyword"))
        matched_root = next((item for item in normalized_roots if item == root_keyword), "")
        if matched_root:
            return {
                "rule_type": "blocked_root",
                "keyword": normalized_keyword,
                "root_keyword": root_keyword,
                "matched_value": matched_root,
                "blocked_sources": matched_sources,
                "reason": str(rule.get("reason") or "").strip(),
                "updated_at": str(rule.get("updated_at") or "").strip(),
                "match_mode": "exact",
            }

    contains_roots = blocked_root_lookup.get("contains") if isinstance(blocked_root_lookup, Mapping) else []
    for rule in contains_roots or []:
        if not isinstance(rule, Mapping):
            continue
        blocked_sources = set(normalize_blocked_sources(rule.get("blocked_sources") or []))
        matched_sources = sorted(blocked_sources.intersection(active_sources))
        if not matched_sources:
            continue
        root_keyword = normalize_keyword_token(rule.get("root_keyword"))
        matched_root = next((item for item in normalized_roots if root_keyword and root_keyword in item), "")
        if matched_root:
            return {
                "rule_type": "blocked_root",
                "keyword": normalized_keyword,
                "root_keyword": root_keyword,
                "matched_value": matched_root,
                "blocked_sources": matched_sources,
                "reason": str(rule.get("reason") or "").strip(),
                "updated_at": str(rule.get("updated_at") or "").strip(),
                "match_mode": "contains",
            }

    return None


def match_exclusion_rule(keyword: str, source_scopes: list[object], exclusion_lookup: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return match_keyword_control_rule(keyword, source_scopes, exclusion_lookup, root_keywords=[keyword])


def _coerce_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_active_keyword_item(
    *,
    stage: str,
    keyword: str,
    row: Mapping[str, Any] | None = None,
    source_scopes: list[str] | None = None,
) -> dict[str, Any]:
    normalized_keyword = normalize_keyword_token(keyword)
    row_payload = dict(row or {})
    root_candidates = _extract_root_candidates(row if isinstance(row, Mapping) else None, normalized_keyword)
    return {
        "keyword": normalized_keyword,
        "display_keyword": str((row_payload.get("display_keyword") or row_payload.get("keyword") or normalized_keyword)).strip(),
        "root_keyword": root_candidates[0] if root_candidates else normalized_keyword,
        "source_type": str(row_payload.get("source_type") or stage).strip().lower(),
        "tracking_mode": str(row_payload.get("tracking_mode") or "").strip().lower(),
        "source_scopes": normalize_blocked_sources(source_scopes or []),
        "priority": int(row_payload.get("priority") or 0),
        "last_seen": str(row_payload.get("last_seen") or "").strip(),
        "last_crawled_at": str(row_payload.get("last_crawled_at") or "").strip(),
        "last_expanded_at": str(row_payload.get("last_expanded_at") or "").strip(),
        "last_analyzed_at": str(row_payload.get("last_analyzed_at") or "").strip(),
        "stages": [stage],
    }


def _merge_active_keyword_items(stage_items: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for stage in ("baseline", "expand", "crawl"):
        for item in stage_items.get(stage) or []:
            keyword = normalize_keyword_token(item.get("keyword"))
            if not keyword:
                continue
            existing = merged.get(keyword)
            if existing is None:
                merged[keyword] = dict(item)
                order.append(keyword)
                continue
            existing["stages"] = sorted({*(existing.get("stages") or []), *(item.get("stages") or [])})
            existing["source_scopes"] = normalize_blocked_sources(
                [*(existing.get("source_scopes") or []), *(item.get("source_scopes") or [])]
            )
            for field in ("display_keyword", "root_keyword", "source_type", "tracking_mode"):
                if not existing.get(field) and item.get(field):
                    existing[field] = item[field]
            if int(existing.get("priority") or 0) <= 0 and int(item.get("priority") or 0) > 0:
                existing["priority"] = int(item.get("priority") or 0)
            for field in ("last_seen", "last_crawled_at", "last_expanded_at", "last_analyzed_at"):
                if not existing.get(field) and item.get(field):
                    existing[field] = item[field]
    return [merged[keyword] for keyword in order if keyword in merged]


def get_monitor_active_keywords(monitor_config: str, baseline_file_override: str = "") -> dict[str, Any]:
    control_state = get_keyword_control_state(monitor_config, baseline_file_override)
    control_lookup = build_keyword_control_lookup(control_state)
    config_path, config_payload = load_monitor_config_payload(monitor_config)

    stage_items: dict[str, list[dict[str, Any]]] = {
        "baseline": [],
        "expand": [],
        "crawl": [],
    }
    blocked_stage_items: dict[str, list[dict[str, Any]]] = {
        "baseline": [],
        "expand": [],
        "crawl": [],
    }

    for keyword in get_effective_baseline_keywords(monitor_config, baseline_file_override):
        rule = match_keyword_control_rule(keyword, ["baseline"], control_lookup, root_keywords=[keyword])
        if rule:
            blocked_stage_items["baseline"].append(
                {
                    "keyword": keyword,
                    "display_keyword": keyword,
                    "root_keyword": keyword,
                    "source_scopes": ["baseline"],
                    "stages": ["baseline"],
                    "rule": rule,
                }
            )
            continue
        stage_items["baseline"].append(
            _build_active_keyword_item(
                stage="baseline",
                keyword=keyword,
                source_scopes=["baseline"],
            )
        )

    from config.product_store import ProductStore
    from config.settings import Settings

    settings = Settings()
    store = ProductStore(settings.product_store_db_ref)
    try:
        expand_limit = _coerce_optional_int(config_payload.get("expand_limit"))
        if expand_limit != 0:
            expand_rows = store.get_keywords_for_expand(
                tracking_mode="tracked",
                limit=None,
                stale_hours=_coerce_optional_int(config_payload.get("expand_stale_hours")),
                include_source_types=config_payload.get("expand_source_types"),
            )
            for row in expand_rows:
                keyword = str(row.get("display_keyword") or row.get("keyword") or "").strip()
                source_scopes = classify_keyword_source_scopes(row)
                root_keywords = _extract_root_candidates(row, keyword)
                rule = match_keyword_control_rule(keyword, source_scopes, control_lookup, root_keywords=root_keywords)
                if rule:
                    blocked_stage_items["expand"].append(
                        {
                            "keyword": normalize_keyword_token(keyword),
                            "display_keyword": str(keyword).strip(),
                            "root_keyword": root_keywords[0] if root_keywords else normalize_keyword_token(keyword),
                            "source_scopes": source_scopes,
                            "stages": ["expand"],
                            "rule": rule,
                        }
                    )
                    continue
                stage_items["expand"].append(
                    _build_active_keyword_item(
                        stage="expand",
                        keyword=keyword,
                        row=row,
                        source_scopes=source_scopes,
                    )
                )

        crawl_limit = _coerce_optional_int(config_payload.get("crawl_limit"))
        if crawl_limit != 0:
            crawl_rows = store.get_keywords_for_crawl_rows(
                tracking_mode="tracked",
                limit=None,
                stale_hours=_coerce_optional_int(config_payload.get("crawl_stale_hours")),
            )
            for row in crawl_rows:
                keyword = str(row.get("display_keyword") or row.get("keyword") or "").strip()
                source_scopes = classify_keyword_source_scopes(row)
                root_keywords = _extract_root_candidates(row, keyword)
                rule = match_keyword_control_rule(keyword, source_scopes, control_lookup, root_keywords=root_keywords)
                if rule:
                    blocked_stage_items["crawl"].append(
                        {
                            "keyword": normalize_keyword_token(keyword),
                            "display_keyword": str(keyword).strip(),
                            "root_keyword": root_keywords[0] if root_keywords else normalize_keyword_token(keyword),
                            "source_scopes": source_scopes,
                            "stages": ["crawl"],
                            "rule": rule,
                        }
                    )
                    continue
                stage_items["crawl"].append(
                    _build_active_keyword_item(
                        stage="crawl",
                        keyword=keyword,
                        row=row,
                        source_scopes=source_scopes,
                    )
                )
    finally:
        store.close()

    items = _merge_active_keyword_items(stage_items)
    return {
        "monitor_config": control_state.get("monitor_config") or str(monitor_config or "").strip() or config_path.name,
        "monitor_label": control_state.get("monitor_label") or config_path.stem.replace("_", " "),
        "monitor_config_path": control_state.get("monitor_config_path") or str(config_path),
        "baseline_file": control_state.get("baseline_file") or "",
        "updated_at": control_state.get("updated_at") or "",
        "summary": {
            "baseline_count": len(stage_items["baseline"]),
            "expand_count": len(stage_items["expand"]),
            "crawl_count": len(stage_items["crawl"]),
            "blocked_baseline_count": len(blocked_stage_items["baseline"]),
            "blocked_expand_count": len(blocked_stage_items["expand"]),
            "blocked_crawl_count": len(blocked_stage_items["crawl"]),
            "keyword_count": len(items),
        },
        "active_keywords": stage_items,
        "blocked_keywords": blocked_stage_items,
        "baseline_keywords": stage_items["baseline"],
        "expand_keywords": stage_items["expand"],
        "crawl_keywords": stage_items["crawl"],
        "items": items,
    }


def get_effective_baseline_keywords(monitor_config: str, baseline_file_override: str = "") -> list[str]:
    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    state = _load_state(monitor_config, baseline_file_override)
    return _apply_legacy_baseline_overrides(_load_keyword_list(baseline_path), state)


def get_keyword_control_state(monitor_config: str, baseline_file_override: str = "") -> dict[str, Any]:
    config_path, config_payload = load_monitor_config_payload(monitor_config)
    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    source_keywords = _load_keyword_list(baseline_path)
    state = _load_state(monitor_config, baseline_file_override)
    baseline_additions = set(normalize_keyword_list(state.get("baseline_additions") or []))
    baseline_removals = set(normalize_keyword_list(state.get("baseline_removals") or []))
    effective_baseline = _apply_legacy_baseline_overrides(source_keywords, state)

    baseline_items: list[dict[str, Any]] = []
    for keyword in effective_baseline:
        baseline_items.append(
            {
                "keyword": keyword,
                "source_scope": "baseline",
                "origin": "baseline_addition" if keyword in baseline_additions and keyword not in source_keywords else "baseline_file",
                "created_at": "",
            }
        )

    disabled_keywords = list(state.get("disabled_keywords") or [])
    blocked_roots = list(state.get("blocked_roots") or [])
    return {
        "monitor_config": str(monitor_config or "").strip() or config_path.name,
        "monitor_label": config_path.stem.replace("_", " "),
        "monitor_config_path": str(config_path),
        "baseline_file": str(baseline_path),
        "baseline_file_exists": baseline_path.exists(),
        "baseline_file_writable": _baseline_file_is_writable(baseline_path),
        "baseline_storage_mode": "baseline_file",
        "legacy_baseline_overlay_count": len(baseline_additions) + len(baseline_removals),
        "baseline_keywords": baseline_items,
        "baseline_file_keywords": source_keywords,
        "baseline_additions": sorted(baseline_additions),
        "baseline_removals": sorted(baseline_removals),
        "disabled_keywords": disabled_keywords,
        "blocked_roots": blocked_roots,
        "exclusions": disabled_keywords,
        "available_sources": list(KEYWORD_CONTROL_SCOPES),
        "available_match_modes": list(KEYWORD_CONTROL_MATCH_MODES),
        "expand_source_types": [
            normalize_keyword_token(item)
            for item in (config_payload.get("expand_source_types") or [])
            if normalize_keyword_token(item) in KEYWORD_CONTROL_SCOPES
        ],
        "updated_at": str(state.get("updated_at") or ""),
    }


def update_monitor_baseline_keywords(
    monitor_config: str,
    *,
    keywords: list[object],
    mode: str,
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keywords = normalize_keyword_list(keywords)
    if not normalized_keywords:
        raise ValueError("keywords is required")
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in {"add", "remove"}:
        raise ValueError("mode must be add or remove")

    baseline_path = resolve_baseline_file_path(monitor_config=monitor_config, baseline_file_override=baseline_file_override)
    state = _load_state(monitor_config, baseline_file_override)
    effective_baseline = _apply_legacy_baseline_overrides(_load_keyword_list(baseline_path), state)
    if normalized_mode == "add":
        _rewrite_baseline_keyword_file(baseline_path, add_keywords=[*effective_baseline, *normalized_keywords])
    else:
        _rewrite_baseline_keyword_file(baseline_path, add_keywords=effective_baseline, remove_keywords=normalized_keywords)

    _save_state(
        monitor_config,
        {
            **state,
            "baseline_additions": [],
            "baseline_removals": [],
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def _update_rule_collection(
    rules: list[dict[str, Any]],
    *,
    keywords: list[str],
    blocked_sources: list[str],
    reason: str,
    match_mode: str,
    mode: str,
) -> list[dict[str, Any]]:
    updated_rules = [dict(item) for item in rules]
    normalized_reason = str(reason or "").strip()
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in {"upsert", "delete"}:
        raise ValueError("mode must be upsert or delete")
    normalized_sources = _default_blocked_sources(blocked_sources)
    for keyword in keywords:
        existing = next(
            (
                item
                for item in updated_rules
                if normalize_keyword_token(item.get("keyword")) == keyword
                and normalize_match_mode(item.get("match_mode"), match_mode) == match_mode
            ),
            None,
        )
        if normalized_mode == "upsert":
            if existing:
                existing["blocked_sources"] = normalize_blocked_sources(
                    [*(existing.get("blocked_sources") or []), *normalized_sources]
                )
                if normalized_reason:
                    existing["reason"] = normalized_reason
                existing["updated_at"] = _now_iso()
            else:
                updated_rules.append(
                    {
                        "id": _rule_id(keyword, match_mode),
                        "keyword": keyword,
                        "match_mode": match_mode,
                        "blocked_sources": normalized_sources,
                        "reason": normalized_reason,
                        "updated_at": _now_iso(),
                    }
                )
            continue

        if not existing:
            continue
        if blocked_sources:
            remaining_sources = [
                item
                for item in normalize_blocked_sources(existing.get("blocked_sources") or [])
                if item not in normalized_sources
            ]
            if remaining_sources:
                existing["blocked_sources"] = remaining_sources
                existing["updated_at"] = _now_iso()
            else:
                updated_rules = [
                    item
                    for item in updated_rules
                    if not (
                        normalize_keyword_token(item.get("keyword")) == keyword
                        and normalize_match_mode(item.get("match_mode"), match_mode) == match_mode
                    )
                ]
        else:
            updated_rules = [
                item
                for item in updated_rules
                if not (
                    normalize_keyword_token(item.get("keyword")) == keyword
                    and normalize_match_mode(item.get("match_mode"), match_mode) == match_mode
                )
            ]
    return updated_rules


def update_monitor_disabled_keywords(
    monitor_config: str,
    *,
    keywords: list[object],
    blocked_sources: list[object] | None = None,
    reason: str = "",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keywords = normalize_keyword_list(keywords)
    if not normalized_keywords:
        raise ValueError("keywords is required")
    state = _load_state(monitor_config, baseline_file_override)
    disabled_keywords = _update_rule_collection(
        list(state.get("disabled_keywords") or []),
        keywords=normalized_keywords,
        blocked_sources=_default_blocked_sources(blocked_sources),
        reason=reason,
        match_mode="exact",
        mode=mode,
    )
    _save_state(
        monitor_config,
        {
            **state,
            "disabled_keywords": disabled_keywords,
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def update_monitor_blocked_roots(
    monitor_config: str,
    *,
    keywords: list[object],
    blocked_sources: list[object] | None = None,
    reason: str = "",
    match_mode: str = "contains",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keywords = normalize_keyword_list(keywords)
    if not normalized_keywords:
        raise ValueError("keywords is required")
    normalized_match_mode = normalize_match_mode(match_mode, "contains")
    state = _load_state(monitor_config, baseline_file_override)
    blocked_roots = _update_rule_collection(
        list(state.get("blocked_roots") or []),
        keywords=normalized_keywords,
        blocked_sources=_default_blocked_sources(blocked_sources),
        reason=reason,
        match_mode=normalized_match_mode,
        mode=mode,
    )
    _save_state(
        monitor_config,
        {
            **state,
            "blocked_roots": blocked_roots,
        },
        baseline_file_override,
    )
    return get_keyword_control_state(monitor_config, baseline_file_override)


def update_monitor_exclusion_rule(
    monitor_config: str,
    *,
    keyword: str,
    blocked_sources: list[object] | None,
    reason: str = "",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_keyword = normalize_keyword_token(keyword)
    if not normalized_keyword:
        raise ValueError("keyword is required")
    return update_monitor_disabled_keywords(
        monitor_config,
        keywords=[normalized_keyword],
        blocked_sources=blocked_sources,
        reason=reason,
        mode=mode,
        baseline_file_override=baseline_file_override,
    )


def build_exclusion_lookup(state: Mapping[str, Any] | None) -> dict[str, Any]:
    lookup: dict[str, Any] = {
        "disabled_keywords": {},
        "blocked_roots_exact": {},
        "blocked_roots_contains": [],
    }
    for item in (state or {}).get("disabled_keywords") or (state or {}).get("exclusions") or []:
        keyword = normalize_keyword_token(item.get("keyword"))
        blocked_sources = _default_blocked_sources(item.get("blocked_sources") or [])
        if not keyword or not blocked_sources:
            continue
        lookup["disabled_keywords"][keyword] = {
            "id": str(item.get("id") or _rule_id(keyword, "exact")).strip() or _rule_id(keyword, "exact"),
            "keyword": keyword,
            "match_mode": "exact",
            "blocked_sources": blocked_sources,
            "reason": str(item.get("reason") or "").strip(),
            "updated_at": str(item.get("updated_at") or "").strip(),
            "rule_type": "disabled_keyword",
        }
    for item in (state or {}).get("blocked_roots") or []:
        keyword = normalize_keyword_token(item.get("keyword"))
        blocked_sources = _default_blocked_sources(item.get("blocked_sources") or [])
        match_mode = normalize_match_mode(item.get("match_mode"), "contains")
        if not keyword or not blocked_sources:
            continue
        payload = {
            "id": str(item.get("id") or _rule_id(keyword, match_mode)).strip() or _rule_id(keyword, match_mode),
            "keyword": keyword,
            "match_mode": match_mode,
            "blocked_sources": blocked_sources,
            "reason": str(item.get("reason") or "").strip(),
            "updated_at": str(item.get("updated_at") or "").strip(),
            "rule_type": "blocked_root",
        }
        if match_mode == "exact":
            lookup["blocked_roots_exact"][keyword] = payload
        else:
            lookup["blocked_roots_contains"].append(payload)
    lookup["blocked_roots_contains"].sort(
        key=lambda item: (-len(str(item.get("keyword") or "")), str(item.get("keyword") or ""))
    )
    return lookup


def _map_candidate_scope(raw_value: object) -> str:
    token = normalize_keyword_token(raw_value)
    if token in KEYWORD_CONTROL_SCOPES:
        return token
    if token in {"expanded", "expand", "autocomplete", "suggestion", "suggestions", "live_suggestion", "live_suggestions", "catalog", "seed", "keyword_seed"}:
        return "generated"
    if token in {"adhoc", "manual"}:
        return "manual"
    if token in {"tracked"}:
        return "tracked"
    return ""


def classify_keyword_source_scopes(row: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(row, Mapping):
        return []
    metadata = row.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
        metadata_json = row.get("metadata_json")
        if isinstance(metadata_json, str) and metadata_json.strip():
            try:
                parsed = json.loads(metadata_json)
                if isinstance(parsed, Mapping):
                    metadata = parsed
            except Exception:
                metadata = {}
    seen: set[str] = set()
    items: list[str] = []
    for candidate in (
        row.get("source_type"),
        row.get("tracking_mode"),
        metadata.get("registration_source"),
        metadata.get("parent_source_type"),
        metadata.get("source_scope"),
        metadata.get("tracking_mode"),
    ):
        scope = _map_candidate_scope(candidate)
        if scope and scope not in seen:
            seen.add(scope)
            items.append(scope)
    return items


def _match_rule_sources(rule: Mapping[str, Any], active_sources: set[str]) -> dict[str, Any] | None:
    blocked_sources = set(_default_blocked_sources(rule.get("blocked_sources") or []))
    matched_sources = sorted(blocked_sources.intersection(active_sources))
    if not matched_sources:
        return None
    return {
        "id": str(rule.get("id") or "").strip(),
        "keyword": normalize_keyword_token(rule.get("keyword")),
        "match_mode": normalize_match_mode(rule.get("match_mode"), "exact"),
        "rule_type": str(rule.get("rule_type") or "").strip(),
        "blocked_sources": matched_sources,
        "reason": str(rule.get("reason") or "").strip(),
        "updated_at": str(rule.get("updated_at") or "").strip(),
    }


def match_exclusion_rule(keyword: str, source_scopes: list[object], exclusion_lookup: Mapping[str, Any] | None) -> dict[str, Any] | None:
    normalized_keyword = normalize_keyword_token(keyword)
    if not normalized_keyword or not exclusion_lookup:
        return None
    active_sources = set(normalize_blocked_sources(source_scopes or [])) or set(KEYWORD_CONTROL_SCOPES)

    exact_rule = exclusion_lookup.get("disabled_keywords", {}).get(normalized_keyword)
    if isinstance(exact_rule, Mapping):
        matched = _match_rule_sources(exact_rule, active_sources)
        if matched:
            matched["matched_keyword"] = normalized_keyword
            return matched

    exact_root_rule = exclusion_lookup.get("blocked_roots_exact", {}).get(normalized_keyword)
    if isinstance(exact_root_rule, Mapping):
        matched = _match_rule_sources(exact_root_rule, active_sources)
        if matched:
            matched["matched_keyword"] = normalized_keyword
            return matched

    for rule in exclusion_lookup.get("blocked_roots_contains", []) or []:
        if not isinstance(rule, Mapping):
            continue
        rule_keyword = normalize_keyword_token(rule.get("keyword"))
        if not rule_keyword or rule_keyword not in normalized_keyword:
            continue
        matched = _match_rule_sources(rule, active_sources)
        if matched:
            matched["matched_keyword"] = rule_keyword
            return matched
    return None


KEYWORD_ROOT_MATCH_MODES = KEYWORD_CONTROL_MATCH_MODES
normalize_root_match_mode = normalize_match_mode

_legacy_get_keyword_control_state = get_keyword_control_state
_legacy_build_exclusion_lookup = build_exclusion_lookup
_legacy_update_monitor_disabled_keywords = update_monitor_disabled_keywords
_legacy_update_monitor_blocked_roots = update_monitor_blocked_roots


def _normalize_disabled_keyword_payload(items: list[object] | tuple[object, ...] | set[object] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        keyword = normalize_keyword_token(item.get("keyword"))
        blocked_sources = normalize_blocked_sources(item.get("blocked_sources") or [])
        if not keyword or not blocked_sources:
            continue
        normalized.append(
            {
                "id": str(item.get("id") or _rule_id(keyword, "exact")).strip() or _rule_id(keyword, "exact"),
                "keyword": keyword,
                "match_mode": "exact",
                "blocked_sources": blocked_sources,
                "reason": str(item.get("reason") or "").strip(),
                "updated_at": str(item.get("updated_at") or "").strip(),
                "rule_type": "disabled_keyword",
            }
        )
    return normalized


def _normalize_blocked_root_payload(items: list[object] | tuple[object, ...] | set[object] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        root_keyword = normalize_keyword_token(item.get("root_keyword") or item.get("keyword"))
        blocked_sources = normalize_blocked_sources(item.get("blocked_sources") or [])
        match_mode = normalize_match_mode(item.get("match_mode"), "exact")
        if not root_keyword or not blocked_sources:
            continue
        normalized.append(
            {
                "id": str(item.get("id") or _rule_id(root_keyword, match_mode)).strip() or _rule_id(root_keyword, match_mode),
                "keyword": root_keyword,
                "root_keyword": root_keyword,
                "match_mode": match_mode,
                "blocked_sources": blocked_sources,
                "reason": str(item.get("reason") or "").strip(),
                "updated_at": str(item.get("updated_at") or "").strip(),
                "rule_type": "blocked_root",
            }
        )
    return normalized


def get_keyword_control_state(monitor_config: str, baseline_file_override: str = "") -> dict[str, Any]:
    payload = _legacy_get_keyword_control_state(monitor_config, baseline_file_override)
    disabled_keywords = _normalize_disabled_keyword_payload(payload.get("disabled_keywords") or payload.get("exclusions") or [])
    blocked_roots = _normalize_blocked_root_payload(payload.get("blocked_roots") or [])
    payload["disabled_keywords"] = disabled_keywords
    payload["blocked_roots"] = blocked_roots
    payload["exclusions"] = list(disabled_keywords)
    payload["exclusion_rules"] = list(disabled_keywords)
    payload["rules"] = list(disabled_keywords)
    payload["disabled_keyword_count"] = len(disabled_keywords)
    payload["blocked_root_count"] = len(blocked_roots)
    payload["available_root_match_modes"] = list(KEYWORD_ROOT_MATCH_MODES)
    payload["available_match_modes"] = list(KEYWORD_ROOT_MATCH_MODES)
    return payload


def update_monitor_disabled_keyword_rules(
    monitor_config: str,
    *,
    keywords: list[object],
    blocked_sources: list[object] | None = None,
    reason: str = "",
    mode: str = "disable",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_mode = normalize_keyword_token(mode)
    if normalized_mode not in {"disable", "restore", "upsert", "delete"}:
        raise ValueError("mode must be disable, restore, upsert, or delete")
    mapped_mode = "upsert" if normalized_mode in {"disable", "upsert"} else "delete"
    return _legacy_update_monitor_disabled_keywords(
        monitor_config,
        keywords=keywords,
        blocked_sources=blocked_sources,
        reason=reason,
        mode=mapped_mode,
        baseline_file_override=baseline_file_override,
    )


def update_monitor_blocked_root_rules(
    monitor_config: str,
    *,
    root_keywords: list[object],
    blocked_sources: list[object] | None = None,
    reason: str = "",
    match_mode: str = "exact",
    mode: str = "upsert",
    baseline_file_override: str = "",
) -> dict[str, Any]:
    normalized_mode = normalize_keyword_token(mode)
    if normalized_mode not in {"upsert", "delete", "add", "remove"}:
        raise ValueError("mode must be upsert, delete, add, or remove")
    mapped_mode = "upsert" if normalized_mode in {"upsert", "add"} else "delete"
    return _legacy_update_monitor_blocked_roots(
        monitor_config,
        keywords=root_keywords,
        blocked_sources=blocked_sources,
        reason=reason,
        match_mode=normalize_match_mode(match_mode, "exact"),
        mode=mapped_mode,
        baseline_file_override=baseline_file_override,
    )


def build_exclusion_lookup(state: Mapping[str, Any] | None) -> dict[str, Any]:
    if isinstance(state, Mapping):
        disabled_keywords = state.get("disabled_keywords")
        blocked_roots_exact = state.get("blocked_roots_exact")
        blocked_roots_contains = state.get("blocked_roots_contains")
        if isinstance(disabled_keywords, Mapping) and isinstance(blocked_roots_exact, Mapping) and isinstance(blocked_roots_contains, list):
            return {
                "disabled_keywords": dict(disabled_keywords),
                "blocked_roots_exact": dict(blocked_roots_exact),
                "blocked_roots_contains": list(blocked_roots_contains),
            }
    lookup = _legacy_build_exclusion_lookup(state)
    lookup.setdefault("disabled_keywords", {})
    lookup.setdefault("blocked_roots_exact", {})
    lookup.setdefault("blocked_roots_contains", [])
    return lookup


def build_keyword_control_lookup(state: Mapping[str, Any] | None) -> dict[str, Any]:
    return build_exclusion_lookup(state)


def match_keyword_control_rule(
    keyword: str,
    source_scopes: list[object],
    control_lookup: Mapping[str, Any] | None,
    *,
    root_keywords: list[object] | tuple[object, ...] | set[object] | None = None,
) -> dict[str, Any] | None:
    normalized_keyword = normalize_keyword_token(keyword)
    if not normalized_keyword or not control_lookup:
        return None
    active_sources = set(normalize_blocked_sources(source_scopes or [])) or set(KEYWORD_CONTROL_SCOPES)
    lookup = build_keyword_control_lookup(control_lookup)

    exact_rule = lookup.get("disabled_keywords", {}).get(normalized_keyword)
    if isinstance(exact_rule, Mapping):
        matched = _match_rule_sources(exact_rule, active_sources)
        if matched:
            matched["keyword"] = normalized_keyword
            matched["matched_keyword"] = normalized_keyword
            return matched

    normalized_roots = normalize_keyword_list(root_keywords or []) or [normalized_keyword]
    for candidate in normalized_roots:
        exact_root_rule = lookup.get("blocked_roots_exact", {}).get(candidate)
        if isinstance(exact_root_rule, Mapping):
            matched = _match_rule_sources(exact_root_rule, active_sources)
            if matched:
                matched["keyword"] = normalized_keyword
                matched["root_keyword"] = normalize_keyword_token(exact_root_rule.get("keyword") or exact_root_rule.get("root_keyword"))
                matched["matched_keyword"] = candidate
                return matched

    for rule in lookup.get("blocked_roots_contains", []) or []:
        if not isinstance(rule, Mapping):
            continue
        rule_keyword = normalize_keyword_token(rule.get("keyword") or rule.get("root_keyword"))
        if not rule_keyword:
            continue
        matched_root = next((candidate for candidate in normalized_roots if rule_keyword in candidate), "")
        if not matched_root:
            continue
        matched = _match_rule_sources(rule, active_sources)
        if matched:
            matched["keyword"] = normalized_keyword
            matched["root_keyword"] = rule_keyword
            matched["matched_keyword"] = matched_root
            return matched
    return None


def match_exclusion_rule(keyword: str, source_scopes: list[object], exclusion_lookup: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return match_keyword_control_rule(keyword, source_scopes, exclusion_lookup, root_keywords=[keyword])
