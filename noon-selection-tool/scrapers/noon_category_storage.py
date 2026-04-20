from __future__ import annotations

import hashlib
import json
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Callable

from scrapers.noon_category_payloads import build_subcategory_payload, merge_effective_subcategory_products


def normalize_subcategory_name(sub_name: str) -> str:
    return " ".join(str(sub_name or "").split()).casefold()


def legacy_subcategory_file_path(sub_dir: Path, sub_name: str) -> Path:
    safe_name = re.sub(r"[^\w\s-]", "", str(sub_name or "")).strip().replace(" ", "_")
    return sub_dir / f"{safe_name}.json"


def subcategory_file_path(sub_dir: Path, sub_name: str) -> Path:
    raw_name = str(sub_name or "")
    normalized = unicodedata.normalize("NFKD", raw_name).encode("ascii", "ignore").decode("ascii")
    safe_slug = re.sub(r"[^\w\s-]", " ", normalized).strip().replace(" ", "_")
    safe_slug = re.sub(r"_+", "_", safe_slug).strip("._-")
    safe_slug = safe_slug or "subcategory"
    slug = safe_slug[:80].rstrip("._-") or "subcategory"
    digest = hashlib.sha1(raw_name.encode("utf-8")).hexdigest()[:12]
    max_stem_length = 120 - len(".json")
    stem = f"{slug}__{digest}"
    if len(stem) > max_stem_length:
        allowed_slug_length = max(1, max_stem_length - len("__") - len(digest))
        slug = slug[:allowed_slug_length].rstrip("._-") or "subcategory"
        stem = f"{slug}__{digest}"
    return sub_dir / f"{stem}.json"


def resolve_subcategory_file_path(sub_dir: Path, sub_name: str) -> Path:
    current_path = subcategory_file_path(sub_dir, sub_name)
    if current_path.exists():
        return current_path

    legacy_path = legacy_subcategory_file_path(sub_dir, sub_name)
    if legacy_path != current_path and legacy_path.exists():
        return legacy_path
    return current_path


def read_json_file(path: Path, *, label: str, logger) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if logger is not None:
            logger.warning("[category-crawler] 忽略无效%s文件 %s: %s", label, path, exc)
        return None


def atomic_write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def is_valid_subcategory_payload(payload, expected_sub_name: str | None = None) -> bool:
    if not isinstance(payload, dict):
        return False
    subcategory = payload.get("subcategory")
    products = payload.get("products")
    if not isinstance(subcategory, str) or not subcategory.strip():
        return False
    if not isinstance(products, list):
        return False
    if expected_sub_name and normalize_subcategory_name(subcategory) != normalize_subcategory_name(expected_sub_name):
        return False
    product_count = payload.get("product_count")
    if product_count is not None and product_count != len(products):
        return False
    return True


def load_subcategory_payload_from_path(
    path: Path,
    *,
    expected_sub_name: str | None = None,
    logger=None,
) -> dict | None:
    payload = read_json_file(path, label="子类目结果", logger=logger)
    if payload is None:
        return None
    if not is_valid_subcategory_payload(payload, expected_sub_name=expected_sub_name):
        if logger is not None:
            logger.warning("[category-crawler] 子类目结果结构无效，将忽略并重新抓取: %s", path)
        return None
    return payload


def save_subcategory(
    *,
    sub_dir: Path,
    sub_name: str,
    products: list[dict],
    meta: dict,
    on_saved: Callable[[str, Path, dict], None] | None = None,
) -> tuple[Path, dict]:
    data = build_subcategory_payload(sub_name, products, meta)
    current_path = subcategory_file_path(sub_dir, sub_name)
    legacy_path = legacy_subcategory_file_path(sub_dir, sub_name)
    atomic_write_json(current_path, data)
    if legacy_path != current_path and legacy_path.exists():
        legacy_path.unlink(missing_ok=True)
    if on_saved:
        on_saved(sub_name, current_path, data)
    return current_path, data


def iter_effective_subcategory_payloads(sub_dir: Path, *, logger=None) -> list[tuple[Path, dict]]:
    selected: dict[str, tuple[Path, dict]] = {}
    for path in sorted(sub_dir.glob("*.json")):
        data = load_subcategory_payload_from_path(path, logger=logger)
        if data is None:
            continue

        sub_name = str(data.get("subcategory") or "").strip()
        normalized_name = normalize_subcategory_name(sub_name)
        if not normalized_name:
            continue

        preferred_path = subcategory_file_path(sub_dir, sub_name)
        current_entry = selected.get(normalized_name)
        if current_entry is None:
            selected[normalized_name] = (path, data)
            continue

        current_path, _ = current_entry
        candidate_is_preferred = path.name == preferred_path.name
        current_is_preferred = current_path.name == preferred_path.name

        should_replace = False
        if candidate_is_preferred and not current_is_preferred:
            should_replace = True
        elif candidate_is_preferred == current_is_preferred:
            try:
                should_replace = path.stat().st_mtime >= current_path.stat().st_mtime
            except OSError:
                should_replace = False

        if should_replace:
            if logger is not None:
                logger.info(
                    "[category-crawler] 检测到重复子类目结果，优先保留: %s -> %s",
                    current_path.name,
                    path.name,
                )
            selected[normalized_name] = (path, data)
        else:
            if logger is not None:
                logger.info(
                    "[category-crawler] 检测到重复子类目结果，跳过较旧文件: %s",
                    path.name,
                )

    return list(selected.values())


def load_seen_ids(sub_dir: Path, *, logger=None) -> set[str]:
    seen_ids: set[str] = set()
    for _, data in iter_effective_subcategory_payloads(sub_dir, logger=logger):
        for product in data.get("products", []):
            product_id = product.get("product_id", "")
            if product_id:
                seen_ids.add(product_id)
    return seen_ids


def load_existing_subcategory_seen_ids(sub_dir: Path, sub_name: str, *, logger=None) -> set[str]:
    seen_ids: set[str] = set()
    path = resolve_subcategory_file_path(sub_dir, sub_name)
    data = load_subcategory_payload_from_path(path, expected_sub_name=sub_name, logger=logger)
    if data is None:
        return seen_ids
    for product in data.get("products", []):
        product_id = product.get("product_id", "")
        if product_id:
            seen_ids.add(product_id)
    return seen_ids


def merge_all_products(sub_dir: Path, *, logger=None) -> list[dict]:
    return merge_effective_subcategory_products(iter_effective_subcategory_payloads(sub_dir, logger=logger))
