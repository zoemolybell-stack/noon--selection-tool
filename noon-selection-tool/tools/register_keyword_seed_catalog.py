from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.product_store import ProductStore
from config.settings import Settings
from keyword_main import _sync_keyword_pool


DEFAULT_CATALOG_PATH = ROOT / "config" / "keyword_seed_catalog.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register grouped keyword seed catalog")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--groups", nargs="+", default=None, help="Optional group slugs")
    parser.add_argument("--runtime-scope", default="keyword")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.catalog.read_text(encoding="utf-8"))
    groups = payload.get("groups") or []
    requested = {item.strip().lower() for item in (args.groups or []) if item.strip()}

    settings = Settings()
    settings.set_runtime_scope(args.runtime_scope or "keyword")
    store = ProductStore(settings.product_store_db_path)
    registered = 0
    try:
        for group in groups:
            slug = str(group.get("slug") or "").strip().lower()
            if requested and slug not in requested:
                continue
            label = str(group.get("label") or slug)
            priority = int(group.get("priority") or 30)
            keywords = [
                str(item).strip().lower()
                for item in (group.get("keywords") or [])
                if str(item).strip()
            ]
            if not keywords:
                continue
            registered += store.upsert_keywords(
                keywords,
                tracking_mode="tracked",
                source_type="catalog",
                source_platform="system",
                status="active",
                priority=priority,
                metadata={
                    "expansion_depth": 0,
                    "registration_source": "catalog",
                    "seed_group_slug": slug,
                    "seed_group_label": label,
                },
                last_snapshot_id=settings.snapshot_id,
            )
        pool = _sync_keyword_pool(settings, store)
        print(
            json.dumps(
                {
                    "registered_keywords": registered,
                    "tracked_pool_size": len(pool),
                    "db_path": str(settings.product_store_db_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
