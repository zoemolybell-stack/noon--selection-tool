from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "product_store.db"
DEFAULT_REPORTS_DIR = ROOT / "data" / "reports"
EXPLICIT_AD_RE = re.compile(r"^(ad|sponsored|promoted|\u0625\u0639\u0644\u0627\u0646)$", re.IGNORECASE)
EMBEDDED_AD_RE = re.compile(r"(\bsponsored\b|\bpromoted\b|\u0625\u0639\u0644\u0627\u0646)", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="修复 Noon 类目历史库中的广告误报")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="类目共享库路径")
    parser.add_argument("--apply", action="store_true", help="实际写回数据库；默认只审计不落库")
    return parser.parse_args()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def has_explicit_ad_signal(public_signals_json: str | None, badge_texts_json: str | None) -> bool:
    candidate_values: list[str] = []

    for payload in (public_signals_json, badge_texts_json):
        if not payload:
            continue
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = payload

        if isinstance(decoded, dict):
            for key in ("badge_texts", "all_signal_texts", "raw_signal_texts", "ad_marker_texts"):
                value = decoded.get(key)
                if isinstance(value, list):
                    candidate_values.extend(str(item) for item in value if item)
                elif value:
                    candidate_values.append(str(value))
        elif isinstance(decoded, list):
            candidate_values.extend(str(item) for item in decoded if item)
        elif decoded:
            candidate_values.append(str(decoded))

    for raw_value in candidate_values:
        normalized = normalize_text(raw_value)
        if not normalized:
            continue
        if EXPLICIT_AD_RE.match(normalized):
            return True
        if EMBEDDED_AD_RE.search(normalized):
            return True

    return False


def main():
    args = parse_args()
    conn = sqlite3.connect(str(args.db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT co.id, co.snapshot_id, co.source_type, co.source_value, co.product_id, co.product_url,
               co.public_signals_json, co.badge_texts_json, p.title
        FROM crawl_observations co
        LEFT JOIN products p ON p.product_id = co.product_id
        WHERE co.source_type = 'category' AND co.is_ad = 1
        ORDER BY co.scraped_at DESC, co.id DESC
        """
    ).fetchall()

    false_positive_rows: list[dict] = []
    for row in rows:
        if has_explicit_ad_signal(row["public_signals_json"], row["badge_texts_json"]):
            continue
        false_positive_rows.append(
            {
                "id": row["id"],
                "snapshot_id": row["snapshot_id"],
                "source_value": row["source_value"],
                "product_id": row["product_id"],
                "title": row["title"],
                "product_url": row["product_url"],
            }
        )

    updated_rows = 0
    if args.apply and false_positive_rows:
        ids = [row["id"] for row in false_positive_rows]
        placeholders = ", ".join("?" for _ in ids)
        conn.execute(f"UPDATE crawl_observations SET is_ad = 0 WHERE id IN ({placeholders})", ids)
        conn.commit()
        updated_rows = conn.total_changes

    summary = {
        "generated_at": datetime.now().isoformat(),
        "db_path": str(args.db_path),
        "apply": bool(args.apply),
        "candidate_ad_rows": len(rows),
        "false_positive_count": len(false_positive_rows),
        "updated_rows": updated_rows,
        "false_positive_rows": false_positive_rows,
    }

    DEFAULT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = DEFAULT_REPORTS_DIR / f"category_ad_flag_repair_{timestamp}.json"
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report_path)

    conn.close()


if __name__ == "__main__":
    main()
