"""
趋势分析器

职责：
- 基于 stage DB 的历史价格、排名、观测记录生成趋势分析
- 兼容当前 `(platform, product_id)` 复合身份模型
- 支持 SQLite 与 PostgreSQL
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from db.config import POSTGRES_SCHEMES

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """趋势分析器。"""

    def __init__(self, db_path: Path | str):
        self.raw_db_path = str(db_path).strip()
        self.is_postgres = self.raw_db_path.lower().startswith(POSTGRES_SCHEMES)
        self.conn = self._connect()
        self.uses_product_key = self._has_product_key()

    def _connect(self):
        if not self.is_postgres:
            conn = sqlite3.connect(self.raw_db_path)
            conn.row_factory = sqlite3.Row
            return conn

        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("psycopg is required for PostgreSQL TrendAnalyzer backend") from exc

        return psycopg.connect(self.raw_db_path, row_factory=dict_row, autocommit=True)

    def _adapt_sql(self, sql: str) -> str:
        if self.is_postgres:
            return sql.replace("?", "%s")
        return sql

    def _fetchone(self, sql: str, params: tuple[Any, ...] = ()):
        cursor = self.conn.cursor()
        cursor.execute(self._adapt_sql(sql), params)
        return cursor.fetchone()

    def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
        cursor = self.conn.cursor()
        cursor.execute(self._adapt_sql(sql), params)
        return cursor.fetchall()

    def _has_product_key(self) -> bool:
        if self.is_postgres:
            row = self._fetchone(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'products' AND column_name = 'product_key'
                LIMIT 1
                """
            )
            return row is not None
        rows = self._fetchall("PRAGMA table_info(products)")
        return any(str(item["name"]) == "product_key" for item in rows)

    def _resolve_product_identity(self, product_id: str, platform: str | None = None) -> Optional[Dict[str, Any]]:
        raw_product_id = str(product_id or "").strip()
        if not raw_product_id:
            return None
        if not self.uses_product_key:
            return {
                "product_id": raw_product_id,
                "platform": str(platform or "").strip().lower() or None,
                "product_key": raw_product_id,
            }

        if platform:
            row = self._fetchone(
                """
                SELECT product_key, product_id, platform
                FROM products
                WHERE product_id = ? AND platform = ?
                ORDER BY COALESCE(last_seen, '') DESC, id DESC
                LIMIT 1
                """,
                (raw_product_id, str(platform).strip().lower()),
            )
        else:
            row = self._fetchone(
                """
                SELECT product_key, product_id, platform
                FROM products
                WHERE product_id = ?
                ORDER BY COALESCE(last_seen, '') DESC, id DESC
                LIMIT 1
                """,
                (raw_product_id,),
            )
        return dict(row) if row else None

    def _history_where_sql(self, identity: Dict[str, Any], alias: str = "") -> tuple[str, tuple[Any, ...]]:
        prefix = f"{alias}." if alias else ""
        if self.uses_product_key:
            return f"{prefix}product_key = ?", (identity["product_key"],)
        conditions = [f"{prefix}product_id = ?"]
        params: list[Any] = [identity["product_id"]]
        if identity.get("platform"):
            conditions.append(f"{prefix}platform = ?")
            params.append(identity["platform"])
        return " AND ".join(conditions), tuple(params)

    def analyze_price_trend(self, product_id: str, days: int = 30, *, platform: str | None = None) -> Dict[str, Any]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return {"status": "not_found"}

        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        where_sql, params = self._history_where_sql(identity)
        rows = [
            dict(row)
            for row in self._fetchall(
                f"""
                SELECT scraped_at, price, original_price
                FROM price_history
                WHERE {where_sql} AND scraped_at >= ?
                ORDER BY scraped_at ASC
                """,
                (*params, cutoff_date),
            )
        ]

        if len(rows) < 2:
            return {"status": "insufficient_data", "records": len(rows)}

        prices = [float(item["price"]) for item in rows if item.get("price") is not None]
        if len(prices) < 2:
            return {"status": "insufficient_data", "records": len(prices)}

        first_price = prices[0]
        last_price = prices[-1]
        avg_price = statistics.mean(prices)
        price_change = last_price - first_price
        price_change_pct = (price_change / first_price) * 100 if first_price else 0.0
        std_dev = statistics.stdev(prices) if len(prices) > 1 else 0.0
        volatility = (std_dev / avg_price) * 100 if avg_price else 0.0

        if price_change_pct > 5:
            trend = "increasing"
        elif price_change_pct < -5:
            trend = "decreasing"
        else:
            trend = "stable"

        anomalies = []
        for index in range(1, len(prices)):
            previous = prices[index - 1]
            if not previous:
                continue
            daily_change = abs(prices[index] - previous) / previous * 100
            if daily_change > 20:
                anomalies.append(
                    {
                        "date": rows[index]["scraped_at"],
                        "change_pct": round(daily_change, 1),
                    }
                )

        return {
            "status": "ok",
            "trend": trend,
            "first_price": round(first_price, 2),
            "last_price": round(last_price, 2),
            "avg_price": round(avg_price, 2),
            "price_change": round(price_change, 2),
            "price_change_pct": round(price_change_pct, 2),
            "volatility": round(volatility, 2),
            "data_points": len(prices),
            "anomalies": anomalies,
            "platform": identity.get("platform"),
            "product_key": identity.get("product_key"),
        }

    def analyze_rank_trend(self, product_id: str, days: int = 30, *, platform: str | None = None) -> Dict[str, Any]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return {"status": "not_found"}

        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        where_sql, params = self._history_where_sql(identity)
        rows = [
            dict(row)
            for row in self._fetchall(
                f"""
                SELECT scraped_at, keyword, search_rank, bsr_rank
                FROM rank_history
                WHERE {where_sql} AND scraped_at >= ?
                ORDER BY scraped_at ASC
                """,
                (*params, cutoff_date),
            )
        ]

        result: Dict[str, Any] = {"status": "insufficient_data"}
        bsr_data = [(item["scraped_at"], item["bsr_rank"]) for item in rows if item.get("bsr_rank") is not None]
        if len(bsr_data) >= 2:
            bsr_values = [int(item[1]) for item in bsr_data]
            first_bsr = bsr_values[0]
            last_bsr = bsr_values[-1]
            avg_bsr = statistics.mean(bsr_values)
            bsr_change = last_bsr - first_bsr
            bsr_change_pct = (bsr_change / first_bsr) * 100 if first_bsr else 0.0
            if bsr_change_pct < -10:
                bsr_trend = "improving"
            elif bsr_change_pct > 10:
                bsr_trend = "declining"
            else:
                bsr_trend = "stable"
            result["bsr"] = {
                "status": "ok",
                "trend": bsr_trend,
                "first_bsr": first_bsr,
                "last_bsr": last_bsr,
                "avg_bsr": round(avg_bsr, 1),
                "bsr_change": bsr_change,
                "bsr_change_pct": round(bsr_change_pct, 2),
                "data_points": len(bsr_values),
            }

        search_ranks: Dict[str, Dict[str, Any]] = {}
        keywords = sorted({str(item["keyword"]) for item in rows if item.get("keyword") and item.get("search_rank") is not None})
        for keyword in keywords:
            keyword_rows = [
                (item["scraped_at"], int(item["search_rank"]))
                for item in rows
                if item.get("keyword") == keyword and item.get("search_rank") is not None
            ]
            if len(keyword_rows) < 2:
                continue
            ranks = [item[1] for item in keyword_rows]
            search_ranks[keyword] = {
                "status": "ok",
                "first_rank": ranks[0],
                "last_rank": ranks[-1],
                "avg_rank": round(statistics.mean(ranks), 1),
                "rank_change": ranks[-1] - ranks[0],
                "trend": "improving" if ranks[-1] < ranks[0] * 0.9 else ("declining" if ranks[-1] > ranks[0] * 1.1 else "stable"),
            }

        if search_ranks:
            result["search_ranks"] = search_ranks
        if "bsr" in result or "search_ranks" in result:
            result["status"] = "ok"
        result["platform"] = identity.get("platform")
        result["product_key"] = identity.get("product_key")
        return result

    def get_product_overview(self, product_id: str, *, platform: str | None = None) -> Optional[Dict[str, Any]]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return None

        if self.uses_product_key:
            row = self._fetchone("SELECT * FROM products WHERE product_key = ?", (identity["product_key"],))
        else:
            where_sql, params = self._history_where_sql(identity)
            row = self._fetchone(f"SELECT * FROM products WHERE {where_sql}", params)
        if row is None:
            return None

        product = dict(row)
        where_sql, params = self._history_where_sql(identity)
        latest_price = self._fetchone(
            f"""
            SELECT price, original_price, currency, scraped_at
            FROM price_history
            WHERE {where_sql}
            ORDER BY scraped_at DESC
            LIMIT 1
            """,
            params,
        )
        if latest_price is not None:
            product["latest_price"] = dict(latest_price)

        latest_bsr = self._fetchone(
            f"""
            SELECT bsr_rank, scraped_at
            FROM rank_history
            WHERE {where_sql} AND bsr_rank IS NOT NULL
            ORDER BY scraped_at DESC
            LIMIT 1
            """,
            params,
        )
        if latest_bsr is not None:
            product["latest_bsr"] = dict(latest_bsr)
        return product

    def analyze_competition(self, category_path: str | None = None, days: int = 7) -> Dict[str, Any]:
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        old_cutoff = (datetime.now() - timedelta(days=days * 2)).isoformat()
        result: Dict[str, Any] = {}

        category_filter_sql = ""
        category_filter_params: tuple[Any, ...] = ()
        if category_path:
            category_filter_sql = " AND category_path = ?"
            category_filter_params = (category_path,)

        result["new_products"] = [
            dict(row)
            for row in self._fetchall(
                f"""
                SELECT COUNT(*) AS count, platform
                FROM products
                WHERE first_seen >= ? {category_filter_sql}
                GROUP BY platform
                """,
                (cutoff_date, *category_filter_params),
            )
        ]
        result["exiting_products"] = [
            dict(row)
            for row in self._fetchall(
                f"""
                SELECT COUNT(*) AS count, platform
                FROM products
                WHERE last_seen < ? AND last_seen IS NOT NULL {category_filter_sql}
                GROUP BY platform
                """,
                (old_cutoff, *category_filter_params),
            )
        ]

        key_column = "product_key" if self.uses_product_key else "product_id"
        price_rows = self._fetchall(
            f"""
            SELECT
                p.product_id,
                p.platform,
                p.title,
                (
                    SELECT ph.price
                    FROM price_history ph
                    WHERE ph.{key_column} = p.{key_column} AND ph.scraped_at >= ?
                    ORDER BY ph.scraped_at ASC
                    LIMIT 1
                ) AS first_price,
                (
                    SELECT ph.price
                    FROM price_history ph
                    WHERE ph.{key_column} = p.{key_column} AND ph.scraped_at >= ?
                    ORDER BY ph.scraped_at DESC
                    LIMIT 1
                ) AS last_price
            FROM products p
            WHERE p.first_seen < ? {category_filter_sql}
            """,
            (cutoff_date, cutoff_date, cutoff_date, *category_filter_params),
        )

        price_changes = []
        for row in price_rows:
            first_price = row["first_price"]
            last_price = row["last_price"]
            if first_price is None or last_price is None or not first_price:
                continue
            change_pct = ((last_price - first_price) / first_price) * 100
            if change_pct < -10:
                price_changes.append(
                    {
                        "product_id": row["product_id"],
                        "platform": row["platform"],
                        "title": row["title"],
                        "change_pct": round(change_pct, 1),
                        "type": "price_drop",
                    }
                )
            elif change_pct > 10:
                price_changes.append(
                    {
                        "product_id": row["product_id"],
                        "platform": row["platform"],
                        "title": row["title"],
                        "change_pct": round(change_pct, 1),
                        "type": "price_increase",
                    }
                )
        result["price_changes"] = price_changes
        return result

    def get_alerts(self, threshold_price_pct: float = 20, threshold_rank_drop: int = 100) -> List[Dict[str, Any]]:
        alerts: List[Dict[str, Any]] = []
        cutoff_date = (datetime.now() - timedelta(days=7)).isoformat()
        key_column = "product_key" if self.uses_product_key else "product_id"

        price_rows = self._fetchall(
            f"""
            SELECT
                p.product_id,
                p.platform,
                p.title,
                (
                    SELECT ph.price
                    FROM price_history ph
                    WHERE ph.{key_column} = p.{key_column} AND ph.scraped_at >= ?
                    ORDER BY ph.scraped_at ASC
                    LIMIT 1
                ) AS first_price,
                (
                    SELECT ph.price
                    FROM price_history ph
                    WHERE ph.{key_column} = p.{key_column} AND ph.scraped_at >= ?
                    ORDER BY ph.scraped_at DESC
                    LIMIT 1
                ) AS last_price
            FROM products p
            WHERE p.is_active = 1
            """,
            (cutoff_date, cutoff_date),
        )
        for row in price_rows:
            first_price = row["first_price"]
            last_price = row["last_price"]
            if first_price is None or last_price is None or first_price <= 0:
                continue
            change_pct = abs((last_price - first_price) / first_price) * 100
            if change_pct > threshold_price_pct:
                alerts.append(
                    {
                        "type": "price_volatility",
                        "severity": "high" if change_pct > 30 else "medium",
                        "product_id": row["product_id"],
                        "platform": row["platform"],
                        "title": row["title"],
                        "detail": f"价格波动 {change_pct:.1f}%",
                        "first_price": first_price,
                        "last_price": last_price,
                    }
                )

        rank_rows = self._fetchall(
            f"""
            SELECT
                p.product_id,
                p.platform,
                p.title,
                (
                    SELECT rh.bsr_rank
                    FROM rank_history rh
                    WHERE rh.{key_column} = p.{key_column} AND rh.scraped_at >= ?
                    ORDER BY rh.scraped_at ASC
                    LIMIT 1
                ) AS first_bsr,
                (
                    SELECT rh.bsr_rank
                    FROM rank_history rh
                    WHERE rh.{key_column} = p.{key_column} AND rh.scraped_at >= ?
                    ORDER BY rh.scraped_at DESC
                    LIMIT 1
                ) AS last_bsr
            FROM products p
            WHERE p.is_active = 1
            """,
            (cutoff_date, cutoff_date),
        )
        for row in rank_rows:
            first_bsr = row["first_bsr"]
            last_bsr = row["last_bsr"]
            if first_bsr is None or last_bsr is None:
                continue
            bsr_change = last_bsr - first_bsr
            if bsr_change > threshold_rank_drop:
                alerts.append(
                    {
                        "type": "rank_decline",
                        "severity": "high" if bsr_change > 500 else "medium",
                        "product_id": row["product_id"],
                        "platform": row["platform"],
                        "title": row["title"],
                        "detail": f"BSR 下降 {bsr_change} 位",
                        "first_bsr": first_bsr,
                        "last_bsr": last_bsr,
                    }
                )

        old_date = (datetime.now() - timedelta(days=30)).isoformat()
        stale_rows = self._fetchall(
            """
            SELECT product_id, platform, title, last_seen
            FROM products
            WHERE last_seen < ? AND is_active = 1
            """,
            (old_date,),
        )
        for row in stale_rows:
            alerts.append(
                {
                    "type": "stale_product",
                    "severity": "low",
                    "product_id": row["product_id"],
                    "platform": row["platform"],
                    "title": row["title"],
                    "detail": f"超过 30 天未更新，最后一次出现: {row['last_seen']}",
                }
            )

        severity_order = {"high": 0, "medium": 1, "low": 2}
        return sorted(alerts, key=lambda item: severity_order.get(str(item.get("severity")), 99))

    def generate_report(
        self,
        product_ids: Optional[List[str]] = None,
        days: int = 30,
        *,
        platform: str | None = None,
    ) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "generated_at": datetime.now().isoformat(),
            "period_days": days,
            "products": [],
            "summary": {},
            "alerts": [],
        }

        if product_ids is None:
            rows = self._fetchall(
                """
                SELECT product_id, platform
                FROM products
                WHERE is_active = 1
                ORDER BY COALESCE(last_seen, '') DESC, id DESC
                """
            )
            identities = [(row["product_id"], row.get("platform")) for row in rows]
        else:
            identities = [(item, platform) for item in product_ids]

        for current_product_id, current_platform in identities:
            overview = self.get_product_overview(current_product_id, platform=current_platform)
            if overview is None:
                continue
            price_trend = self.analyze_price_trend(current_product_id, days, platform=current_platform)
            rank_trend = self.analyze_rank_trend(current_product_id, days, platform=current_platform)
            report["products"].append(
                {
                    "product_id": current_product_id,
                    "platform": current_platform,
                    "overview": overview,
                    "price_trend": price_trend,
                    "rank_trend": rank_trend,
                }
            )

        total_products = len(report["products"])
        price_increasing = sum(1 for item in report["products"] if item["price_trend"].get("trend") == "increasing")
        price_decreasing = sum(1 for item in report["products"] if item["price_trend"].get("trend") == "decreasing")
        bsr_improving = sum(
            1
            for item in report["products"]
            if item.get("rank_trend", {}).get("bsr", {}).get("trend") == "improving"
        )
        report["summary"] = {
            "total_products_analyzed": total_products,
            "price_increasing": price_increasing,
            "price_decreasing": price_decreasing,
            "price_stable": total_products - price_increasing - price_decreasing,
            "bsr_improving": bsr_improving,
        }
        report["alerts"] = self.get_alerts()
        return report

    def export_to_csv(self, product_id: str, output_path: Path, *, platform: str | None = None) -> None:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            raise ValueError(f"product not found: {product_id}")
        output_path.mkdir(parents=True, exist_ok=True)
        where_sql, params = self._history_where_sql(identity)

        price_file = output_path / f"{identity['product_id']}_price_history.csv"
        price_rows = self._fetchall(
            f"""
            SELECT scraped_at, price, original_price, currency
            FROM price_history
            WHERE {where_sql}
            ORDER BY scraped_at DESC
            """,
            params,
        )
        with price_file.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["日期", "价格", "原价", "货币"])
            for row in price_rows:
                writer.writerow([row["scraped_at"], row["price"], row["original_price"], row["currency"]])

        rank_file = output_path / f"{identity['product_id']}_rank_history.csv"
        rank_rows = self._fetchall(
            f"""
            SELECT scraped_at, keyword, search_rank, bsr_rank
            FROM rank_history
            WHERE {where_sql}
            ORDER BY scraped_at DESC
            """,
            params,
        )
        with rank_file.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["日期", "关键词", "搜索排名", "BSR 排名"])
            for row in rank_rows:
                writer.writerow([row["scraped_at"], row["keyword"], row["search_rank"], row["bsr_rank"]])

        logger.info("CSV 已导出到: %s", output_path)

    def close(self) -> None:
        if getattr(self, "conn", None):
            self.conn.close()

    def __del__(self) -> None:
        self.close()
