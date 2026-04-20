"""
数据清洗与标准化

加载 data/raw/ 下各平台 JSON → pandas DataFrame
统一货币为 SAR，关键词级聚合，品类映射
"""
import json
import logging
from pathlib import Path
from statistics import median

import pandas as pd

logger = logging.getLogger(__name__)


def load_platform_data(data_dir: Path, platform: str) -> list[dict]:
    """加载指定平台的所有 JSON 文件。
    data_dir 可以是 snapshot_dir（新）或传统 data_dir（兼容旧数据）。
    优先查找 data_dir/{platform}/，fallback 到 data_dir/raw/{platform}/。
    """
    platform_dir = data_dir / platform
    if not platform_dir.exists():
        platform_dir = data_dir / "raw" / platform
    if not platform_dir.exists():
        return []

    records = []
    for f in platform_dir.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            records.append(data)
        except Exception as e:
            logger.warning(f"加载 {f.name} 失败: {e}")
    return records


def aggregate_noon(records: list[dict]) -> pd.DataFrame:
    """Noon 数据聚合：每个关键词一行"""
    rows = []
    for rec in records:
        kw = rec.get("keyword", "")
        products = rec.get("products", [])
        prices = [p["price"] for p in products if p.get("price", 0) > 0]
        reviews = [p["review_count"] for p in products if p.get("review_count")]
        ratings = [p["rating"] for p in products if p.get("rating")]
        express_count = sum(1 for p in products if p.get("is_express"))
        images = [p.get("image_count", 0) for p in products]

        # 品类 — 优先用 Category 筛选器的 primary_category
        top_category = rec.get("primary_category", "")
        category_path = rec.get("category_path", [])
        if not top_category:
            # fallback: 从商品 category_path 提取
            categories = [p.get("category_path", "") for p in products if p.get("category_path")]
            top_category = max(set(categories), key=categories.count) if categories else ""

        rows.append({
            "keyword": kw,
            "noon_total": rec.get("total_results", 0),
            "noon_express": rec.get("express_results", express_count),
            "noon_express_ratio": express_count / len(products) if products else 0,
            "noon_product_count": len(products),
            "noon_median_price": round(median(prices), 2) if prices else 0,
            "noon_min_price": min(prices) if prices else 0,
            "noon_max_price": max(prices) if prices else 0,
            "noon_avg_reviews": round(sum(reviews) / len(reviews), 1) if reviews else 0,
            "noon_avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
            "noon_avg_images": round(sum(images) / len(images), 1) if images else 0,
            "noon_category_path": top_category,
            "noon_category_tree": category_path,
            "noon_suggestions": rec.get("suggested_keywords", []),
            "noon_error": rec.get("error", ""),
        })

    return pd.DataFrame(rows)


def aggregate_amazon(records: list[dict]) -> pd.DataFrame:
    """Amazon 数据聚合"""
    rows = []
    for rec in records:
        kw = rec.get("keyword", "")
        products = rec.get("products", [])
        prices = [p["price"] for p in products if p.get("price", 0) > 0]
        reviews = [p["review_count"] for p in products if p.get("review_count")]
        bsr_count = sum(1 for p in products if p.get("is_bestseller"))
        choice_count = sum(1 for p in products if p.get("is_amazon_choice") or p.get("is_express"))

        rows.append({
            "keyword": kw,
            "amazon_total": rec.get("total_results", 0),
            "amazon_product_count": len(products),
            "amazon_median_price": round(median(prices), 2) if prices else 0,
            "amazon_avg_reviews": round(sum(reviews) / len(reviews), 1) if reviews else 0,
            "amazon_bsr_count": bsr_count,
            "amazon_choice_count": choice_count,
            "amazon_error": rec.get("error", ""),
        })

    return pd.DataFrame(rows)


def aggregate_temu(records: list[dict]) -> pd.DataFrame:
    """Temu 数据聚合"""
    rows = []
    for rec in records:
        kw = rec.get("keyword", "")
        products = rec.get("products", [])
        prices = [p["price"] for p in products if p.get("price", 0) > 0]
        sold = [p["sold_count"] for p in products if p.get("sold_count", 0) > 0]

        rows.append({
            "keyword": kw,
            "temu_total": rec.get("total_results", 0),
            "temu_median_price": round(median(prices), 2) if prices else None,
            "temu_avg_sold": round(sum(sold) / len(sold), 0) if sold else None,
            "temu_error": rec.get("error", ""),
        })

    return pd.DataFrame(rows)


def aggregate_shein(records: list[dict]) -> pd.DataFrame:
    """SHEIN 数据聚合"""
    rows = []
    for rec in records:
        kw = rec.get("keyword", "")
        products = rec.get("products", [])
        prices = [p["price"] for p in products if p.get("price", 0) > 0]
        sold = [p["sold_count"] for p in products if p.get("sold_count", 0) > 0]

        rows.append({
            "keyword": kw,
            "shein_total": rec.get("total_results", 0),
            "shein_median_price": round(median(prices), 2) if prices else None,
            "shein_avg_sold": round(sum(sold) / len(sold), 0) if sold else None,
            "shein_error": rec.get("error", ""),
        })

    return pd.DataFrame(rows)


def aggregate_alibaba(records: list[dict]) -> pd.DataFrame:
    """Alibaba 供应端数据聚合"""
    rows = []
    for rec in records:
        kw = rec.get("keyword", "")

        rows.append({
            "keyword": kw,
            "alibaba_total": rec.get("total_results", 0),
            "alibaba_median_fob_usd": rec.get("median_price_usd"),
            "alibaba_error": rec.get("error", ""),
        })

    return pd.DataFrame(rows)


def aggregate_google_trends(records: list[dict]) -> pd.DataFrame:
    """Google Trends 数据聚合"""
    rows = []
    for rec in records:
        rows.append({
            "keyword": rec.get("keyword", ""),
            "google_trend": rec.get("trend_direction", "unknown"),
            "google_interest": rec.get("average_interest", 0),
            "google_peak_month": rec.get("peak_month", ""),
        })

    return pd.DataFrame(rows)


def merge_all(data_dir: Path) -> pd.DataFrame:
    """
    合并所有平台数据为一个 DataFrame（每个关键词一行）。
    """
    noon_records = load_platform_data(data_dir, "noon")
    amazon_records = load_platform_data(data_dir, "amazon")
    temu_records = load_platform_data(data_dir, "temu")
    shein_records = load_platform_data(data_dir, "shein")
    alibaba_records = load_platform_data(data_dir, "alibaba")
    trends_records = load_platform_data(data_dir, "google_trends")

    df_noon = aggregate_noon(noon_records) if noon_records else pd.DataFrame(columns=["keyword"])
    df_amazon = aggregate_amazon(amazon_records) if amazon_records else pd.DataFrame(columns=["keyword"])
    df_temu = aggregate_temu(temu_records) if temu_records else pd.DataFrame(columns=["keyword"])
    df_shein = aggregate_shein(shein_records) if shein_records else pd.DataFrame(columns=["keyword"])
    df_alibaba = aggregate_alibaba(alibaba_records) if alibaba_records else pd.DataFrame(columns=["keyword"])
    df_trends = aggregate_google_trends(trends_records) if trends_records else pd.DataFrame(columns=["keyword"])

    # 以 Noon 数据为基准 merge
    df = df_noon
    for other in [df_amazon, df_temu, df_shein, df_alibaba, df_trends]:
        if not other.empty and "keyword" in other.columns:
            df = df.merge(other, on="keyword", how="left")

    # 计算价格地板
    df["price_floor"] = df.apply(
        lambda r: min(
            x for x in [r.get("shein_median_price"), r.get("temu_median_price")]
            if x is not None and x > 0
        ) if any(
            r.get(k) is not None and r.get(k, 0) > 0
            for k in ["shein_median_price", "temu_median_price"]
        ) else None,
        axis=1,
    )

    logger.info(f"[cleaner] 合并完成: {len(df)} 行, {len(df.columns)} 列")
    return df
