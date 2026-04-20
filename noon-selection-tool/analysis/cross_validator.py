"""
交叉比对引擎 — 需求验证、供给缺口、价格空间、竞争校准

从合并后的 DataFrame 计算衍生指标，供评分模型使用。
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def compute_cross_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算交叉比对衍生指标，新增列到 DataFrame。
    """
    # ── 需求置信度 ──
    # Google趋势上升=3分, 稳定=2分, 下降=1分, 无数据=0分
    trend_map = {"rising": 3, "stable": 2, "declining": 1, "unknown": 0}
    google_trend = df.get("google_trend", pd.Series("", index=df.index)).fillna("")
    df["_trend_score"] = google_trend.map(trend_map).fillna(0)

    google_interest = df.get("google_interest", pd.Series(0, index=df.index)).fillna(0)
    amazon_bsr_count = df.get("amazon_bsr_count", pd.Series(0, index=df.index)).fillna(0)
    amazon_total = df.get("amazon_total", pd.Series(0, index=df.index)).fillna(0)

    # 需求指数 = Google兴趣 * 趋势权重 + Amazon BSR数 * 10
    # Google Trends 为非必要数据源，全部缺失时需求指数退化为纯 Amazon 信号
    has_google = (google_interest > 0).any()
    if has_google:
        df["demand_index"] = google_interest * df["_trend_score"] + amazon_bsr_count * 10
    else:
        # 无 Google Trends 数据：用 Amazon 搜索结果数 + BSR 数替代
        logger.info("[cross_validator] Google Trends 数据缺失，需求指数退化为 Amazon 信号")
        df["demand_index"] = amazon_total * 0.01 + amazon_bsr_count * 10

    # ── 供给缺口比 ──
    # amazon_total / noon_total — 越大说明 Noon 相对 Amazon 越不饱和
    df["supply_gap_ratio"] = amazon_total / df["noon_total"].clip(lower=1)

    # ── Express 渗透率 ──
    df["express_penetration"] = (
        df.get("noon_express", pd.Series(0, index=df.index)).fillna(0)
        / df["noon_total"].clip(lower=1)
    )

    # ── 竞争密度比 ──
    # 有 Google 数据: noon商品数 / google兴趣，越小竞争越轻
    # 无 Google 数据: noon商品数 / amazon总数，同样越小越好
    if has_google:
        df["competition_density"] = df["noon_total"] / google_interest.clip(lower=1)
    else:
        logger.info("[cross_validator] 竞争密度退化为 noon_total / amazon_total")
        df["competition_density"] = df["noon_total"] / amazon_total.clip(lower=1)

    # ── 价格安全指数 ──
    # noon中位价 / price_floor — 越高说明被低价冲击风险越大
    price_floor = df.get("price_floor", pd.Series(dtype=float))
    # 如果无 SHEIN/Temu 数据，用 Amazon 中位价替代
    amazon_median = df.get("amazon_median_price", pd.Series(0, index=df.index)).fillna(0)
    df["_effective_floor"] = price_floor.fillna(amazon_median).clip(lower=0.01)
    df["price_safety_ratio"] = df["noon_median_price"] / df["_effective_floor"]

    # ── 竞品质量指数 ── 越低=竞品越弱=机会越大
    df["competitor_quality_index"] = (
        df.get("noon_avg_reviews", pd.Series(0, index=df.index)).fillna(0)
        + df.get("noon_avg_images", pd.Series(0, index=df.index)).fillna(0) * 5
    )

    # Google Trends 数据可用性标记
    df["has_google_trends"] = has_google

    # 清理临时列
    df.drop(columns=["_trend_score", "_effective_floor"], inplace=True, errors="ignore")

    logger.info(f"[cross_validator] 计算完成，共 {len(df)} 行")
    return df
