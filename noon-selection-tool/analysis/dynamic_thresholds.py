"""
动态分档阈值 — 基于全量数据的百分位分布

核心原则：分档标准来自数据分布，不预设固定数字。
使用 P20/P40/P60/P80 作为 E/D/C/B/A 五档分界线。
"""
import pandas as pd


def compute_thresholds(series: pd.Series) -> dict:
    """
    计算一个指标的动态分档阈值。

    返回:
        {"p20": x, "p40": x, "p60": x, "p80": x}
    """
    clean = series.dropna()
    if clean.empty:
        return {"p20": 0, "p40": 0, "p60": 0, "p80": 0}

    return {
        "p20": float(clean.quantile(0.20)),
        "p40": float(clean.quantile(0.40)),
        "p60": float(clean.quantile(0.60)),
        "p80": float(clean.quantile(0.80)),
    }


def score_value(
    value: float | None,
    thresholds: dict,
    higher_is_better: bool = True,
) -> int:
    """
    根据动态阈值对单个值评分（1-10）。

    higher_is_better=True:  值越大分越高（如利润率）
    higher_is_better=False: 值越小分越高（如竞争密度）

    分档:
      A (9-10): > P80
      B (7-8):  P60-P80
      C (5-6):  P40-P60
      D (3-4):  P20-P40
      E (1-2):  < P20
    """
    if value is None:
        return 5  # 无数据给中间分

    p20 = thresholds["p20"]
    p40 = thresholds["p40"]
    p60 = thresholds["p60"]
    p80 = thresholds["p80"]

    if higher_is_better:
        if value >= p80:
            return 10
        elif value >= p60:
            return 8
        elif value >= p40:
            return 5
        elif value >= p20:
            return 3
        else:
            return 1
    else:
        # 反转：值越小越好
        if value <= p20:
            return 10
        elif value <= p40:
            return 8
        elif value <= p60:
            return 5
        elif value <= p80:
            return 3
        else:
            return 1
