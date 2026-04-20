#!/usr/bin/env python3
"""
Noon Weekly Report Auto-Generator
==================================
Reads product analysis Excel data and generates a Markdown weekly report
with core metrics, category opportunities, high-profit recommendations,
risk alerts, and actionable suggestions.

Usage:
    python noon_weekly_report.py --data unified_automotive.xlsx --output weekly_report.md

Falls back to report_automotive_enhanced.xlsx or report_automotive.xlsx if
the specified file is not available.
"""

import sys
sys.path.insert(0, 'D:/claude noon')
from noon_config import CONFIG

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np


# ─── Column name aliases (handle variations across report versions) ───────────
COL_ALIASES = {
    'subcategory': ['_subcategory', 'subcategory', 'Subcategory', 'sub_category'],
    'price': ['price', 'noon_median_price', 'Price'],
    'margin': ['margin_war_pct', 'margin_pct', 'margin', 'profit_margin'],
    'margin_peace': ['margin_peace_pct', 'margin_peace'],
    'sold_recently': ['sold_recently', 'sold', 'units_sold', 'sales'],
    'review_count': ['review_count', 'reviews', 'num_reviews'],
    'rating': ['rating', 'avg_rating'],
    'is_express': ['is_express', 'express', 'is_Express'],
    'title': ['title', 'product_name', 'name', 'Title'],
    'product_id': ['product_id', 'sku', 'asin', 'Product ID'],
    'product_url': ['product_url', 'url', 'link'],
    'competition_score': ['competition_score', 'comp_score'],
    'opportunity_score': ['opportunity_score', 'opp_score'],
    'demand_score': ['demand_score'],
}


def resolve_col(df, key):
    """Find the actual column name in df for a logical key."""
    for candidate in COL_ALIASES.get(key, [key]):
        if candidate in df.columns:
            return candidate
    return None


def safe_col(df, key, default=None):
    """Get a column series safely, returning default series if missing."""
    col = resolve_col(df, key)
    if col is not None:
        return df[col]
    if default is not None:
        return pd.Series(default, index=df.index)
    return pd.Series(0, index=df.index)


def find_data_file(specified_path):
    """Try specified path, then fallbacks."""
    base_dir = Path('D:/claude noon')
    candidates = [
        Path(specified_path),
        base_dir / specified_path,
        base_dir / 'report_automotive_enhanced.xlsx',
        base_dir / 'report_automotive.xlsx',
        base_dir / 'noon-selection-tool' / 'data' / 'monitoring' / 'categories' / 'automotive' / 'report_automotive.xlsx',
        base_dir / 'selection_report_automotive.xlsx',
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def load_data(file_path):
    """Load Excel data, trying multiple sheets to find product-level data."""
    print(f"[INFO] Loading data from: {file_path}")
    xls = pd.ExcelFile(file_path)
    print(f"[INFO] Available sheets: {xls.sheet_names}")

    product_df = None
    subcat_df = None

    # Try to find product-level sheet (largest row count with 'price' column)
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            if len(df) == 0:
                continue
            has_price = any(c in df.columns for c in COL_ALIASES['price'])
            has_title = any(c in df.columns for c in COL_ALIASES['title'])

            if has_price and has_title:
                if product_df is None or len(df) > len(product_df):
                    product_df = df
                    print(f"[INFO] Product data found in sheet '{sheet}': {len(df)} rows, {len(df.columns)} cols")

            # Detect subcategory-level sheet
            has_subcat_name = 'Subcategory' in df.columns
            if has_subcat_name and 'opportunity_score' in df.columns:
                subcat_df = df
                print(f"[INFO] Subcategory data found in sheet '{sheet}': {len(df)} rows")
        except Exception as e:
            print(f"[WARN] Could not read sheet '{sheet}': {e}")

    if product_df is None:
        # Fallback: just use the first sheet
        product_df = pd.read_excel(xls, sheet_name=0)
        print(f"[WARN] No product sheet detected, using first sheet: {len(product_df)} rows")

    return product_df, subcat_df


def compute_core_metrics(df):
    """Compute core metrics from product-level data."""
    metrics = {}
    metrics['total_products'] = len(df)

    # Products with sales
    sold_col = resolve_col(df, 'sold_recently')
    if sold_col:
        sold = pd.to_numeric(df[sold_col], errors='coerce').fillna(0)
        metrics['products_with_sales'] = int((sold > 0).sum())
    else:
        metrics['products_with_sales'] = 'N/A'

    # Express ratio
    express_col = resolve_col(df, 'is_express')
    if express_col:
        expr = df[express_col].fillna(False).astype(bool)
        metrics['express_count'] = int(expr.sum())
        metrics['express_pct'] = round(expr.sum() / len(df) * 100, 1) if len(df) > 0 else 0
    else:
        metrics['express_count'] = 'N/A'
        metrics['express_pct'] = 'N/A'

    # Median margin
    margin_col = resolve_col(df, 'margin')
    if margin_col:
        margins = pd.to_numeric(df[margin_col], errors='coerce').dropna()
        metrics['median_margin'] = round(margins.median(), 1) if len(margins) > 0 else 'N/A'
        metrics['positive_margin_pct'] = round((margins > 0).sum() / len(margins) * 100, 1) if len(margins) > 0 else 'N/A'
        metrics['negative_margin_count'] = int((margins < 0).sum())
    else:
        metrics['median_margin'] = 'N/A'
        metrics['positive_margin_pct'] = 'N/A'
        metrics['negative_margin_count'] = 'N/A'

    # Price stats
    price_col = resolve_col(df, 'price')
    if price_col:
        prices = pd.to_numeric(df[price_col], errors='coerce').dropna()
        metrics['median_price'] = round(prices.median(), 1) if len(prices) > 0 else 'N/A'
        metrics['price_range'] = f"{prices.min():.0f} - {prices.max():.0f}" if len(prices) > 0 else 'N/A'
    else:
        metrics['median_price'] = 'N/A'
        metrics['price_range'] = 'N/A'

    return metrics


def compute_subcategory_ranking(df):
    """Compute subcategory opportunity ranking from product-level data."""
    subcat_col = resolve_col(df, 'subcategory')
    if not subcat_col:
        print("[WARN] No subcategory column found, skipping category ranking")
        return None

    margin_col = resolve_col(df, 'margin')
    sold_col = resolve_col(df, 'sold_recently')
    express_col = resolve_col(df, 'is_express')

    groups = []
    for name, grp in df.groupby(subcat_col):
        if pd.isna(name) or str(name).strip() == '':
            continue
        row = {'subcategory': name, 'product_count': len(grp)}

        # Express %
        if express_col:
            expr = grp[express_col].fillna(False).astype(bool)
            row['express_pct'] = round(expr.sum() / len(grp) * 100, 1)
        else:
            row['express_pct'] = 0

        # Median margin
        if margin_col:
            margins = pd.to_numeric(grp[margin_col], errors='coerce').dropna()
            row['median_margin'] = round(margins.median(), 1) if len(margins) > 0 else 0
        else:
            row['median_margin'] = 0

        # Sales products
        if sold_col:
            sold = pd.to_numeric(grp[sold_col], errors='coerce').fillna(0)
            row['products_with_sales'] = int((sold > 0).sum())
            row['total_sold'] = int(sold.sum())
        else:
            row['products_with_sales'] = 0
            row['total_sold'] = 0

        # Opportunity score: composite of margin, sales evidence, express gap
        margin_score = max(0, min(100, (row['median_margin'] + 20) * 2))  # -20%->0, 30%->100
        sales_score = min(100, row['products_with_sales'] * 20)
        express_gap_score = max(0, 100 - row['express_pct'])  # Less Express = more opportunity
        row['opportunity_score'] = round(margin_score * 0.4 + sales_score * 0.3 + express_gap_score * 0.3, 1)

        groups.append(row)

    if not groups:
        return None

    result = pd.DataFrame(groups)
    result = result.sort_values('opportunity_score', ascending=False).reset_index(drop=True)
    return result


def compute_high_profit_products(df, top_n=20):
    """Get top N high-profit products."""
    margin_col = resolve_col(df, 'margin')
    if not margin_col:
        return None

    title_col = resolve_col(df, 'title')
    price_col = resolve_col(df, 'price')
    subcat_col = resolve_col(df, 'subcategory')
    review_col = resolve_col(df, 'review_count')
    express_col = resolve_col(df, 'is_express')
    sold_col = resolve_col(df, 'sold_recently')

    work = df.copy()
    work['_margin'] = pd.to_numeric(work[margin_col], errors='coerce')
    work = work.dropna(subset=['_margin'])
    work = work[work['_margin'] > 0]

    # Prefer products with sales evidence
    if sold_col:
        work['_sold'] = pd.to_numeric(work[sold_col], errors='coerce').fillna(0)
        work = work.sort_values(['_sold', '_margin'], ascending=[False, False])
    else:
        work = work.sort_values('_margin', ascending=False)

    work = work.head(top_n)

    results = []
    for _, row in work.iterrows():
        item = {
            'title': str(row.get(title_col, 'N/A'))[:60] if title_col else 'N/A',
            'subcategory': str(row.get(subcat_col, 'N/A')) if subcat_col else 'N/A',
            'price': round(float(row.get(price_col, 0)), 1) if price_col else 'N/A',
            'margin': round(float(row['_margin']), 1),
            'reviews': int(row.get(review_col, 0)) if review_col else 0,
            'express': 'Yes' if (express_col and row.get(express_col)) else 'No',
        }
        results.append(item)

    return results


def compute_risk_alerts(df):
    """Identify risk items: negative margin categories, high competition."""
    alerts = {'negative_margin_cats': [], 'high_competition_cats': []}

    subcat_col = resolve_col(df, 'subcategory')
    margin_col = resolve_col(df, 'margin')

    if not subcat_col or not margin_col:
        return alerts

    for name, grp in df.groupby(subcat_col):
        if pd.isna(name) or str(name).strip() == '':
            continue
        margins = pd.to_numeric(grp[margin_col], errors='coerce').dropna()
        if len(margins) == 0:
            continue

        median_m = margins.median()
        neg_pct = (margins < 0).sum() / len(margins) * 100

        if median_m < 0:
            alerts['negative_margin_cats'].append({
                'name': name,
                'median_margin': round(median_m, 1),
                'negative_pct': round(neg_pct, 1),
                'count': len(grp),
            })

        # High competition: many products, low margin variance, high express coverage
        express_col = resolve_col(df, 'is_express')
        if express_col:
            expr_pct = grp[express_col].fillna(False).astype(bool).sum() / len(grp) * 100
            if len(grp) >= 10 and expr_pct > 70 and margins.std() < 5:
                alerts['high_competition_cats'].append({
                    'name': name,
                    'product_count': len(grp),
                    'express_pct': round(expr_pct, 1),
                    'margin_std': round(margins.std(), 1),
                })

    alerts['negative_margin_cats'].sort(key=lambda x: x['median_margin'])
    alerts['high_competition_cats'].sort(key=lambda x: -x['product_count'])

    return alerts


def generate_suggestions(metrics, subcat_ranking, risk_alerts):
    """Auto-generate weekly action suggestions based on data patterns."""
    suggestions = []

    # 1. Express coverage opportunity
    if isinstance(metrics.get('express_pct'), (int, float)) and metrics['express_pct'] < 50:
        suggestions.append(
            f"Express 覆盖率仅 {metrics['express_pct']}%，建议优先将高利润产品转为 Express 发货以提升曝光"
        )

    # 2. Negative margin warning
    if isinstance(metrics.get('negative_margin_count'), int) and metrics['negative_margin_count'] > 0:
        suggestions.append(
            f"共 {metrics['negative_margin_count']} 款产品利润为负，建议检查定价或考虑下架止损"
        )

    # 3. Top opportunity categories
    if subcat_ranking is not None and len(subcat_ranking) > 0:
        top = subcat_ranking.iloc[0]
        suggestions.append(
            f"最佳机会品类: **{top['subcategory']}** (机会评分 {top['opportunity_score']})，"
            f"建议加大选品投入"
        )

    # 4. Risk categories
    if risk_alerts['negative_margin_cats']:
        worst = risk_alerts['negative_margin_cats'][0]
        suggestions.append(
            f"高风险品类: **{worst['name']}** 中位利润 {worst['median_margin']}%，"
            f"{worst['negative_pct']}% 产品亏损，建议调价或减少库存"
        )

    # 5. High competition warning
    if risk_alerts['high_competition_cats']:
        comp = risk_alerts['high_competition_cats'][0]
        suggestions.append(
            f"红海品类: **{comp['name']}** 共 {comp['product_count']} 款产品，"
            f"Express 占比 {comp['express_pct']}%，差异化空间有限"
        )

    # 6. General advice
    if isinstance(metrics.get('median_margin'), (int, float)):
        if metrics['median_margin'] > 15:
            suggestions.append("整体利润率健康，可考虑适度扩品或加大广告投入")
        elif metrics['median_margin'] > 5:
            suggestions.append("整体利润率一般，建议优化头程成本或寻找更高利润品类")
        else:
            suggestions.append("整体利润率偏低，建议重新评估选品策略，聚焦高利润赛道")

    return suggestions


def format_report(metrics, subcat_ranking, high_profit, risk_alerts, suggestions, data_file):
    """Format everything into Markdown."""
    today = datetime.now().strftime('%Y-%m-%d')
    lines = []

    lines.append(f"# Noon 运营周报 — {today}\n")
    lines.append(f"> 数据来源: `{data_file}`\n")

    # ── Core Metrics ──
    lines.append("## 核心指标\n")
    lines.append(f"- **追踪产品数**: {metrics['total_products']}")
    lines.append(f"- **有销量产品**: {metrics['products_with_sales']}")
    lines.append(f"- **Express 产品占比**: {metrics['express_pct']}%"
                 if isinstance(metrics.get('express_pct'), (int, float))
                 else f"- **Express 产品占比**: {metrics.get('express_pct', 'N/A')}")
    lines.append(f"- **中位利润率**: {metrics['median_margin']}%"
                 if isinstance(metrics.get('median_margin'), (int, float))
                 else f"- **中位利润率**: {metrics.get('median_margin', 'N/A')}")
    lines.append(f"- **正利润产品占比**: {metrics.get('positive_margin_pct', 'N/A')}%"
                 if isinstance(metrics.get('positive_margin_pct'), (int, float))
                 else f"- **正利润产品占比**: {metrics.get('positive_margin_pct', 'N/A')}")
    lines.append(f"- **中位售价**: {metrics.get('median_price', 'N/A')} SAR")
    lines.append(f"- **价格区间**: {metrics.get('price_range', 'N/A')}")
    lines.append("")

    # ── Category Opportunity Ranking ──
    lines.append("## 品类机会排名 (Top 10)\n")
    if subcat_ranking is not None and len(subcat_ranking) > 0:
        lines.append("| 排名 | 子类目 | 产品数 | Express% | 中位利润% | 有销量产品 | 机会评分 |")
        lines.append("|------|--------|--------|----------|-----------|-----------|----------|")
        for i, row in subcat_ranking.head(10).iterrows():
            rank = i + 1
            lines.append(
                f"| {rank} | {row['subcategory']} | {row['product_count']} | "
                f"{row['express_pct']}% | {row['median_margin']}% | "
                f"{row['products_with_sales']} | {row['opportunity_score']} |"
            )
    else:
        lines.append("*数据不足，无法生成品类排名*")
    lines.append("")

    # ── High Profit Recommendations ──
    lines.append("## 高利润推荐 (Top 20)\n")
    if high_profit:
        lines.append("| 产品 | 子类目 | 价格 | 利润率 | 评论 | Express |")
        lines.append("|------|--------|------|--------|------|---------|")
        for item in high_profit:
            lines.append(
                f"| {item['title']} | {item['subcategory']} | "
                f"{item['price']} | {item['margin']}% | "
                f"{item['reviews']} | {item['express']} |"
            )
    else:
        lines.append("*无正利润产品数据*")
    lines.append("")

    # ── Risk Alerts ──
    lines.append("## 风险关注\n")
    lines.append("### 负利润品类\n")
    if risk_alerts['negative_margin_cats']:
        for cat in risk_alerts['negative_margin_cats'][:10]:
            lines.append(
                f"- **{cat['name']}**: 中位利润 {cat['median_margin']}%, "
                f"{cat['negative_pct']}% 产品亏损 ({cat['count']} 款)"
            )
    else:
        lines.append("- 无负利润品类")
    lines.append("")

    lines.append("### 高竞争品类\n")
    if risk_alerts['high_competition_cats']:
        for cat in risk_alerts['high_competition_cats'][:10]:
            lines.append(
                f"- **{cat['name']}**: {cat['product_count']} 款产品, "
                f"Express {cat['express_pct']}%, 利润标准差仅 {cat['margin_std']}%"
            )
    else:
        lines.append("- 无明显红海品类")
    lines.append("")

    # ── Suggestions ──
    lines.append("## 本周建议操作\n")
    if suggestions:
        for i, s in enumerate(suggestions, 1):
            lines.append(f"{i}. {s}")
    else:
        lines.append("- 数据不足，暂无自动建议")
    lines.append("")

    lines.append("---")
    lines.append(f"*报告自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Noon Weekly Report Generator')
    parser.add_argument('--data', default='unified_automotive.xlsx',
                        help='Input Excel data file (default: unified_automotive.xlsx)')
    parser.add_argument('--output', default='weekly_report.md',
                        help='Output Markdown file (default: weekly_report.md)')
    args = parser.parse_args()

    print("=" * 60)
    print("  Noon 运营周报生成器")
    print("=" * 60)

    # 1. Find data file
    print(f"\n[1/5] Locating data file: {args.data}")
    data_path = find_data_file(args.data)
    if data_path is None:
        print(f"[ERROR] Cannot find data file '{args.data}' or any fallback.")
        print("  Tried: unified_automotive.xlsx, report_automotive_enhanced.xlsx, report_automotive.xlsx")
        sys.exit(1)
    print(f"  -> Using: {data_path}")

    # 2. Load data
    print("\n[2/5] Loading data...")
    try:
        product_df, subcat_df = load_data(data_path)
        print(f"  -> Product data: {len(product_df)} rows, {len(product_df.columns)} columns")
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")
        sys.exit(1)

    # 3. Compute metrics
    print("\n[3/5] Computing core metrics...")
    try:
        metrics = compute_core_metrics(product_df)
        for k, v in metrics.items():
            print(f"  {k}: {v}")
    except Exception as e:
        print(f"[ERROR] Metrics computation failed: {e}")
        metrics = {'total_products': len(product_df), 'products_with_sales': 'N/A',
                   'express_pct': 'N/A', 'median_margin': 'N/A'}

    # 4. Compute rankings and recommendations
    print("\n[4/5] Computing rankings and recommendations...")
    try:
        subcat_ranking = compute_subcategory_ranking(product_df)
        if subcat_ranking is not None:
            print(f"  -> {len(subcat_ranking)} subcategories ranked")
        else:
            print("  -> No subcategory ranking available")
    except Exception as e:
        print(f"[WARN] Subcategory ranking failed: {e}")
        subcat_ranking = None

    try:
        high_profit = compute_high_profit_products(product_df)
        if high_profit:
            print(f"  -> {len(high_profit)} high-profit products found")
        else:
            print("  -> No high-profit products found")
    except Exception as e:
        print(f"[WARN] High-profit computation failed: {e}")
        high_profit = None

    try:
        risk_alerts = compute_risk_alerts(product_df)
        print(f"  -> {len(risk_alerts['negative_margin_cats'])} negative-margin categories")
        print(f"  -> {len(risk_alerts['high_competition_cats'])} high-competition categories")
    except Exception as e:
        print(f"[WARN] Risk alert computation failed: {e}")
        risk_alerts = {'negative_margin_cats': [], 'high_competition_cats': []}

    suggestions = generate_suggestions(metrics, subcat_ranking, risk_alerts)

    # 5. Generate report
    print("\n[5/5] Generating Markdown report...")
    try:
        report = format_report(metrics, subcat_ranking, high_profit, risk_alerts,
                               suggestions, data_path.name)
        output_path = Path(args.output)
        output_path.write_text(report, encoding='utf-8')
        print(f"  -> Report saved to: {output_path.absolute()}")
        print(f"  -> Report size: {len(report)} chars, {report.count(chr(10))} lines")
    except Exception as e:
        print(f"[ERROR] Report generation failed: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
