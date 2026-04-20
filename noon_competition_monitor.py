#!/usr/bin/env python3
"""
Noon Competition Monitoring & Alerting Tool
=============================================
Compares two report snapshots (current vs baseline) to detect competitive
changes, or generates a snapshot analysis from a single report.

Usage:
    # Two-report comparison:
    python noon_competition_monitor.py --current report_new.xlsx --baseline report_old.xlsx --output alerts.md

    # Single-report snapshot analysis:
    python noon_competition_monitor.py --current report_new.xlsx --output alerts.md

Monitoring dimensions:
    1. New competitor detection (Express count changes, new high-review sellers)
    2. Price war detection (median price drops, price variance increases)
    3. Demand changes (sold_recently growth, review acceleration)
    4. Opportunity detection (Express coverage gaps, price increases without competition)
"""

import sys
sys.path.insert(0, 'D:/claude noon')
from noon_config import CONFIG

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np


# ─── Column aliases ───────────────────────────────────────────────────────────
COL_ALIASES = {
    'subcategory': ['_subcategory', 'subcategory', 'Subcategory', 'sub_category'],
    'price': ['price', 'noon_median_price', 'Price'],
    'margin': ['margin_war_pct', 'margin_pct', 'margin', 'profit_margin'],
    'sold_recently': ['sold_recently', 'sold', 'units_sold', 'sales'],
    'review_count': ['review_count', 'reviews', 'num_reviews'],
    'rating': ['rating', 'avg_rating'],
    'is_express': ['is_express', 'express', 'is_Express'],
    'title': ['title', 'product_name', 'name', 'Title'],
    'product_id': ['product_id', 'sku', 'asin', 'Product ID'],
    'seller_name': ['seller_name', 'seller', 'brand'],
}


def resolve_col(df, key):
    """Find actual column name for a logical key."""
    for candidate in COL_ALIASES.get(key, [key]):
        if candidate in df.columns:
            return candidate
    return None


def load_report(file_path):
    """Load product-level data from an Excel report."""
    path = Path(file_path)
    if not path.exists():
        # Try with base dir prefix
        path = Path('D:/claude noon') / file_path
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {file_path}")

    print(f"[INFO] Loading: {path}")
    xls = pd.ExcelFile(path)
    print(f"[INFO] Sheets: {xls.sheet_names}")

    best_df = None
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            if len(df) == 0:
                continue
            has_price = any(c in df.columns for c in COL_ALIASES['price'])
            has_title = any(c in df.columns for c in COL_ALIASES['title'])
            if has_price and has_title:
                if best_df is None or len(df) > len(best_df):
                    best_df = df
                    print(f"  -> Product data in '{sheet}': {len(df)} rows")
        except Exception:
            pass

    if best_df is None:
        best_df = pd.read_excel(xls, sheet_name=0)
        print(f"  -> Fallback to first sheet: {len(best_df)} rows")

    return best_df, path.name


def safe_numeric(series):
    """Convert to numeric, filling NaN with 0."""
    return pd.to_numeric(series, errors='coerce').fillna(0)


def subcategory_stats(df):
    """Compute per-subcategory statistics."""
    subcat_col = resolve_col(df, 'subcategory')
    if not subcat_col:
        return pd.DataFrame()

    price_col = resolve_col(df, 'price')
    margin_col = resolve_col(df, 'margin')
    sold_col = resolve_col(df, 'sold_recently')
    review_col = resolve_col(df, 'review_count')
    express_col = resolve_col(df, 'is_express')

    rows = []
    for name, grp in df.groupby(subcat_col):
        if pd.isna(name) or str(name).strip() == '':
            continue
        stat = {'subcategory': name, 'count': len(grp)}

        if price_col:
            prices = safe_numeric(grp[price_col])
            stat['median_price'] = round(prices.median(), 2)
            stat['price_std'] = round(prices.std(), 2) if len(prices) > 1 else 0
            stat['min_price'] = round(prices.min(), 2)
            stat['max_price'] = round(prices.max(), 2)

        if margin_col:
            margins = safe_numeric(grp[margin_col])
            stat['median_margin'] = round(margins.median(), 1)
            stat['negative_margin_pct'] = round((margins < 0).sum() / len(grp) * 100, 1)

        if sold_col:
            sold = safe_numeric(grp[sold_col])
            stat['total_sold'] = int(sold.sum())
            stat['products_with_sales'] = int((sold > 0).sum())

        if review_col:
            reviews = safe_numeric(grp[review_col])
            stat['total_reviews'] = int(reviews.sum())
            stat['avg_reviews'] = round(reviews.mean(), 1)

        if express_col:
            expr = grp[express_col].fillna(False).astype(bool)
            stat['express_count'] = int(expr.sum())
            stat['express_pct'] = round(expr.sum() / len(grp) * 100, 1)

        rows.append(stat)

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# Comparison Analysis (two reports)
# ═══════════════════════════════════════════════════════════════════════════════

def detect_new_competitors(current_stats, baseline_stats):
    """Detect Express product count changes and new high-review sellers."""
    alerts = []
    if current_stats.empty or baseline_stats.empty:
        return alerts

    merged = current_stats.merge(baseline_stats, on='subcategory', suffixes=('_cur', '_base'), how='outer')

    for _, row in merged.iterrows():
        subcat = row['subcategory']

        # New subcategory appeared
        if pd.isna(row.get('count_base')):
            alerts.append({
                'priority': 'HIGH',
                'type': 'New Subcategory',
                'message': f"新子类目出现: **{subcat}** ({int(row.get('count_cur', 0))} 款产品)",
            })
            continue

        # Express count increase
        expr_cur = row.get('express_count_cur', 0) or 0
        expr_base = row.get('express_count_base', 0) or 0
        if expr_base > 0 and expr_cur > expr_base * 1.3:
            alerts.append({
                'priority': 'MEDIUM',
                'type': 'Express Increase',
                'message': (f"**{subcat}**: Express 产品从 {int(expr_base)} 增至 "
                            f"{int(expr_cur)} (+{int(expr_cur - expr_base)})"),
            })

        # Review acceleration
        rev_cur = row.get('avg_reviews_cur', 0) or 0
        rev_base = row.get('avg_reviews_base', 0) or 0
        if rev_base > 0 and rev_cur > rev_base * 1.5:
            alerts.append({
                'priority': 'MEDIUM',
                'type': 'Review Surge',
                'message': (f"**{subcat}**: 平均评论从 {rev_base:.0f} 增至 "
                            f"{rev_cur:.0f} (+{((rev_cur/rev_base)-1)*100:.0f}%)"),
            })

    return alerts


def detect_price_wars(current_stats, baseline_stats):
    """Detect subcategory median price drops and price variance increases."""
    alerts = []
    if current_stats.empty or baseline_stats.empty:
        return alerts

    merged = current_stats.merge(baseline_stats, on='subcategory', suffixes=('_cur', '_base'), how='inner')

    for _, row in merged.iterrows():
        subcat = row['subcategory']

        # Median price drop > 10%
        p_cur = row.get('median_price_cur', 0) or 0
        p_base = row.get('median_price_base', 0) or 0
        if p_base > 0 and p_cur < p_base * 0.9:
            drop_pct = (1 - p_cur / p_base) * 100
            alerts.append({
                'priority': 'HIGH',
                'type': 'Price Drop',
                'message': (f"**{subcat}**: 中位价从 {p_base:.1f} 降至 "
                            f"{p_cur:.1f} (-{drop_pct:.1f}%)，可能存在价格战"),
            })

        # Price variance increase > 50%
        std_cur = row.get('price_std_cur', 0) or 0
        std_base = row.get('price_std_base', 0) or 0
        if std_base > 5 and std_cur > std_base * 1.5:
            alerts.append({
                'priority': 'MEDIUM',
                'type': 'Price Dispersion',
                'message': (f"**{subcat}**: 价格离散度从 {std_base:.1f} 增至 "
                            f"{std_cur:.1f}，市场定价混乱"),
            })

    return alerts


def detect_demand_changes(current_stats, baseline_stats):
    """Detect sold_recently growth and review acceleration."""
    alerts = []
    if current_stats.empty or baseline_stats.empty:
        return alerts

    merged = current_stats.merge(baseline_stats, on='subcategory', suffixes=('_cur', '_base'), how='inner')

    for _, row in merged.iterrows():
        subcat = row['subcategory']

        # Sales growth > 30%
        sold_cur = row.get('total_sold_cur', 0) or 0
        sold_base = row.get('total_sold_base', 0) or 0
        if sold_base > 0 and sold_cur > sold_base * 1.3:
            growth = (sold_cur / sold_base - 1) * 100
            alerts.append({
                'priority': 'MEDIUM' if growth < 100 else 'HIGH',
                'type': 'Demand Growth',
                'message': (f"**{subcat}**: 近期销量从 {int(sold_base)} 增至 "
                            f"{int(sold_cur)} (+{growth:.0f}%)"),
            })
        elif sold_base > 0 and sold_cur < sold_base * 0.5:
            drop = (1 - sold_cur / sold_base) * 100
            alerts.append({
                'priority': 'MEDIUM',
                'type': 'Demand Decline',
                'message': (f"**{subcat}**: 近期销量从 {int(sold_base)} 降至 "
                            f"{int(sold_cur)} (-{drop:.0f}%)"),
            })

    return alerts


def detect_opportunities_comparison(current_stats, baseline_stats):
    """Detect Express coverage gaps and price increases without competition."""
    alerts = []
    if current_stats.empty or baseline_stats.empty:
        return alerts

    merged = current_stats.merge(baseline_stats, on='subcategory', suffixes=('_cur', '_base'), how='inner')

    for _, row in merged.iterrows():
        subcat = row['subcategory']

        # Low Express coverage with demand evidence
        expr_pct = row.get('express_pct_cur', 0) or 0
        sold = row.get('total_sold_cur', 0) or 0
        margin = row.get('median_margin_cur', 0) or 0
        if expr_pct < 30 and sold > 0 and margin > 10:
            alerts.append({
                'priority': 'OPP',
                'type': 'Express Gap',
                'message': (f"**{subcat}**: Express 仅 {expr_pct:.0f}%，但有销量 "
                            f"({int(sold)}) 且利润 {margin:.1f}%，可切入 Express"),
            })

        # Price increase without competition increase
        p_cur = row.get('median_price_cur', 0) or 0
        p_base = row.get('median_price_base', 0) or 0
        cnt_cur = row.get('count_cur', 0) or 0
        cnt_base = row.get('count_base', 0) or 0
        if p_base > 0 and p_cur > p_base * 1.1 and cnt_cur <= cnt_base:
            alerts.append({
                'priority': 'OPP',
                'type': 'Price Rise',
                'message': (f"**{subcat}**: 中位价上涨 {((p_cur/p_base)-1)*100:.1f}% "
                            f"但竞品数未增加 ({int(cnt_base)}->{int(cnt_cur)})，利润空间扩大"),
            })

    return alerts


# ═══════════════════════════════════════════════════════════════════════════════
# Snapshot Analysis (single report)
# ═══════════════════════════════════════════════════════════════════════════════

def snapshot_analysis(df, stats):
    """Generate alerts from a single snapshot."""
    alerts_high = []
    alerts_medium = []
    alerts_opp = []

    subcat_col = resolve_col(df, 'subcategory')
    margin_col = resolve_col(df, 'margin')
    express_col = resolve_col(df, 'is_express')
    price_col = resolve_col(df, 'price')
    sold_col = resolve_col(df, 'sold_recently')
    review_col = resolve_col(df, 'review_count')

    for _, row in stats.iterrows():
        subcat = row['subcategory']

        # HIGH: Negative median margin
        median_m = row.get('median_margin', None)
        if median_m is not None and median_m < -5:
            alerts_high.append({
                'type': 'Negative Margin',
                'message': (f"**{subcat}**: 中位利润 {median_m:.1f}%，"
                            f"{row.get('negative_margin_pct', 0):.0f}% 产品亏损"),
            })

        # HIGH: Extreme price dispersion (possible dumping)
        price_std = row.get('price_std', 0) or 0
        median_price = row.get('median_price', 1) or 1
        cv = price_std / median_price if median_price > 0 else 0
        if cv > 1.0 and row.get('count', 0) >= 5:
            alerts_high.append({
                'type': 'Price Chaos',
                'message': (f"**{subcat}**: 价格变异系数 {cv:.2f} (中位价 {median_price:.0f}, "
                            f"标准差 {price_std:.0f})，市场定价极度混乱"),
            })

        # MEDIUM: High competition (many products, high express)
        expr_pct = row.get('express_pct', 0) or 0
        count = row.get('count', 0) or 0
        if count >= 20 and expr_pct > 60:
            alerts_medium.append({
                'type': 'High Competition',
                'message': (f"**{subcat}**: {count} 款产品，Express 占比 {expr_pct:.0f}%，竞争激烈"),
            })

        # MEDIUM: Low margin with high sales (margin squeeze)
        if median_m is not None and -5 <= median_m < 5 and row.get('total_sold', 0) > 0:
            alerts_medium.append({
                'type': 'Margin Squeeze',
                'message': (f"**{subcat}**: 有销量但中位利润仅 {median_m:.1f}%，利润空间受挤压"),
            })

        # OPP: Low Express coverage with sales
        if expr_pct < 30 and row.get('products_with_sales', 0) > 0:
            alerts_opp.append({
                'type': 'Express Gap',
                'message': (f"**{subcat}**: Express 仅 {expr_pct:.0f}%，"
                            f"但 {row.get('products_with_sales', 0)} 款有销量，Express 切入机会"),
            })

        # OPP: High margin, low competition
        if median_m is not None and median_m > 20 and count < 15:
            alerts_opp.append({
                'type': 'Blue Ocean',
                'message': (f"**{subcat}**: 中位利润 {median_m:.1f}%，"
                            f"仅 {count} 款产品，蓝海品类"),
            })

    return alerts_high, alerts_medium, alerts_opp


# ═══════════════════════════════════════════════════════════════════════════════
# Report Formatting
# ═══════════════════════════════════════════════════════════════════════════════

def format_alerts_report(alerts_high, alerts_medium, alerts_opp,
                         mode, current_name, baseline_name=None):
    """Format all alerts into Markdown."""
    today = datetime.now().strftime('%Y-%m-%d')
    lines = []

    lines.append(f"# Noon 竞争监控报告 — {today}\n")
    if mode == 'comparison':
        lines.append(f"> 对比模式: `{baseline_name}` -> `{current_name}`\n")
    else:
        lines.append(f"> 快照模式: `{current_name}`\n")

    # Summary
    total = len(alerts_high) + len(alerts_medium) + len(alerts_opp)
    lines.append(f"**总计发现 {total} 条警报**: "
                 f"{len(alerts_high)} 高优先级, "
                 f"{len(alerts_medium)} 中优先级, "
                 f"{len(alerts_opp)} 机会\n")

    # HIGH Priority
    lines.append("## \U0001f534 High Priority 高优先级警报\n")
    if alerts_high:
        for a in alerts_high:
            tag = a.get('type', 'Alert')
            lines.append(f"- **[{tag}]** {a['message']}")
    else:
        lines.append("- 无高优先级警报")
    lines.append("")

    # MEDIUM Priority
    lines.append("## \U0001f7e1 Medium Priority 中优先级警报\n")
    if alerts_medium:
        for a in alerts_medium:
            tag = a.get('type', 'Alert')
            lines.append(f"- **[{tag}]** {a['message']}")
    else:
        lines.append("- 无中优先级警报")
    lines.append("")

    # Opportunities
    lines.append("## \U0001f7e2 Opportunities 机会发现\n")
    if alerts_opp:
        for a in alerts_opp:
            tag = a.get('type', 'Opportunity')
            lines.append(f"- **[{tag}]** {a['message']}")
    else:
        lines.append("- 无明显机会")
    lines.append("")

    # Market config reference
    lines.append("## 参考配置\n")
    lines.append("| 市场 | 货币 | VAT | 汇率(→CNY) |")
    lines.append("|------|------|-----|-----------|")
    for market, cfg in CONFIG.items():
        lines.append(f"| {market} | {cfg['currency']} | {cfg['vat_rate']*100:.0f}% | {cfg['exchange_rate']} |")
    lines.append("")

    lines.append("---")
    lines.append(f"*报告自动生成于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Noon Competition Monitoring & Alerting')
    parser.add_argument('--current', required=True,
                        help='Current (newer) report Excel file')
    parser.add_argument('--baseline', default=None,
                        help='Baseline (older) report Excel file for comparison (optional)')
    parser.add_argument('--output', default='alerts.md',
                        help='Output Markdown file (default: alerts.md)')
    args = parser.parse_args()

    print("=" * 60)
    print("  Noon 竞争监控工具")
    print("=" * 60)

    # 1. Load current report
    print(f"\n[1/4] Loading current report: {args.current}")
    try:
        current_df, current_name = load_report(args.current)
        print(f"  -> {len(current_df)} products loaded")
    except Exception as e:
        print(f"[ERROR] Cannot load current report: {e}")
        sys.exit(1)

    # 2. Compute current stats
    print("\n[2/4] Computing subcategory statistics...")
    current_stats = subcategory_stats(current_df)
    print(f"  -> {len(current_stats)} subcategories analyzed")

    # 3. Compare or snapshot
    mode = 'comparison' if args.baseline else 'snapshot'
    baseline_name = None

    if mode == 'comparison':
        print(f"\n[3/4] Loading baseline report: {args.baseline}")
        try:
            baseline_df, baseline_name = load_report(args.baseline)
            print(f"  -> {len(baseline_df)} products loaded")
        except Exception as e:
            print(f"[WARN] Cannot load baseline report: {e}")
            print("  -> Falling back to snapshot mode")
            mode = 'snapshot'

    if mode == 'comparison':
        print("\n[3/4] Running comparison analysis...")
        baseline_stats = subcategory_stats(baseline_df)
        print(f"  -> Baseline: {len(baseline_stats)} subcategories")

        # Run all detection dimensions
        alerts_comp = detect_new_competitors(current_stats, baseline_stats)
        alerts_price = detect_price_wars(current_stats, baseline_stats)
        alerts_demand = detect_demand_changes(current_stats, baseline_stats)
        alerts_opp_list = detect_opportunities_comparison(current_stats, baseline_stats)

        # Classify by priority
        alerts_high = [a for a in (alerts_comp + alerts_price + alerts_demand)
                       if a.get('priority') == 'HIGH']
        alerts_medium = [a for a in (alerts_comp + alerts_price + alerts_demand)
                         if a.get('priority') == 'MEDIUM']
        alerts_opp = alerts_opp_list

        print(f"  -> Competitor alerts: {len(alerts_comp)}")
        print(f"  -> Price war alerts: {len(alerts_price)}")
        print(f"  -> Demand change alerts: {len(alerts_demand)}")
        print(f"  -> Opportunity alerts: {len(alerts_opp)}")
    else:
        print("\n[3/4] Running snapshot analysis (single report)...")
        alerts_high, alerts_medium, alerts_opp = snapshot_analysis(current_df, current_stats)
        print(f"  -> High priority: {len(alerts_high)}")
        print(f"  -> Medium priority: {len(alerts_medium)}")
        print(f"  -> Opportunities: {len(alerts_opp)}")

    # 4. Generate report
    print(f"\n[4/4] Generating alert report...")
    try:
        report = format_alerts_report(
            alerts_high, alerts_medium, alerts_opp,
            mode, current_name, baseline_name
        )
        output_path = Path(args.output)
        output_path.write_text(report, encoding='utf-8')
        print(f"  -> Report saved to: {output_path.absolute()}")
        print(f"  -> {len(alerts_high)} HIGH / {len(alerts_medium)} MEDIUM / {len(alerts_opp)} OPP")
    except Exception as e:
        print(f"[ERROR] Report generation failed: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
