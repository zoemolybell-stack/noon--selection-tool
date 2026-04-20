#!/bin/bash
# 全自动流程：等待爬虫完成 → 分析 → 报告 → 输出关键发现
cd "$(dirname "$0")"

SNAPSHOT="2026-03-17_114142"
LOG="analysis_auto.log"
KEYWORD_DATA_ROOT="runtime_data/keyword"
SNAPSHOT_ROOT="$KEYWORD_DATA_ROOT/snapshots/$SNAPSHOT"

echo "========================================" | tee "$LOG"
echo "全自动分析流程启动 $(date '+%H:%M:%S')" | tee -a "$LOG"
echo "Snapshot: $SNAPSHOT" | tee -a "$LOG"
echo "========================================" | tee -a "$LOG"

# Step 1: 等待爬虫完成
echo "" | tee -a "$LOG"
echo "[$(date '+%H:%M:%S')] 等待爬虫完成..." | tee -a "$LOG"
while true; do
    NOON_DONE=$(ls "$SNAPSHOT_ROOT/noon/"*.json 2>/dev/null | wc -l | tr -d ' ')
    if [ "$NOON_DONE" -ge 900 ]; then
        # 检查爬虫进程是否还在跑
        if ! pgrep -f "keyword_main.py --step scrape" > /dev/null 2>&1; then
            echo "[$(date '+%H:%M:%S')] 爬虫已完成: Noon=$NOON_DONE" | tee -a "$LOG"
            break
        fi
    fi
    sleep 30
done

# 清理残留浏览器
pkill -f chromium 2>/dev/null
sleep 3

# Step 2: 分析
echo "" | tee -a "$LOG"
echo "[$(date '+%H:%M:%S')] ═══ 开始分析 ═══" | tee -a "$LOG"
python3 keyword_main.py --step analyze --snapshot "$SNAPSHOT" --data-root "$KEYWORD_DATA_ROOT" >> "$LOG" 2>&1
ANALYZE_EXIT=$?
echo "[$(date '+%H:%M:%S')] 分析完成 (exit=$ANALYZE_EXIT)" | tee -a "$LOG"

# Step 3: 报告
echo "" | tee -a "$LOG"
echo "[$(date '+%H:%M:%S')] ═══ 生成报告 ═══" | tee -a "$LOG"
python3 keyword_main.py --step report --snapshot "$SNAPSHOT" --data-root "$KEYWORD_DATA_ROOT" >> "$LOG" 2>&1
REPORT_EXIT=$?
echo "[$(date '+%H:%M:%S')] 报告完成 (exit=$REPORT_EXIT)" | tee -a "$LOG"

# Step 4: 产品级分析 + 关键发现
echo "" | tee -a "$LOG"
echo "[$(date '+%H:%M:%S')] ═══ 产品级分析 + 关键发现 ═══" | tee -a "$LOG"
python3 -c "
import json, sys
sys.path.insert(0, '.')
import pandas as pd
from config.settings import Settings
from config.category_mapping import map_category
from config.cost_defaults import get_defaults
from analysis.product_analyzer import analyze_products

settings = Settings()
settings.set_runtime_scope('keyword')
settings.set_data_dir('$KEYWORD_DATA_ROOT')
settings.set_snapshot_id('$SNAPSHOT')

# 读取评分结果
scored_path = settings.snapshot_dir / 'processed' / 'scored.parquet'
if not scored_path.exists():
    scored_path = settings.data_dir / 'processed' / 'scored.parquet'
df = pd.read_parquet(scored_path)

# 产品级分析
product_df = analyze_products(settings.snapshot_dir, df, settings, top_n=50, output_n=100)

# 保存产品级报告到 Excel（追加Sheet或独立文件）
from output.excel_report import generate_report
report_path = settings.snapshot_dir / f'report_{settings.snapshot_id}.xlsx'
generate_report(df, report_path, product_df=product_df)

# ═══ 输出关键发现 ═══
print()
print('=' * 70)
print('  全量扫描关键发现')
print('=' * 70)

# 总览
print(f'\n  关键词总数: {len(df)}')
if 'grade' in df.columns:
    for g in ['A','B','C','D','E']:
        c = len(df[df['grade']==g])
        if c > 0:
            print(f'  {g}级: {c} 个')

# Top 20 赛道
print(f'\n  ── Top 20 赛道 ──')
top20 = df.nsmallest(20, 'rank') if 'rank' in df.columns else df.head(20)
for _, r in top20.iterrows():
    m = r.get('margin_war_pct')
    ms = f'{m:.1f}%' if pd.notna(m) else 'N/A'
    print(f'  #{int(r[\"rank\"]):3d} [{r[\"grade\"]}] {r[\"keyword\"]:<30s} '
          f'score={r[\"total_score\"]:.2f}  margin={ms}  '
          f'noon_total={int(r.get(\"noon_total\",0)):>6,}  cat={r.get(\"v6_category\",\"\")}')

# 高利润赛道
if 'margin_war_pct' in df.columns:
    high_margin = df[df['margin_war_pct'] > 30].sort_values('margin_war_pct', ascending=False)
    print(f'\n  ── 战时利润率 > 30% 的赛道: {len(high_margin)} 个 ──')
    for _, r in high_margin.head(15).iterrows():
        print(f'  [{r[\"grade\"]}] {r[\"keyword\"]:<30s} margin={r[\"margin_war_pct\"]:.1f}%  '
              f'noon_total={int(r.get(\"noon_total\",0)):>6,}')

# 产品级 Top 20
if product_df is not None and not product_df.empty:
    print(f'\n  ── Top 20 推荐产品 ──')
    for _, p in product_df.head(20).iterrows():
        m = p.get('margin_war_pct')
        ms = f'{m:.1f}%' if pd.notna(m) else 'N/A'
        opp = p.get('opportunity_type', '')
        rel = p.get('relevance_flag', '')
        print(f'  #{int(p[\"product_rank\"]):3d} [{rel}] {p[\"title\"][:45]:<45s} '
              f'{p[\"price_sar\"]:>7.1f} SAR  margin={ms}  [{opp}]')

    # 按机会类型统计
    print(f'\n  ── 机会类型分布 ──')
    for t, c in product_df['opportunity_type'].value_counts().items():
        print(f'  {t}: {c} 条')

# 品类分布
if 'v6_category' in df.columns:
    print(f'\n  ── 品类分布 (Top 10) ──')
    for cat, c in df['v6_category'].value_counts().head(10).items():
        cat_df = df[df['v6_category']==cat]
        avg_m = cat_df['margin_war_pct'].mean() if 'margin_war_pct' in cat_df.columns else 0
        print(f'  {cat:<35s} {c:>3} 词  avg_margin={avg_m:.1f}%')

print()
print(f'  报告路径: {report_path}')
print('=' * 70)
" >> "$LOG" 2>&1

# 把关键发现也输出到独立文件方便查看
python3 -c "
with open('$LOG') as f:
    lines = f.readlines()
# 找到'全量扫描关键发现'开始的部分
start = 0
for i, l in enumerate(lines):
    if '全量扫描关键发现' in l:
        start = i
        break
with open('$SNAPSHOT_ROOT/findings.txt', 'w') as f:
    f.writelines(lines[start:])
"

echo "" | tee -a "$LOG"
echo "[$(date '+%H:%M:%S')] ✅ 全部完成！" | tee -a "$LOG"
echo "关键发现: data/snapshots/$SNAPSHOT/findings.txt" | tee -a "$LOG"
echo "Excel报告: data/snapshots/$SNAPSHOT/report_$SNAPSHOT.xlsx" | tee -a "$LOG"
