#!/bin/bash
# 自动续跑包装器 — 进程意外退出后自动用同一个 snapshot 继续
# 用法: ./run_scrape.sh [snapshot_id]
#   新扫描: ./run_scrape.sh
#   续跑:   ./run_scrape.sh 2026-03-17_114142

cd "$(dirname "$0")"

SNAPSHOT_ID="$1"
LOG_FILE="scrape_auto.log"
KEYWORD_DATA_ROOT="runtime_data/keyword"
SNAPSHOT_ROOT="$KEYWORD_DATA_ROOT/snapshots"
MAX_RETRIES=20          # 最多自动重启次数
RETRY_WAIT=10           # 重启前等待秒数

# 第一次运行时获取 snapshot_id
if [ -z "$SNAPSHOT_ID" ]; then
    # 启动一次获取 snapshot_id
    python3 -c "
from config.settings import Settings
s = Settings()
s.set_runtime_scope('keyword')
print(s.snapshot_id)
" > /tmp/noon_snap_id.txt 2>/dev/null
    SNAPSHOT_ID=$(cat /tmp/noon_snap_id.txt)
    echo "新建快照: $SNAPSHOT_ID"
fi

echo "========================================" | tee -a "$LOG_FILE"
echo "自动续跑模式启动" | tee -a "$LOG_FILE"
echo "Snapshot: $SNAPSHOT_ID" | tee -a "$LOG_FILE"
echo "日志: $LOG_FILE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

for i in $(seq 1 $MAX_RETRIES); do
    echo "" | tee -a "$LOG_FILE"
    echo "[$(date '+%H:%M:%S')] 第 ${i} 次运行..." | tee -a "$LOG_FILE"

    python3 keyword_main.py --step scrape --snapshot "$SNAPSHOT_ID" --data-root "$KEYWORD_DATA_ROOT" >> "$LOG_FILE" 2>&1
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date '+%H:%M:%S')] ✅ 爬取正常完成！" | tee -a "$LOG_FILE"
        break
    fi

    # 检查是否还有未完成的关键词
    NOON_DONE=$(ls "$SNAPSHOT_ROOT/$SNAPSHOT_ID/noon/"*.json 2>/dev/null | wc -l | tr -d ' ')
    AMAZON_DONE=$(ls "$SNAPSHOT_ROOT/$SNAPSHOT_ID/amazon/"*.json 2>/dev/null | wc -l | tr -d ' ')
    echo "[$(date '+%H:%M:%S')] 退出码=$EXIT_CODE, Noon=$NOON_DONE/905, Amazon=$AMAZON_DONE/905" | tee -a "$LOG_FILE"

    if [ "$NOON_DONE" -ge 905 ] && [ "$AMAZON_DONE" -ge 905 ]; then
        echo "[$(date '+%H:%M:%S')] ✅ 所有数据已完成（可能是后续步骤报错）" | tee -a "$LOG_FILE"
        break
    fi

    # 清理残留浏览器进程
    pkill -f chromium 2>/dev/null

    echo "[$(date '+%H:%M:%S')] ${RETRY_WAIT}秒后自动续跑..." | tee -a "$LOG_FILE"
    sleep $RETRY_WAIT
done

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "最终结果: Noon=$NOON_DONE, Amazon=$AMAZON_DONE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
