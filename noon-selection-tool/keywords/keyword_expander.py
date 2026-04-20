"""
关键词扩展器

对种子词进行联想词扩展（Noon + Amazon），去重后输出 500-1000 个候选关键词。
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def expand_keywords(
    seeds: list[str],
    noon_data_dir: Path,
    amazon_data_dir: Path,
    bsr_keywords: list[str] | None = None,
) -> list[str]:
    """
    从已爬取的 Noon/Amazon 搜索数据中提取联想词，
    合并 BSR 反向关键词，去重后返回扩展关键词列表。

    参数:
        seeds: 种子关键词列表
        noon_data_dir: Noon 原始数据目录
        amazon_data_dir: Amazon 原始数据目录
        bsr_keywords: Amazon BSR 反向提取的关键词
    """
    all_keywords = set(kw.lower().strip() for kw in seeds)

    # 1. 从 Noon 搜索结果提取联想词
    noon_suggestions = _extract_suggestions(noon_data_dir)
    all_keywords.update(noon_suggestions)
    logger.info(f"[expander] Noon 联想词: +{len(noon_suggestions)} 个")

    # 2. 从 Amazon 搜索结果提取联想词
    amazon_suggestions = _extract_suggestions(amazon_data_dir)
    all_keywords.update(amazon_suggestions)
    logger.info(f"[expander] Amazon 联想词: +{len(amazon_suggestions)} 个")

    # 3. BSR 反向关键词
    if bsr_keywords:
        bsr_clean = set(kw.lower().strip() for kw in bsr_keywords if kw.strip())
        all_keywords.update(bsr_clean)
        logger.info(f"[expander] BSR 反向关键词: +{len(bsr_clean)} 个")

    # 4. 去重（忽略大小写、简单复数处理）
    deduped = _deduplicate(all_keywords)

    logger.info(f"[expander] 去重后总计: {len(deduped)} 个关键词")
    return sorted(deduped)


def _extract_suggestions(data_dir: Path) -> set[str]:
    """从已爬取的 JSON 文件中提取联想词"""
    suggestions = set()
    if not data_dir.exists():
        return suggestions

    for json_file in data_dir.glob("*.json"):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            for kw in data.get("suggested_keywords", []):
                clean = kw.strip().lower()
                if clean and len(clean) >= 3 and clean.isascii():
                    suggestions.add(clean)
        except Exception:
            continue

    return suggestions


def _deduplicate(keywords: set[str]) -> list[str]:
    """
    关键词去重：
    - 忽略大小写（已经 lower）
    - 去除空白和无效词
    - 简单复数处理（去掉尾部 s 后如果存在则合并）
    """
    cleaned = set()
    for kw in keywords:
        kw = kw.strip()
        if not kw or len(kw) < 3:
            continue
        # 过滤纯数字
        if kw.isdigit():
            continue
        cleaned.add(kw)

    # 简单复数合并：如果 "bags" 和 "bag" 都存在，保留 "bag"
    final = set()
    for kw in sorted(cleaned):
        singular = kw.rstrip("s") if kw.endswith("s") and len(kw) > 4 else None
        if singular and singular in cleaned:
            continue  # 复数形式已有单数，跳过
        final.add(kw)

    return sorted(final)


def extract_bsr_keywords(bsr_items: list[dict]) -> list[str]:
    """
    从 Amazon BSR 热销品标题中提取品类关键词。
    简单实现：取标题中的2-3词短语。
    """
    import re

    keywords = set()
    stop_words = {
        "the", "a", "an", "and", "or", "for", "with", "in", "on", "to",
        "of", "is", "it", "at", "by", "from", "as", "set", "pack", "pcs",
        "new", "hot", "best", "top", "free", "shipping",
    }

    for item in bsr_items:
        title = item.get("title", "")
        # 清洗标题
        clean = re.sub(r'[^\w\s]', ' ', title.lower())
        words = [w for w in clean.split() if w not in stop_words and len(w) > 2 and w.isascii()]

        # 提取 2-3 词组合
        if len(words) >= 2:
            keywords.add(" ".join(words[:2]))
        if len(words) >= 3:
            keywords.add(" ".join(words[:3]))

    return sorted(keywords)
