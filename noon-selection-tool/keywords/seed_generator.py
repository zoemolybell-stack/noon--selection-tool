"""
种子关键词生成器

从 Noon 品类导航树爬取2级/3级品类英文名 → ~100-150 种子词
"""
import logging
import re

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

NOON_BASE = "https://www.noon.com/saudi-en"


def _is_valid_seed(text: str) -> bool:
    """过滤明显不是品类/搜索关键词的垃圾文本"""
    if not text or len(text) < 3 or len(text) > 40:
        return False
    # URL 参数/编码/品牌前缀
    if text.startswith("?") or text.startswith("~") or text.startswith("@"):
        return False
    if "%" in text:
        return False
    # 过长（>5个词大概率是商品标题不是品类词）
    if len(text.split()) > 5:
        return False
    # 数字开头
    if text[0].isdigit():
        return False
    # 特殊字符
    if any(c in text for c in "=&[]{}|<>@"):
        return False
    # 纯非ASCII
    if not text.isascii():
        return False
    # 包含 Noon 内部品类ID（如 "accessories 16176"）
    if re.search(r'\b\d{4,}\b', text):
        return False
    # 单个词（大概率是品牌名而非品类搜索词，除非在 fallback 列表中）
    # 不在这里过滤，留给去重阶段
    return True


async def generate_seeds() -> list[str]:
    """
    爬取 Noon 品类导航树，提取种子关键词。
    返回去重后的英文关键词列表。
    """
    seeds = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-9999,-9999",
            ],
        )
        ctx = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        stealth = Stealth()
        await stealth.apply_stealth_async(ctx)
        page = await ctx.new_page()

        try:
            await page.goto(NOON_BASE, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # 用 JS 批量提取所有链接的文本和 href（比逐个 locator 快100倍）
            link_data = await page.evaluate("""() => {
                const results = [];
                const links = document.querySelectorAll('nav a, [class*="category"] a, [class*="menu"] a, a[href*="/saudi-en/"]');
                const seen = new Set();
                for (const a of links) {
                    const href = a.getAttribute('href') || '';
                    const text = a.innerText.trim();
                    const key = href + '|' + text;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    results.push({text, href});
                }
                return results;
            }""")

            logger.info(f"[seed_generator] 提取到 {len(link_data)} 个链接")

            for item in link_data:
                text = item.get("text", "").strip()
                href = item.get("href", "")

                # 从链接文本提取品类名
                if text and len(text) <= 40:
                    # 清洗阿拉伯文
                    clean = re.sub(r'[^\x00-\x7F]+', '', text).strip()
                    clean = re.sub(r'\s+', ' ', clean).strip().lower()

                    skip_words = [
                        "sign in", "account", "cart", "sell", "help",
                        "track", "download", "noon", "home",
                        "view all", "shop now", "see all", "more",
                    ]
                    if clean and not any(sw in clean for sw in skip_words):
                        if _is_valid_seed(clean):
                            seeds.add(clean)

                # 从 URL 路径提取品类词
                if href and "/saudi-en/" in href:
                    path_part = href.split("?")[0].split("#")[0]
                    after = path_part.split("/saudi-en/")[-1]
                    parts = [p for p in after.rstrip("/").split("/") if p]
                    for part in parts:
                        if part.startswith("~") or part.startswith("?") or part.startswith("%"):
                            continue
                        # 跳过看起来像产品 ID 的路径段（含数字+字母混合的长串）
                        if re.match(r'^[a-z0-9]{20,}$', part):
                            continue
                        clean = part.replace("-", " ").replace("_", " ").lower()
                        if _is_valid_seed(clean):
                            seeds.add(clean)

        except Exception as e:
            logger.error(f"[seed_generator] Noon 品类树爬取失败: {e}")
        finally:
            await browser.close()

    # 添加常见中东电商品类作为备用种子
    fallback_seeds = [
        "kitchen organizer", "storage boxes", "phone case", "earphones",
        "laptop bag", "perfume", "makeup", "skincare", "hair care",
        "kitchen utensils", "bedding", "curtains", "bathroom accessories",
        "toys", "baby products", "sports", "fitness", "outdoor",
        "watches", "sunglasses", "jewelry", "bags", "shoes",
        "home decor", "lighting", "furniture", "cleaning supplies",
        "pet supplies", "office supplies", "stationery",
        "gaming accessories", "camera accessories", "smart watch",
        "travel accessories", "car accessories", "tools",
        "water bottle", "lunch box", "backpack", "wallet",
    ]
    for s in fallback_seeds:
        seeds.add(s.lower())

    result = sorted(seeds)
    logger.info(f"[seed_generator] 生成 {len(result)} 个种子关键词")
    return result
