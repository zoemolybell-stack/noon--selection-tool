"""
Noon 类目树爬取工具 - 实时从 Noon 网站爬取最新类目结构

功能:
- 访问 Noon 各一级类目页面
- 从左侧 Category 筛选器提取多级类目结构
- 输出为 JSON 格式 (category_tree.json)

使用方式:
1. 命令行：python category_tree_crawler.py --output config/category_tree.json
2. GUI: python category_mapper_gui.py
"""
import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Any
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

# Noon 一级类目列表 (从首页导航栏提取)
NOON_CATEGORIES = [
    ("electronics", "Electronics & Mobiles", "https://www.noon.com/saudi-en/electronics-and-mobiles/"),
    ("fashion", "Fashion", "https://www.noon.com/saudi-en/fashion/"),
    ("beauty", "Beauty & Personal Care", "https://www.noon.com/saudi-en/beauty/"),
    ("home_kitchen", "Home & Kitchen", "https://www.noon.com/saudi-en/home-and-kitchen/"),
    ("baby", "Baby, Kids & Toys", "https://www.noon.com/saudi-en/baby-kids-and-toys/"),
    ("sports", "Sports & Fitness", "https://www.noon.com/saudi-en/sports-and-fitness/"),
    ("grocery", "Grocery & Food", "https://www.noon.com/saudi-en/grocery-and-food/"),
    ("pets", "Pet Supplies", "https://www.noon.com/saudi-en/pet-supplies/"),
    ("automotive", "Automotive", "https://www.noon.com/saudi-en/automotive/"),
    ("office", "Office & Stationery", "https://www.noon.com/saudi-en/office-and-stationery/"),
    ("tools", "Tools & Home Improvement", "https://www.noon.com/saudi-en/tools-and-home-improvement/"),
    ("garden", "Garden & Outdoor", "https://www.noon.com/saudi-en/garden-and-outdoor/"),
    ("health", "Health & Nutrition", "https://www.noon.com/saudi-en/health-and-nutrition/"),
    ("toys", "Toys & Games", "https://www.noon.com/saudi-en/toys-and-games/"),
]


class CategoryTreeCrawler:
    """Noon 类目树爬取器"""

    def __init__(self, timeout: int = 30000, max_depth: int = 3):
        self.timeout = timeout
        self.max_depth = max_depth
        self.category_tree = {}

    async def crawl_category_page(self, page: Page, url: str) -> List[Dict]:
        """
        从类目页面左侧 Category 筛选器提取子类目

        Args:
            page: Playwright page 对象
            url: 类目页面 URL

        Returns:
            子类目列表
        """
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)
            await page.wait_for_timeout(3000)

            # 滚动页面加载动态内容
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(1000)

            # 提取 Category 筛选器中的所有类目
            categories = await page.evaluate("""() => {
                const container = document.querySelector('[data-qa="Category"]');
                if (!container) return [];

                const results = [];

                // 提取所有一级类目按钮
                const level1Buttons = container.querySelectorAll(
                    ':scope > ul > li > button, ' +
                    ':scope > ul > li > div > button'
                );

                for (const btn of level1Buttons) {
                    const nameEl = btn.querySelector('[class*="name"], span');
                    const name = nameEl ? nameEl.innerText.trim() : '';

                    if (name && name.length > 1) {
                        const item = {
                            name_en: name,
                            children: []
                        };

                        // 检查是否有展开的子类目
                        const parentLi = btn.closest('li');
                        if (parentLi) {
                            // 提取直接子级目的链接和名称
                            const subList = parentLi.querySelector('ul');
                            if (subList) {
                                const subItems = subList.querySelectorAll('li > a, li > button');
                                for (const subBtn of subItems) {
                                    const subNameEl = subBtn.querySelector('[class*="name"], span');
                                    const subName = subNameEl ? subNameEl.innerText.trim() : '';

                                    if (subName && subName.length > 1 && subName !== name) {
                                        item.children.push({
                                            name_en: subName
                                        });
                                    }
                                }
                            }
                        }

                        results.push(item);
                    }
                }

                return results;
            }""")

            return categories

        except Exception as e:
            logger.debug(f"爬取类目页失败 {url}: {e}")
            return []

    async def crawl_all_categories(self) -> Dict:
        """
        爬取所有 primary 类目的子类目结构

        Returns:
            完整的类目树结构
        """
        result = {
            "name": "Noon 平台类目树",
            "version": "2.1",
            "updated_at": "",
            "description": "从 Noon 网站实时爬取的类目结构",
            "categories": []
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            for cat_id, cat_name, cat_url in NOON_CATEGORIES:
                logger.info(f"正在爬取：{cat_name} ({cat_id})")

                category_data = {
                    "id": cat_id,
                    "name_en": cat_name,
                    "name_zh": "",  # 中文名称需要手动翻译
                    "children": []
                }

                # 爬取子类目
                sub_categories = await self.crawl_category_page(page, cat_url)

                for sub in sub_categories:
                    sub_item = {
                        "id": sub.get("name_en", "").lower().replace(" & ", "_").replace(" ", "_"),
                        "name_en": sub.get("name_en", ""),
                        "name_zh": "",
                        "children": sub.get("children", [])
                    }
                    category_data["children"].append(sub_item)

                result["categories"].append(category_data)

                # 类目间间隔
                await asyncio.sleep(1)

            await browser.close()

        result["updated_at"] = asyncio.get_event_loop().time()
        from datetime import datetime
        result["updated_at"] = datetime.now().strftime("%Y-%m-%d")

        self.category_tree = result
        return result

    def save_to_json(self, output_path: Path):
        """保存类目树到 JSON 文件"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(self.category_tree, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        logger.info(f"类目树已保存到：{output_path}")


async def crawl_category_tree(output_path: Path = None, max_depth: int = 3) -> Dict:
    """
    便捷函数：爬取 Noon 类目树并保存

    Args:
        output_path: 输出文件路径
        max_depth: 最大深度

    Returns:
        类目树字典
    """
    crawler = CategoryTreeCrawler(max_depth=max_depth)
    result = await crawler.crawl_all_categories()

    if output_path:
        crawler.save_to_json(output_path)

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Noon 类目树爬取工具")
    parser.add_argument("--output", "-o", type=str,
                        default="config/category_tree_crawled.json",
                        help="输出 JSON 文件路径")
    parser.add_argument("--max-depth", type=int, default=3,
                        help="最大爬取深度")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="显示详细日志")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO,
                          format="[%(levelname)s] %(message)s")

    print("开始爬取 Noon 类目树...")
    print(f"输出文件：{args.output}")

    result = asyncio.run(crawl_category_tree(Path(args.output), args.max_depth))

    print(f"\n爬取完成!")
    print(f"一级类目数：{len(result['categories'])}")
    for cat in result["categories"]:
        print(f"  - {cat['name_en']}: {len(cat['children'])} 个子类目")
    print(f"\n已保存到：{args.output}")
