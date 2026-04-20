"""
从 noon 首页 mega menu 抓取平台前台导航树。

目标：
- 以首页导航为真源，提取一级类目、二级分组、三级入口
- 输出为稳定 JSON，供类目爬虫和报告目录复用
- 为后续补充 bestseller / breadcrumb / 左侧筛选树打基础
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from playwright.async_api import Browser, BrowserContext, async_playwright


logger = logging.getLogger(__name__)

HOME_URL = "https://www.noon.com/saudi-en/"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "config" / "noon_home_navigation_saudi_en.json"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "unknown"


@dataclass
class CrawlStats:
    top_categories: int = 0
    groups: int = 0
    leaves: int = 0


class NoonHomeNavigationCrawler:
    def __init__(self, market_url: str = HOME_URL):
        self.market_url = market_url
        self._playwright = None
        self._browser: Browser | None = None

    async def _start_browser(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--no-sandbox",
                "--window-position=-9999,-9999",
            ],
        )

    async def _stop_browser(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
        self._playwright = None

    async def _new_context(self) -> BrowserContext:
        ctx = await self._browser.new_context(
            viewport={"width": 1920, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        try:
            from playwright_stealth import Stealth

            stealth = Stealth()
            await stealth.apply_stealth_async(ctx)
        except Exception:
            logger.warning("[home-nav] stealth 应用失败，继续使用原始上下文")
        return ctx

    async def _list_menu_items(self, page) -> list[dict]:
        return await page.evaluate(
            """() => {
                return Array.from(document.querySelectorAll('li[data-qa^="btn_main_menu_"]')).map((el, index) => {
                    const anchor = el.querySelector(':scope > a');
                    const name = (anchor?.innerText || el.innerText || '').trim();
                    const href = anchor?.href || '';
                    return {
                        index,
                        qa: el.getAttribute('data-qa') || '',
                        name,
                        href,
                    };
                }).filter(item => item.qa && item.name);
            }"""
        )

    async def _extract_menu_payload(self, page, qa: str) -> dict:
        selector = f'li[data-qa="{qa}"]'
        menu = page.locator(selector)
        await menu.hover()
        await page.wait_for_timeout(1200)

        return await menu.evaluate(
            """(el) => {
                const toAbs = (href) => {
                    if (!href) return '';
                    try {
                        return new URL(href, location.origin).toString();
                    } catch {
                        return href;
                    }
                };

                const dedupeLinks = (rows) => {
                    const seen = new Set();
                    const results = [];
                    for (const row of rows) {
                        const name = row.name_en || row.text || '';
                        const key = `${name}|||${row.url}`;
                        if (!name || !row.url || seen.has(key)) continue;
                        seen.add(key);
                        results.push(row);
                    }
                    return results;
                };

                const anchor = el.querySelector(':scope > a');
                const topName = (anchor?.innerText || el.innerText || '').trim();
                const topUrl = toAbs(anchor?.getAttribute('href') || '');
                const menuPanel = el.querySelector(':scope div[class*="mainCategoryMenu"]');
                const groups = [];

                if (menuPanel) {
                    const groupLis = menuPanel.querySelectorAll('div[class*="gridCats"] > ul > li');
                    for (const groupLi of groupLis) {
                        const groupAnchor = groupLi.querySelector(':scope > div a, :scope > a');
                        const groupName = (groupAnchor?.innerText || '').trim();
                        const groupUrl = toAbs(groupAnchor?.getAttribute('href') || '');
                        const children = [];

                        for (const childAnchor of groupLi.querySelectorAll('ul a[href]')) {
                            const childText = (childAnchor.innerText || '').trim();
                            const childUrl = toAbs(childAnchor.getAttribute('href') || '');
                            if (!childText || !childUrl) continue;
                            if (groupUrl && childUrl === groupUrl) continue;
                            children.push({
                                id: '',
                                name_en: childText,
                                url: childUrl,
                            });
                        }

                        if (groupName || children.length) {
                            groups.push({
                                id: '',
                                name_en: groupName,
                                url: groupUrl,
                                children: dedupeLinks(children),
                            });
                        }
                    }
                }

                return {
                    id: '',
                    name_en: topName,
                    url: topUrl,
                    groups: groups,
                };
            }"""
        )

    def _finalize_payload(self, payload: dict) -> dict:
        payload["id"] = _slugify(payload.get("name_en", ""))
        for group in payload.get("groups", []):
            group["id"] = _slugify(group.get("name_en", ""))
            for child in group.get("children", []):
                child["id"] = _slugify(child.get("name_en", ""))
                child["path"] = [
                    payload.get("name_en", ""),
                    group.get("name_en", ""),
                    child.get("name_en", ""),
                ]
            group["path"] = [payload.get("name_en", ""), group.get("name_en", "")]
        return payload

    async def crawl(self) -> dict:
        stats = CrawlStats()
        await self._start_browser()
        ctx = await self._new_context()
        page = await ctx.new_page()

        try:
            await page.goto(self.market_url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(6000)

            menu_items = await self._list_menu_items(page)
            categories = []
            for item in menu_items:
                try:
                    payload = await self._extract_menu_payload(page, item["qa"])
                    payload = self._finalize_payload(payload)
                    categories.append(payload)
                    stats.top_categories += 1
                    stats.groups += len(payload.get("groups", []))
                    stats.leaves += sum(len(group.get("children", [])) for group in payload.get("groups", []))
                    logger.info(
                        "[home-nav] %s -> %s groups / %s leaves",
                        payload.get("name_en"),
                        len(payload.get("groups", [])),
                        sum(len(group.get("children", [])) for group in payload.get("groups", [])),
                    )
                except Exception as exc:
                    logger.warning("[home-nav] 提取导航失败 %s: %s", item.get("name"), exc)

            return {
                "name": "Noon Saudi Home Navigation Tree",
                "market": "saudi-en",
                "source": "homepage_navigation",
                "source_url": self.market_url,
                "captured_at": datetime.now().isoformat(),
                "top_category_count": stats.top_categories,
                "group_count": stats.groups,
                "leaf_count": stats.leaves,
                "categories": categories,
            }
        finally:
            await page.close()
            await ctx.close()
            await self._stop_browser()

    async def crawl_to_file(self, output_path: Path = DEFAULT_OUTPUT) -> Path:
        payload = await self.crawl()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[home-nav] 已写入 %s", output_path)
        return output_path


async def main_async(output_path: Path):
    crawler = NoonHomeNavigationCrawler()
    path = await crawler.crawl_to_file(output_path)
    print(path)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="抓取 noon 首页 mega menu 导航树")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="输出 JSON 路径",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细日志")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    asyncio.run(main_async(args.output))


if __name__ == "__main__":
    main()
