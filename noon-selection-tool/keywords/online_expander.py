"""
关键词在线扩展器 - 实时爬取 Noon/Amazon 搜索框联想词

功能:
- 输入 1 个核心词 (如 "car")
- 自动从 Noon 搜索框爬取 50+ 联想词
- 自动从 Amazon 搜索框爬取 50+ 联想词
- 合并去重后输出 100+ 关键词列表

使用场景:
- 首次运行无历史数据时
- 需要扩展关键词库
- 发现新的长尾关键词
"""
import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict, Set
from urllib.parse import quote_plus, urlencode
from urllib.request import Request, urlopen

from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)


class KeywordExpander:
    """关键词在线扩展器"""

    def __init__(self, timeout: int = 30000):
        self.timeout = timeout
        self.noon_keywords: List[str] = []
        self.amazon_keywords: List[str] = []

    def _normalize_suggestion(self, seed_keyword: str, prefix: str, text: str) -> str:
        """尽量把联想词片段重建为完整 query，过滤明显噪声。"""
        raw = " ".join(str(text or "").strip().lower().split())
        if not raw:
            return ""
        if raw == seed_keyword:
            return ""
        if seed_keyword in raw:
            return raw

        prefix = " ".join(str(prefix or "").strip().lower().split())
        if not prefix:
            return ""

        if raw.startswith(prefix):
            candidate = raw
        else:
            last_token = prefix.split()[-1]
            if len(last_token) == 1 or len(prefix.replace(" ", "")) < len(seed_keyword.replace(" ", "")):
                candidate = f"{prefix}{raw}"
            else:
                candidate = f"{prefix} {raw}"

        candidate = " ".join(candidate.split())
        if candidate == seed_keyword or len(candidate) < max(4, len(seed_keyword)):
            return ""
        if seed_keyword not in candidate and not candidate.startswith(seed_keyword[:-1]):
            return ""
        if len(candidate.split()) < 2:
            return ""
        return candidate

    async def _goto_first_success(self, page: Page, urls: List[str]) -> str:
        """依次尝试多个入口 URL，优先选择更稳定的搜索页入口。"""
        last_error = None
        for url in urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)
                await page.wait_for_timeout(1500)
                return url
            except Exception as exc:
                last_error = exc
                logger.warning("入口访问失败 %s: %s", url, exc)
                try:
                    await page.goto("about:blank", wait_until="load", timeout=5000)
                except Exception:
                    pass
                await page.wait_for_timeout(800)
        if last_error:
            raise last_error
        raise RuntimeError("no candidate url provided")

    async def _find_search_input(self, page: Page, selectors: List[str]):
        """逐个 selector 寻找搜索框，避免命中隐藏节点或等待状态误判。"""
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if await page.locator(selector).count() == 0:
                    continue
                return locator
            except Exception:
                continue
        return None

    async def _collect_suggestion_texts(
        self,
        page: Page,
        search_input,
        seed_keyword: str,
        prefixes: List[str],
        suggestion_selector: str,
    ) -> Set[str]:
        keywords: Set[str] = set()
        for prefix in prefixes:
            try:
                try:
                    await search_input.scroll_into_view_if_needed(timeout=2000)
                except Exception:
                    pass
                try:
                    await search_input.click(timeout=3000)
                except Exception:
                    await search_input.focus()
                await search_input.fill("")
                await page.wait_for_timeout(300)
                await search_input.type(prefix, delay=50)
                await page.wait_for_timeout(1800)

                suggestion_items = page.locator(suggestion_selector)
                count = await suggestion_items.count()
                for i in range(min(count, 15)):
                    try:
                        text = (await suggestion_items.nth(i).inner_text(timeout=2000)).strip().lower()
                        normalized = self._normalize_suggestion(seed_keyword, prefix, text)
                        if normalized:
                            keywords.add(normalized)
                    except Exception:
                        break

                await page.wait_for_timeout(400)
            except Exception as exc:
                logger.debug("前缀 '%s' 提取失败: %s", prefix, exc)
                continue
        return keywords

    async def _collect_noon_via_api(self, seed_keyword: str) -> List[str]:
        loader = await self._build_noon_probe()
        if loader is None:
            return []
        _, scraper = loader

        keywords: Set[str] = set()
        try:
            await scraper.start_browser()
            ctx = await scraper.new_context()
            page = await ctx.new_page()
            try:
                await page.goto(
                    f"https://www.noon.com/saudi-en/search/?q={quote_plus(seed_keyword)}&page=1",
                    wait_until="domcontentloaded",
                    timeout=self.timeout,
                )
                await page.wait_for_timeout(2000)
                for prefix in self._generate_prefixes(seed_keyword):
                    try:
                        payload = await page.evaluate(
                            """
                            async (query) => {
                                const controller = new AbortController();
                                const timer = setTimeout(() => controller.abort(), 5000);
                                try {
                                    const response = await fetch(
                                        `/_vs/nc/mp-customer-catalog-api/api/v3/suggestions?q=${encodeURIComponent(query)}`,
                                        {
                                            signal: controller.signal,
                                            headers: {
                                                'accept': 'application/json, text/plain, */*'
                                            }
                                        }
                                    );
                                    return await response.json();
                                } catch (error) {
                                    return { suggestions: [], _error: String(error || '') };
                                } finally {
                                    clearTimeout(timer);
                                }
                            }
                            """,
                            prefix,
                        )
                        if payload.get("_error"):
                            logger.debug("[Noon] API prefix '%s' returned error: %s", prefix, payload["_error"])
                        for item in payload.get("suggestions") or []:
                            normalized = self._normalize_suggestion(
                                seed_keyword,
                                prefix,
                                item.get("query", ""),
                            )
                            if normalized:
                                keywords.add(normalized)
                    except Exception as exc:
                        logger.debug("[Noon] API prefix '%s' 提取失败: %s", prefix, exc)
                        continue
                return sorted(keywords)
            finally:
                await page.close()
                await ctx.close()
        except Exception as exc:
            logger.warning("[Noon] API 扩词失败: %s", exc)
            return []
        finally:
            await scraper.stop_browser()

    async def _build_noon_probe(self):
        try:
            from config.settings import Settings
            from scrapers.noon_scraper import NoonScraper
        except Exception as exc:
            logger.warning("[Noon] 无法加载 NoonScraper probe: %s", exc)
            return None

        settings = Settings()
        settings.set_runtime_scope("keyword_expand_probe")
        object.__setattr__(settings, "products_per_keyword", 1)
        object.__setattr__(settings, "concurrent_browsers", 1)

        scraper = NoonScraper(settings, skip_suggestions=False, max_products=1)
        return settings, scraper

    async def _collect_noon_via_scraper_context(self, seed_keyword: str) -> List[str]:
        """DOM fallback：当 API 也不可用时，复用 NoonScraper 的浏览器配置和 stealth 上下文。"""
        loader = await self._build_noon_probe()
        if loader is None:
            return []
        _, scraper = loader
        try:
            await scraper.start_browser()
            ctx = await scraper.new_context()
            page = await ctx.new_page()
            try:
                await page.goto(
                    f"https://www.noon.com/saudi-en/search/?q={quote_plus(seed_keyword)}&page=1",
                    wait_until="domcontentloaded",
                    timeout=self.timeout,
                )
                await page.wait_for_timeout(2500)
                search_input = await self._find_search_input(
                    page,
                    [
                        '#search-input',
                        'input[data-qa="header-search"]',
                        'input[name="site-search"]',
                        'input[data-qa="txt_searchBar"]',
                        'header input[name="q"]',
                        'header input[placeholder*="search" i]',
                        'input[name="q"]',
                        'input[type="search"]:not([id*="filter"]):not([name*="filter"])',
                    ],
                )
                if search_input is None:
                    logger.warning("[Noon] fallback 仍未找到搜索框")
                    return []

                keywords = await self._collect_suggestion_texts(
                    page,
                    search_input,
                    seed_keyword,
                    self._generate_prefixes(seed_keyword),
                    '[class*="suggestion"] a, '
                    '[class*="Suggestion"] a, '
                    '[class*="autocomplete"] li, '
                    '[class*="Autocomplete"] li, '
                    '[class*="Dropdown"] a[href*="/search"], '
                    '[data-qa*="suggest"] a, '
                    '[data-qa*="suggest"] li',
                )
                return sorted(keywords)
            finally:
                await page.close()
                await ctx.close()
        except Exception as exc:
            logger.warning("[Noon] fallback 扩词失败: %s", exc)
            return []
        finally:
            await scraper.stop_browser()

    async def expand_from_noon(self, seed_keyword: str) -> List[str]:
        """
        从 Noon 搜索框获取联想词

        策略:
        1. 输入核心词前缀 (如 "car" → "car", "ca", "c")
        2. 每次输入后等待联想词下拉框出现
        3. 提取所有联想词
        4. 多轮采集，合并去重

        Args:
            seed_keyword: 核心关键词

        Returns:
            联想词列表 (50-100 个)
        """
        keywords = await self._collect_noon_via_api(seed_keyword)
        if not keywords:
            logger.info("[Noon] API 扩词结果为空，尝试 DOM fallback")
            keywords = await self._collect_noon_via_scraper_context(seed_keyword)

        logger.info(f"[Noon] 从 '{seed_keyword}' 扩展得到 {len(keywords)} 个联想词")
        self.noon_keywords = list(keywords)
        return self.noon_keywords

    async def _legacy_expand_from_amazon(self, seed_keyword: str) -> List[str]:
        """
        从 Amazon.sa 搜索框获取联想词

        策略同 Noon

        Args:
            seed_keyword: 核心关键词

        Returns:
            联想词列表 (50-100 个)
        """
        keywords: Set[str] = set()

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-http2",
                    "--no-sandbox",
                    "--window-position=-9999,-9999",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page = await context.new_page()

            try:
                await self._goto_first_success(
                    page,
                    [
                        f"https://www.amazon.sa/s?k={quote_plus(seed_keyword)}",
                        "https://www.amazon.sa/",
                    ],
                )

                search_input = await self._find_search_input(
                    page,
                    [
                        '#twotabsearchtextbox',
                        'input[name="field-keywords"]',
                        'form[role="search"] input[type="text"]',
                        'input[aria-label*="Search" i]',
                        'input[type="search"]',
                    ],
                )
                if search_input is None:
                    logger.warning("[Amazon] 未找到搜索框")
                    return []

                prefixes = self._generate_prefixes(seed_keyword)
                keywords = await self._collect_suggestion_texts(
                    page,
                    search_input,
                    seed_keyword,
                    prefixes,
                    'div.s-suggestion-container, '
                    'div.suggestion-item, '
                    'li.suggestion, '
                    'div[class*="suggestion"] a, '
                    'div[class*="autocomplete"] span, '
                    'span[class*="autocomplete"]',
                )

                logger.info(f"[Amazon] 从 '{seed_keyword}' 扩展得到 {len(keywords)} 个联想词")

            except Exception as e:
                logger.error(f"[Amazon] 扩展失败：{e}")
            finally:
                await page.close()
                await context.close()
                await browser.close()

        self.amazon_keywords = list(keywords)
        return self.amazon_keywords

    async def _collect_amazon_via_api(self, seed_keyword: str) -> List[str]:
        keywords: Set[str] = set()
        prefixes = self._generate_prefixes(seed_keyword)

        def fetch_prefix(prefix: str) -> List[str]:
            query = urlencode(
                {
                    "limit": 11,
                    "prefix": prefix,
                    "alias": "aps",
                    "site-variant": "desktop",
                    "client-info": "amazon-search-ui",
                    "mid": "A17E79C6D8DWNP",
                }
            )
            request = Request(
                f"https://completion.amazon.sa/api/2017/suggestions?{query}",
                headers={
                    "user-agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "accept": "application/json, text/plain, */*",
                },
            )
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8", "replace"))

            results: List[str] = []
            for item in payload.get("suggestions") or []:
                normalized = self._normalize_suggestion(
                    seed_keyword,
                    prefix,
                    item.get("value") or item.get("keyword") or item.get("query") or "",
                )
                if normalized:
                    results.append(normalized)
            return results

        for prefix in prefixes:
            try:
                for normalized in await asyncio.to_thread(fetch_prefix, prefix):
                    keywords.add(normalized)
            except Exception as exc:
                logger.debug("[Amazon] API prefix '%s' failed: %s", prefix, exc)

        return sorted(keywords)

    async def expand_from_amazon(self, seed_keyword: str) -> List[str]:
        keywords = await self._collect_amazon_via_api(seed_keyword)
        logger.info("[Amazon] API returned %s suggestions for '%s'", len(keywords), seed_keyword)
        self.amazon_keywords = list(keywords)
        return self.amazon_keywords

    def _generate_prefixes(self, keyword: str) -> List[str]:
        """
        生成多轮前缀用于采集更多联想词

        策略:
        - 完整词："car"
        - 逐字母后退："ca", "c"
        - 加空格："car ", "car w"

        Args:
            keyword: 核心关键词

        Returns:
            前缀列表
        """
        prefixes = [keyword]  # 完整词

        # 逐字母后退 (至少保留 2 个字母)
        for i in range(len(keyword) - 1, 1, -1):
            prefixes.append(keyword[:i])

        # 加空格 + 单字母
        for c in 'abcdefghijk':  # 限制字母数量
            prefixes.append(f"{keyword} {c}")

        # 去重
        return list(dict.fromkeys(prefixes))[:12]  # 限制最多 12 轮

    async def expand(self, seed_keyword: str,
                     platforms: List[str] = None) -> Dict[str, List[str]]:
        """
        主函数：从指定平台扩展关键词

        Args:
            seed_keyword: 核心关键词
            platforms: 平台列表 ['noon', 'amazon']，默认两者都爬取

        Returns:
            字典：{'noon': [...], 'amazon': [...], 'all': [...]}
        """
        if platforms is None:
            platforms = ['noon', 'amazon']

        tasks = []
        if 'noon' in platforms:
            tasks.append(self.expand_from_noon(seed_keyword))
        if 'amazon' in platforms:
            tasks.append(self.expand_from_amazon(seed_keyword))

        # 并发爬取
        results = await asyncio.gather(*tasks, return_exceptions=True)

        result = {
            'noon': self.noon_keywords if 'noon' in platforms else [],
            'amazon': self.amazon_keywords if 'amazon' in platforms else [],
        }

        # 合并去重
        all_keywords: Set[str] = set()
        for kw_list in result.values():
            all_keywords.update(kw_list)
        result['all'] = sorted(list(all_keywords))

        return result


def expand_keywords(seed_keyword: str,
                    platforms: List[str] = None,
                    output_path: Path = None) -> Dict[str, List[str]]:
    """
    便捷函数：扩展关键词并可选保存到文件

    Args:
        seed_keyword: 核心关键词
        platforms: 平台列表，默认 ['noon', 'amazon']
        output_path: 输出文件路径 (.txt 或 .json)

    Returns:
        关键词字典
    """
    import json

    expander = KeywordExpander()
    result = asyncio.run(expander.expand(seed_keyword, platforms))

    # 保存到文件
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix == '.json':
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        else:  # .txt
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"# 关键词扩展结果 - 种子词：{seed_keyword}\n")
                f.write(f"# 来源：Noon ({len(result['noon'])}) + Amazon ({len(result['amazon'])})\n")
                f.write(f"# 总计：{len(result['all'])}\n\n")
                for kw in result['all']:
                    f.write(f"{kw}\n")

    return result


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='关键词在线扩展器')
    parser.add_argument('--keyword', '-k', type=str, required=True,
                        help='核心关键词')
    parser.add_argument('--platform', type=str, nargs='+',
                        default=['noon', 'amazon'],
                        help='平台列表 (noon/amazon)')
    parser.add_argument('--output', '-o', type=str,
                        help='输出文件路径 (.txt 或 .json)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='显示详细日志')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO,
                          format='[%(levelname)s] %(message)s')

    print(f"开始扩展关键词：{args.keyword}")
    print(f"平台：{', '.join(args.platform)}")

    result = expand_keywords(args.keyword, args.platform, args.output)

    print(f"\n扩展结果:")
    print(f"  Noon: {len(result['noon'])} 个关键词")
    print(f"  Amazon: {len(result['amazon'])} 个关键词")
    print(f"  总计 (去重): {len(result['all'])} 个关键词")

    if result['all']:
        print(f"\n前 20 个关键词:")
        for i, kw in enumerate(result['all'][:20], 1):
            print(f"  {i:2d}. {kw}")

    if args.output:
        print(f"\n已保存到：{args.output}")
