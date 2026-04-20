"""
Temu 沙特站爬虫 — Playwright 自建

爬取 temu.com 沙特站搜索结果：
  total_results, product_title, price, discount_price, sold_count, review_count, rating

反爬较强，需要 playwright-stealth + 较长延迟。
数据不可用时（爬取失败/被封）相关字段留空，不影响整体流程。
"""
import logging
import re

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class TemuScraper(BaseScraper):

    def __init__(self, settings):
        super().__init__("temu", settings)
        # Temu 反爬强，增加延迟
        self._min_delay = max(settings.min_delay, 3.0)
        self._max_delay = max(settings.max_delay, 7.0)

    async def scrape_keyword(self, keyword: str) -> dict:
        """爬取 Temu 沙特站搜索结果"""
        ctx = await self.new_context()
        page = await ctx.new_page()
        result = {
            "keyword": keyword,
            "total_results": 0,
            "products": [],
        }

        try:
            # Temu 沙特站搜索 URL
            url = f"https://www.temu.com/search_result.html?search_key={keyword}"

            # 设置沙特地区 Cookie
            await ctx.add_cookies([
                {"name": "region", "value": "SA", "domain": ".temu.com", "path": "/"},
                {"name": "currency", "value": "SAR", "domain": ".temu.com", "path": "/"},
            ])

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)  # Temu 需要更长等待

            # 总结果数
            result["total_results"] = await self._get_total_results(page)

            # 商品列表
            result["products"] = await self._parse_products(page)

        except Exception as e:
            logger.warning(f"[temu] '{keyword}' 爬取失败（可能被反爬拦截）: {e}")
            result["error"] = str(e)
        finally:
            await page.close()
            await ctx.close()

        return result

    async def _get_total_results(self, page) -> int:
        """解析总结果数"""
        try:
            for sel in [
                '[class*="resultCount"]',
                'span:has-text("result")',
                '[class*="total"]',
            ]:
                el = page.locator(sel)
                if await el.count() > 0:
                    text = await el.first.inner_text()
                    nums = re.findall(r'[\d,]+', text)
                    if nums:
                        return int(nums[0].replace(",", ""))
        except Exception:
            pass
        return 0

    async def _parse_products(self, page) -> list[dict]:
        """解析商品列表"""
        products = []
        try:
            # 滚动加载
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(1000)

            # Temu 商品卡片选择器
            cards = page.locator('[class*="product-card"], [class*="goods-card"], [class*="search-item"]')
            count = await cards.count()

            for i in range(min(count, 20)):  # Temu 取前20条
                try:
                    card = cards.nth(i)
                    product = {
                        "title": "",
                        "price": 0.0,
                        "discount_price": None,
                        "sold_count": 0,
                        "review_count": 0,
                        "rating": None,
                    }

                    # 标题
                    title_el = card.locator('[class*="title"], [class*="name"]')
                    if await title_el.count() > 0:
                        product["title"] = (await title_el.first.inner_text()).strip()

                    # 价格
                    price_el = card.locator('[class*="price"]')
                    if await price_el.count() > 0:
                        text = await price_el.first.inner_text()
                        nums = re.findall(r'[\d.]+', text.replace(",", ""))
                        if nums:
                            product["price"] = float(nums[0])

                    # 销量
                    sold_el = card.locator('[class*="sold"], [class*="sale"]')
                    if await sold_el.count() > 0:
                        text = await sold_el.first.inner_text()
                        nums = re.findall(r'[\d,]+', text)
                        if nums:
                            sold_str = nums[0].replace(",", "")
                            product["sold_count"] = int(sold_str)

                    # 评分
                    rating_el = card.locator('[class*="rating"], [class*="star"]')
                    if await rating_el.count() > 0:
                        text = await rating_el.first.inner_text()
                        nums = re.findall(r'[\d.]+', text)
                        if nums:
                            product["rating"] = float(nums[0])

                    if product["title"]:
                        products.append(product)
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"[temu] 商品列表解析失败: {e}")

        return products
