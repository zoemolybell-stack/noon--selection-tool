"""
Alibaba.com 供应端爬虫 — 英文关键词搜索 FOB 价格

策略:
1. 首选: Playwright 直接爬取 alibaba.com（需要代理）
2. 备选: 用 httpx 通过 Alibaba 的公开产品 API
3. Fallback: 品类默认采购比例

反爬现状:
  Alibaba 搜索页对自动化检测极严，无代理基本 100% 触发 captcha。
  配置 PROXY_HOST 后可大幅提高成功率。
"""
import logging
import re
import json
from pathlib import Path

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class AlibabaScraper(BaseScraper):

    def __init__(self, settings):
        super().__init__("alibaba", settings)
        self._captcha_count = 0

    async def scrape_keyword(self, keyword: str) -> dict:
        """爬取 Alibaba.com 搜索结果"""
        ctx = await self.new_context()
        page = await ctx.new_page()
        result = {
            "keyword": keyword,
            "total_results": 0,
            "products": [],
            "median_price_usd": None,
        }

        try:
            url = f"https://www.alibaba.com/trade/search?SearchText={keyword}"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # 检测 captcha
            content = await page.content()
            if "captcha" in content.lower() or "access denied" in content.lower():
                self._captcha_count += 1
                if self._captcha_count <= 2:
                    logger.warning(f"[alibaba] '{keyword}' 触发 captcha (第{self._captcha_count}次)")
                elif self._captcha_count == 3:
                    logger.error(
                        "[alibaba] 连续触发 captcha，Alibaba 反爬已拦截。\n"
                        "  解决方案:\n"
                        "  1. 在 .env 中配置代理: PROXY_HOST / PROXY_PORT\n"
                        "  2. 或手动在 alibaba.com 搜索后导出价格到 CSV\n"
                        "  3. 当前自动 fallback 到品类默认采购比例"
                    )
                result["error"] = "captcha"
                return result

            # 总结果数
            result["total_results"] = await self._get_total_results(page)

            # 商品列表（前10条）— 尝试多种选择器
            result["products"] = await self._parse_products(page)

            # 计算中位FOB价格
            prices = []
            for p in result["products"]:
                if p.get("price_low"):
                    prices.append(p["price_low"])
                elif p.get("price_high"):
                    prices.append(p["price_high"])
            if prices:
                prices.sort()
                mid = len(prices) // 2
                result["median_price_usd"] = round(
                    prices[mid] if len(prices) % 2 == 1
                    else (prices[mid - 1] + prices[mid]) / 2,
                    2
                )
                self._captcha_count = 0  # 重置连续失败计数

        except Exception as e:
            logger.warning(f"[alibaba] '{keyword}' 爬取失败: {e}")
            result["error"] = str(e)
        finally:
            await page.close()
            await ctx.close()

        return result

    async def _get_total_results(self, page) -> int:
        """解析总结果数"""
        try:
            for sel in [
                '[class*="result-count"]',
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
        """解析商品列表（前10条）— 多种选择器适配"""
        products = []
        try:
            # 方案1: 标准选择器
            cards = page.locator(
                '[class*="organic-list"] [class*="list-card"], '
                '[class*="J-offer-wrapper"], '
                '[class*="product-card"], '
                '[class*="fy23-search-card"], '
                '[class*="search-card-e"], '
                '[class*="gallery-card"]'
            )
            count = await cards.count()

            if count == 0:
                # 方案2: 通过 product-detail 链接找卡片
                links = page.locator('a[href*="product-detail"]')
                count = await links.count()
                if count > 0:
                    for i in range(min(count, 10)):
                        try:
                            link = links.nth(i)
                            card = link  # 链接本身就是卡片
                            product = await self._extract_from_card(card)
                            if product and product["title"]:
                                products.append(product)
                        except Exception:
                            continue
                    return products

            for i in range(min(count, 10)):
                try:
                    card = cards.nth(i)
                    product = await self._extract_from_card(card)
                    if product and product["title"]:
                        products.append(product)
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"[alibaba] 商品列表解析失败: {e}")

        return products

    async def _extract_from_card(self, card) -> dict:
        """从卡片元素提取产品信息"""
        product = {
            "title": "",
            "price_low": None,
            "price_high": None,
            "price_text": "",
            "min_order": "",
            "supplier_name": "",
            "supplier_level": "",
            "transaction_count": 0,
        }

        # 标题
        title_el = card.locator('[class*="title"], h2, h3, [class*="name"]')
        if await title_el.count() > 0:
            product["title"] = (await title_el.first.inner_text()).strip()

        # 价格区间（如 "$1.50 - $3.00"）
        price_el = card.locator('[class*="price"], [class*="Price"]')
        if await price_el.count() > 0:
            text = await price_el.first.inner_text()
            product["price_text"] = text.strip()
            nums = re.findall(r'[\d.]+', text)
            if len(nums) >= 2:
                product["price_low"] = float(nums[0])
                product["price_high"] = float(nums[1])
            elif len(nums) == 1:
                product["price_low"] = float(nums[0])
                product["price_high"] = float(nums[0])

        # 最低起订量
        moq_el = card.locator('[class*="moq"], [class*="min-order"], [class*="MOQ"]')
        if await moq_el.count() > 0:
            product["min_order"] = (await moq_el.first.inner_text()).strip()

        # 供应商名称
        supplier_el = card.locator('[class*="company"], [class*="supplier"], [class*="Supplier"]')
        if await supplier_el.count() > 0:
            product["supplier_name"] = (await supplier_el.first.inner_text()).strip()

        # 交易量
        trade_el = card.locator('[class*="transaction"], [class*="trade"]')
        if await trade_el.count() > 0:
            text = await trade_el.first.inner_text()
            nums = re.findall(r'[\d,]+', text)
            if nums:
                product["transaction_count"] = int(nums[0].replace(",", ""))

        return product

    async def run(self, keywords: list[str]) -> list[dict]:
        """
        批量爬取，带连续 captcha 快速中断逻辑。
        连续 3 次 captcha 后跳过剩余关键词（避免浪费时间）。
        """
        pending = [kw for kw in keywords if not self.is_completed(kw)]
        skipped = len(keywords) - len(pending)
        if skipped > 0:
            logger.info(f"[alibaba] 跳过 {skipped} 个已完成关键词")

        if not pending:
            logger.info(f"[alibaba] 所有关键词已完成")
            return [self.load_result(kw) for kw in keywords if self.load_result(kw)]

        await self.start_browser()
        results = []
        import time
        start_time = time.time()

        try:
            for i, kw in enumerate(pending, 1):
                # 连续 captcha 超过 3 次，提前终止
                if self._captcha_count >= 3:
                    remaining = len(pending) - i + 1
                    logger.warning(
                        f"[alibaba] 连续 captcha，跳过剩余 {remaining} 个关键词"
                    )
                    break

                result = await self.scrape_with_retry(kw)
                if result:
                    results.append(result)

                if i % 10 == 0 or i == len(pending):
                    elapsed = time.time() - start_time
                    logger.info(
                        f"[alibaba] 进度: {i}/{len(pending)} "
                        f"(成功: {len(results)}, captcha: {self._captcha_count})"
                    )

                await self.random_delay()
        finally:
            await self.stop_browser()

        return results


class AlibabaCsvImporter:
    """
    手动导入方案: 用户在 Alibaba.com 手动搜索后导出价格到 CSV。

    CSV 格式:
        keyword,fob_price_usd
        treadmill,185.50
        yoga mat,5.20
        dash camera,28.00

    用法:
        importer = AlibabaCsvImporter("data/alibaba_prices.csv")
        price = importer.get_price("treadmill")  # -> 185.50
    """

    def __init__(self, csv_path: str | Path):
        self.prices = {}
        path = Path(csv_path)
        if path.exists():
            import csv
            with open(path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    kw = row.get("keyword", "").strip().lower()
                    price = row.get("fob_price_usd", "").strip()
                    if kw and price:
                        try:
                            self.prices[kw] = float(price)
                        except ValueError:
                            pass
            logger.info(f"[alibaba_csv] 已加载 {len(self.prices)} 条手动价格")

    def get_price(self, keyword: str) -> float | None:
        return self.prices.get(keyword.lower().strip())
