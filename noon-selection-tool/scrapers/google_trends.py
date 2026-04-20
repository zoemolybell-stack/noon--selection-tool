"""
Google Trends 数据获取 — Playwright 浏览器版（非必要数据源）

通过 Playwright 访问 Google Trends 页面，拦截内部 API 响应获取数据。
分批策略：每批最多30个关键词，关键词间隔10-15s，批次间休息5分钟。

此数据源为非必要：获取失败不阻塞主流程，评分模型在无数据时自动降级。
"""
import asyncio
import json
import logging
import random
import re
import time
from pathlib import Path

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

TRENDS_URL = "https://trends.google.com/trends/explore"

# 分批策略参数
BATCH_SIZE = 30              # 每批最多30个关键词
KW_INTERVAL_MIN = 10.0       # 关键词间隔最小秒
KW_INTERVAL_MAX = 15.0       # 关键词间隔最大秒
BATCH_REST_SECONDS = 300     # 批次间休息5分钟


class GoogleTrendsFetcher:
    """Google Trends 数据获取器（Playwright 版，非必要数据源）"""

    def __init__(self, settings):
        self.settings = settings
        self.data_dir = settings.snapshot_dir / "google_trends"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def is_completed(self, keyword: str) -> bool:
        return self._result_path(keyword).exists()

    def _result_path(self, keyword: str) -> Path:
        safe_name = keyword.replace("/", "_").replace("\\", "_").replace(" ", "_")
        return self.data_dir / f"{safe_name}.json"

    def save_result(self, keyword: str, data: dict):
        path = self._result_path(keyword)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    async def fetch_all(self, keywords: list[str]) -> list[dict]:
        """
        分批获取所有关键词的趋势数据。
        - 每批最多 BATCH_SIZE 个关键词
        - 关键词间隔 10-15s
        - 批次间休息 5 分钟
        - 支持断点续跑（已完成的自动跳过）
        - 遇到 429 自动停止当前批次
        """
        pending = [kw for kw in keywords if not self.is_completed(kw)]
        skipped = len(keywords) - len(pending)
        if skipped > 0:
            logger.info(f"[google_trends] 跳过 {skipped} 个已完成")

        if not pending:
            logger.info("[google_trends] 所有关键词已完成")
            return [self._load_or_empty(kw) for kw in keywords]

        # 分批
        batches = [pending[i:i + BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
        logger.info(f"[google_trends] 待采集 {len(pending)} 个，分 {len(batches)} 批")

        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth

        total_done = 0
        rate_limited = False

        for batch_idx, batch in enumerate(batches):
            if rate_limited:
                logger.warning(f"[google_trends] 已被限流，跳过剩余 {len(pending) - total_done} 个关键词")
                break

            logger.info(f"[google_trends] 第 {batch_idx+1}/{len(batches)} 批，{len(batch)} 个关键词")

            pw = await async_playwright().start()
            launch_args = [
                "--disable-http2",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-9999,-9999",
            ]
            # 代理支持
            launch_kwargs = {
                "headless": False,
                "args": launch_args,
            }
            proxy_url = self.settings.proxy_url
            if proxy_url:
                launch_kwargs["proxy"] = {"server": proxy_url}
                logger.info(f"[google_trends] 使用代理: {self.settings.proxy_host}")

            browser = await pw.chromium.launch(**launch_kwargs)
            ctx = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            stealth = Stealth()
            await stealth.apply_stealth_async(ctx)

            try:
                for i, kw in enumerate(batch):
                    data = await self._fetch_keyword(ctx, kw)

                    # 429 检测：停止当前批次
                    if data.get("error") == "429 rate limited":
                        logger.warning(f"[google_trends] 429 限流，停止本批次")
                        rate_limited = True
                        break

                    self.save_result(kw, data)
                    total_done += 1

                    if total_done % 10 == 0 or total_done == len(pending):
                        logger.info(f"[google_trends] 总进度: {total_done}/{len(pending)}")

                    # 关键词间隔 10-15s
                    if i < len(batch) - 1:
                        delay = random.uniform(KW_INTERVAL_MIN, KW_INTERVAL_MAX)
                        await asyncio.sleep(delay)
            finally:
                await browser.close()
                await pw.stop()

            # 批次间休息（非最后一批 且未被限流）
            if batch_idx < len(batches) - 1 and not rate_limited:
                logger.info(f"[google_trends] 批次间休息 {BATCH_REST_SECONDS//60} 分钟...")
                await asyncio.sleep(BATCH_REST_SECONDS)

        completed = sum(1 for kw in keywords if self.is_completed(kw))
        logger.info(f"[google_trends] 完成 {completed}/{len(keywords)} 个关键词")
        return [self._load_or_empty(kw) for kw in keywords]

    async def _fetch_keyword(self, ctx, keyword: str) -> dict:
        """获取单个关键词的趋势数据"""
        page = await ctx.new_page()
        captured_data = {}

        # 拦截 multiline API 响应
        async def handle_response(response):
            if "api/widgetdata/multiline" in response.url:
                try:
                    text = await response.text()
                    # Google Trends API 响应前面有 ")]}'\n" 前缀
                    clean = text.lstrip(")]}'\n")
                    captured_data["multiline"] = json.loads(clean)
                except Exception:
                    pass

        page.on("response", handle_response)

        try:
            url = f"{TRENDS_URL}?q={keyword}&geo=SA&date=today+12-m&hl=en-US"
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(6000)

            # 检查是否被限流
            title = await page.title()
            if "429" in title:
                logger.warning(f"[google_trends] '{keyword}' 被限流 (429)")
                return self._empty_result(keyword, error="429 rate limited")

            # 等待 API 响应
            for _ in range(5):
                if "multiline" in captured_data:
                    break
                await page.wait_for_timeout(1000)

            if "multiline" not in captured_data:
                # 备用方案：从页面文本提取
                return await self._parse_from_page(page, keyword)

            return self._parse_api_response(keyword, captured_data["multiline"])

        except Exception as e:
            logger.warning(f"[google_trends] '{keyword}' 失败: {e}")
            return self._empty_result(keyword, error=str(e))
        finally:
            page.remove_listener("response", handle_response)
            await page.close()

    def _parse_api_response(self, keyword: str, api_data: dict) -> dict:
        """从 Google Trends multiline API 响应解析数据"""
        try:
            timeline = api_data.get("default", {}).get("timelineData", [])
            if not timeline:
                return self._empty_result(keyword)

            monthly_values = []
            monthly_dates = []
            for point in timeline:
                val = point.get("value", [0])[0]
                monthly_values.append(val)
                monthly_dates.append(point.get("formattedAxisTime", ""))

            avg = sum(monthly_values) / len(monthly_values) if monthly_values else 0

            # 趋势方向：后3个数据点 vs 前3个
            if len(monthly_values) >= 6:
                recent = sum(monthly_values[-3:]) / 3
                earlier = sum(monthly_values[:3]) / 3
                if earlier == 0:
                    direction = "rising" if recent > 0 else "stable"
                elif recent > earlier * 1.2:
                    direction = "rising"
                elif recent < earlier * 0.8:
                    direction = "declining"
                else:
                    direction = "stable"
            else:
                direction = "unknown"

            # 峰值月份
            peak_idx = monthly_values.index(max(monthly_values))
            peak_month = monthly_dates[peak_idx] if peak_idx < len(monthly_dates) else ""

            return {
                "keyword": keyword,
                "trend_direction": direction,
                "average_interest": round(avg, 1),
                "peak_month": peak_month,
                "monthly_data": monthly_values,
                "monthly_dates": monthly_dates,
            }

        except Exception as e:
            logger.debug(f"[google_trends] API 解析失败: {e}")
            return self._empty_result(keyword)

    async def _parse_from_page(self, page, keyword: str) -> dict:
        """备用：从页面文本提取趋势信息"""
        try:
            body = await page.inner_text("body")

            # 看是否有 "Interest over time" 图表数据
            if "interest over time" not in body.lower():
                return self._empty_result(keyword)

            # 简单判断趋势方向（从页面相关区域提取）
            direction = "unknown"
            if "breakout" in body.lower() or "rising" in body.lower():
                direction = "rising"

            return {
                "keyword": keyword,
                "trend_direction": direction,
                "average_interest": 0,
                "peak_month": "",
                "monthly_data": [],
                "monthly_dates": [],
                "note": "parsed from page text, limited data",
            }
        except Exception:
            return self._empty_result(keyword)

    def _empty_result(self, keyword: str, error: str = "") -> dict:
        result = {
            "keyword": keyword,
            "trend_direction": "unknown",
            "average_interest": 0,
            "peak_month": "",
            "monthly_data": [],
            "monthly_dates": [],
        }
        if error:
            result["error"] = error
        return result

    def _load_or_empty(self, keyword: str) -> dict:
        path = self._result_path(keyword)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return self._empty_result(keyword)

    # ── 兼容 main.py 的同步接口 ──
    def fetch_batch(self, keywords: list[str]) -> list[dict]:
        """同步包装器"""
        return asyncio.run(self.fetch_all(keywords))
