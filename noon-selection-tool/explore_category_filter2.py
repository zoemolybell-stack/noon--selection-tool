"""探索 Category 筛选器的详细 DOM 结构 — 提取品类树 + 链接"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from playwright.async_api import async_playwright
from playwright_stealth import Stealth


async def explore_keyword(page, keyword):
    url = f"https://www.noon.com/saudi-en/search/?q={keyword.replace(' ', '+')}"
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(6000)

    # 提取 Category 筛选器的完整树形结构
    tree = await page.evaluate("""() => {
        const container = document.querySelector('[data-qa="Category"]');
        if (!container) return {error: 'Category filter not found'};

        // 递归提取树形结构
        function extractTree(el, depth) {
            const items = [];
            const links = el.querySelectorAll(':scope > a, :scope > div > a, :scope > ul > li > a');

            // 尝试另一种方式：找所有直接子元素
            for (const child of el.children) {
                const text = child.innerText?.trim() || '';
                const href = child.getAttribute?.('href') || '';
                const tag = child.tagName;
                const cls = child.className?.substring?.(0, 80) || '';

                if (text && text.length < 100) {
                    items.push({
                        tag, text: text.split('\\n')[0], href,
                        className: cls, depth,
                        childCount: child.children.length,
                    });
                }
            }
            return items;
        }

        // 获取 content div
        const content = container.querySelector('[class*="content"]') || container;

        // 获取所有链接和文本节点
        const allLinks = content.querySelectorAll('a');
        const linkData = [];
        for (const a of allLinks) {
            linkData.push({
                text: a.innerText.trim(),
                href: a.getAttribute('href') || '',
                className: a.className?.substring?.(0, 100) || '',
                depth: 0, // 通过缩进/层级判断
            });
        }

        // 获取完整内部HTML结构（截取前3000字符）
        const html = content.innerHTML.substring(0, 3000);

        // 获取完整文本，保留缩进
        const fullText = content.innerText;

        return {
            linkCount: allLinks.length,
            links: linkData,
            html: html,
            fullText: fullText,
        };
    }""")

    return tree


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-http2", "--disable-blink-features=AutomationControlled",
                  "--window-position=-9999,-9999"],
        )
        ctx = await browser.new_context(viewport={"width": 1920, "height": 1080}, locale="en-US")
        stealth = Stealth()
        await stealth.apply_stealth_async(ctx)
        page = await ctx.new_page()

        keywords = ["kitchen organizer", "car phone holder", "yoga mat"]

        for kw in keywords:
            print(f"\n{'=' * 60}")
            print(f"  关键词: {kw}")
            print(f"{'=' * 60}")

            tree = await explore_keyword(page, kw)

            if "error" in tree:
                print(f"  ERROR: {tree['error']}")
                continue

            print(f"\n  链接数: {tree['linkCount']}")

            print(f"\n  --- 全部文本 ---")
            for line in tree['fullText'].split('\n'):
                if line.strip():
                    print(f"    {line.strip()}")

            print(f"\n  --- 全部链接 ---")
            for link in tree['links']:
                print(f"    '{link['text']}' → {link['href'][:120]}")

            print(f"\n  --- HTML结构(前1500字符) ---")
            print(tree['html'][:1500])

            await page.wait_for_timeout(3000)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
