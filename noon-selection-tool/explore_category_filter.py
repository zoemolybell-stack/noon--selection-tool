"""探索 Noon 搜索结果页的 Category 筛选器 DOM 结构"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from playwright.async_api import async_playwright
from playwright_stealth import Stealth


async def main():
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

        url = "https://www.noon.com/saudi-en/search/?q=kitchen+organizer"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(6000)

        # 1. 用JS探索整个筛选器区域
        print("=" * 60)
        print("  方法1: 查找含 'category' 文本的元素")
        print("=" * 60)
        result1 = await page.evaluate("""() => {
            const results = [];
            // 找所有包含 "category" 文本的元素
            const walker = document.createTreeWalker(
                document.body,
                NodeFilter.SHOW_ELEMENT,
                null
            );
            while (walker.nextNode()) {
                const el = walker.currentNode;
                // 直接文本内容（非子元素文本）
                const directText = Array.from(el.childNodes)
                    .filter(n => n.nodeType === 3)
                    .map(n => n.textContent.trim())
                    .join('');
                if (/category/i.test(directText)) {
                    results.push({
                        tag: el.tagName,
                        text: directText.substring(0, 100),
                        className: el.className?.substring?.(0, 100) || '',
                        parentTag: el.parentElement?.tagName || '',
                        parentClass: el.parentElement?.className?.substring?.(0, 100) || '',
                    });
                }
            }
            return results;
        }""")
        for item in result1:
            print(f"  <{item['tag']}> text='{item['text']}' class='{item['className']}'")
            print(f"    parent: <{item['parentTag']}> class='{item['parentClass']}'")

        # 2. 查找筛选器组 — 通常是 sidebar / filter 区域
        print("\n" + "=" * 60)
        print("  方法2: 查找筛选器区域 (sidebar/filter)")
        print("=" * 60)
        result2 = await page.evaluate("""() => {
            const results = [];
            // 查找可能是筛选器的容器
            const candidates = document.querySelectorAll(
                '[class*="filter" i], [class*="sidebar" i], [class*="facet" i], ' +
                '[class*="refinement" i], [data-qa*="filter"], [data-qa*="sidebar"]'
            );
            for (const el of candidates) {
                const text = el.innerText.substring(0, 500);
                if (text.length > 10) {
                    results.push({
                        tag: el.tagName,
                        className: el.className?.substring?.(0, 150) || '',
                        dataQa: el.getAttribute('data-qa') || '',
                        textPreview: text.substring(0, 300),
                        childCount: el.children.length,
                    });
                }
            }
            return results.slice(0, 10);
        }""")
        for i, item in enumerate(result2):
            print(f"\n  [{i}] <{item['tag']}> class='{item['className']}'")
            print(f"      data-qa='{item['dataQa']}' children={item['childCount']}")
            print(f"      text: {item['textPreview'][:200]}")

        # 3. 直接查看所有 data-qa 属性中含 filter/category 的
        print("\n" + "=" * 60)
        print("  方法3: data-qa 属性搜索")
        print("=" * 60)
        result3 = await page.evaluate("""() => {
            const results = [];
            const all = document.querySelectorAll('[data-qa]');
            for (const el of all) {
                const qa = el.getAttribute('data-qa');
                if (/category|filter|facet|sidebar|browse/i.test(qa)) {
                    results.push({
                        tag: el.tagName,
                        dataQa: qa,
                        text: el.innerText.substring(0, 200),
                        childCount: el.children.length,
                    });
                }
            }
            return results;
        }""")
        for item in result3:
            print(f"  data-qa='{item['dataQa']}' <{item['tag']}> children={item['childCount']}")
            print(f"    text: {item['text'][:150]}")

        # 4. 查找所有链接中含 category_id 或 f[category] 参数的
        print("\n" + "=" * 60)
        print("  方法4: URL参数中含 category 的链接")
        print("=" * 60)
        result4 = await page.evaluate("""() => {
            const results = [];
            const links = document.querySelectorAll('a[href*="category"]');
            for (const a of links) {
                results.push({
                    text: a.innerText.trim().substring(0, 100),
                    href: a.getAttribute('href')?.substring(0, 200) || '',
                });
            }
            return results.slice(0, 30);
        }""")
        for item in result4:
            print(f"  '{item['text']}' → {item['href']}")

        # 5. 宽泛搜索：左侧面板的全部结构
        print("\n" + "=" * 60)
        print("  方法5: 页面全部筛选区域文本（含子类目名称）")
        print("=" * 60)
        result5 = await page.evaluate("""() => {
            // Noon 的筛选器通常在主内容左侧
            // 尝试找到包含多个筛选项的面板
            const body = document.body.innerText;
            const lines = body.split('\\n').filter(l => l.trim());
            // 找到 "Category" 或类似标题附近的内容
            const results = [];
            for (let i = 0; i < lines.length; i++) {
                if (/^\\s*(category|categories|browse by)/i.test(lines[i].trim())) {
                    // 打印这行及后面20行
                    for (let j = i; j < Math.min(i + 25, lines.length); j++) {
                        results.push(lines[j].trim());
                    }
                    break;
                }
            }
            // 也找 "Fulfilled by" 附近看筛选器结构
            for (let i = 0; i < lines.length; i++) {
                if (/fulfilled by/i.test(lines[i].trim())) {
                    results.push('--- near Fulfilled by ---');
                    for (let j = Math.max(0, i - 5); j < Math.min(i + 10, lines.length); j++) {
                        results.push(lines[j].trim());
                    }
                    break;
                }
            }
            return results;
        }""")
        for line in result5:
            print(f"  {line}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
