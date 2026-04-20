const { test, expect } = require("@playwright/test");
const path = require("path");

const BASE_URL = "http://127.0.0.1:8865";
const OUTPUT_DIR = path.join(__dirname, "..", "data", "reports", "ui_iteration_r8");

async function ensureSelectionLoaded(page) {
  await page.goto(`${BASE_URL}/?view=selection`, { waitUntil: "networkidle" });
  await page.waitForSelector("#products-body tr");
}

async function ensureKeywordLoaded(page) {
  await page.goto(`${BASE_URL}/?view=keyword`, { waitUntil: "networkidle" });
  await page.waitForSelector("#keyword-answer-panel");
  await page.waitForTimeout(1200);
}

test.describe("ERP selection and keyword iteration", () => {
  test("desktop captures", async ({ page }) => {
    await page.setViewportSize({ width: 1600, height: 1180 });

    await ensureSelectionLoaded(page);
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_desktop.png"), fullPage: true });

    await page.click('[data-action="open-category-selector"]');
    await page.waitForSelector("#category-selector-modal:not([hidden])");
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_desktop.png"), fullPage: true });

    await page.click('#category-tree [data-action="pick-category"]');
    await page.waitForTimeout(200);
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_multi_desktop.png"), fullPage: true });

    await page.click("#category-selector-confirm");
    await page.waitForSelector("#products-body tr");
    await page.click("#products-body tr");
    await page.waitForSelector("#product-drawer[aria-hidden='false']");
    await expect(page.locator("#drawer-keyword-rankings")).toBeVisible();
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_desktop.png"), fullPage: true });

    await ensureKeywordLoaded(page);
    await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_desktop.png"), fullPage: true });
  });

  test("tablet captures", async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 1280 });

    await ensureSelectionLoaded(page);
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_tablet.png"), fullPage: true });

    await page.click('[data-action="open-category-selector"]');
    await page.waitForSelector("#category-selector-modal:not([hidden])");
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_tablet.png"), fullPage: true });

    await page.click("#category-selector-confirm");
    await page.waitForSelector("#products-body tr");
    await page.click("#products-body tr");
    await page.waitForSelector("#product-drawer[aria-hidden='false']");
    await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_tablet.png"), fullPage: true });

    await ensureKeywordLoaded(page);
    await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_tablet.png"), fullPage: true });
  });
});
