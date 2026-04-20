const path = require("path");
const fs = require("fs");
const { chromium } = require("playwright");

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

async function captureDesktop(browser) {
  const page = await browser.newPage({ viewport: { width: 1600, height: 1180 } });
  await ensureSelectionLoaded(page);
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_desktop.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_desktop_view.png") });

  await page.click('#products-filters [data-action="open-category-selector"]');
  await page.waitForSelector("#category-selector-modal:not([hidden])");
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_desktop.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_desktop_view.png") });

  const pickTargets = page.locator('#category-tree [data-action="pick-category"]');
  if (await pickTargets.count()) {
    await pickTargets.nth(0).click();
    if (await pickTargets.count() > 1) await pickTargets.nth(1).click();
    await page.waitForTimeout(250);
  }
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_multi_desktop.png"), fullPage: true });

  await page.click("#category-selector-confirm");
  await page.waitForSelector("#products-body tr");
  await page.locator("#products-body tr").nth(0).click();
  await page.waitForSelector("#product-drawer[aria-hidden='false']");
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_desktop.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_desktop_view.png") });

  await ensureKeywordLoaded(page);
  await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_desktop.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_desktop_view.png") });
  await page.close();
}

async function captureTablet(browser) {
  const page = await browser.newPage({ viewport: { width: 1024, height: 1280 } });
  await ensureSelectionLoaded(page);
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_tablet.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_tablet_view.png") });

  await page.click('#products-filters [data-action="open-category-selector"]');
  await page.waitForSelector("#category-selector-modal:not([hidden])");
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_tablet.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_modal_tablet_view.png") });

  await page.click("#category-selector-confirm");
  await page.waitForSelector("#products-body tr");
  await page.locator("#products-body tr").nth(0).click();
  await page.waitForSelector("#product-drawer[aria-hidden='false']");
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_tablet.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "selection_drawer_tablet_view.png") });

  await ensureKeywordLoaded(page);
  await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_tablet.png"), fullPage: true });
  await page.screenshot({ path: path.join(OUTPUT_DIR, "keyword_tablet_view.png") });
  await page.close();
}

(async () => {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  try {
    await captureDesktop(browser);
    await captureTablet(browser);
  } finally {
    await browser.close();
  }
})();
