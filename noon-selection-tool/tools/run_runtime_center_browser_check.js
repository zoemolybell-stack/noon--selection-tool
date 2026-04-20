const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const repoRoot = path.resolve(__dirname, "..");
const baseUrl = process.argv[2] || "http://127.0.0.1:8865";
const outputRoot = path.join(repoRoot, "output", "playwright", "web_beta_runtime_center");
const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const runDir = path.join(outputRoot, stamp);

fs.mkdirSync(runDir, { recursive: true });

async function main() {
  const result = {
    baseUrl,
    runDir,
    browser: {
      consoleErrors: [],
      pageErrors: [],
      screenshots: {},
      runs: {},
      home: {},
    },
    failures: [],
  };

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } });
  page.setDefaultNavigationTimeout(60000);
  page.setDefaultTimeout(60000);
  page.on("console", (msg) => {
    if (msg.type() === "error") result.browser.consoleErrors.push(msg.text());
  });
  page.on("pageerror", (error) => {
    result.browser.pageErrors.push(String(error));
  });

  await page.goto(`${baseUrl}/?view=runs`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelectorAll("#runtime-alert-list .runtime-alert-card").length > 0, { timeout: 30000 });
  await page.waitForTimeout(1000);
  result.browser.runs = {
    activeNav: ((await page.locator("#primary-nav .nav-item.active").textContent()) || "").trim(),
    activeNavCount: await page.locator("#primary-nav .nav-item.active").count(),
    runtimeAlertSummary: ((await page.locator("#runtime-alert-summary").textContent()) || "").trim(),
    runtimeAlertCards: await page.locator("#runtime-alert-list .runtime-alert-card").count(),
  };
  const runsShot = path.join(runDir, "runs.png");
  await page.screenshot({ path: runsShot, fullPage: true });
  result.browser.screenshots.runs = runsShot;

  await page.goto(`${baseUrl}/?view=home`, { waitUntil: "domcontentloaded" });
  await page.evaluate((scopePath) => {
    const url = new URL(window.location.href);
    url.searchParams.set("view", "home");
    url.searchParams.set("home_scope", scopePath);
    window.history.pushState({}, "", url.toString());
    window.dispatchEvent(new PopStateEvent("popstate"));
  }, "Home > Sports, Fitness & Outdoors");
  await page.waitForSelector("#dashboard-scope-meta", { timeout: 30000 });
  await page.waitForTimeout(2500);
  result.browser.home = {
    activeNav: ((await page.locator("#primary-nav .nav-item.active").textContent()) || "").trim(),
    activeNavCount: await page.locator("#primary-nav .nav-item.active").count(),
    scopeBeforeClear: ((await page.locator("#dashboard-scope-meta").textContent()) || "").trim(),
  };
  await page.click("#dashboard-home-scope-clear");
  await page.waitForTimeout(1500);
  result.browser.home.scopeAfterClear = ((await page.locator("#dashboard-scope-meta").textContent()) || "").trim();
  const homeShot = path.join(runDir, "home.png");
  await page.screenshot({ path: homeShot, fullPage: true });
  result.browser.screenshots.home = homeShot;

  await browser.close();

  if ((result.browser.runs.activeNavCount || 0) !== 1) result.failures.push("runs nav active count invalid");
  if ((result.browser.runs.runtimeAlertCards || 0) < 1) result.failures.push("runtime alert panel missing cards");
  if (!String(result.browser.runs.runtimeAlertSummary || "").trim()) result.failures.push("runtime alert summary missing");
  if ((result.browser.home.activeNavCount || 0) !== 1) result.failures.push("home nav active count invalid");
  if (!/\u5168\u5e73\u53f0/.test(String(result.browser.home.scopeAfterClear || ""))) result.failures.push("home scope clear failed");
  if ((result.browser.consoleErrors || []).length) result.failures.push("console errors present");
  if ((result.browser.pageErrors || []).length) result.failures.push("page errors present");

  const resultPath = path.join(runDir, "result.json");
  fs.writeFileSync(resultPath, JSON.stringify(result, null, 2), "utf8");
  console.log(JSON.stringify(result, null, 2));
  process.exit(result.failures.length ? 1 : 0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
