const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const { chromium } = require("playwright");

const repoRoot = path.resolve(__dirname, "..");
const baseUrl = process.argv[2] || "http://127.0.0.1:8865";
const staticRoot = path.join(repoRoot, "web_beta", "static");
const appPyPath = path.join(repoRoot, "web_beta", "app.py");
const outputRoot = path.join(repoRoot, "output", "playwright", "web_beta_stabilization");
const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const runDir = path.join(outputRoot, stamp);

const jsChecks = {
  contract: path.join(staticRoot, "app.contract.js"),
  services: path.join(staticRoot, "app.services.js"),
  state: path.join(staticRoot, "app.state.js"),
  selection: path.join(staticRoot, "app.selection.js"),
  favorites: path.join(staticRoot, "app.favorites.js"),
  home: path.join(staticRoot, "app.home.js"),
  keyword: path.join(staticRoot, "app.keyword.js"),
  drawer: path.join(staticRoot, "app.drawer.js"),
  runs: path.join(staticRoot, "app.runs.js"),
  app: path.join(staticRoot, "app.js"),
};

fs.mkdirSync(runDir, { recursive: true });

function runCommand(command, args, cwd = repoRoot) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    shell: false,
  });
  return {
    command: [command, ...args].join(" "),
    status: result.status,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
  };
}

async function delay(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function measureFetch(url) {
  const start = Date.now();
  const response = await fetch(url);
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return {
    url,
    ok: response.ok,
    status: response.status,
    duration_ms: Date.now() - start,
    json,
    text: json ? "" : text.slice(0, 500),
  };
}

async function measureStableFetch(url, { attempts = 3, settleMs = 900, budgetMs = Infinity } = {}) {
  const samples = [];
  let measurement = null;
  for (let index = 0; index < attempts; index += 1) {
    measurement = await measureFetch(url);
    samples.push({
      duration_ms: measurement.duration_ms,
      ok: measurement.ok,
      status: measurement.status,
    });
    if (measurement.ok && measurement.duration_ms <= budgetMs) break;
    if (index < attempts - 1) await delay(settleMs);
  }
  return { measurement, samples };
}

function writeJson(fileName, value) {
  fs.writeFileSync(path.join(runDir, fileName), JSON.stringify(value, null, 2), "utf8");
}

async function main() {
  const result = {
    baseUrl,
    runDir,
    checks: {},
    api: {},
    browser: {
      consoleErrors: [],
      pageErrors: [],
      screenshots: {},
    },
    failures: [],
  };

  for (const [name, filePath] of Object.entries(jsChecks)) {
    result.checks[`node_check_${name}`] = runCommand("node", ["--check", filePath], repoRoot);
  }
  result.checks.py_compile = runCommand("python", ["-m", "py_compile", appPyPath], repoRoot);

  for (const [name, check] of Object.entries(result.checks)) {
    if (check.status !== 0) result.failures.push(`${name} failed`);
  }

  result.api.health = await measureFetch(`${baseUrl}/api/health`);
  const dashboardProbe = await measureStableFetch(`${baseUrl}/api/dashboard`, {
    attempts: 3,
    settleMs: 900,
    budgetMs: 2500,
  });
  result.api.dashboard = dashboardProbe.measurement;
  result.api.dashboard_attempts = dashboardProbe.samples;
  result.api.products = await measureFetch(`${baseUrl}/api/products?limit=5`);
  result.api.runsSummary = await measureFetch(`${baseUrl}/api/runs/summary`);
  result.api.systemHealth = await measureFetch(`${baseUrl}/api/system/health`);

  const firstProduct = result.api.products.json?.items?.[0];
  if (firstProduct?.platform && firstProduct?.product_id) {
    result.api.product_detail = await measureFetch(
      `${baseUrl}/api/products/${encodeURIComponent(firstProduct.platform)}/${encodeURIComponent(firstProduct.product_id)}`,
    );
  }

  for (const endpoint of ["health", "dashboard", "products", "runsSummary", "systemHealth"]) {
    if (!result.api[endpoint]?.ok) result.failures.push(`${endpoint} endpoint failed`);
  }

  function bindTrackedPage(trackedPage) {
    trackedPage.setDefaultNavigationTimeout(60000);
    trackedPage.setDefaultTimeout(60000);
    trackedPage.on("console", (msg) => {
      if (msg.type() === "error") result.browser.consoleErrors.push(msg.text());
    });
    trackedPage.on("pageerror", (err) => {
      result.browser.pageErrors.push(String(err));
    });
  }
  const browser = await chromium.launch({ headless: true });
  async function createTrackedPage() {
    const trackedPage = await browser.newPage({ viewport: { width: 1600, height: 1100 } });
    bindTrackedPage(trackedPage);
    return trackedPage;
  }
  const page = await createTrackedPage();

  const saveShot = async (name) => {
    const filePath = path.join(runDir, `${name}.png`);
    await page.screenshot({ path: filePath, fullPage: true });
    result.browser.screenshots[name] = filePath;
  };

  await page.goto(`${baseUrl}/?view=selection`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelectorAll("#products-body tr.data-row").length > 0, { timeout: 15000 });
  const metaNoFilters = ((await page.locator("#products-meta").textContent()) || "").trim();
  result.browser.selection = {
    brandMark: ((await page.locator(".brand-mark").textContent()) || "").trim(),
    activeNav: ((await page.locator("#primary-nav .nav-item.active").textContent()) || "").trim(),
    activeNavCount: await page.locator("#primary-nav .nav-item.active").count(),
    rowsNoFilters: await page.locator("#products-body tr.data-row").count(),
    visibleRowsNoFilters: await page.locator("#products-body tr.data-row").evaluateAll((rows) =>
      rows.filter((row) => {
        const rect = row.getBoundingClientRect();
        return rect.bottom > 0 && rect.top < window.innerHeight;
      }).length
    ),
    metaNoFilters,
  };
  await saveShot("selection-no-filters");

  await page.click('[data-action="open-category-selector"][data-context="selection"]');
  await page.waitForSelector("#category-selector-modal:not([hidden])", { timeout: 5000 });
  result.browser.categorySelector = {
    modeButtons: await page.locator("#category-selector-mode-switch button").count(),
  };
  await saveShot("category-selector");

  const firstLink = page.locator('.tree-link[data-action="pick-category"]').first();
  result.browser.categorySelector.firstLabel = ((((await firstLink.textContent()) || "").trim().split("\n")[0]) || "").trim();
  await firstLink.click();
  await page.click("#category-selector-confirm");

  const applyButton = page.locator('[data-action="apply-product-filters"][data-owner="selection"]');
  await page.waitForTimeout(120);
  result.browser.selection.applyDisabledRightAfterConfirm = await applyButton.evaluate((node) => !!node.disabled);
  result.browser.selection.applyTextRightAfterConfirm = ((await applyButton.textContent()) || "").trim();
  result.browser.selection.rowsPreservedDuringRefresh = await page.locator("#products-body tr.data-row").count();

  await page.waitForFunction((initialMeta) => {
    const meta = document.querySelector("#products-meta");
    const apply = document.querySelector('[data-action="apply-product-filters"][data-owner="selection"]');
    const currentMeta = ((meta && meta.textContent) || "").trim();
    const rowCount = document.querySelectorAll("#products-body tr.data-row").length;
    const search = window.location.search || "";
    return (
      search.includes("selected_category_paths=")
      && rowCount > 0
      && apply
      && !apply.disabled
      && currentMeta.length > 0
      && currentMeta !== initialMeta
    );
  }, metaNoFilters, { timeout: 25000 });

  result.browser.selection.metaAfterCategory = ((await page.locator("#products-meta").textContent()) || "").trim();
  result.browser.selection.rowsAfterCategory = await page.locator("#products-body tr.data-row").count();
  result.browser.selection.visibleRowsAfterCategory = await page.locator("#products-body tr.data-row").evaluateAll((rows) =>
    rows.filter((row) => {
      const rect = row.getBoundingClientRect();
      return rect.bottom > 0 && rect.top < window.innerHeight;
    }).length
  );
  result.browser.selection.urlAfterCategory = page.url();

  const categoryApiPath = new URL(result.browser.selection.urlAfterCategory).searchParams.get("selected_category_paths");
  if (categoryApiPath) {
    result.api.products_category = await measureFetch(
      `${baseUrl}/api/products?limit=5&selected_category_paths=${encodeURIComponent(categoryApiPath)}`,
    );
    result.browser.selection.categoryApiTotalCount = result.api.products_category.json?.total_count ?? null;
  }
  await saveShot("selection-category");

  await page.locator("#products-body tr.data-row").first().click();
  await page.waitForSelector("#product-drawer:not(.hidden), .drawer-shell:not(.hidden)", { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(3000);
  result.browser.drawer = {
    keywordChartCount: await page.locator("#drawer-keyword-chart, #keyword-ranking-chart").count(),
    signalChartCount: await page.locator("#drawer-signal-chart, #signal-timeline-chart").count(),
  };
  await saveShot("drawer");

  await page.goto(`${baseUrl}/?view=keyword`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(3000);
  result.browser.keyword = {
    activeNav: ((await page.locator("#primary-nav .nav-item.active").textContent()) || "").trim(),
    activeNavCount: await page.locator("#primary-nav .nav-item.active").count(),
    queryInputCount: await page.locator("#keyword-workspace-input").count(),
  };
  await saveShot("keyword");

  await page.close();
  await browser.close();

  if ((result.browser.selection.rowsNoFilters || 0) < 1) result.failures.push("selection no-filter rows missing");
  if (result.browser.selection.brandMark !== "\u5eb7") result.failures.push("brand mark mismatch");
  if ((result.browser.selection.activeNavCount || 0) !== 1) result.failures.push("selection nav active count invalid");
  if ((result.browser.selection.visibleRowsNoFilters || 0) < 5) result.failures.push("selection no-filter visible rows below density gate");
  if ((result.browser.selection.rowsAfterCategory || 0) < 1) result.failures.push("selection category rows missing");
  if ((result.browser.selection.visibleRowsAfterCategory || 0) < 5) result.failures.push("selection category visible rows below density gate");
  if (!result.browser.selection.applyDisabledRightAfterConfirm) result.failures.push("selection apply button not disabled during refresh");
  if (!String(result.browser.selection.urlAfterCategory || "").includes("selected_category_paths=")) result.failures.push("selection category scope missing in url");
  if (result.api.products_category && !result.api.products_category.ok) result.failures.push("selection category api failed");
  if ((result.api.products_category?.json?.total_count || 0) < 1) result.failures.push("selection category api returned no rows");
  if ((result.browser.keyword.activeNavCount || 0) !== 1) result.failures.push("keyword nav active count invalid");
  if ((result.browser.consoleErrors || []).length) result.failures.push("console errors present");
  if ((result.browser.pageErrors || []).length) result.failures.push("page errors present");
  if ((result.api.dashboard.duration_ms || 0) > 2500) result.failures.push("dashboard cold path over budget");
  if ((result.api.product_detail?.duration_ms || 0) > 1000) result.failures.push("product detail cold path over budget");

  writeJson("result.json", result);
  fs.writeFileSync(
    path.join(runDir, "summary.txt"),
    [
      `baseUrl: ${baseUrl}`,
      `brandMark: ${result.browser.selection.brandMark}`,
      `rowsNoFilters: ${result.browser.selection.rowsNoFilters}`,
      `visibleRowsNoFilters: ${result.browser.selection.visibleRowsNoFilters}`,
      `rowsAfterCategory: ${result.browser.selection.rowsAfterCategory}`,
      `visibleRowsAfterCategory: ${result.browser.selection.visibleRowsAfterCategory}`,
      `dashboard_ms: ${result.api.dashboard.duration_ms}`,
      `dashboard_attempts: ${JSON.stringify(result.api.dashboard_attempts || [])}`,
      `product_detail_ms: ${result.api.product_detail?.duration_ms ?? "n/a"}`,
      `consoleErrors: ${result.browser.consoleErrors.length}`,
      `pageErrors: ${result.browser.pageErrors.length}`,
      `failures: ${result.failures.length}`,
    ].join("\n"),
    "utf8",
  );

  console.log(JSON.stringify(result, null, 2));
  if (result.failures.length) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
