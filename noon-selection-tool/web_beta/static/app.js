const PAGE_SIZE = 50;
const UI_BUILD_TAG = "ui-v3-workbench-r46";
const contract = window.WEB_BETA_CONTRACT;
if (!contract) {
  throw new Error("WEB_BETA_CONTRACT is missing");
}
const services = window.WEB_BETA_SERVICES;
if (!services) {
  throw new Error("WEB_BETA_SERVICES is missing");
}
const stateModule = window.WEB_BETA_STATE;
if (!stateModule) {
  throw new Error("WEB_BETA_STATE is missing");
}
const selectionModuleFactory = window.WEB_BETA_SELECTION;
if (!selectionModuleFactory) {
  throw new Error("WEB_BETA_SELECTION is missing");
}
const favoritesModuleFactory = window.WEB_BETA_FAVORITES;
if (!favoritesModuleFactory) {
  throw new Error("WEB_BETA_FAVORITES is missing");
}
const homeModuleFactory = window.WEB_BETA_HOME;
if (!homeModuleFactory) {
  throw new Error("WEB_BETA_HOME is missing");
}
const keywordModuleFactory = window.WEB_BETA_KEYWORD;
if (!keywordModuleFactory) {
  throw new Error("WEB_BETA_KEYWORD is missing");
}
const drawerModuleFactory = window.WEB_BETA_DRAWER;
if (!drawerModuleFactory) {
  throw new Error("WEB_BETA_DRAWER is missing");
}
const runsModuleFactory = window.WEB_BETA_RUNS;
if (!runsModuleFactory) {
  throw new Error("WEB_BETA_RUNS is missing");
}
const {
  STORAGE_KEYS,
  LEGACY_STORAGE_KEYS,
  MAX_SAVED_VIEWS,
  MAX_RECENT_CONTEXT,
  MAX_COMPARE_PRODUCTS,
  MAX_COMPARE_KEYWORDS,
  VIEW_META,
  WORKSPACE_META,
  ROUTE_DEFAULTS,
  createDefaultWorkbenchState,
} = contract;
const {
  getJson,
  postJson,
  deleteJson,
  getJsonSafe,
} = services;
const {
  normalizeDensity,
  normalizeTab,
  normalizeKeywordMode,
  encodeSelectedCategoryPaths,
  getSelectedCategoryPaths,
  normalizeView,
  normalizeRoute,
  buildRouteUrl,
  parseRouteFromUrl,
} = stateModule;
const DELIVERY_LABELS = {
  express: "Express / 平台快配",
  supermall: "Supermall / 小时达",
  global: "Global / 全球购",
  marketplace: "Marketplace / 店铺发货",
};

const SOURCE_LABELS = {
  category: "类目 / Category",
  keyword: "关键词 / Keyword",
  both: "双来源 / Both",
};

const MARKET_LABELS = {
  ksa: "KSA / Saudi",
  uae: "UAE / Emirates",
};

const OPPORTUNITY_TYPE_LABELS = {
  supply_gap: "供需缺口 / Supply Gap",
  momentum: "趋势上升 / Momentum",
  margin: "利润空间 / Margin",
  watchlist: "观察列表 / Watchlist",
};

const RISK_LEVEL_LABELS = {
  low: "低风险 / Low",
  medium: "中风险 / Medium",
  high: "高风险 / High",
};

const TASK_TYPE_LABELS = {
  category_ready_scan: "Category Ready Scan / 类目批量扫描",
  category_single: "Category Single / 单类目",
  keyword_once: "Keyword Once / 单次关键词",
  keyword_monitor: "Keyword Monitor / 关键词监控",
  warehouse_sync: "Warehouse Sync / 仓库同步",
  keyword_batch: "Keyword Batch / 关键词批量",
};

const TASK_STATUS_LABELS = {
  pending: "Pending / 待执行",
  leased: "Leased / 已租约",
  running: "Running / 运行中",
  completed: "Completed / 已完成",
  failed: "Failed / 失败",
  cancelled: "Cancelled / 已取消",
  skipped: "Skipped / 已跳过",
};

const state = createDefaultWorkbenchState();

let shellMetricFrame = 0;
let imagePreviewHideTimer = 0;
let imagePreviewAnchor = null;

const PRODUCT_IMAGE_PREVIEW_WIDTH = 360;
const PRODUCT_IMAGE_PREVIEW_HEIGHT = 430;
const PRODUCT_IMAGE_PREVIEW_OFFSET = 16;

const el = (id) => document.getElementById(id);

const homeModule = homeModuleFactory.createHomeModule({
  state,
  el,
  asArray,
  num,
  formatNumber,
  formatPercent,
  formatPrice,
  formatDate,
  getHomeScopePaths,
  renderKpiCards,
  renderLineChart,
  renderBarChart,
  renderDonutChart,
  setTableLoading,
  nextModuleRequestKey,
  isActiveModuleRequest,
  clearModuleRequest,
  clearModuleTimer,
  getJson,
  escapeHtml,
});

const keywordModule = keywordModuleFactory.createKeywordModule({
  state,
  el,
  asArray,
  escapeHtml,
  truncate,
  formatNumber,
  formatPercent,
  formatPrice,
  formatScore,
  formatDate,
  renderKpiCards,
  renderLineChart,
  renderBarChart,
  renderDonutChart,
  getOpportunityTypeLabel,
  getRiskLevelLabel,
  filterSeriesByDays,
  getKeywordPoolItems,
  renderTabbar,
  getMarketLabel,
  loadKeywordProducts,
  loadSignalOptions,
  nextModuleRequestKey,
  isActiveModuleRequest,
  clearModuleRequest,
  clearModuleTimer,
  getJsonSafe,
  navigateWithPatch,
  setKeywordCoreLoading,
  setKeywordSecondaryLoading,
  formatInlineList,
});

const drawerModule = drawerModuleFactory.createDrawerModule({
  state,
  el,
  asArray,
  num,
  escapeHtml,
  truncate,
  formatNumber,
  formatPercent,
  formatPrice,
  formatScore,
  formatSignedNumber,
  formatDate,
  formatBsr,
  formatSignalDisplay,
  getRankStateLabel,
  getChart,
  hideProductImagePreview,
  makeProductKey,
  syncRouteToUrl,
  syncFocusedProductRows,
  rememberContext,
  renderRecentContext,
  isFavoriteProduct,
  prefillDrawerFromRow,
  getJson,
});

const runsModule = runsModuleFactory.createRunsModule({
  state,
  el,
  asArray,
  num,
  escapeHtml,
  truncate,
  formatNumber,
  formatDate,
  formatAgeSeconds,
  formatSignedNumber,
  renderOpsCard,
  renderHealthChip,
  renderWarningBlock,
  normalizeFreshnessWarningText,
  getTaskTypeLabel,
  getTaskStatusLabel,
  renderTaskStatusChip,
  safeJsonPreview,
  setTaskFormMessage,
  setTaskFormBusy,
  resetTaskFormFields,
  setListLoading,
  nextModuleRequestKey,
  isActiveModuleRequest,
  clearModuleRequest,
  getJsonSafe,
  postJson,
  navigateWithPatch,
});

function makeProductKey(platform, productId) {
  return `${String(platform || "").trim().toLowerCase()}::${String(productId || "").trim()}`;
}

function supportsHoverPreview() {
  return typeof window !== "undefined"
    && typeof window.matchMedia === "function"
    && window.matchMedia("(hover: hover) and (pointer: fine)").matches;
}

function clearProductImagePreviewTimer() {
  if (!imagePreviewHideTimer) return;
  window.clearTimeout(imagePreviewHideTimer);
  imagePreviewHideTimer = 0;
}

function scheduleProductImagePreviewHide() {
  clearProductImagePreviewTimer();
  imagePreviewHideTimer = window.setTimeout(() => hideProductImagePreview(), 110);
}

function positionProductImagePreview(anchor, preview) {
  if (!anchor || !preview) return;
  const rect = anchor.getBoundingClientRect();
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
  const canPlaceRight = viewportWidth - rect.right >= PRODUCT_IMAGE_PREVIEW_WIDTH + PRODUCT_IMAGE_PREVIEW_OFFSET + 12;
  const canPlaceLeft = rect.left >= PRODUCT_IMAGE_PREVIEW_WIDTH + PRODUCT_IMAGE_PREVIEW_OFFSET + 12;
  const placeRight = canPlaceRight || !canPlaceLeft;
  const left = placeRight
    ? rect.right + PRODUCT_IMAGE_PREVIEW_OFFSET
    : Math.max(12, rect.left - PRODUCT_IMAGE_PREVIEW_WIDTH - PRODUCT_IMAGE_PREVIEW_OFFSET);
  const top = Math.min(
    Math.max(12, rect.top - 12),
    Math.max(12, viewportHeight - PRODUCT_IMAGE_PREVIEW_HEIGHT - 12),
  );

  preview.style.left = `${Math.round(left)}px`;
  preview.style.top = `${Math.round(top)}px`;
  preview.dataset.placement = placeRight ? "right" : "left";
}

function hideProductImagePreview(immediate = false) {
  clearProductImagePreviewTimer();
  const preview = el("product-image-preview");
  imagePreviewAnchor = null;
  if (!preview) return;
  if (immediate) {
    preview.classList.remove("active");
    preview.hidden = true;
    preview.setAttribute("aria-hidden", "true");
    preview.innerHTML = "";
    return;
  }
  preview.classList.remove("active");
  window.setTimeout(() => {
    if (preview.classList.contains("active")) return;
    preview.hidden = true;
    preview.setAttribute("aria-hidden", "true");
    preview.innerHTML = "";
  }, 120);
}

function showProductImagePreview(anchor) {
  if (!supportsHoverPreview() || !anchor?.dataset.previewImage) return;
  const preview = el("product-image-preview");
  if (!preview) return;
  clearProductImagePreviewTimer();
  imagePreviewAnchor = anchor;
  const title = anchor.dataset.previewTitle || "Product Image";
  const meta = anchor.dataset.previewMeta || "";
  preview.innerHTML = `
    <div class="product-image-preview-card">
      <div class="product-image-preview-media">
        <img src="${escapeHtml(anchor.dataset.previewImage)}" alt="${escapeHtml(title)}" loading="eager" />
      </div>
      <div class="product-image-preview-copy">
        <div class="product-image-preview-title">${escapeHtml(truncate(title, 96))}</div>
        ${meta ? `<div class="product-image-preview-meta">${escapeHtml(truncate(meta, 96))}</div>` : ""}
      </div>
    </div>
  `;
  preview.hidden = false;
  preview.setAttribute("aria-hidden", "false");
  positionProductImagePreview(anchor, preview);
  requestAnimationFrame(() => preview.classList.add("active"));
}

function bindProductImagePreviewTargets(scope = document) {
  scope.querySelectorAll(".product-thumb-shell[data-preview-image]").forEach((anchor) => {
    if (anchor.dataset.previewBound === "1") return;
    anchor.dataset.previewBound = "1";
    anchor.addEventListener("pointerenter", () => {
      clearProductImagePreviewTimer();
      showProductImagePreview(anchor);
    });
    anchor.addEventListener("pointermove", () => {
      const preview = el("product-image-preview");
      if (!preview || imagePreviewAnchor !== anchor || preview.hidden) return;
      positionProductImagePreview(anchor, preview);
    });
    anchor.addEventListener("pointerleave", (event) => {
      const nextTarget = event.relatedTarget;
      if (el("product-image-preview")?.contains(nextTarget)) return;
      scheduleProductImagePreviewHide();
    });
    anchor.addEventListener("focus", () => showProductImagePreview(anchor));
    anchor.addEventListener("blur", (event) => {
      const nextTarget = event.relatedTarget;
      if (el("product-image-preview")?.contains(nextTarget)) return;
      scheduleProductImagePreviewHide();
    });
  });
}

const PRODUCT_FILTER_KEYS = [
  "market",
  "delivery",
  "is_ad",
  "tab",
  "sort",
  "bsr_min",
  "bsr_max",
  "review_min",
  "review_max",
  "rating_min",
  "rating_max",
  "price_min",
  "price_max",
  "sales_min",
  "sales_max",
  "inventory_min",
  "inventory_max",
  "review_growth_7d_min",
  "review_growth_14d_min",
  "rating_growth_7d_min",
  "rating_growth_14d_min",
  "selected_category_paths",
  "signal_tags",
  "has_sold_signal",
  "has_stock_signal",
  "has_lowest_price_signal",
  "signal_text",
];

const LOCAL_ONLY_ROUTE_KEYS = new Set([
  "density",
  "focus_platform",
  "focus_product",
  "compare",
  "time",
]);

const KEYWORD_PRODUCT_ROUTE_KEYS = new Set([
  "q",
  "market",
  "platform",
  "source",
  "delivery",
  "is_ad",
  "tab",
  "sort",
  "bsr_min",
  "bsr_max",
  "review_min",
  "review_max",
  "price_min",
  "price_max",
  "signal_tags",
  "has_sold_signal",
  "has_stock_signal",
  "has_lowest_price_signal",
  "signal_text",
  "keyword_offset",
]);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object") return [value];
  return [];
}

if (!Array.isArray(state.crawlerKeywordControlScopes) || !state.crawlerKeywordControlScopes.length) {
  state.crawlerKeywordControlScopes = ["baseline", "generated", "tracked", "manual"];
}
const CRAWLER_KEYWORD_MANAGER_TABS = ["active", "add", "disabled", "roots"];
const CRAWLER_KEYWORD_ROOT_RULE_PREFIX = "[root-rule]";
state.crawlerKeywordControls = state.crawlerKeywordControls || {
  monitor_config: "",
  monitor_label: "",
  baseline_keywords: [],
  disabled_keywords: [],
  blocked_roots: [],
  exclusions: [],
  effective_keyword_stats: {},
  updated_at: "",
};
state.crawlerKeywordActiveKeywords = Array.isArray(state.crawlerKeywordActiveKeywords)
  ? state.crawlerKeywordActiveKeywords
  : [];
state.crawlerKeywordControlMessage = state.crawlerKeywordControlMessage || "";
state.crawlerKeywordManagerOpen = Boolean(state.crawlerKeywordManagerOpen);
state.crawlerKeywordManagerTab = CRAWLER_KEYWORD_MANAGER_TABS.includes(state.crawlerKeywordManagerTab)
  ? state.crawlerKeywordManagerTab
  : "active";
state.crawlerKeywordManagerQuery = String(state.crawlerKeywordManagerQuery || "").trim();
state.crawlerKeywordManagerSourceFilter = String(state.crawlerKeywordManagerSourceFilter || "").trim().toLowerCase();
state.crawlerKeywordManagerTrackingFilter = String(state.crawlerKeywordManagerTrackingFilter || "").trim().toLowerCase();
state.crawlerKeywordManagerRootMatchMode = ["exact", "contains"].includes(String(state.crawlerKeywordManagerRootMatchMode || "").trim().toLowerCase())
  ? String(state.crawlerKeywordManagerRootMatchMode || "").trim().toLowerCase()
  : "contains";
state.crawlerKeywordControlDraft = String(state.crawlerKeywordControlDraft || "");
state.crawlerKeywordManagerSelection = state.crawlerKeywordManagerSelection && typeof state.crawlerKeywordManagerSelection === "object"
  ? state.crawlerKeywordManagerSelection
  : { active: [], disabled: [], roots: [] };
state.crawlerCategorySubcategoryOverridesDraft = Array.isArray(state.crawlerCategorySubcategoryOverridesDraft)
  ? state.crawlerCategorySubcategoryOverridesDraft
  : [];
state.crawlerCategoryControlMessage = state.crawlerCategoryControlMessage || "";
state.crawlerSubcategoryCatalog = Array.isArray(state.crawlerSubcategoryCatalog)
  ? state.crawlerSubcategoryCatalog
  : [];

function num(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") return "-";
  return new Intl.NumberFormat("en-US").format(num(value));
}

function formatScore(value) {
  if (value === null || value === undefined || value === "") return "-";
  return Number(value).toFixed(2);
}

function formatPercent(value) {
  if (value === null || value === undefined || value === "") return "-";
  return `${Number(value).toFixed(2)}%`;
}

function formatPrice(value, currency = "SAR") {
  if (value === null || value === undefined || value === "") return "-";
  return `${currency} ${Number(value).toFixed(2)}`;
}

function formatSignedNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  const prefix = number > 0 ? "+" : "";
  return `${prefix}${number.toFixed(digits)}`;
}

function formatDate(value) {
  if (!value) return "-";
  return String(value).replace("T", " ").slice(0, 16);
}

function pickSignalText(...values) {
  for (const value of values) {
    const text = String(value || "").trim();
    if (text) return text;
  }
  return "";
}

function getEffectiveSignalText(item, signalName) {
  return pickSignalText(
    item?.[signalName],
    item?.[`latest_${signalName}`],
    item?.[`sticky_${signalName}`],
  );
}

function getStickySeenField(signalName) {
  return {
    delivery_eta_signal_text: "sticky_delivery_eta_signal_seen_at",
    lowest_price_signal_text: "sticky_lowest_price_signal_seen_at",
    sold_recently_text: "sticky_sold_recently_seen_at",
    stock_signal_text: "sticky_stock_signal_seen_at",
    ranking_signal_text: "sticky_ranking_signal_seen_at",
  }[signalName] || "";
}

function isStickySignalFallback(item, signalName) {
  const latest = pickSignalText(item?.[`latest_${signalName}`], item?.[signalName]);
  const sticky = pickSignalText(item?.[`sticky_${signalName}`]);
  if (typeof item?.[`${signalName}_is_sticky`] === "boolean") {
    return item[`${signalName}_is_sticky`];
  }
  return !latest && Boolean(sticky);
}

function getSignalLastSeen(item, signalName) {
  if (isStickySignalFallback(item, signalName)) {
    const stickySeenField = getStickySeenField(signalName);
    return pickSignalText(
      item?.[`${signalName}_last_seen_at`],
      stickySeenField ? item?.[stickySeenField] : "",
    );
  }
  return "";
}

function formatSignalDisplay(text, lastSeen) {
  const cleanText = String(text || "").trim();
  if (!cleanText) return "-";
  if (!lastSeen) return cleanText;
  return `${cleanText} | last seen ${formatDate(lastSeen)}`;
}

function formatAgeSeconds(value) {
  if (value === null || value === undefined || value === "") return "-";
  const seconds = num(value, -1);
  if (seconds < 0) return "-";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function truncate(value, maxLength = 80) {
  if (!value) return "-";
  const text = String(value).trim();
  return text.length > maxLength ? `${text.slice(0, maxLength - 1)}...` : text;
}

function formatInlineList(value, fallback = "-") {
  const items = asArray(value).map((item) => String(item ?? "").trim()).filter(Boolean);
  return items.length ? items.join(" | ") : fallback;
}

function normalizeStateToken(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "-");
}

function getHealthChipClass(value) {
  const token = normalizeStateToken(value);
  if (["healthy", "fresh", "recent"].includes(token)) return "health-chip state-healthy";
  if (["active"].includes(token)) return "health-chip state-active";
  if (["warning", "stale", "attention", "delayed"].includes(token)) return "health-chip state-warning";
  return "health-chip state-neutral";
}

function renderHealthChip(label, value) {
  const text = value || "-";
  return `<span class="${getHealthChipClass(text)}">${escapeHtml(label)}: ${escapeHtml(text)}</span>`;
}

function renderWarningBlock(title, messages) {
  const items = asArray(messages).map((item) => String(item || "").trim()).filter(Boolean);
  if (!items.length) return "";
  return `
    <div class="warning-callout">
      <div class="item-title">${escapeHtml(title)}</div>
      <div class="warning-list compact">
        ${items.map((item) => `<div class="warning-item">${escapeHtml(item)}</div>`).join("")}
      </div>
    </div>
  `;
}

function normalizeFreshnessWarningText(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.includes("Latest warehouse sync failed")) {
    return "统一仓库最近一次同步失败 / Latest warehouse sync failed.";
  }
  if (text.includes("Category warehouse import looks stale")) {
    return "类目导入新鲜度偏旧 / Category warehouse import looks stale.";
  }
  if (text.includes("Keyword warehouse import looks stale")) {
    return "关键词导入新鲜度偏旧 / Keyword warehouse import looks stale.";
  }
  if (text.includes("Shared warehouse sync status not reported yet")) {
    return "共享同步状态尚未上报 / Shared warehouse sync status not reported yet.";
  }
  return text;
}

function renderOpsCard(title, summary, chips = [], metaLines = []) {
  return `
    <div class="list-card ops-card">
      <div class="item-title">${escapeHtml(title)}</div>
      <div class="list-meta ops-summary">${escapeHtml(summary || "-")}</div>
      ${chips.length ? `<div class="chip-row">${chips.join("")}</div>` : ""}
      ${metaLines.length ? `<div class="ops-meta">${metaLines.map((line) => `<div>${escapeHtml(line)}</div>`).join("")}</div>` : ""}
    </div>
  `;
}

function getDeliveryLabel(value) {
  return value ? (DELIVERY_LABELS[value] || value) : "未识别 / Blank";
}

function getSourceLabel(value) {
  return value ? (SOURCE_LABELS[value] || value) : "-";
}

function getMarketLabel(value) {
  return MARKET_LABELS[String(value || "").trim().toLowerCase()] || "全市场";
}

function getOpportunityTypeLabel(value) {
  return value ? (OPPORTUNITY_TYPE_LABELS[value] || value) : "-";
}

function getRiskLevelLabel(value) {
  return value ? (RISK_LEVEL_LABELS[value] || value) : "-";
}

function getTaskTypeLabel(value) {
  return value ? (TASK_TYPE_LABELS[value] || value) : "-";
}

function getTaskStatusLabel(value) {
  return value ? (TASK_STATUS_LABELS[value] || value) : "-";
}

function getTaskStatusChipClass(value) {
  const token = normalizeStateToken(value);
  if (["completed"].includes(token)) return "health-chip state-healthy";
  if (["running", "leased", "active"].includes(token)) return "health-chip state-active";
  if (["failed", "cancelled", "skipped"].includes(token)) return "health-chip state-warning";
  return "health-chip state-neutral";
}

function renderTaskStatusChip(value) {
  const text = getTaskStatusLabel(value);
  return `<span class="${getTaskStatusChipClass(value)}">${escapeHtml(text)}</span>`;
}

function safeJsonPreview(value, fallback = "-") {
  if (value === null || value === undefined || value === "") return fallback;
  try {
    const raw = typeof value === "string" ? value : JSON.stringify(value);
    return truncate(raw, 180);
  } catch {
    return truncate(String(value), 180);
  }
}

function syncRouteToUrl(replace = false) {
  const url = buildRouteUrl(state.route);
  if (replace) window.history.replaceState({}, "", url);
  else window.history.pushState({}, "", url);
}

function parseStoredJson(key, fallback) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function buildProductFilterMemoryRoute(sourceRoute = state.route) {
  return {
    q: sourceRoute.q || "",
    market: sourceRoute.market || "",
    platform: sourceRoute.platform || "",
    source: sourceRoute.source || "",
    category_path: sourceRoute.category_path || "",
    selected_category_paths: sourceRoute.selected_category_paths || "",
    keyword: sourceRoute.keyword || "",
    delivery: sourceRoute.delivery || "",
    is_ad: sourceRoute.is_ad || "",
    tab: sourceRoute.tab || "all",
    sort: sourceRoute.sort || "sales_desc",
    bsr_min: sourceRoute.bsr_min || "",
    bsr_max: sourceRoute.bsr_max || "",
    review_min: sourceRoute.review_min || "",
    review_max: sourceRoute.review_max || "",
    rating_min: sourceRoute.rating_min || "",
    rating_max: sourceRoute.rating_max || "",
    price_min: sourceRoute.price_min || "",
    price_max: sourceRoute.price_max || "",
    sales_min: sourceRoute.sales_min || "",
    sales_max: sourceRoute.sales_max || "",
    inventory_min: sourceRoute.inventory_min || "",
    inventory_max: sourceRoute.inventory_max || "",
    review_growth_7d_min: sourceRoute.review_growth_7d_min || "",
    review_growth_14d_min: sourceRoute.review_growth_14d_min || "",
    rating_growth_7d_min: sourceRoute.rating_growth_7d_min || "",
    rating_growth_14d_min: sourceRoute.rating_growth_14d_min || "",
    signal_tags: sourceRoute.signal_tags || "",
    signal_text: sourceRoute.signal_text || "",
  };
}

function getProductFilterSummary(routePayload = {}) {
  return buildRouteSummary(routePayload);
}

function buildRouteSummary(routePayload = state.route) {
  const parts = [];
  const categoryPaths = getSelectedCategoryPaths(routePayload);
  if (categoryPaths.length === 1) parts.push(categoryPaths[0].split(" > ").slice(-1)[0]);
  if (categoryPaths.length > 1) parts.push(`${categoryPaths.length} 个类目`);
  if (routePayload.market) parts.push(getMarketLabel(routePayload.market));
  if (routePayload.keyword) parts.push(routePayload.keyword);
  if (routePayload.delivery) parts.push(getDeliveryLabel(routePayload.delivery));
  if (routePayload.is_ad === "1") parts.push("广告");
  if (routePayload.is_ad === "0") parts.push("排除广告");
  if (routePayload.sales_min || routePayload.sales_max) parts.push(`销量 ${routePayload.sales_min || 0}-${routePayload.sales_max || "—"}`);
  if (routePayload.bsr_max) parts.push(`BSR ≤ ${routePayload.bsr_max}`);
  if (routePayload.review_max) parts.push(`Review ≤ ${routePayload.review_max}`);
  if (routePayload.price_min || routePayload.price_max) parts.push(`价格 ${routePayload.price_min || 0}-${routePayload.price_max || "—"}`);
  if (routePayload.signal_tags) parts.push(`${routePayload.signal_tags.split(",").filter(Boolean).length} 个信号标签`);
  return parts.join(" 路 ") || "当前商品筛选";
}

function isLocalUiMemoryMode() {
  return getEnvironmentDescriptor().kind === "beta";
}

function setProductFilterMemoryFromPayload(payload) {
  state.productFilterPresets = Array.isArray(payload?.presets) ? payload.presets : [];
  state.productFilterHistory = Array.isArray(payload?.history) ? payload.history : [];
  state.uiFilterMemoryLoaded = true;
}

function persistLocalProductFilterMemory() {
  if (!isLocalUiMemoryMode()) return;
  window.localStorage.setItem(STORAGE_KEYS.productFilterPresets, JSON.stringify(state.productFilterPresets.slice(0, 16)));
  window.localStorage.setItem(STORAGE_KEYS.productFilterHistory, JSON.stringify(state.productFilterHistory.slice(0, 12)));
}

async function deleteProductFilterPreset(presetId) {
  if (!presetId) return;
  if (isLocalUiMemoryMode()) {
    state.productFilterPresets = state.productFilterPresets.filter((item) => String(item.id) !== String(presetId));
    persistLocalProductFilterMemory();
    renderFilterForms();
    return;
  }
  await deleteJson(`/api/ui/filter-presets/${encodeURIComponent(presetId)}`);
  await loadProductFilterMemory();
  renderFilterForms();
}

async function setDefaultProductFilterPreset(presetId) {
  const preset = state.productFilterPresets.find((item) => String(item.id) === String(presetId));
  if (!preset) return;
  if (isLocalUiMemoryMode()) {
    state.productFilterPresets = state.productFilterPresets.map((item) => ({
      ...item,
      is_default: String(item.id) === String(presetId),
    }));
    persistLocalProductFilterMemory();
    renderFilterForms();
    return;
  }
  await postJson("/api/ui/filter-presets/" + encodeURIComponent(presetId), { is_default: true }, "PATCH");
  await loadProductFilterMemory();
  renderFilterForms();
}

function parseSignalTagCsv(rawValue = state.route.signal_tags) {
  return String(rawValue || "").split(",").map((item) => item.trim()).filter(Boolean);
}

function loadWorkbenchMemory() {
  const savedViews = parseStoredJson(STORAGE_KEYS.savedViews, []);
  state.savedViews = Array.isArray(savedViews) ? savedViews.slice(0, MAX_SAVED_VIEWS) : [];

  const compareTray = parseStoredJson(STORAGE_KEYS.compareTray, {});
  state.compareTray = {
    products: Array.isArray(compareTray.products) ? compareTray.products.slice(0, MAX_COMPARE_PRODUCTS) : [],
    keywords: Array.isArray(compareTray.keywords) ? compareTray.keywords.slice(0, MAX_COMPARE_KEYWORDS) : [],
  };

  const recentContext = parseStoredJson(STORAGE_KEYS.recentContext, {});
  state.recentContext = {
    categories: Array.isArray(recentContext.categories) ? recentContext.categories.slice(0, MAX_RECENT_CONTEXT) : [],
    keywords: Array.isArray(recentContext.keywords) ? recentContext.keywords.slice(0, MAX_RECENT_CONTEXT) : [],
    products: Array.isArray(recentContext.products) ? recentContext.products.slice(0, MAX_RECENT_CONTEXT) : [],
  };

  const storedRole = window.localStorage.getItem(STORAGE_KEYS.userRole)
    || window.localStorage.getItem(LEGACY_STORAGE_KEYS.userRole);
  state.userRole = storedRole === "admin" ? "admin" : "operator";
}

function persistUserRole() {
  if (!state.userSession?.allow_role_switch) return;
  window.localStorage.setItem(STORAGE_KEYS.userRole, state.userRole);
  window.localStorage.setItem(LEGACY_STORAGE_KEYS.userRole, state.userRole);
}

function setUserRole(role, syncSelect = true) {
  const normalizedRole = role === "admin" ? "admin" : "operator";
  const sessionRole = state.userSession?.role === "admin" ? "admin" : "operator";
  state.userRole = state.userSession?.allow_role_switch ? normalizedRole : sessionRole;
  persistUserRole();
  document.body.dataset.role = state.userRole;
  if (syncSelect && el("beta-role")) el("beta-role").value = state.userRole;
  if (
    (state.route.view === "runs" && !canAccessRuns())
    || (state.route.view === "crawler" && !canAccessCrawler())
  ) {
    state.route = normalizeRoute({ ...state.route, view: "home" });
    syncRouteToUrl(true);
  }
}

function canAccessRuns() {
  return ["admin", "operator"].includes(state.userRole);
}

function canAccessCrawler() {
  return state.userRole === "admin";
}

function canManageTasks() {
  return state.userRole === "admin";
}

function syncRoleControl() {
  const roleWrap = document.querySelector(".role-switch");
  const roleSelect = el("beta-role");
  if (!roleWrap || !roleSelect) return;
  const allowRoleSwitch = Boolean(state.userSession?.allow_role_switch);
  const displayRole = state.userRole === "admin" ? "admin" : "operator";
  roleSelect.value = displayRole;
  roleSelect.disabled = !allowRoleSwitch;
  roleWrap.classList.toggle("is-readonly", !allowRoleSwitch);
  roleWrap.title = allowRoleSwitch ? "" : "当前账号角色由后端权限控制";
  if (!allowRoleSwitch) {
    roleWrap.querySelector(".field-label")?.replaceChildren(document.createTextNode("角色"));
  }
}

async function loadUiSession() {
  try {
    const session = await getJson("/api/session");
    state.userSession = {
      email: String(session?.email || "").trim(),
      display_name: String(session?.display_name || "").trim(),
      role: String(session?.role || "operator").trim().toLowerCase() === "admin" ? "admin" : "operator",
      allow_role_switch: Boolean(session?.allow_role_switch),
      auth_mode: String(session?.auth_mode || "unknown").trim(),
    };
    state.userRole = state.userSession.allow_role_switch
      ? ((window.localStorage.getItem(STORAGE_KEYS.userRole) || window.localStorage.getItem(LEGACY_STORAGE_KEYS.userRole)) === "admin"
        ? "admin"
        : state.userSession.role)
      : state.userSession.role;
  } catch (error) {
    console.error("loadUiSession failed", error);
    state.userSession = {
      email: "",
      display_name: "",
      role: state.userRole === "admin" ? "admin" : "operator",
      allow_role_switch: false,
      auth_mode: "fallback",
    };
    state.userRole = "operator";
  }
  document.body.dataset.role = state.userRole;
  syncRoleControl();
}

function getEnvironmentDescriptor() {
  const host = window.location.hostname || "";
  if (host === "127.0.0.1" || host === "localhost") {
    return { label: "Local Beta", detail: "localhost", kind: "beta" };
  }
  if (host === "192.168.100.20" || host === "erp.huihaokang.uk") {
    return { label: "NAS Stable", detail: host, kind: "stable" };
  }
  return { label: "Unknown Env", detail: host || "-", kind: "unknown" };
}

function syncEnvironmentBadges() {
  const envNode = el("header-environment");
  const buildNode = el("header-build");
  const descriptor = getEnvironmentDescriptor();
  if (envNode) {
    envNode.textContent = `${descriptor.label} 路 ${descriptor.detail}`;
    envNode.dataset.env = descriptor.kind;
  }
  if (buildNode) buildNode.textContent = UI_BUILD_TAG;
  document.body.dataset.environment = descriptor.kind;
}

function updateWorkspaceContextInline() {
  const node = el("workspace-context-inline");
  if (!node) return;
  if (state.route.view === "home") {
    const paths = getHomeScopePaths(state.route);
    node.textContent = paths.length
      ? `当前首页范围：${paths.length} 个类目`
      : "当前工作区：全平台大盘";
    return;
  }
  if (state.route.view === "selection") {
    const paths = getSelectionScopePaths(state.route);
    node.textContent = paths.length
      ? `当前选品范围：${paths.length} 个类目`
      : "当前工作区：选品全表扫描";
    return;
  }
  if (state.route.view === "favorites") {
    const favoriteCount = num(state.favoriteSummary?.favorite_count, 0);
    node.textContent = favoriteCount
      ? `当前收藏夹：${formatNumber(favoriteCount)} 个商品`
      : "当前工作区：收藏夹";
    return;
  }
  if (state.route.view === "keyword") {
    node.textContent = state.route.keyword
      ? `当前关键词：${state.route.keyword}`
      : "当前工作区：关键词研究";
    return;
  }
  node.textContent = WORKSPACE_META[state.route.view]?.title || "研究工作台";
}

function syncWorkspaceMeta() {
  const meta = WORKSPACE_META[state.route.view] || WORKSPACE_META.home;
  const eyebrow = el("workspace-eyebrow");
  const title = el("workspace-title");
  const description = el("workspace-description");
  if (eyebrow) eyebrow.textContent = meta.eyebrow || "Workspace";
  if (title) title.textContent = meta.title || "研究工作台";
  if (description) {
    description.textContent = meta.description || "";
    description.hidden = !meta.description;
  }
  document.body.dataset.view = state.route.view;
  document.body.dataset.role = state.userRole;
  document.querySelectorAll("#primary-nav .nav-item[data-view]").forEach((node) => {
    const isActive = (node.dataset.view || "") === state.route.view;
    node.classList.toggle("active", isActive);
    if (isActive) node.setAttribute("aria-current", "page");
    else node.removeAttribute("aria-current");
  });
  updateWorkspaceContextInline();
}

function updateSidebarContext() {
  const node = el("sidebar-context");
  if (!node) return;
  node.textContent = WORKSPACE_META[state.route.view]?.description || "";
}

function renderSecondaryNav() {
  const tabsNode = el("secondary-nav-tabs");
  const controlsNode = el("secondary-nav-view-controls");
  if (!tabsNode || !controlsNode) return;
  const chips = [];
  if (state.route.view === "home") {
    const scopePaths = getHomeScopePaths(state.route);
    chips.push(`<span class="filter-chip accent-chip">${escapeHtml(scopePaths.length ? `已选 ${scopePaths.length} 个类目` : "全平台大盘")}</span>`);
    chips.push('<span class="filter-chip">研究首页 / Dashboard</span>');
  } else if (state.route.view === "selection") {
    const scopePaths = getSelectionScopePaths(state.route);
    chips.push(`<span class="filter-chip accent-chip">${escapeHtml(scopePaths.length ? `已选 ${scopePaths.length} 个类目` : "未限定类目")}</span>`);
    chips.push(`<span class="filter-chip">${escapeHtml(getMarketLabel(state.route.market))}</span>`);
    chips.push('<span class="filter-chip">Filter-first 工作台</span>');
  } else if (state.route.view === "favorites") {
    chips.push(`<span class="filter-chip accent-chip">${escapeHtml(`收藏 ${formatNumber(state.favoriteSummary?.favorite_count || 0)}`)}</span>`);
    chips.push('<span class="filter-chip">Favorites Workspace</span>');
  } else if (state.route.view === "keyword") {
    chips.push(`<span class="filter-chip accent-chip">${escapeHtml(state.route.keyword || "未选择关键词")}</span>`);
    chips.push(`<span class="filter-chip">${escapeHtml(getMarketLabel(state.route.market))}</span>`);
    chips.push('<span class="filter-chip">Query-first 工作台</span>');
  } else if (state.route.view === "crawler") {
    chips.push('<span class="filter-chip accent-chip">admin only</span>');
    chips.push('<span class="filter-chip">Crawler Control</span>');
  } else if (state.route.view === "runs") {
    chips.push('<span class="filter-chip accent-chip">运行中心</span>');
  }
  tabsNode.innerHTML = "";
  controlsNode.innerHTML = `<div class="workbench-context-strip">${chips.join("")}</div>`;
  bindSecondaryNavEvents();
  updateWorkspaceContextInline();
  scheduleShellMetrics();
}

function bindSecondaryNavEvents() {
  updateWorkspaceContextInline();
}

function scheduleShellMetrics() {
  window.requestAnimationFrame(() => {
    updateWorkspaceContextInline();
    if (typeof updateSidebarContext === "function") updateSidebarContext();
  });
}

function syncGlobalControls() {
  document.body.dataset.density = state.route.density || "compact";
}

function renderGlobalFilterSummary() {
  document.body.dataset.density = state.route.density || "compact";
}

function persistSavedViews() {
  window.localStorage.setItem(STORAGE_KEYS.savedViews, JSON.stringify(state.savedViews.slice(0, MAX_SAVED_VIEWS)));
}

function persistCompareTray() {
  window.localStorage.setItem(STORAGE_KEYS.compareTray, JSON.stringify(state.compareTray));
}

function persistRecentContext() {
  window.localStorage.setItem(STORAGE_KEYS.recentContext, JSON.stringify(state.recentContext));
}

function syncCompareRouteState() {
  const total = state.compareTray.products.length + state.compareTray.keywords.length;
  state.route.compare = total ? "1" : "";
}

function buildFavoriteLookup(items = []) {
  return asArray(items).reduce((acc, item) => {
    const key = makeProductKey(item.platform, item.product_id);
    if (key !== "::") acc[key] = true;
    return acc;
  }, {});
}

function isFavoriteProduct(platform, productId) {
  return Boolean(state.favoriteLookup?.[makeProductKey(platform, productId)]);
}

async function loadFavoriteProducts({ silent = false } = {}) {
  if (state.favoriteLoadPromise) return state.favoriteLoadPromise;
  if (!silent) {
    setTableLoading("favorites-body", "favorites-meta", 11, "正在加载收藏夹 / Loading favorites...");
  }
  let loadPromise = null;
  loadPromise = (async () => {
    const result = await getJsonSafe("/api/ui/product-favorites", { items: [], summary: {} }, "收藏夹");
    const payload = result.data || { items: [], summary: {} };
    state.favoriteProducts = payload;
    state.favoriteSummary = payload?.summary || {};
    state.favoriteLookup = buildFavoriteLookup(payload?.items || []);
    if (state.route.view === "favorites") {
      renderFavoritesWorkspace();
    }
    return payload;
  })();
  state.favoriteLoadPromise = loadPromise;
  try {
    return await loadPromise;
  } finally {
    state.favoriteLoadPromise = null;
  }
}

async function toggleProductFavorite(platform, productId) {
  if (!platform || !productId || state.favoriteBusy) return;
  state.favoriteBusy = true;
  try {
    if (isFavoriteProduct(platform, productId)) {
      await deleteJson(`/api/ui/product-favorites/${encodeURIComponent(platform)}/${encodeURIComponent(productId)}`);
    } else {
      await postJson("/api/ui/product-favorites", {
        platform,
        product_id: productId,
      });
    }
    await loadFavoriteProducts({ silent: state.route.view !== "favorites" });
    if (state.route.view === "favorites") renderFavoritesWorkspace();
    else renderVisibleProductTables();
    if (state.productDetail?.summary) renderProductDrawer(Boolean(state.productHistory));
  } finally {
    state.favoriteBusy = false;
  }
}

function renderSavedViews() {
  const saveButtons = document.querySelectorAll('[data-action="save-view"]');
  saveButtons.forEach((button) => {
    if (!(button instanceof HTMLElement)) return;
    button.dataset.savedViewCount = String(asArray(state.savedViews).length);
  });
}

function renderRecentContext() {
  const workspace = el("workspace-shell");
  if (!workspace) return;
  const productCount = asArray(state.recentContext?.products).length;
  const keywordCount = asArray(state.recentContext?.keywords).length;
  const categoryCount = asArray(state.recentContext?.categories).length;
  workspace.dataset.recentContext = `${categoryCount}:${keywordCount}:${productCount}`;
}

function renderCompareTray() {
  const tray = el("compare-tray");
  const productsNode = el("compare-products");
  const keywordsNode = el("compare-keywords");
  if (!tray || !productsNode || !keywordsNode) return;
  const products = asArray(state.compareTray?.products);
  const keywords = asArray(state.compareTray?.keywords);
  const isEmpty = !products.length && !keywords.length;
  tray.classList.toggle("is-empty", isEmpty);
  tray.classList.toggle("is-collapsed", isEmpty);
  productsNode.innerHTML = products.length
    ? products.map((item) => `
        <div class="compare-item">
          <div class="compare-item-main">
            <strong>${escapeHtml(item.title || item.label || item.id || "-")}</strong>
            <div class="table-subtitle">${escapeHtml(item.subtitle || item.product_id || "")}</div>
          </div>
          <button type="button" class="ghost-button mini-button" data-action="remove-compare-product" data-id="${escapeHtml(item.id || item.product_id || "")}">移除</button>
        </div>
      `).join("")
    : '<div class="table-subtitle">当前没有加入对比的商品。</div>';
  keywordsNode.innerHTML = keywords.length
    ? keywords.map((item) => `
        <div class="compare-item">
          <div class="compare-item-main">
            <strong>${escapeHtml(item.keyword || item.label || item.id || "-")}</strong>
            <div class="table-subtitle">${escapeHtml(item.source || item.market || "")}</div>
          </div>
          <button type="button" class="ghost-button mini-button" data-action="remove-compare-keyword" data-id="${escapeHtml(item.id || item.keyword || "")}">移除</button>
        </div>
      `).join("")
    : '<div class="table-subtitle">当前没有加入对比的关键词。</div>';
}

function rememberContext(kind, item) {
  if (!item) return;
  const list = Array.isArray(state.recentContext[kind]) ? state.recentContext[kind] : [];
  const next = { ...item, touched_at: new Date().toISOString() };
  state.recentContext[kind] = [next, ...list.filter((entry) => entry.id !== next.id)].slice(0, MAX_RECENT_CONTEXT);
  persistRecentContext();
}

function getChart(id) {
  const node = el(id);
  if (!node) return null;
  const existing = state.charts[id];
  if (existing) {
    try {
      if (existing.isDisposed && existing.isDisposed()) {
        delete state.charts[id];
      } else if (existing.getDom && existing.getDom() !== node) {
        existing.dispose();
        delete state.charts[id];
      }
    } catch {
      delete state.charts[id];
    }
  }
  if (!state.charts[id]) state.charts[id] = echarts.init(node);
  return state.charts[id];
}

function nextModuleRequestKey(name) {
  const token = `${name}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
  state.moduleRequestKeys[name] = token;
  return token;
}

function isActiveModuleRequest(name, token) {
  return state.moduleRequestKeys[name] === token;
}

function clearModuleRequest(name, token) {
  if (state.moduleRequestKeys[name] === token) {
    delete state.moduleRequestKeys[name];
  }
}

function clearModuleTimer(name) {
  if (state.moduleTimers[name]) {
    window.clearTimeout(state.moduleTimers[name]);
    delete state.moduleTimers[name];
  }
}

function setTableLoading(bodyId, metaId, colSpan, message) {
  const body = el(bodyId);
  const meta = el(metaId);
  if (meta) meta.textContent = message;
  if (body) {
    body.innerHTML = `
      <tr class="loading-row">
        <td colspan="${colSpan}">
          <div class="loading-state">${escapeHtml(message)}</div>
        </td>
      </tr>
    `;
  }
}

function setListLoading(containerId, message) {
  const container = el(containerId);
  if (!container) return;
  container.innerHTML = `<div class="loading-state">${escapeHtml(message)}</div>`;
}

function setLoadingText(id, message) {
  const node = el(id);
  if (node) node.textContent = message;
}

function getChangedRouteKeys(previousRoute, nextRoute) {
  const keys = new Set([
    ...Object.keys(previousRoute || {}),
    ...Object.keys(nextRoute || {}),
  ]);
  return Array.from(keys).filter((key) => String(previousRoute?.[key] ?? "") !== String(nextRoute?.[key] ?? ""));
}

function filterSeriesByDays(items, field) {
  const days = Math.max(1, num(state.route.time, 30));
  const cutoff = Date.now() - (days * 24 * 60 * 60 * 1000);
  return asArray(items).filter((item) => {
    const raw = item?.[field];
    if (!raw) return false;
    const value = new Date(raw).getTime();
    return Number.isFinite(value) ? value >= cutoff : false;
  });
}

function renderLineChart(id, xAxisData, series, yAxis, emptyText) {
  const chart = getChart(id);
  if (!chart) return;
  if (!xAxisData.length || !series.length) {
    chart.setOption({
      title: {
        text: emptyText,
        left: "center",
        top: "middle",
        textStyle: { color: "#6d665e", fontSize: 13, fontWeight: "normal" },
      },
      xAxis: [],
      yAxis: [],
      series: [],
    });
    return;
  }

  chart.setOption({
    color: ["#ffd11a", "#2563eb", "#dd6b20", "#0f8a5f"],
    tooltip: { trigger: "axis" },
    grid: { left: 42, right: 28, top: 24, bottom: 34 },
    xAxis: {
      type: "category",
      data: xAxisData,
      axisLabel: { color: "#6d665e" },
      axisLine: { lineStyle: { color: "rgba(25,22,19,.12)" } },
    },
    yAxis,
    series,
  });
}

function renderBarChart(id, labels, values, emptyText = "暂无数据 / No data") {
  const chart = getChart(id);
  if (!chart) return;
  if (!labels.length) {
    chart.setOption({
      title: {
        text: emptyText,
        left: "center",
        top: "middle",
        textStyle: { color: "#6d665e", fontSize: 13, fontWeight: "normal" },
      },
      xAxis: [],
      yAxis: [],
      series: [],
    });
    return;
  }

  chart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 40, right: 18, top: 20, bottom: 40 },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#6d665e", interval: 0, rotate: labels.length > 5 ? 20 : 0 },
      axisLine: { lineStyle: { color: "rgba(25,22,19,.12)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#6d665e" },
      splitLine: { lineStyle: { color: "rgba(25,22,19,.08)" } },
    },
    series: [{
      type: "bar",
      data: values,
      barMaxWidth: 34,
      itemStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: "#ffd11a" },
          { offset: 1, color: "#f1b400" },
        ]),
        borderRadius: [8, 8, 0, 0],
      },
    }],
  });
}

function renderDonutChart(id, items, palette = ["#0f8a5f", "#dd6b20", "#2563eb", "#ffd11a"], emptyText = "暂无数据 / No data") {
  const chart = getChart(id);
  if (!chart) return;
  const normalized = asArray(items)
    .filter((item) => num(item?.value || item?.product_count || 0) > 0)
    .map((item) => ({
      name: item.name || getDeliveryLabel(item.delivery_type),
      value: num(item.value || item.product_count || 0),
    }));

  if (!normalized.length) {
    chart.setOption({
      title: {
        text: emptyText,
        left: "center",
        top: "middle",
        textStyle: { color: "#6d665e", fontSize: 13, fontWeight: "normal" },
      },
      series: [],
    });
    return;
  }

  chart.setOption({
    color: palette,
    tooltip: { trigger: "item" },
    legend: { bottom: 0, left: "center", textStyle: { color: "#6d665e" } },
    series: [{
      type: "pie",
      radius: ["55%", "76%"],
      center: ["50%", "45%"],
      label: { formatter: "{b}\n{d}%" },
      data: normalized,
    }],
  });
}

function renderKpiCards(containerId, items) {
  const container = el(containerId);
  if (!container) return;
  container.innerHTML = asArray(items).map((item) => `
    <div class="kpi-card">
      <div class="kpi-label">${escapeHtml(item.label)}</div>
      <div class="kpi-value">${escapeHtml(item.value)}</div>
      <div class="kpi-note">${escapeHtml(item.note || "")}</div>
    </div>
  `).join("");
}

function renderSummaryChips(chips, emptyLabel) {
  return chips.length
    ? chips.map((item) => `<span class="filter-chip">${escapeHtml(item)}</span>`).join("")
    : `<span class="filter-chip">${escapeHtml(emptyLabel)}</span>`;
}

function buildProductFilterSummaryHtml(owner) {
  const chips = [];
  if (state.route.market) chips.push(`市场 ${getMarketLabel(state.route.market)}`);
  if (owner === "selection") {
    const selectedPaths = getSelectedCategoryPaths();
    if (selectedPaths.length === 1) chips.push(`类目 ${selectedPaths[0].split(" > ").slice(-1)[0]}`);
    else if (selectedPaths.length > 1) chips.push(`已选 ${selectedPaths.length} 个类目`);
  }
  if (owner === "keyword" && state.route.keyword) chips.push(`关键词 ${state.route.keyword}`);
  if (state.route.tab === "unranked") chips.push("未定级");
  if (state.route.sort && state.route.sort !== "bsr_asc") chips.push(getProductSortLabel(state.route.sort));
  if (state.route.delivery) chips.push(`配送 ${getDeliveryLabel(state.route.delivery)}`);
  if (state.route.is_ad === "1") chips.push("仅广告");
  if (state.route.is_ad === "0") chips.push("排除广告");
  if (state.route.bsr_min || state.route.bsr_max) chips.push(`BSR ${state.route.bsr_min || 1} - ${state.route.bsr_max || "—"}`);
  if (state.route.review_min || state.route.review_max) chips.push(`Review ${state.route.review_min || 0} - ${state.route.review_max || "—"}`);
  if (state.route.price_min || state.route.price_max) chips.push(`价格 ${state.route.price_min || 0} - ${state.route.price_max || "—"}`);
  if (state.route.signal_tags) chips.push(`${parseSignalTagCsv().length} 个信号标签`);
  if (state.route.signal_text) chips.push(`原始文本 ${truncate(state.route.signal_text, 18)}`);
  return renderSummaryChips(chips, "当前未启用额外筛选");
}

function renderDashboard() {
  if (homeModule?.renderDashboard) return homeModule.renderDashboard();
  const payload = state.dashboard;
  if (!payload) return;
  const overview = payload.overview || {};
  const importSeries = payload.import_series || {};
  const scope = payload.scope || {};
  const summary = scope.summary || {};
  const scopeLabel = scope.scope_label || "全平台 / 大盘";

  renderDashboardScopeSelectorOptions();
  renderKpiCards("dashboard-kpis", [
    { label: "商品总量", value: formatNumber(overview.product_count), note: "统一仓库唯一商品" },
    { label: "关键词总量", value: formatNumber(overview.keyword_count), note: "当前已收录关键词" },
    { label: "双来源商品", value: formatNumber(overview.dual_source_product_count ?? overview.overlap_count), note: "同时来自类目与关键词链路" },
    { label: "最近同步", value: overview.last_sync_at ? formatDate(overview.last_sync_at) : "-", note: "以 shared sync 为准" },
  ]);

  renderLineChart(
    "dashboard-category-import-chart",
    asArray(importSeries.days),
    [{
      name: "Category",
      type: "line",
      smooth: true,
      data: asArray(importSeries.category_products),
      itemStyle: { color: "#8d7a34" },
      areaStyle: { color: "rgba(141,122,52,.12)" },
    }],
    [{ type: "value" }],
    "暂无类目导入趋势",
  );

  renderLineChart(
    "dashboard-keyword-import-chart",
    asArray(importSeries.days),
    [{
      name: "Keyword",
      type: "line",
      smooth: true,
      data: asArray(importSeries.keyword_products),
      itemStyle: { color: "#215d7a" },
      areaStyle: { color: "rgba(33,93,122,.12)" },
    }],
    [{ type: "value" }],
    "暂无关键词导入趋势",
  );

  const scopeMeta = el("dashboard-scope-meta");
  if (scopeMeta) {
    scopeMeta.textContent = [
      scopeLabel,
      "Express " + formatPercent(summary.express_share_pct),
      "广告 " + formatPercent(summary.ad_share_pct),
      "均价 " + formatPrice(summary.avg_price),
      "最近采集 " + formatDate(summary.latest_observed_at),
    ].join(" · ");
  }

  renderKpiCards("dashboard-scope-cards", [
    { label: "商品数", value: formatNumber(summary.product_count), note: scopeLabel },
    { label: "Express 占比", value: formatPercent(summary.express_share_pct), note: "平台快配覆盖" },
    { label: "广告占比", value: formatPercent(summary.ad_share_pct), note: "当前广告密度" },
    { label: "BSR 覆盖", value: formatPercent(summary.bsr_coverage_pct), note: "可见排名覆盖" },
    { label: "信号覆盖", value: formatPercent(summary.signal_coverage_pct), note: "公开信号覆盖" },
    { label: "均价", value: formatPrice(summary.avg_price), note: "当前范围均价" },
  ]);

  renderBarChart(
    "dashboard-price-band-chart",
    asArray(scope.price_bands).map((item) => item.label || "-"),
    asArray(scope.price_bands).map((item) => num(item.count, 0)),
    "暂无价格带分布",
  );
  renderDonutChart(
    "dashboard-delivery-mix-chart",
    asArray(scope.delivery_breakdown).map((item) => ({ name: getDeliveryLabel(item.delivery_type), value: num(item.product_count, 0) })),
    undefined,
    "暂无配送结构",
  );
  renderDonutChart(
    "dashboard-ad-structure-chart",
    asArray(scope.ad_structure).map((item) => ({ name: item.label || "-", value: num(item.count, 0) })),
    ["#d3c3ab", "#2457d6"],
    "暂无广告结构",
  );
  renderDonutChart(
    "dashboard-signal-coverage-chart",
    asArray(scope.signal_structure).map((item) => ({ name: item.label || "-", value: num(item.count, 0) })),
    ["#0f8a5f", "#dd6b20", "#cf9f00"],
    "暂无信号覆盖",
  );
  renderDashboardCategoryTable(scope.child_categories || []);
}

function buildSignalGroupsMarkup(owner) {
  const groups = groupSignalOptionsForRender(owner);
  const selected = new Set(parseSignalTagCsv());
  if (!groups.length) return '<div class="empty-state compact-empty">当前范围内没有可用信号标签。</div>';
  return groups.map((group) => {
    const itemsHtml = group.items.map((item) => {
      const activeClass = selected.has(item.key) ? "active" : "";
      return '<button type="button" class="signal-tag-chip ' + activeClass
        + '" data-action="toggle-signal-tag" data-owner="' + escapeHtml(owner)
        + '" data-tag="' + escapeHtml(item.key)
        + '" title="' + escapeHtml(item.example_text || item.label || "")
        + '"><span>' + escapeHtml(item.label || "-")
        + '</span><span class="signal-tag-count">' + formatNumber(item.match_count)
        + "</span></button>";
    }).join("");
    return '<div class="signal-group-block"><div class="subsection-title">'
      + escapeHtml(group.label)
      + '</div><div class="signal-chip-grid">'
      + itemsHtml
      + "</div></div>";
  }).join("");
}

function buildSelectionPresetMarkup() {
  if (!state.productFilterPresets.length) {
    return '<div class="table-subtitle">当前还没有保存设置。</div>';
  }
  const cards = state.productFilterPresets.map((item) => {
    const defaultText = item.is_default ? "默认中" : "设为默认";
    const defaultClass = item.is_default ? "is-default" : "";
    return '<div class="memory-chip-card ' + defaultClass + '">'
      + '<button type="button" class="mini-button" data-action="apply-product-filter-preset" data-preset-id="' + escapeHtml(item.id) + '">'
      + escapeHtml(item.preset_name || "未命名设置")
      + '</button>'
      + '<button type="button" class="mini-button subtle" data-action="set-default-product-filter-preset" data-preset-id="' + escapeHtml(item.id) + '">'
      + defaultText
      + '</button>'
      + '<button type="button" class="mini-button subtle" data-action="delete-product-filter-preset" data-preset-id="' + escapeHtml(item.id) + '">删除</button>'
      + "</div>";
  }).join("");
  return '<div class="selection-preset-row">' + cards + "</div>";
}

const selectionModule = selectionModuleFactory.createSelectionModule({
  state,
  el,
  asArray,
  num,
  escapeHtml,
  truncate,
  formatNumber,
  formatPercent,
  getDeliveryLabel,
  getMarketLabel,
  getSelectedCategoryPaths,
  getSelectionScopePaths,
  encodeSelectedCategoryPaths,
  buildProductFilterSummaryHtml,
  buildSelectionPresetMarkup,
  renderBarChart,
  renderDonutChart,
});
const {
  renderSelectionSummaryStrip,
  renderSelectionContextPanel,
  renderSelectionFilterSummary,
  buildSelectionToolbarMarkup,
  syncOwnerFilters,
  collectProductPatch,
} = selectionModule;
const favoritesModule = favoritesModuleFactory.createFavoritesModule({
  state,
  el,
  asArray,
  num,
  escapeHtml,
  formatNumber,
  formatDate,
});
const {
  renderFavoritesSummaryStrip,
} = favoritesModule;

renderDashboardScopeSelectorOptions = homeModule.renderDashboardScopeSelectorOptions;
renderDashboardCategoryTable = homeModule.renderDashboardCategoryTable;
renderDashboard = homeModule.renderDashboard;
loadHomeCore = homeModule.loadHomeCore;
loadHomeSecondary = homeModule.loadHomeSecondary;
loadHomeView = homeModule.loadHomeView;

renderKeywordPoolTree = keywordModule.renderKeywordPoolTree;
renderKeywordPoolBrief = keywordModule.renderKeywordPoolBrief;
renderKeywordWorkspace = keywordModule.renderKeywordWorkspace;
renderKeywordIntelligence = keywordModule.renderKeywordIntelligence;
loadKeywordCore = keywordModule.loadKeywordCore;
loadKeywordSecondary = keywordModule.loadKeywordSecondary;
loadKeywordView = keywordModule.loadKeywordView;
refreshSignalOptionsForView = keywordModule.refreshSignalOptionsForView;

syncDrawerLayoutState = drawerModule.syncDrawerLayoutState;
closeProductDrawer = drawerModule.closeProductDrawer;
openProductDrawer = drawerModule.openProductDrawer;
renderDrawerKeyfacts = drawerModule.renderDrawerKeyfacts;
renderDrawerSignalTimeline = drawerModule.renderDrawerSignalTimeline;
renderDrawerKeywordRankingSection = drawerModule.renderDrawerKeywordRankingSection;
renderDrawerContextSection = drawerModule.renderDrawerContextSection;
renderProductDrawer = drawerModule.renderProductDrawer;

function renderFilterForms() {
  const selectionToolbar = el("products-filters");
  const keywordToolbar = el("keyword-workspace-toolbar");
  const keywordFilters = el("keyword-product-filters");
  const categoryScreening = el("category-screening-filters");
  const categoryProductFilters = el("category-product-filters");
  if (categoryScreening) categoryScreening.innerHTML = "";
  if (categoryProductFilters) categoryProductFilters.innerHTML = "";
  if (selectionToolbar) selectionToolbar.innerHTML = buildSelectionToolbarMarkup();
  if (keywordToolbar) keywordToolbar.innerHTML = buildKeywordToolbarMarkup();
  if (keywordFilters) keywordFilters.innerHTML = "";
  syncOwnerFilters();
  bindDynamicProductFilterUi();
  prepareWorkspaceDom();
  syncSelectionActionButtons();
}

function syncSelectionActionButtons() {
  const busy = Boolean(state.selectionLoadPromise || state.selectionInteractionBusy);
  const filterShell = el("products-filters");
  if (filterShell) {
    filterShell.dataset.busy = busy ? "true" : "false";
    filterShell
      .querySelectorAll("input, select, button")
      .forEach((node) => {
        node.disabled = busy;
      });
  }
  const applyButton = document.querySelector('[data-action="apply-product-filters"][data-owner="selection"]');
  const resetButton = document.querySelector('[data-action="reset-product-filters"][data-owner="selection"]');
  const categoryButton = document.querySelector('[data-action="open-category-selector"][data-context="selection"]');
  if (applyButton) {
    applyButton.disabled = busy;
    applyButton.textContent = busy ? "加载中..." : "应用";
  }
  if (resetButton) resetButton.disabled = busy;
  if (categoryButton) categoryButton.disabled = busy;
}

function renderVisibleProductTables() {
  if (state.route.view === "selection") {
    renderProductTable("products-body", state.productsPayload, "products-meta", "当前筛选下没有商品结果 / No product opportunities.");
  } else if (state.route.view === "favorites") {
    renderProductTable("favorites-body", state.favoriteProducts, "favorites-meta", "收藏夹还没有商品 / No favorites yet.");
  } else if (state.route.view === "keyword") {
    renderProductTable("keyword-products-body", state.keywordProducts, "keyword-products-meta", "当前关键词下没有商品结果 / No keyword products.");
  }
}

function renderSelectionWorkspace() {
  renderSelectionSummaryStrip(state.productsPayload, state.dashboard);
  renderSelectionFilterSummary();
  renderProductTable("products-body", state.productsPayload, "products-meta", "当前筛选下没有商品结果 / No product opportunities.");
  updatePagination("products-pagination", "products-prev", "products-next", state.productsPayload);
}

function renderFavoritesWorkspace() {
  renderFavoritesSummaryStrip(state.favoriteProducts || { items: [], summary: {} });
  renderProductTable("favorites-body", state.favoriteProducts, "favorites-meta", "收藏夹还没有商品 / No favorites yet.");
}

async function ensureSelectionWorkspaceHydrated(forceReload = false) {
  if (state.route.view !== "selection") return;
  const body = el("products-body");
  const hasRenderedRows = Boolean(body?.querySelector("tr.data-row"));
  const hasLoadingRow = Boolean(body?.querySelector(".loading-row"));
  if (forceReload || !state.productsPayload) {
    await loadSelectionView();
    return;
  }
  if (hasLoadingRow || !hasRenderedRows) {
    renderSelectionWorkspace();
  }
}

function scheduleSelectionWorkspaceHydration(forceReload = false) {
  clearModuleTimer("selection-hydration");
  state.moduleTimers["selection-hydration"] = window.setTimeout(() => {
    ensureSelectionWorkspaceHydrated(forceReload).catch((error) => {
      console.error("selection workspace hydration failed", error);
    });
  }, 0);
}

function buildSelectionLoadSignature(routePayload = state.route, productsOffset = null) {
  const route = normalizeRoute({ ...routePayload });
  const dashboardParams = new URLSearchParams();
  getSelectedCategoryPaths(route).forEach((item) => dashboardParams.append("selected_category_paths", item));
  const productParams = buildProductQueryParams({
    ...route,
    limit: PAGE_SIZE,
    offset: productsOffset ?? route.products_offset ?? 0,
  });
  return `${dashboardParams.toString()}||${productParams.toString()}`;
}

async function loadSelectionView() {
  const routeSnapshot = normalizeRoute({ ...state.route });
  const routeSignature = buildSelectionLoadSignature(routeSnapshot, routeSnapshot.products_offset || 0);
  if (state.selectionLoadPromise && state.selectionLoadSignature === routeSignature) {
    return state.selectionLoadPromise;
  }

  let loadPromise = null;
  loadPromise = (async () => {
    await ensureCategoryTree();
    renderCategoryTree();
    const existingSelectionRows = document.querySelectorAll("#products-body tr.data-row").length;
    if (existingSelectionRows > 0) {
      setLoadingText("products-meta", "正在刷新选品结果 / Refreshing selection products...");
    } else {
      setTableLoading("products-body", "products-meta", 11, "正在加载选品结果 / Loading selection products...");
    }
    const dashboardParams = new URLSearchParams();
    getSelectedCategoryPaths(routeSnapshot).forEach((item) => dashboardParams.append("selected_category_paths", item));
    try {
      const productsPayload = await getJson(
        `/api/products?${buildProductQueryParams({ ...routeSnapshot, limit: PAGE_SIZE, offset: routeSnapshot.products_offset || 0 }).toString()}`
      );
      if (state.route.view !== "selection") return;
      if (buildSelectionLoadSignature(state.route, state.route.products_offset || 0) !== routeSignature) return;
      state.productsPayload = productsPayload;
      renderSelectionWorkspace();
      const selectedPaths = getSelectedCategoryPaths(routeSnapshot);
      if (selectedPaths.length === 1) {
        rememberContext("categories", {
          id: selectedPaths[0],
          label: selectedPaths[0].split(" > ").slice(-1)[0] || selectedPaths[0],
          meta: selectedPaths[0],
        });
        renderRecentContext();
      }

      getJsonSafe(`/api/dashboard?${dashboardParams.toString()}`, { overview: {}, import_series: {}, scope: {} }, "选品摘要")
        .then((dashboardPayload) => {
          if (state.route.view !== "selection") return;
          if (buildSelectionLoadSignature(state.route, state.route.products_offset || 0) !== routeSignature) return;
          state.dashboard = dashboardPayload.data || dashboardPayload;
          renderSelectionSummaryStrip(state.productsPayload, state.dashboard);
        })
        .catch((error) => {
          console.error("selection summary load failed", error);
        });
    } catch (error) {
      renderSelectionSummaryStrip(null, null);
      renderSelectionContextPanel({});
      throw error;
    } finally {
      if (state.selectionLoadPromise === loadPromise) {
        state.selectionLoadPromise = null;
        state.selectionLoadSignature = "";
      }
      state.selectionInteractionBusy = false;
      syncSelectionActionButtons();
    }
  })();

  state.selectionInteractionBusy = false;
  state.selectionLoadSignature = routeSignature;
  state.selectionLoadPromise = loadPromise;
  syncSelectionActionButtons();
  return loadPromise;
}

async function loadFavoritesView() {
  await loadFavoriteProducts();
  renderFavoritesWorkspace();
}

function syncWorkbenchOverlayState() {
  document.body.classList.toggle("selector-open", Boolean(state.categorySelectorOpen || state.crawlerKeywordManagerOpen));
}

function closeCategorySelector() {
  state.categorySelectorOpen = false;
  const modal = el("category-selector-modal");
  if (modal) modal.hidden = true;
  syncWorkbenchOverlayState();
  renderCategoryTree();
  scheduleShellMetrics();
}

function buildSourcePills(item) {
  const pills = [];
  if (item.has_category && item.has_keyword) pills.push('<span class="source-pill">双来源 / Both</span>');
  else if (item.has_category) pills.push('<span class="source-pill">类目 / Category</span>');
  else if (item.has_keyword) pills.push('<span class="source-pill">关键词 / Keyword</span>');
  if (item.latest_source_type) pills.push(`<span class="source-pill">${escapeHtml(getSourceLabel(item.latest_source_type))}</span>`);
  return pills.join("");
}

function getSalesSignalSummary(item, maxLength = 28) {
  const text = getEffectiveSignalText(item, "sold_recently_text");
  const displayText = formatSignalDisplay(text, getSignalLastSeen(item, "sold_recently_text"));
  return text ? truncate(displayText, maxLength) : "";
}

function buildSignalChips(item) {
  const chips = [];
  if (item.latest_is_ad) chips.push('<span class="signal-chip ad">Ad / 广告</span>');
  if (item.latest_is_bestseller) chips.push('<span class="signal-chip">Best Seller</span>');
  const stockSignal = getEffectiveSignalText(item, "stock_signal_text");
  if (stockSignal) {
    const stockLabel = isStickySignalFallback(item, "stock_signal_text") ? `${stockSignal} (last)` : stockSignal;
    chips.push(`<span class="signal-chip">${escapeHtml(truncate(stockLabel, 22))}</span>`);
  }
  const lowestSignal = getEffectiveSignalText(item, "lowest_price_signal_text");
  if (lowestSignal) {
    const lowestLabel = isStickySignalFallback(item, "lowest_price_signal_text") ? `${lowestSignal} (last)` : lowestSignal;
    chips.push(`<span class="signal-chip">${escapeHtml(truncate(lowestLabel, 22))}</span>`);
  }
  if (!chips.length) return '<span class="table-subtitle">-</span>';
  return chips.slice(0, 2).join("");
}

function getVisibleBsr(item) {
  if (item?.latest_visible_bsr_rank === null || item?.latest_visible_bsr_rank === undefined || item?.latest_visible_bsr_rank === "") return null;
  return Number(item.latest_visible_bsr_rank);
}

function getDominantDeliveryLabel(summary = {}) {
  const items = [
    { label: "Express", value: num(summary.express_share_pct, 0) },
    { label: "Supermall", value: num(summary.supermall_share_pct, 0) },
    { label: "Global", value: num(summary.global_share_pct, 0) },
    { label: "Marketplace", value: num(summary.marketplace_share_pct, 0) },
  ].sort((left, right) => right.value - left.value);
  if (!items.length || items[0].value <= 0) return "Mixed / 混合";
  return `${items[0].label} ${formatPercent(items[0].value)}`;
}

function buildCategoryResearchSummaryHtml(payload) {
  const summary = payload?.summary || {};
  return '<div class="detail-card"><div class="detail-label">类目说明</div><div class="detail-value">'
    + escapeHtml(summary.path || state.route.category_path || "当前 beta 已将类目研究主路径并入选品工作台。")
    + '</div></div>';
}
function buildCategoryPathButtons(categoryPaths, primaryCategoryPath) {
  if (!categoryPaths.length) {
    return `
      <div class="detail-card">
        <div class="detail-label">类目归属 / Category Paths</div>
        <div class="detail-value">-</div>
      </div>
    `;
  }

  const primary = primaryCategoryPath || categoryPaths[0]?.category_path || "";
  return `
    <div class="detail-card">
      <div class="detail-label">类目归属 / Category Paths</div>
      <div class="membership-chip-group">
        ${categoryPaths.map((item) => {
          const path = item.category_path || "";
          const label = path.split(" > ").slice(-1)[0] || path;
          const primaryClass = path === primary ? "primary" : "muted";
          return `<button class="ghost-button path-chip ${primaryClass}" data-action="open-category" data-path="${escapeHtml(path)}" title="${escapeHtml(path)}">${escapeHtml(label)}</button>`;
        }).join("")}
      </div>
      <div class="drawer-note">Primary Path / 主路径 ${escapeHtml(primary || "-")}</div>
    </div>
  `;
}

function buildKeywordMembershipButtons(keywords) {
  if (!keywords.length) {
    return `
      <div class="detail-card">
        <div class="detail-label">关键词归属 / Keyword Membership</div>
        <div class="detail-value">-</div>
      </div>
    `;
  }

  return `
    <div class="detail-card">
      <div class="detail-label">关键词归属 / Keyword Membership</div>
      <div class="membership-chip-group">
        ${keywords.map((item) => `
          <button class="ghost-button path-chip keyword" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword || "")}">
            ${escapeHtml(item.keyword || "-")}
          </button>
        `).join("")}
      </div>
    </div>
  `;
}

function syncProductTableHeaders() {
  document.querySelectorAll(".product-table thead tr").forEach((row) => {
    const headers = row.querySelectorAll("th");
    if (headers.length >= 11) {
      const labels = ["商品", "价格", "销量", "库存", "BSR", "Reviews", "Rating", "Rating 增长", "配送", "广告", "最近采集"];
      labels.forEach((label, index) => {
        headers[index].textContent = label;
      });
    }
  });
}

function syncFocusedProductRows(scope = document) {
  const focusPlatform = state.route.focus_platform || "";
  const focusProduct = state.route.focus_product || "";
  scope.querySelectorAll("tr.data-row[data-platform][data-product-id]").forEach((row) => {
    const active = Boolean(
      focusPlatform
      && focusProduct
      && row.dataset.platform === focusPlatform
      && row.dataset.productId === focusProduct,
    );
    row.classList.toggle("active", active);
  });
}

function buildOpportunityChips(item) {
  const chips = [];
  const visibleBsr = getVisibleBsr(item);
  if (visibleBsr && visibleBsr <= 100) chips.push('<span class="badge">Top 100 / 前 100</span>');
  if (num(item.latest_review_count, 0) <= 10) chips.push('<span class="badge">低评论 / Low Review</span>');
  if (item.latest_delivery_type) {
    chips.push(`<span class="badge ${escapeHtml(item.latest_delivery_type)}">${escapeHtml(getDeliveryLabel(item.latest_delivery_type))}</span>`);
  }
  if (item.latest_is_ad) chips.push('<span class="badge ad">广告 / Ad</span>');
  return chips.slice(0, 3).join("");
}

function getRankStateLabel(item) {
  return getVisibleBsr(item) ? "有 BSR" : "无 BSR";
}

function formatBsr(item) {
  const rank = getVisibleBsr(item);
  return rank ? `#${formatNumber(rank)}` : "-";
}

function buildProductRowHtml(item) {
  const key = makeProductKey(item.platform, item.product_id);
  state.rowCache[key] = item;
  const active = state.route.focus_platform === item.platform && state.route.focus_product === item.product_id;
  const favoriteActive = isFavoriteProduct(item.platform, item.product_id);
  const previewMeta = [item.brand || "", item.seller_name || ""].filter(Boolean).join(" | ");
  const imageHtml = item.image_url
    ? '<div class="product-thumb-shell" data-preview-image="' + escapeHtml(item.image_url)
      + '" data-preview-title="' + escapeHtml(item.title || item.product_id)
      + '" data-preview-meta="' + escapeHtml(previewMeta)
      + '" tabindex="0" aria-label="预览商品主图">'
      + '<img class="product-thumb" src="' + escapeHtml(item.image_url)
      + '" alt="' + escapeHtml(item.title || item.product_id)
      + '" loading="lazy" /></div>'
    : '<div class="product-thumb-shell placeholder"><div class="product-thumb placeholder">NO IMAGE</div></div>';
  const sourceValue = item.latest_source_value || item.latest_observed_category_path || item.latest_category_path || "-";
  const opportunityChips = buildOpportunityChips(item);
  const reviewCount = num(item.latest_review_count, 0);
  const visibleBsr = getVisibleBsr(item);
  const monthlySales = num(item.monthly_sales_estimate ?? item.sales_estimate ?? item.latest_sales_estimate, 0);
  const inventoryEstimate = num(item.inventory_left_estimate, 0);
  const stockSignalText = getEffectiveSignalText(item, "stock_signal_text");
  const hasInventorySignal = Boolean(item.has_stock_signal || item.has_inventory_signal || inventoryEstimate || stockSignalText);
  const salesSignalText = getSalesSignalSummary(item, 28);
  const rankingSignalText = getEffectiveSignalText(item, "ranking_signal_text");
  const deliveryEtaSignalText = getEffectiveSignalText(item, "delivery_eta_signal_text");
  const salesPrimary = monthlySales > 0 ? formatNumber(monthlySales) : "-";
  const salesSecondary = salesSignalText || "No sales signal";
  const sourceLabel = item.latest_source_type ? getSourceLabel(item.latest_source_type) : "Unknown";
  const metaLine = [item.brand || "-", item.seller_name || "-"].filter(Boolean).join(" · ") || "-";
  const contextParts = [];
  if (state.route.view === "favorites" && item.favorite_created_at) {
    contextParts.push(`收藏于 ${formatDate(item.favorite_created_at)}`);
  }
  contextParts.push(getCompactCategoryPath(item.latest_observed_category_path || item.latest_category_path || "-", 2));
  contextParts.push("Obs " + formatNumber(item.observation_count));
  const contextLine = contextParts.join(" · ");
  const rankLabel = visibleBsr && visibleBsr <= 100
    ? "Top 100"
    : truncate(rankingSignalText || getRankStateLabel(item), 24);
  const ratingGrowth = num(item.rating_growth_7d ?? item.latest_rating_growth_7d, 0);
  const inventoryLabel = hasInventorySignal ? formatInventoryEstimate(item) : "-";
  const inventorySecondary = hasInventorySignal
    ? truncate(stockSignalText || "Inventory signal detected", 26)
    : "No inventory signal";
  const title = escapeHtml(truncate(item.title, 54));
  const activeClass = active ? "active" : "";
  const favoriteLabel = favoriteActive
    ? (state.route.view === "favorites" ? "移出收藏" : "已收藏")
    : "收藏";
  const lowReviewNote = reviewCount <= 10 ? " · Low review" : "";
  const ratingValue = formatScore(item.latest_rating);
  const ratingGrowth14d = num(item.rating_growth_14d ?? item.latest_rating_growth_14d, 0);
  return '<tr class="data-row ' + activeClass + '" data-action="open-product" data-platform="' + escapeHtml(item.platform) + '" data-product-id="' + escapeHtml(item.product_id) + '">'
    + '<td><div class="product-cell">' + imageHtml
    + '<div class="meta-stack">'
    + '<div class="product-title-row"><div class="table-title">' + title + '</div>'
    + '<button type="button" class="mini-button subtle favorite-toggle ' + (favoriteActive ? "active" : "")
    + '" data-action="toggle-product-favorite" data-platform="' + escapeHtml(item.platform)
    + '" data-product-id="' + escapeHtml(item.product_id)
    + '" aria-pressed="' + (favoriteActive ? "true" : "false") + '">' + escapeHtml(favoriteLabel) + '</button></div>'
    + '<div class="table-subtitle">' + escapeHtml(truncate(metaLine, 38)) + '</div>'
    + '<div class="table-subtitle table-subline">' + escapeHtml(truncate(contextLine, 44)) + '</div>'
    + (opportunityChips ? '<div class="product-badge-row compact-badge-row">' + opportunityChips + '</div>' : '')
    + '</div></div></td>'
    + '<td><div class="table-title">' + formatPrice(item.latest_price, item.latest_currency || "SAR") + '</div>'
    + '<div class="table-subtitle">' + (item.latest_original_price ? "Original " + formatPrice(item.latest_original_price, item.latest_currency || "SAR") : "No original price") + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(salesPrimary) + '</div><div class="table-subtitle">' + escapeHtml(truncate(salesSecondary, 26)) + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(inventoryLabel) + '</div><div class="table-subtitle">' + escapeHtml(inventorySecondary) + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(formatBsr(item)) + '</div><div class="table-subtitle">' + escapeHtml(rankLabel) + '</div></td>'
    + '<td><div class="table-title">' + formatNumber(item.latest_review_count) + '</div><div class="table-subtitle">Rating ' + ratingValue + lowReviewNote + '</div></td>'
    + '<td><div class="table-title">' + ratingValue + '</div><div class="table-subtitle">7D ' + escapeHtml(formatSignedNumber(ratingGrowth || 0, 1)) + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(formatSignedNumber(ratingGrowth || 0, 1)) + '</div><div class="table-subtitle">14D ' + escapeHtml(formatSignedNumber(ratingGrowth14d || 0, 1)) + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(getDeliveryLabel(item.latest_delivery_type)) + '</div><div class="table-subtitle">' + escapeHtml(truncate(deliveryEtaSignalText || "-", 22)) + '</div></td>'
    + '<td><div class="table-title">' + (item.latest_is_ad ? "Ad" : "Organic") + '</div><div class="table-subtitle">' + escapeHtml(sourceLabel) + '</div></td>'
    + '<td><div class="table-title">' + escapeHtml(formatDate(item.latest_observed_at)) + '</div><div class="table-subtitle">' + escapeHtml((item.platform || "-").toUpperCase()) + ' · ' + escapeHtml(truncate(sourceValue, 14)) + '</div></td>'
    + '</tr>';
}

function renderProductTable(bodyId, payload, metaId, emptyMessage) {
  const body = el(bodyId);
  const meta = el(metaId);
  if (!body || !meta) return;
  if (!payload || !asArray(payload.items).length) {
    body.innerHTML = `<tr><td colspan="11"><div class="empty-state">${escapeHtml(emptyMessage)}</div></td></tr>`;
    meta.textContent = emptyMessage;
    hideProductImagePreview(true);
    return;
  }
  body.innerHTML = asArray(payload.items).map((item) => buildProductRowHtml(item)).join("");
  bindProductImagePreviewTargets(body);
  syncFocusedProductRows(body);
  syncProductTableHeaders();
  meta.textContent = formatNumber(payload.total_count) + " results";
}

function updatePagination(metaId, prevId, nextId, payload) {
  const meta = el(metaId);
  const prev = el(prevId);
  const next = el(nextId);
  if (!meta || !prev || !next) return;
  if (!payload) {
    meta.textContent = "-";
    prev.disabled = true;
    next.disabled = true;
    return;
  }
  const total = num(payload.total_count, 0);
  const offset = num(payload.offset, 0);
  const limit = num(payload.limit, PAGE_SIZE);
  const start = total ? offset + 1 : 0;
  const end = Math.min(total, offset + limit);
  meta.textContent = `${formatNumber(start)} - ${formatNumber(end)} / ${formatNumber(total)}`;
  prev.disabled = offset <= 0;
  next.disabled = offset + limit >= total;
}

function renderTabbar(id) {
  const container = el(id);
  if (!container) return;
  container.innerHTML = `
      <button class="tab-pill ${state.route.tab === "ranked" ? "active" : ""}" data-action="switch-tab" data-tab="ranked">有定级 / Ranked</button>
      <button class="tab-pill ${state.route.tab === "unranked" ? "active" : ""}" data-action="switch-tab" data-tab="unranked">未定级 / Unranked</button>
  `;
}

function renderKeywordList() {
  const container = el("keyword-list");
  if (!container) return;
  const items = asArray(state.keywordSummary?.items);
  container.innerHTML = items.length
    ? items.map((item) => `
      <div class="list-card ${item.keyword === state.route.keyword ? "active" : ""}">
        <div class="item-title">${escapeHtml(item.display_keyword || item.keyword)}</div>
        <div class="list-meta">${escapeHtml(item.grade || "-")} | score ${formatScore(item.total_score)} | matched ${formatNumber(item.matched_product_count)}</div>
        <div class="list-meta">${escapeHtml(formatDate(item.last_analyzed_at))}</div>
        <div class="inline-actions">
          <button class="mini-button" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword)}">打开 / Open</button>
          <button class="mini-button" data-action="add-compare-keyword" data-keyword="${escapeHtml(item.keyword)}">对比 / Compare</button>
        </div>
      </div>
    `).join("")
    : '<div class="empty-state">关键词样本仍在积累 / Keyword sample is still accumulating.</div>';
}

function getKeywordPoolItems() {
  const summaryItems = asArray(state.keywordSummary?.items);
  const opportunityItems = asArray(state.keywordOpportunities?.items);
  const summaryMap = new Map(summaryItems.map((item) => [item.keyword, item]));
  const merged = new Map();

  opportunityItems.forEach((item) => {
    const summary = summaryMap.get(item.keyword) || {};
    merged.set(item.keyword, {
      keyword: item.keyword,
      display_keyword: item.display_keyword || summary.display_keyword || item.keyword,
      opportunity_type: item.opportunity_type || "watchlist",
      priority_band: item.priority_band || "unassigned",
      root_seed_keyword: item.root_seed_keyword || summary.root_seed_keyword || "unseeded",
      score: item.opportunity_score ?? summary.total_score ?? 0,
      matched_product_count: item.matched_product_count ?? summary.matched_product_count ?? 0,
    });
  });

  summaryItems.forEach((item) => {
    if (!merged.has(item.keyword)) {
      merged.set(item.keyword, {
        keyword: item.keyword,
        display_keyword: item.display_keyword || item.keyword,
        opportunity_type: "watchlist",
        priority_band: "watchlist",
        root_seed_keyword: item.root_seed_keyword || item.keyword,
        score: item.total_score ?? 0,
        matched_product_count: item.matched_product_count ?? 0,
      });
    }
  });

  return [...merged.values()].sort((left, right) => {
    const scoreGap = num(right.score, 0) - num(left.score, 0);
    if (scoreGap !== 0) return scoreGap;
    return (right.matched_product_count || 0) - (left.matched_product_count || 0);
  });
}

function renderKeywordPoolTree() {
  if (keywordModule?.renderKeywordPoolTree) return keywordModule.renderKeywordPoolTree();
  const container = el("keyword-pool-tree");
  if (!container) return;

  const mergedItems = getKeywordPoolItems();
  const selectedKeyword = state.route.keyword || "";
  const query = (state.keywordPoolQuery || "").trim().toLowerCase();

  const hierarchy = {};
  mergedItems.forEach((item) => {
    const haystack = `${item.keyword} ${item.display_keyword} ${item.root_seed_keyword}`.toLowerCase();
    if (query && !haystack.includes(query)) return;
    hierarchy[item.opportunity_type] ||= {};
    hierarchy[item.opportunity_type][item.priority_band] ||= {};
    hierarchy[item.opportunity_type][item.priority_band][item.root_seed_keyword] ||= [];
    hierarchy[item.opportunity_type][item.priority_band][item.root_seed_keyword].push(item);
  });

  const renderLeaves = (items) => items
    .sort((left, right) => (right.score || 0) - (left.score || 0))
    .map((item) => `
      <div class="keyword-pool-leaf">
        <button type="button" class="${item.keyword === selectedKeyword ? "active" : ""}" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword)}">
          <div class="item-title">${escapeHtml(item.display_keyword || item.keyword)}</div>
          <div class="list-meta">score ${formatScore(item.score)} | matched ${formatNumber(item.matched_product_count)}</div>
        </button>
      </div>
    `).join("");

  const renderGroup = (title, content, level) => `
    <div class="keyword-pool-group level-${level}">
      <button type="button" class="${level < 3 ? "active" : ""}">
        <div class="item-title">${escapeHtml(title)}</div>
      </button>
      <div class="keyword-pool-children">${content}</div>
    </div>
  `;

  const rootMarkup = Object.entries(hierarchy).map(([type, priorityGroups]) => renderGroup(
    getOpportunityTypeLabel(type),
    Object.entries(priorityGroups).map(([priority, seeds]) => renderGroup(
      priority,
      Object.entries(seeds).map(([seed, items]) => renderGroup(seed, renderLeaves(items), 3)).join(""),
      2,
    )).join(""),
    1,
  )).join("");

  container.innerHTML = rootMarkup || '<div class="empty-state">当前关键词池没有可展示结果。</div>';
}

function renderKeywordPoolBrief() {
  if (keywordModule?.renderKeywordPoolBrief) return keywordModule.renderKeywordPoolBrief();
  const node = el("keyword-pool-brief");
  if (!node) return;
  const mergedItems = getKeywordPoolItems();
  const selected = mergedItems.find((item) => item.keyword === state.route.keyword) || null;
  if (!selected) {
    node.innerHTML = `
      <div class="detail-card">
        <div class="detail-label">当前状态</div>
        <div class="detail-value">先搜索或点击一个关键词，再在右侧查看摘要，并继续在下方查看命中商品。</div>
      </div>
    `;
    return;
  }
  node.innerHTML = `
    <div class="detail-card">
      <div class="detail-label">当前关键词</div>
      <div class="detail-value">${escapeHtml(selected.display_keyword || selected.keyword)}</div>
    </div>
    <div class="detail-card">
      <div class="detail-label">机会类型</div>
      <div class="detail-value">${escapeHtml(getOpportunityTypeLabel(selected.opportunity_type))}</div>
    </div>
    <div class="detail-card">
      <div class="detail-label">优先级 / Root Seed</div>
      <div class="detail-value">${escapeHtml(selected.priority_band || "-")} | ${escapeHtml(selected.root_seed_keyword || "-")}</div>
    </div>
    <div class="detail-card">
      <div class="detail-label">命中规模</div>
      <div class="detail-value">${formatNumber(selected.matched_product_count)} | score ${formatScore(selected.score || selected.total_score)}</div>
    </div>
  `;
}

function renderKeywordGraph() {
  const chart = getChart("keyword-graph-chart");
  if (!chart) return;
  const payload = state.keywordGraph;
  const nodes = asArray(payload?.nodes);
  if (!nodes.length) {
    chart.setOption({
      title: {
        text: "暂无扩词图谱 / No expansion graph yet",
        left: "center",
        top: "middle",
        textStyle: { color: "#6d665e", fontSize: 13, fontWeight: "normal" },
      },
      series: [],
    });
    return;
  }

  chart.setOption({
    tooltip: {
      trigger: "item",
        formatter: (params) => {
          if (params.dataType === "edge") {
            return `${escapeHtml(params.data.source)} -> ${escapeHtml(params.data.target)}<br />${escapeHtml(params.data.value || "-")}`;
          }
          const item = params.data || {};
          const flags = asArray(item.quality_flags).join(" | ");
          return [
            `<strong>${escapeHtml(item.display_keyword || item.keyword || "-")}</strong>`,
            `depth ${escapeHtml(item.depth ?? "-")}`,
            `opportunity ${escapeHtml(getOpportunityTypeLabel(item.opportunity_type))}`,
            `priority ${escapeHtml(item.priority_band || "-")} | evidence ${escapeHtml(item.evidence_strength || "-")}`,
            `score ${escapeHtml(formatScore(item.opportunity_score || item.latest_total_score))}`,
            item.decision_summary ? escapeHtml(item.decision_summary) : "",
            flags ? `flags ${escapeHtml(flags)}` : "",
          ].filter(Boolean).join("<br />");
        },
    },
    legend: [{ bottom: 0, left: "center" }],
    series: [{
      type: "graph",
      layout: "force",
      roam: true,
      draggable: true,
      label: {
        show: true,
        formatter: (params) => truncate(params.data?.display_keyword || params.data?.keyword || "-", 18),
      },
      data: nodes.map((item) => ({
        ...item,
        id: item.keyword,
        name: item.display_keyword || item.keyword,
        symbolSize: Math.max(26, Math.min(64, 24 + num(item.opportunity_score || item.latest_total_score, 0))),
        category: item.depth === 0 ? 0 : item.depth === 1 ? 1 : item.depth === 2 ? 2 : 3,
        itemStyle: {
          color: item.is_root ? "#ffd11a" : asArray(item.quality_flags).length ? "#dd6b20" : "#2563eb",
        },
      })),
      links: asArray(payload?.edges).map((item) => ({
        source: item.parent_keyword,
        target: item.child_keyword,
        value: item.source_platform || item.source_type || "",
        lineStyle: {
          width: item.source_platform === "noon" ? 2.2 : 1.6,
          opacity: 0.72,
          color: item.source_platform === "noon" ? "#0f8a5f" : "#2563eb",
        },
      })),
      categories: [
        { name: "Root" },
        { name: "Depth 1" },
        { name: "Depth 2" },
        { name: "Other" },
      ],
      force: {
        repulsion: 220,
        edgeLength: [70, 130],
        gravity: 0.08,
      },
      emphasis: { focus: "adjacency" },
    }],
  });
}

function renderKeywordWorkspace() {
  if (keywordModule?.renderKeywordWorkspace) return keywordModule.renderKeywordWorkspace();
  el("view-keyword")?.classList.toggle("pool-mode", state.route.keyword_mode === "pool");
  const shell = el("keyword-secondary-shell");
  if (shell) {
    shell.hidden = !(state.keywordSecondaryOpen || state.route.keyword_mode === "pool");
  }
  if (el("keyword-pool-search")) el("keyword-pool-search").value = state.keywordPoolQuery || "";
  if (el("keyword-subnav-search")) el("keyword-subnav-search").value = state.keywordPoolQuery || "";
  renderKeywordPoolTree();
  renderKeywordPoolBrief();
  renderTabbar("keyword-tabbar");

  const mergedItems = getKeywordPoolItems();
  if (!state.route.keyword && mergedItems.length) {
    window.setTimeout(() => {
      navigateWithPatch({ keyword: mergedItems[0].keyword, keyword_offset: 0 }, true);
    }, 0);
    return;
  }

  const selected = mergedItems.find((item) => item.keyword === state.route.keyword) || null;
  const summary = state.keywordBenchmarks?.summary || {};
  const title = el("keyword-headline");
  const subtitle = el("keyword-subtitle");
  const profile = el("keyword-profile");
  const snapshot = el("keyword-snapshot");

  if (!selected || !state.keywordBenchmarks) {
    if (title) title.textContent = "关键词研究 / Keyword Research";
    if (subtitle) subtitle.textContent = "先从顶部选择关键词，再查看关键词答案、关键词池和命中商品。";
    renderKpiCards("keyword-cards", []);
    renderLineChart("keyword-trend-chart", [], [], [{ type: "value" }], "选择关键词后显示趋势");
    renderBarChart("keyword-category-chart", [], [], "选择关键词后显示类目分布");
    renderDonutChart("keyword-delivery-chart", [], undefined, "选择关键词后显示配送结构");
    if (profile) profile.innerHTML = '<div class="empty-state">选择关键词后显示关键词画像。</div>';
    if (snapshot) snapshot.innerHTML = '<div class="empty-state">选择关键词后显示机会快照。</div>';
    return;
  }

  if (title) title.textContent = (selected.display_keyword || selected.keyword) + " / Keyword Research";
  if (subtitle) subtitle.textContent = "先看关键词答案，再看关键词池和命中商品。";

  renderKpiCards("keyword-cards", [
    { label: "关键词评分", value: formatScore(selected.total_score), note: "Grade " + (selected.grade || "-") },
    { label: "命中商品", value: formatNumber(summary.matched_product_count), note: "统一仓库命中商品数" },
    { label: "广告占比", value: formatPercent(summary.ad_share_pct), note: "命中商品中的广告占比" },
    { label: "双来源占比", value: formatPercent(summary.both_source_pct), note: "同时来自类目与关键词" },
    { label: "均价", value: formatPrice(summary.avg_price), note: "Avg Rating " + formatScore(summary.avg_rating) },
    { label: "平均评论", value: formatNumber(summary.avg_review_count), note: "Last Analyze " + formatDate(selected.last_analyzed_at) },
  ]);

  const trendItems = filterSeriesByDays(state.keywordHistory?.items || [], "analyzed_at");
  renderLineChart(
    "keyword-trend-chart",
    trendItems.map((item) => formatDate(item.analyzed_at)),
    [
      { name: "Score", type: "line", smooth: true, data: trendItems.map((item) => item.total_score), itemStyle: { color: "#8f7b39" } },
      { name: "Competition", type: "line", smooth: true, yAxisIndex: 1, data: trendItems.map((item) => item.competition_density), itemStyle: { color: "#2457d6" } },
    ],
    [
      { type: "value", name: "Score" },
      { type: "value", name: "Competition" },
    ],
    "当前时间窗口内暂无关键词趋势"
  );
  renderBarChart("keyword-category-chart", asArray(state.keywordBenchmarks.top_categories).map((item) => truncate(item.category_path.split(" > ").slice(-1)[0] || item.category_path, 18)), asArray(state.keywordBenchmarks.top_categories).map((item) => item.product_count), "暂无类目分布");
  renderDonutChart("keyword-delivery-chart", asArray(state.keywordBenchmarks.delivery_breakdown).map((item) => ({ name: getDeliveryLabel(item.delivery_type), value: item.product_count })), undefined, "暂无配送结构");

  if (profile) {
    profile.innerHTML = '<div class="detail-card"><div class="detail-label">关键词画像</div><div class="detail-value">'
      + 'Keyword: ' + escapeHtml(selected.display_keyword || selected.keyword) + '<br />'
      + 'Status: ' + escapeHtml(selected.status || '-') + '<br />'
      + 'Tracking: ' + escapeHtml(selected.tracking_mode || '-') + '<br />'
      + 'Source Platform: ' + escapeHtml(selected.source_platform || '-') + '<br />'
      + 'Last Crawl: ' + escapeHtml(formatDate(selected.last_crawled_at)) + '<br />'
      + 'Last Analyze: ' + escapeHtml(formatDate(selected.last_analyzed_at))
      + '</div></div>';
  }

  if (snapshot) {
    snapshot.innerHTML = [
      { label: "Demand", value: formatScore(selected.demand_index) },
      { label: "Competition", value: formatScore(selected.competition_density) },
      { label: "Supply Gap", value: formatScore(selected.supply_gap_ratio) },
      { label: "Margin Peace", value: formatPercent(selected.margin_peace_pct) },
    ].map((item) => '<div class="fact-card"><div class="fact-label">' + item.label + '</div><div class="fact-value">' + item.value + '</div></div>').join("");
  }
}
function renderKeywordIntelligence() {
  if (keywordModule?.renderKeywordIntelligence) return keywordModule.renderKeywordIntelligence();
  const opportunities = asArray(state.keywordOpportunities?.items);
  const opportunitySummary = state.keywordOpportunities?.summary || {};
  const qualitySummary = state.keywordQualityIssues?.summary || {};
  const qualityItems = asArray(state.keywordQualityIssues?.items);
  const seedGroups = asArray(state.keywordSeedGroups?.items);
  const runs = asArray(state.runsSummary?.keyword_runs);
  const priorityNode = el("keyword-opportunity-priority");
  const evidenceNode = el("keyword-opportunity-evidence");
  if (priorityNode) priorityNode.value = state.route.opp_priority || "";
  if (evidenceNode) evidenceNode.value = state.route.opp_evidence || "";
  const opportunityFilterBits = [
    opportunitySummary.priority_band ? `priority ${opportunitySummary.priority_band}` : "",
    opportunitySummary.evidence_strength ? `evidence ${opportunitySummary.evidence_strength}` : "",
    opportunitySummary.root_keyword ? `root ${opportunitySummary.root_keyword}` : "",
  ].filter(Boolean);
  const topRootKeyword = asArray(opportunitySummary.top_root_keywords)[0]?.keyword || "";
  el("keyword-opportunity-meta").textContent = [
    `${formatNumber(opportunitySummary.item_count ?? opportunities.length)} shown`,
    `${formatNumber(opportunitySummary.available_count ?? opportunities.length)} available`,
    opportunitySummary.avg_opportunity_score !== undefined ? `avg score ${formatScore(opportunitySummary.avg_opportunity_score)}` : "",
    topRootKeyword ? `top root ${topRootKeyword}` : "",
    opportunityFilterBits.length ? `filters ${opportunityFilterBits.join(" | ")}` : "filters none",
  ].filter(Boolean).join(" | ");

  el("keyword-opportunity-list").innerHTML = opportunities.length
    ? opportunities.map((item) => `
      <div class="list-card">
        <div class="item-title">${escapeHtml(item.display_keyword || item.keyword)}</div>
        <div class="list-meta">${escapeHtml(getOpportunityTypeLabel(item.opportunity_type))} | score ${formatScore(item.opportunity_score)} | ${escapeHtml(getRiskLevelLabel(item.risk_level))}</div>
        <div class="list-meta">priority ${escapeHtml(item.priority_band || "-")} | evidence ${escapeHtml(item.evidence_strength || "-")}</div>
        <div class="list-meta">root ${escapeHtml(item.root_seed_keyword || "-")} | depth ${escapeHtml(String(item.expansion_depth ?? "-"))} | matched ${formatNumber(item.matched_product_count)}</div>
        <div class="list-meta">${escapeHtml(formatInlineList(item.reason_summary))}</div>
        <div class="list-meta">Next: ${escapeHtml(item.action_hint || "-")}</div>
        <div class="inline-actions">
          <button class="mini-button" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword)}">查看 / Open</button>
        </div>
      </div>
    `).join("")
    : '<div class="empty-state">暂无机会词结果 / No keyword opportunities yet.</div>';

  if (!opportunities.length) {
    el("keyword-opportunity-list").innerHTML = `<div class="empty-state">${escapeHtml(opportunityFilterBits.length ? `No keyword opportunities under current filters: ${opportunityFilterBits.join(" | ")}` : "No keyword opportunities yet.")}</div>`;
  }

  const topQualityRoot = asArray(qualitySummary.top_root_keywords)[0]?.keyword || "";
  const topQualityFlag = Object.entries(qualitySummary.flag_counts || {})
    .sort((left, right) => Number(right[1] || 0) - Number(left[1] || 0))[0]?.[0] || "";
  el("keyword-quality-meta").textContent = [
    `${formatNumber(qualitySummary.item_count ?? qualityItems.length)} shown`,
    `${formatNumber(qualitySummary.available_count ?? qualityItems.length)} available`,
    `high risk ${formatNumber(qualitySummary.high_risk_count ?? 0)}`,
    topQualityRoot ? `top root ${topQualityRoot}` : "",
    topQualityFlag ? `top flag ${topQualityFlag}` : "",
  ].filter(Boolean).join(" | ");

  el("keyword-quality-issues").innerHTML = qualityItems.length
    ? qualityItems.map((item) => `
      <div class="list-card">
        <div class="item-title">${escapeHtml(item.display_keyword || item.keyword)}</div>
        <div class="list-meta">${escapeHtml(asArray(item.quality_flags).join(" | ") || "quality flags")}</div>
        <div class="list-meta">root ${escapeHtml(item.root_seed_keyword || "-")} | ${escapeHtml(getRiskLevelLabel(item.risk_level))}</div>
        <div class="list-meta">${escapeHtml(formatInlineList(item.reason_summary, "review keyword quality and source context"))}</div>
        <div class="inline-actions">
          <button class="mini-button" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword)}">查看 / Open</button>
        </div>
      </div>
    `).join("")
    : '<div class="empty-state">当前没有关键词质量问题。</div>';

  el("keyword-seed-groups").innerHTML = seedGroups.length
    ? seedGroups.map((item) => `
      <div class="list-card">
        <div class="item-title">${escapeHtml(item.label || item.slug)}</div>
        <div class="list-meta">Seeds ${formatNumber(item.seed_count)} | Tracked ${formatNumber(item.tracked_count)} | Crawled ${formatNumber(item.crawled_count)}</div>
        <div class="list-meta">Catalog ${formatNumber(item.keyword_count)} | Priority ${formatNumber(item.priority)}</div>
      </div>
    `).join("")
    : '<div class="empty-state">暂无种子覆盖数据 / No seed group coverage yet.</div>';

  el("keyword-runs-list").innerHTML = runs.length
    ? runs.slice(0, 8).map((item) => `
      <div class="list-card">
        <div class="item-title">${escapeHtml(item.run_type || "-")} / ${escapeHtml(item.status || "-")}</div>
        <div class="list-meta">${escapeHtml(item.seed_keyword || item.snapshot_id || "-")}</div>
        <div class="list-meta">${formatDate(item.started_at)} -> ${formatDate(item.finished_at)}</div>
      </div>
    `).join("")
    : '<div class="empty-state">暂无关键词运行记录。</div>';

  el("keyword-graph-meta").textContent = state.keywordGraph?.root_keyword
    ? `root ${state.keywordGraph.root_keyword} | ${formatNumber(state.keywordGraph.node_count)} nodes | ${formatNumber(state.keywordGraph.edge_count)} edges`
    : "暂无图谱数据。";
  renderKeywordGraph();
}

function formatCrawlerScheduleLabel(kind, scheduleJson = {}) {
  const normalized = String(kind || "manual").trim().toLowerCase() || "manual";
  if (normalized === "manual") return "manual";
  if (normalized === "once") return scheduleJson.run_at ? `once @ ${formatDate(scheduleJson.run_at)}` : "once";
  if (normalized === "interval") {
    const seconds = num(scheduleJson.seconds, 0);
    return seconds > 0 ? `every ${Math.max(1, Math.round(seconds / 3600))}h` : "interval";
  }
  if (normalized === "weekly") {
    const days = asArray(scheduleJson.days).join(", ").toUpperCase();
    return `weekly ${days || "-"} ${scheduleJson.time || ""}`.trim();
  }
  return normalized || "-";
}

function renderCrawlerStageStrip(progress) {
  const stages = asArray(progress?.stages);
  if (!stages.length) return '<div class="crawler-phase-strip"><span class="crawler-phase-chip" data-status="pending">No progress yet</span></div>';
  return `
    <div class="crawler-phase-strip">
      ${stages.map((stage) => `
        <span class="crawler-phase-chip" data-status="${escapeHtml(stage.status || "pending")}">
          ${escapeHtml(stage.label || stage.key || "-")}
        </span>
      `).join("")}
    </div>
  `;
}

function renderCrawlerProgressMeta(progress) {
  if (!progress) return "";
  const metrics = Object.entries(progress.metrics || {})
    .map(([key, value]) => `${key}: ${typeof value === "number" ? formatNumber(value) : value}`)
    .filter(Boolean);
  const details = Object.entries(progress.details || {})
    .map(([key, value]) => `${key}: ${typeof value === "string" ? value : safeJsonPreview(value, "-")}`)
    .filter(Boolean);
  const lines = [
    progress.message ? `message: ${progress.message}` : "",
    progress.updated_at ? `updated: ${formatDate(progress.updated_at)}` : "",
    metrics.length ? `metrics: ${metrics.join(" | ")}` : "",
    details.length ? `details: ${details.join(" | ")}` : "",
  ].filter(Boolean);
  return lines.length ? `<div class="ops-meta crawler-progress-meta">${lines.map((line) => `<div>${escapeHtml(line)}</div>`).join("")}</div>` : "";
}

function parseMultilineList(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseCategoryOverridesText(value) {
  const overrides = {};
  parseMultilineList(value).forEach((line) => {
    const parts = line.split(/[=:]/, 2);
    const key = String(parts[0] || "").trim();
    const count = num(parts[1], 0);
    if (key && count > 0) overrides[key] = count;
  });
  return overrides;
}

function parsePlatformList(value) {
  return String(value || "")
    .replace(/,/g, " ")
    .split(/\s+/)
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function normalizeCrawlerSubcategoryItem(item) {
  if (!item) return null;
  const configId = String(item.config_id ?? item.id ?? item.category_id ?? item.subcategory_id ?? "").trim();
  const breadcrumbPath = String(item.breadcrumb_path ?? item.category_path ?? item.path ?? item.display_name ?? item.subcategory_name ?? "").trim();
  const displayName = String(item.display_name ?? item.label ?? item.subcategory_name ?? item.name ?? breadcrumbPath.split(" > ").slice(-1)[0] ?? configId).trim();
  const topLevelCategory = String(item.top_level_category ?? item.top_category ?? item.category ?? "").trim();
  const parentConfigId = String(item.parent_config_id ?? item.parent_id ?? "").trim();
  const productCount = num(item.product_count ?? item.count ?? item.leaf_product_count, 0);
  const normalizedId = configId || breadcrumbPath || displayName;
  if (!normalizedId) return null;
  return {
    config_id: normalizedId,
    display_name: displayName || normalizedId,
    breadcrumb_path: breadcrumbPath || displayName || normalizedId,
    parent_config_id: parentConfigId,
    top_level_category: topLevelCategory,
    product_count: productCount,
  };
}

function extractCrawlerSubcategoryCatalog(catalog = {}) {
  const items = [];
  const seen = new Set();
  const pushItem = (item) => {
    const normalized = normalizeCrawlerSubcategoryItem(item);
    if (!normalized || seen.has(normalized.config_id)) return;
    seen.add(normalized.config_id);
    items.push(normalized);
  };
  const collectArray = (value) => {
    asArray(value).forEach(pushItem);
  };
  collectArray(catalog.subcategory_items);
  collectArray(catalog.subcategory_catalog);
  collectArray(catalog.subcategories);
  collectArray(catalog.category_subcategories);
  collectArray(catalog.ready_subcategories);
  collectArray(catalog.target_subcategories);
  collectArray(catalog.leaf_categories);
  collectArray(catalog.items);
  if (catalog.subcategory_map && typeof catalog.subcategory_map === "object") {
    Object.entries(catalog.subcategory_map).forEach(([configId, item]) => {
      pushItem({ ...(item || {}), config_id: configId });
    });
  }
  asArray(catalog.ready_categories).forEach((category) => {
    const nested = [
      category?.subcategories,
      category?.children,
      category?.leaf_categories,
      category?.items,
    ];
    nested.forEach(collectArray);
  });
  return items.sort((left, right) => {
    const topGap = String(left.top_level_category || "").localeCompare(String(right.top_level_category || ""));
    if (topGap !== 0) return topGap;
    return String(left.breadcrumb_path || left.display_name || left.config_id)
      .localeCompare(String(right.breadcrumb_path || right.display_name || right.config_id));
  });
}

function getCrawlerMonitorConfigOptions() {
  return asArray(state.crawlerCatalog?.monitor_configs).map((item) => ({
    name: String(item?.name || item?.path || "").trim(),
    label: String(item?.label || item?.name || item?.path || "-").trim(),
    path: String(item?.path || item?.name || "").trim(),
    baseline_file: String(item?.baseline_file || "").trim(),
    raw: item || {},
  })).filter((item) => item.name || item.path);
}

function getSelectedCrawlerMonitorConfig() {
  const value = String(el("crawler-monitor-config")?.value || "").trim();
  if (!value) return null;
  return getCrawlerMonitorConfigOptions().find((item) => item.name === value || item.path === value) || null;
}

function getSelectedCrawlerTargetSubcategoryConfig() {
  const value = String(el("crawler-category-target-subcategory")?.value || "").trim();
  if (!value) return null;
  return asArray(state.crawlerSubcategoryCatalog).find((item) => item.config_id === value) || null;
}

function getSelectedCrawlerCategorySubcategoryPickerConfig() {
  const value = String(el("crawler-category-subcategory-picker")?.value || "").trim();
  if (!value) return null;
  return asArray(state.crawlerSubcategoryCatalog).find((item) => item.config_id === value) || null;
}

function normalizeCrawlerKeywordControlKeyword(item, sourceScope = "baseline") {
  if (item === null || item === undefined) return null;
  const rawKeyword = typeof item === "string"
    ? item
    : (item.keyword ?? item.value ?? item.name ?? item.label ?? item.text ?? "");
  const keyword = String(rawKeyword || "").trim().toLowerCase();
  if (!keyword) return null;
  const resolvedSourceScope = String(item?.source_scope ?? item?.sourceScope ?? sourceScope ?? "baseline").trim().toLowerCase() || "baseline";
  return {
    keyword,
    source_scope: resolvedSourceScope,
    origin: String(item?.origin ?? item?.source_type ?? item?.source ?? resolvedSourceScope).trim().toLowerCase() || resolvedSourceScope,
    created_at: String(item?.created_at ?? item?.added_at ?? item?.updated_at ?? "").trim(),
    raw: item,
  };
}

function normalizeCrawlerKeywordControlRule(item) {
  if (item === null || item === undefined) return null;
  const rawKeyword = typeof item === "string"
    ? item
    : (item.keyword ?? item.root_keyword ?? item.value ?? item.name ?? item.label ?? item.text ?? "");
  const keyword = String(rawKeyword || "").trim().toLowerCase();
  if (!keyword) return null;
  const blockedSources = Array.isArray(item?.blocked_sources)
    ? item.blocked_sources.map((value) => String(value || "").trim().toLowerCase()).filter(Boolean)
    : String(item?.blocked_sources ?? item?.blocked_source ?? "").split(/[\s,]+/).map((value) => value.trim().toLowerCase()).filter(Boolean);
  return {
    id: String(item?.id ?? item?.rule_id ?? item?.keyword_id ?? keyword).trim(),
    keyword,
    match_mode: String(item?.match_mode ?? "exact").trim().toLowerCase() || "exact",
    blocked_sources: blockedSources.length ? Array.from(new Set(blockedSources)) : ["baseline", "generated", "tracked", "manual"],
    reason: String(item?.reason ?? item?.note ?? "").trim(),
    updated_at: String(item?.updated_at ?? item?.created_at ?? "").trim(),
    raw: item,
  };
}

function normalizeCrawlerKeywordActiveItem(item) {
  if (item === null || item === undefined) return null;
  const rawKeyword = typeof item === "string"
    ? item
    : (item.keyword ?? item.display_keyword ?? item.value ?? item.name ?? item.label ?? item.text ?? "");
  const keyword = String(rawKeyword || "").trim().toLowerCase();
  if (!keyword) return null;
  return {
    keyword,
    display_keyword: String(item?.display_keyword ?? item?.keyword ?? rawKeyword).trim() || keyword,
    source_type: String(item?.source_type ?? "").trim().toLowerCase(),
    tracking_mode: String(item?.tracking_mode ?? "").trim().toLowerCase(),
    priority: num(item?.priority, 0),
    last_crawled_at: String(item?.last_crawled_at ?? "").trim(),
    last_expanded_at: String(item?.last_expanded_at ?? "").trim(),
    last_snapshot_id: String(item?.last_snapshot_id ?? "").trim(),
    status: String(item?.status ?? "active").trim().toLowerCase(),
    source_scopes: asArray(item?.source_scopes).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean),
    raw: item,
  };
}

function normalizeCrawlerKeywordControls(payload = {}) {
  const baselineKeywords = asArray(payload.baseline_keywords || payload.baseline_items || payload.keywords)
    .map((item) => normalizeCrawlerKeywordControlKeyword(item, "baseline"))
    .filter(Boolean);
  const disabledKeywords = asArray(payload.disabled_keywords || payload.exclusions || payload.exclusion_rules || payload.rules)
    .map((item) => normalizeCrawlerKeywordControlRule(item))
    .filter(Boolean);
  const blockedRoots = asArray(payload.blocked_roots || payload.root_rules)
    .map((item) => normalizeCrawlerKeywordControlRule(item))
    .filter(Boolean);
  return {
    monitor_config: String(payload.monitor_config || payload.config || payload.monitor || "").trim(),
    monitor_label: String(payload.monitor_label || payload.label || "").trim(),
    baseline_file: String(payload.baseline_file || "").trim(),
    baseline_file_exists: Boolean(payload.baseline_file_exists),
    baseline_file_writable: payload.baseline_file_writable !== false,
    baseline_storage_mode: String(payload.baseline_storage_mode || "baseline_file").trim(),
    legacy_baseline_overlay_count: num(payload.legacy_baseline_overlay_count, 0),
    baseline_keywords: baselineKeywords,
    disabled_keywords: disabledKeywords,
    blocked_roots: blockedRoots,
    exclusions: disabledKeywords,
    effective_keyword_stats: payload?.effective_keyword_stats && typeof payload.effective_keyword_stats === "object"
      ? { ...payload.effective_keyword_stats }
      : {},
    updated_at: String(payload.updated_at || payload.last_updated_at || "").trim(),
    status: String(payload.status || payload.state || "").trim(),
    message: String(payload.message || payload.note || "").trim(),
  };
}

function parseCrawlerKeywordListFromInput(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function getCrawlerKeywordControlScopes() {
  const scopes = asArray(state.crawlerKeywordControlScopes)
    .map((item) => String(item || "").trim().toLowerCase())
    .filter(Boolean);
  return scopes.length ? Array.from(new Set(scopes)) : ["baseline", "generated", "tracked", "manual"];
}

function setCrawlerKeywordControlScopes(scopes = []) {
  const nextScopes = asArray(scopes).map((item) => String(item || "").trim().toLowerCase()).filter(Boolean);
  state.crawlerKeywordControlScopes = nextScopes.length ? Array.from(new Set(nextScopes)) : ["baseline", "generated", "tracked", "manual"];
}

function toggleCrawlerKeywordControlScope(scope = "") {
  const nextScope = String(scope || "").trim().toLowerCase();
  if (!nextScope) return;
  const scopes = new Set(getCrawlerKeywordControlScopes());
  if (scopes.has(nextScope)) scopes.delete(nextScope);
  else scopes.add(nextScope);
  setCrawlerKeywordControlScopes(Array.from(scopes));
}

function buildCrawlerCategorySubcategoryOverridesPayload() {
  const overrides = {};
  asArray(state.crawlerCategorySubcategoryOverridesDraft).forEach((item) => {
    const configId = String(item?.config_id || "").trim();
    const depth = num(item?.product_count ?? item?.depth, 0);
    if (configId && depth > 0) overrides[configId] = depth;
  });
  return overrides;
}

function upsertCrawlerCategorySubcategoryOverrideDraft(item, depth) {
  const normalized = normalizeCrawlerSubcategoryItem(item);
  if (!normalized) return;
  const productCount = num(depth, 0);
  if (productCount <= 0) return;
  const nextDraft = new Map(asArray(state.crawlerCategorySubcategoryOverridesDraft).map((entry) => [String(entry.config_id || ""), entry]));
  nextDraft.set(normalized.config_id, {
    config_id: normalized.config_id,
    display_name: normalized.display_name,
    breadcrumb_path: normalized.breadcrumb_path,
    parent_config_id: normalized.parent_config_id,
    top_level_category: normalized.top_level_category,
    product_count: productCount,
  });
  state.crawlerCategorySubcategoryOverridesDraft = Array.from(nextDraft.values());
}

function removeCrawlerCategorySubcategoryOverrideDraft(configId = "") {
  const nextConfigId = String(configId || "").trim();
  if (!nextConfigId) return;
  state.crawlerCategorySubcategoryOverridesDraft = asArray(state.crawlerCategorySubcategoryOverridesDraft)
    .filter((item) => String(item.config_id || "") !== nextConfigId);
}

function clearCrawlerCategorySubcategoryOverrideDraft() {
  state.crawlerCategorySubcategoryOverridesDraft = [];
}

function buildCrawlerScheduleJson(kind) {
  const normalized = String(kind || "manual").trim().toLowerCase() || "manual";
  if (normalized === "manual") return {};
  if (normalized === "once") {
    const raw = String(el("crawler-once-at")?.value || "").trim();
    if (!raw) throw new Error("Run time is required for once schedule");
    return { run_at: new Date(raw).toISOString() };
  }
  if (normalized === "interval") {
    const hours = num(el("crawler-interval-hours")?.value, 0);
    if (hours <= 0) throw new Error("Interval hours must be > 0");
    return { seconds: hours * 3600 };
  }
  if (normalized === "weekly") {
    const days = String(el("crawler-weekly-days")?.value || "")
      .split(",")
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean);
    const time = String(el("crawler-weekly-time")?.value || "").trim();
    if (!days.length) throw new Error("Weekly days are required");
    if (!time) throw new Error("Weekly time is required");
    return { days, time };
  }
  return {};
}

function buildCrawlerPlanPayload(planType) {
  if (planType === "category_single") {
    const targetSubcategory = getSelectedCrawlerTargetSubcategoryConfig();
    const fallbackCategory = String(el("crawler-category-single")?.value || "").trim();
    const category = fallbackCategory || String(targetSubcategory?.top_level_category || "").trim();
    if (targetSubcategory && !category) {
      throw new Error("Selected subcategory is missing its top-level category");
    }
    if (!category && !targetSubcategory) throw new Error("Single category is required");
    return {
      category,
      target_subcategory: targetSubcategory?.config_id || "",
      product_count: num(el("crawler-category-default-depth")?.value, 100),
      persist: true,
      export_excel: false,
    };
  }
  if (planType === "category_ready_scan") {
    return {
      categories: parseMultilineList(el("crawler-category-selected")?.value || ""),
      default_product_count_per_leaf: num(el("crawler-category-default-depth")?.value, 100),
      category_overrides: parseCategoryOverridesText(el("crawler-category-overrides")?.value || ""),
      subcategory_overrides: buildCrawlerCategorySubcategoryOverridesPayload(),
      persist: true,
      export_excel: false,
    };
  }
  if (planType === "keyword_batch") {
    const keywords = parseMultilineList(el("crawler-keyword-batch")?.value || "");
    if (!keywords.length) throw new Error("Keyword batch is required");
    return {
      keywords,
      platforms: parsePlatformList(el("crawler-platforms")?.value || "noon,amazon"),
      noon_count: num(el("crawler-noon-count")?.value, 30),
      amazon_count: num(el("crawler-amazon-count")?.value, 30),
      persist: true,
    };
  }
  if (planType === "keyword_monitor") {
    const monitorConfig = String(el("crawler-monitor-config")?.value || "").trim();
    if (!monitorConfig) throw new Error("Monitor config is required");
    return {
      monitor_config: monitorConfig,
      noon_count: num(el("crawler-noon-count")?.value, 30),
      amazon_count: num(el("crawler-amazon-count")?.value, 30),
      persist: true,
    };
  }
  throw new Error(`Unsupported plan type: ${planType}`);
}

function suggestCrawlerPlanName(planType, payload) {
  if (planType === "category_single") {
    const subcategory = getSelectedCrawlerTargetSubcategoryConfig();
    return `Category | ${subcategory?.display_name || subcategory?.breadcrumb_path || payload.category}`;
  }
  if (planType === "category_ready_scan") {
    const subcategoryOverrides = asArray(state.crawlerCategorySubcategoryOverridesDraft).length;
    const categoryCount = payload.categories?.length || 0;
    const suffix = subcategoryOverrides ? ` | ${subcategoryOverrides} subcats` : "";
    return categoryCount ? `Ready Scan | ${categoryCount} categories${suffix}` : `Ready Scan${suffix}`;
  }
  if (planType === "keyword_batch") return `Keyword Batch | ${payload.keywords?.length || 0} keywords`;
  if (planType === "keyword_monitor") return `Keyword Monitor | ${payload.monitor_config || "config"}`;
  return planType;
}

function buildCrawlerPlanRequest() {
  const planType = String(el("crawler-plan-type")?.value || "category_ready_scan").trim();
  const payload = buildCrawlerPlanPayload(planType);
  const scheduleKind = String(el("crawler-schedule-kind")?.value || "manual").trim().toLowerCase();
  const name = String(el("crawler-plan-name")?.value || "").trim() || suggestCrawlerPlanName(planType, payload);
  return {
    plan_type: planType,
    name,
    created_by: "crawler_console",
    enabled: (el("crawler-enabled")?.value || "true") === "true",
    schedule_kind: scheduleKind,
    schedule_json: buildCrawlerScheduleJson(scheduleKind),
    payload,
  };
}

function setCrawlerPlanMessage(message, level = "neutral") {
  const node = el("crawler-plan-message");
  if (!node) return;
  node.textContent = message || "-";
  node.dataset.state = level;
}

function setCrawlerPlanBusy(busy) {
  state.crawlerPlanBusy = Boolean(busy);
  [
    "crawler-plan-type",
    "crawler-plan-name",
    "crawler-schedule-kind",
    "crawler-enabled",
    "crawler-category-target-subcategory",
    "crawler-category-single",
    "crawler-category-selected",
    "crawler-category-default-depth",
    "crawler-category-overrides",
    "crawler-keyword-batch",
    "crawler-monitor-config",
    "crawler-platforms",
    "crawler-noon-count",
    "crawler-amazon-count",
    "crawler-once-at",
    "crawler-interval-hours",
    "crawler-weekly-days",
    "crawler-weekly-time",
    "crawler-plan-submit",
    "crawler-plan-launch-now",
  ].forEach((id) => {
    const node = el(id);
    if (node) node.disabled = state.crawlerPlanBusy;
  });
}

function syncCrawlerPlanFormVisibility() {
  const planType = String(el("crawler-plan-type")?.value || "category_ready_scan");
  const scheduleKind = String(el("crawler-schedule-kind")?.value || "manual");
  const categorySingle = planType === "category_single";
  const categoryBatch = planType === "category_ready_scan";
  const keywordBatch = planType === "keyword_batch";
  const keywordMonitor = planType === "keyword_monitor";

  const setDisabled = (id, disabled) => {
    const node = el(id);
    if (node) node.disabled = disabled || state.crawlerPlanBusy;
  };

  setDisabled("crawler-category-single", !categorySingle);
  setDisabled("crawler-category-target-subcategory", !categorySingle);
  setDisabled("crawler-category-selected", !categoryBatch);
  setDisabled("crawler-category-default-depth", !(categorySingle || categoryBatch));
  setDisabled("crawler-category-overrides", !categoryBatch);
  setDisabled("crawler-category-subcategory-picker", !categoryBatch);
  setDisabled("crawler-category-subcategory-depth", !categoryBatch);
  setDisabled("crawler-category-subcategory-add", !categoryBatch);
  setDisabled("crawler-category-subcategory-clear", !categoryBatch);
  setDisabled("crawler-keyword-batch", !keywordBatch);
  setDisabled("crawler-monitor-config", !keywordMonitor);
  setDisabled("crawler-platforms", !(keywordBatch || keywordMonitor));
  setDisabled("crawler-noon-count", !(keywordBatch || keywordMonitor));
  setDisabled("crawler-amazon-count", !(keywordBatch || keywordMonitor));
  setDisabled("crawler-once-at", scheduleKind !== "once");
  setDisabled("crawler-interval-hours", scheduleKind !== "interval");
  setDisabled("crawler-weekly-days", scheduleKind !== "weekly");
  setDisabled("crawler-weekly-time", scheduleKind !== "weekly");
  renderCrawlerKeywordControls();
  renderCrawlerCategoryControls();
}

function resetCrawlerPlanForm() {
  if (el("crawler-plan-type")) el("crawler-plan-type").value = "category_ready_scan";
  if (el("crawler-plan-name")) el("crawler-plan-name").value = "";
  if (el("crawler-schedule-kind")) el("crawler-schedule-kind").value = "manual";
  if (el("crawler-enabled")) el("crawler-enabled").value = "true";
  if (el("crawler-category-target-subcategory")) el("crawler-category-target-subcategory").value = "";
  if (el("crawler-category-single")) el("crawler-category-single").value = "";
  if (el("crawler-category-selected")) el("crawler-category-selected").value = "";
  if (el("crawler-category-default-depth")) el("crawler-category-default-depth").value = "100";
  if (el("crawler-category-overrides")) el("crawler-category-overrides").value = "";
  if (el("crawler-category-subcategory-picker")) el("crawler-category-subcategory-picker").value = "";
  if (el("crawler-category-subcategory-depth")) el("crawler-category-subcategory-depth").value = "100";
  clearCrawlerCategorySubcategoryOverrideDraft();
  if (el("crawler-keyword-batch")) el("crawler-keyword-batch").value = "";
  if (el("crawler-platforms")) el("crawler-platforms").value = "noon,amazon";
  if (el("crawler-noon-count")) el("crawler-noon-count").value = "30";
  if (el("crawler-amazon-count")) el("crawler-amazon-count").value = "30";
  if (el("crawler-once-at")) el("crawler-once-at").value = "";
  if (el("crawler-interval-hours")) el("crawler-interval-hours").value = "24";
  if (el("crawler-weekly-days")) el("crawler-weekly-days").value = "MO,WE,FR";
  if (el("crawler-weekly-time")) el("crawler-weekly-time").value = "09:00";
  setCrawlerPlanMessage("Create crawler plans here. Admin only.", "neutral");
  syncCrawlerPlanFormVisibility();
}

function populateCrawlerCatalogForm(catalog) {
  const defaults = catalog?.defaults || {};
  state.crawlerSubcategoryCatalog = extractCrawlerSubcategoryCatalog(catalog);
  const monitorSelect = el("crawler-monitor-config");
  if (monitorSelect) {
    const options = asArray(catalog?.monitor_configs);
    const currentValue = String(monitorSelect.value || state.crawlerKeywordControls?.monitor_config || "").trim();
    monitorSelect.innerHTML = options.length
      ? options.map((item) => `<option value="${escapeHtml(item.name || item.path || "")}">${escapeHtml(item.label || item.name || item.path || "-")}</option>`).join("")
      : '<option value="">No monitor config</option>';
    const optionValues = new Set(options.map((item) => String(item.name || item.path || "").trim()).filter(Boolean));
    if (currentValue && optionValues.has(currentValue)) {
      monitorSelect.value = currentValue;
    } else if (!monitorSelect.value && options.length) {
      monitorSelect.value = String(options[0].name || options[0].path || "").trim();
    }
  }
  if (el("crawler-category-default-depth")) {
    const current = num(el("crawler-category-default-depth").value, 0);
    if (current <= 0) el("crawler-category-default-depth").value = String(defaults.category_default_product_count_per_leaf || 100);
  }
  if (el("crawler-noon-count")) {
    const current = num(el("crawler-noon-count").value, 0);
    if (current <= 0) el("crawler-noon-count").value = String(defaults.keyword_noon_count || 30);
  }
  if (el("crawler-amazon-count")) {
    const current = num(el("crawler-amazon-count").value, 0);
    if (current <= 0) el("crawler-amazon-count").value = String(defaults.keyword_amazon_count || 30);
  }
  refreshCrawlerCategorySubcategorySelectors();
  syncCrawlerPlanFormVisibility();
}

function renderCrawlerOverview() {
  const plans = asArray(state.crawlerPlans?.items);
  const runs = asArray(state.crawlerRuns?.items);
  const workers = asArray(state.workers?.items).filter((item) => ["category", "keyword", "scheduler"].includes(String(item.worker_type || "")));
  const autoDispatch = state.crawlerPlans?.auto_dispatch || {};
  const enabledPlans = plans.filter((item) => item.enabled).length;
  const activeRuns = runs.filter((item) => ["pending", "leased", "running"].includes(String(item.status || ""))).length;
  const failedRuns = runs.filter((item) => ["failed"].includes(String(item.status || ""))).length;
  const readyCategories = asArray(state.crawlerCatalog?.ready_categories).length;
  const cards = [
    renderOpsCard("Plan Inventory", `${formatNumber(plans.length)} plans`, [
      renderHealthChip("enabled", String(enabledPlans)),
      renderHealthChip("active", String(activeRuns)),
    ], [
      `ready categories: ${formatNumber(readyCategories)}`,
      `monitor configs: ${formatNumber(asArray(state.crawlerCatalog?.monitor_configs).length)}`,
    ]),
    renderOpsCard("Run Status", `${formatNumber(activeRuns)} active`, [
      renderHealthChip("failed", String(failedRuns)),
      renderHealthChip("workers", String(workers.length)),
    ], [
      `history: ${formatNumber(runs.length)} crawler tasks`,
      `ui build: ${UI_BUILD_TAG}`,
    ]),
    renderOpsCard("Auto Dispatch", String(autoDispatch.auto_dispatch_entry || "crawl_plans"), [
      renderHealthChip("scheduler", autoDispatch.scheduler_heartbeat_ok ? "ok" : "missing"),
      renderHealthChip("entry", autoDispatch.auto_dispatch_conflict ? "conflict" : "single"),
    ], asArray(autoDispatch.canonical_auto_plans).map((item) => {
      const status = item.enabled ? "enabled" : (item.exists ? "disabled" : "missing");
      return `${item.family || "plan"}: ${item.name || "-"} | ${status} | next ${formatDate(item.next_run_at)}`;
    })),
  ];
  if (state.crawlerLoadWarnings.length) {
    cards.push(renderOpsCard("Load Warnings", state.crawlerLoadWarnings[0], [], state.crawlerLoadWarnings));
  }
  const node = el("crawler-overview-cards");
  if (node) node.innerHTML = cards.join("");
}

function summarizeCrawlerPayload(planType, payload) {
  if (planType === "category_single") {
    const subcategory = getSelectedCrawlerTargetSubcategoryConfig();
    const target = subcategory?.display_name || subcategory?.breadcrumb_path || payload.target_subcategory || payload.category || "-";
    return `${target} | ${formatNumber(payload.product_count)} / leaf`;
  }
  if (planType === "category_ready_scan") {
    const categories = asArray(payload.categories);
    const subcategoryOverrides = Object.keys(payload.subcategory_overrides || {}).length;
    return `${categories.length ? `${categories.length} categories` : "ready set"} | default ${formatNumber(payload.default_product_count_per_leaf)} / leaf | overrides ${formatNumber(Object.keys(payload.category_overrides || {}).length)} | subcats ${formatNumber(subcategoryOverrides)}`;
  }
  if (planType === "keyword_batch") {
    return `${formatNumber(asArray(payload.keywords).length)} keywords | ${asArray(payload.platforms).join(", ") || "-"}`;
  }
  if (planType === "keyword_monitor") {
    return `${payload.monitor_config || "-"} | noon ${formatNumber(payload.noon_count)} | amazon ${formatNumber(payload.amazon_count)}`;
  }
  return safeJsonPreview(payload);
}

function renderCrawlerPlans() {
  const plans = asArray(state.crawlerPlans?.items);
  const activeRuns = new Map(
    asArray(state.crawlerRuns?.items)
      .filter((item) => ["pending", "leased", "running"].includes(String(item.status || "")) && item.plan_id)
      .map((item) => [Number(item.plan_id), item]),
  );
  const node = el("crawler-plan-list");
  if (!node) return;
  node.innerHTML = plans.length
    ? plans.map((plan) => {
      const currentRun = activeRuns.get(Number(plan.id));
      const autoChips = [
        plan.is_canonical_auto_plan ? renderHealthChip("auto", `canonical ${plan.auto_plan_family || "auto"}`) : "",
        plan.is_legacy_auto_plan_candidate ? renderHealthChip("legacy", "auto candidate") : "",
      ].filter(Boolean).join("");
      return `
        <div class="list-card task-card crawler-plan-card">
          <div class="task-card-head">
            <div class="item-title">${escapeHtml(plan.name || "-")}</div>
            ${renderTaskStatusChip(plan.enabled ? "running" : "cancelled")}
          </div>
          <div class="ops-meta">
            ${autoChips ? `<div>${autoChips}</div>` : ""}
            <div>Type: ${escapeHtml(getTaskTypeLabel(plan.plan_type))}</div>
            <div>Schedule: ${escapeHtml(formatCrawlerScheduleLabel(plan.schedule_kind, plan.schedule_json || {}))}</div>
            <div>Payload: ${escapeHtml(summarizeCrawlerPayload(plan.plan_type, plan.payload || {}))}</div>
            <div>Next run: ${escapeHtml(formatDate(plan.next_run_at))} | Last status: ${escapeHtml(plan.last_run_status || "-")}</div>
            <div>Last task: ${plan.last_run_task_id ? `#${formatNumber(plan.last_run_task_id)}` : "-"} | Updated: ${escapeHtml(formatDate(plan.updated_at))}</div>
            ${currentRun ? `<div>Active run: #${formatNumber(currentRun.task_id)} | ${escapeHtml(currentRun.status || "-")} | ${escapeHtml(currentRun.progress?.message || "-")}</div>` : ""}
          </div>
          <div class="button-row inline-actions">
            <button type="button" class="ghost-button mini-button" data-action="launch-crawler-plan" data-plan-id="${plan.id}">Launch now</button>
            ${plan.enabled
              ? `<button type="button" class="ghost-button mini-button" data-action="pause-crawler-plan" data-plan-id="${plan.id}">Pause</button>`
              : `<button type="button" class="ghost-button mini-button" data-action="resume-crawler-plan" data-plan-id="${plan.id}">Resume</button>`}
          </div>
        </div>
      `;
    }).join("")
    : '<div class="empty-state">No crawler plans yet.</div>';
}

function renderCrawlerRunCard(item) {
  return `
    <div class="list-card task-card crawler-run-card">
      <div class="task-card-head">
        <div class="item-title">${escapeHtml(item.display_name || getTaskTypeLabel(item.task_type))}</div>
        ${renderTaskStatusChip(item.status)}
      </div>
      <div class="ops-meta">
        <div>Task: #${formatNumber(item.task_id)} | Plan: ${item.plan_id ? `#${formatNumber(item.plan_id)}` : "-"} | Worker: ${escapeHtml(item.worker_type || "-")}</div>
        <div>Type: ${escapeHtml(getTaskTypeLabel(item.task_type))} | Updated: ${escapeHtml(formatDate(item.updated_at))}</div>
        <div>Payload: ${escapeHtml(summarizeCrawlerPayload(item.task_type, item.payload || {}))}</div>
        ${item.latest_run?.error_text ? `<div class="task-error-line">Run error: ${escapeHtml(truncate(item.latest_run.error_text, 260))}</div>` : ""}
      </div>
      ${renderCrawlerStageStrip(item.progress)}
      ${renderCrawlerProgressMeta(item.progress)}
    </div>
  `;
}

function renderCrawlerRuns() {
  const runs = asArray(state.crawlerRuns?.items);
  const active = runs.filter((item) => ["pending", "leased", "running"].includes(String(item.status || "")));
  const node = el("crawler-run-list");
  if (!node) return;
  node.innerHTML = active.length
    ? active.map(renderCrawlerRunCard).join("")
    : '<div class="empty-state">No active crawler runs.</div>';
}

function renderCrawlerHistory() {
  const runs = asArray(state.crawlerRuns?.items);
  const history = runs.filter((item) => ["completed", "failed", "cancelled", "skipped"].includes(String(item.status || "")));
  const node = el("crawler-history-list");
  if (!node) return;
  node.innerHTML = history.length
    ? history.slice(0, 20).map(renderCrawlerRunCard).join("")
    : '<div class="empty-state">No crawler history yet.</div>';
}

function setCrawlerKeywordControlMessage(message, level = "neutral") {
  state.crawlerKeywordControlMessage = message || "";
  ["crawler-keyword-control-message", "crawler-keyword-manager-message"].forEach((nodeId) => {
    const node = el(nodeId);
    if (!node) return;
    node.textContent = message || "-";
    node.dataset.state = level;
  });
}

function setCrawlerCategoryControlMessage(message, level = "neutral") {
  state.crawlerCategoryControlMessage = message || "";
  const node = el("crawler-category-control-message");
  if (!node) return;
  node.textContent = message || "-";
  node.dataset.state = level;
}

function normalizeCrawlerKeywordManagerTab(value) {
  return CRAWLER_KEYWORD_MANAGER_TABS.includes(String(value || "").trim().toLowerCase())
    ? String(value || "").trim().toLowerCase()
    : "active";
}

function getCrawlerKeywordManagerSelectionBucket(tab = state.crawlerKeywordManagerTab) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  if (normalizedTab === "add") return [];
  if (!Array.isArray(state.crawlerKeywordManagerSelection[normalizedTab])) {
    state.crawlerKeywordManagerSelection[normalizedTab] = [];
  }
  return state.crawlerKeywordManagerSelection[normalizedTab];
}

function setCrawlerKeywordManagerSelection(tab, values = []) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  if (normalizedTab === "add") return;
  const nextValues = asArray(values)
    .map((item) => String(item || "").trim().toLowerCase())
    .filter(Boolean);
  state.crawlerKeywordManagerSelection[normalizedTab] = Array.from(new Set(nextValues));
}

function toggleCrawlerKeywordManagerSelection(tab, key, forceSelected) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  const normalizedKey = String(key || "").trim().toLowerCase();
  if (normalizedTab === "add" || !normalizedKey) return;
  const selected = new Set(getCrawlerKeywordManagerSelectionBucket(normalizedTab));
  const shouldSelect = typeof forceSelected === "boolean" ? forceSelected : !selected.has(normalizedKey);
  if (shouldSelect) selected.add(normalizedKey);
  else selected.delete(normalizedKey);
  setCrawlerKeywordManagerSelection(normalizedTab, Array.from(selected));
}

function clearCrawlerKeywordManagerSelection(tab) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  if (normalizedTab === "add") return;
  state.crawlerKeywordManagerSelection[normalizedTab] = [];
}

function clearAllCrawlerKeywordManagerSelections() {
  CRAWLER_KEYWORD_MANAGER_TABS.forEach((tab) => clearCrawlerKeywordManagerSelection(tab));
}

function renderCrawlerKeywordScopeChips(containerId = "crawler-keyword-source-scope-chips") {
  const node = el(containerId);
  if (!node) return;
  const activeScopes = new Set(getCrawlerKeywordControlScopes());
  const scopeLabels = {
    baseline: "baseline / 长期词池",
    generated: "generated / 扩词结果",
    tracked: "tracked / 跟踪词",
    manual: "manual / 手工词",
  };
  node.innerHTML = Object.keys(scopeLabels).map((scope) => `
    <button type="button" class="filter-chip crawler-scope-chip ${activeScopes.has(scope) ? "active" : ""}" data-action="toggle-crawler-keyword-control-scope" data-scope="${escapeHtml(scope)}">
      ${escapeHtml(scopeLabels[scope])}
    </button>
  `).join("");
}

function getCrawlerKeywordManagerItemKey(item, tab) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  if (normalizedTab === "active") return String(item?.keyword || "").trim().toLowerCase();
  return String(item?.id || item?.keyword || "").trim().toLowerCase();
}

function compareCrawlerKeywordItems(left, right) {
  const leftKey = String(left?.keyword || left?.id || "").trim().toLowerCase();
  const rightKey = String(right?.keyword || right?.id || "").trim().toLowerCase();
  return leftKey.localeCompare(rightKey, "en");
}

function isCrawlerKeywordRootRule(item) {
  const reason = String(item?.reason || "").trim().toLowerCase();
  return reason.startsWith("[root-rule]")
    || reason.startsWith("[root]")
    || reason.startsWith("root rule")
    || reason.startsWith("blocked root");
}

function stripCrawlerKeywordRootRuleReason(reason = "") {
  let text = String(reason || "").trim();
  text = text.replace(/^\[root-rule\]\s*/i, "");
  text = text.replace(/^\[root\]\s*/i, "");
  text = text.replace(/^root rule[:\s-]*/i, "");
  text = text.replace(/^blocked root[:\s-]*/i, "");
  return text.trim();
}

function buildCrawlerKeywordRootRuleReason(reason = "") {
  const cleanReason = stripCrawlerKeywordRootRuleReason(reason);
  return cleanReason ? `${CRAWLER_KEYWORD_ROOT_RULE_PREFIX} ${cleanReason}` : CRAWLER_KEYWORD_ROOT_RULE_PREFIX;
}

function crawlerKeywordRuleBlocksSource(rule, source) {
  const normalizedSource = String(source || "").trim().toLowerCase();
  return asArray(rule?.blocked_sources)
    .map((item) => String(item || "").trim().toLowerCase())
    .includes(normalizedSource);
}

function buildCrawlerKeywordManagerBuckets() {
  const controls = normalizeCrawlerKeywordControls(state.crawlerKeywordControls || {});
  const activeKeywords = asArray(state.crawlerKeywordActiveKeywords)
    .map((item) => normalizeCrawlerKeywordActiveItem(item))
    .filter(Boolean)
    .slice()
    .sort(compareCrawlerKeywordItems);
  const disabledRules = controls.disabled_keywords
    .slice()
    .sort(compareCrawlerKeywordItems);
  const rootRules = controls.blocked_roots
    .slice()
    .sort(compareCrawlerKeywordItems);
  return { controls, activeKeywords, disabledRules, rootRules };
}

function filterCrawlerKeywordManagerItems(items, tab) {
  const query = String(state.crawlerKeywordManagerQuery || "").trim().toLowerCase();
  const sourceFilter = String(state.crawlerKeywordManagerSourceFilter || "").trim().toLowerCase();
  const trackingFilter = String(state.crawlerKeywordManagerTrackingFilter || "").trim().toLowerCase();
  const tokens = query ? query.split(/\s+/).map((item) => item.trim()).filter(Boolean) : [];
  return asArray(items).filter((item) => {
    if (sourceFilter) {
      if (normalizeCrawlerKeywordManagerTab(tab) === "active") {
        if (String(item?.source_type || "").trim().toLowerCase() !== sourceFilter) return false;
      } else if (!asArray(item?.blocked_sources).map((value) => String(value || "").trim().toLowerCase()).includes(sourceFilter)) {
        return false;
      }
    }
    if (trackingFilter && normalizeCrawlerKeywordManagerTab(tab) === "active") {
      if (String(item?.tracking_mode || "").trim().toLowerCase() !== trackingFilter) return false;
    }
    if (!tokens.length) return true;
    const haystack = normalizeCrawlerKeywordManagerTab(tab) === "active"
      ? `${item.keyword || ""} ${item.source_type || ""} ${item.tracking_mode || ""} ${item.last_snapshot_id || ""} ${item.last_crawled_at || ""} ${item.last_expanded_at || ""}`
      : `${item.keyword || ""} ${item.match_mode || ""} ${(item.blocked_sources || []).join(" ")} ${item.reason || ""} ${item.updated_at || ""}`;
    const normalizedHaystack = haystack.toLowerCase();
    return tokens.every((token) => normalizedHaystack.includes(token));
  });
}

function getCrawlerKeywordManagerTabData(tab = state.crawlerKeywordManagerTab) {
  const normalizedTab = normalizeCrawlerKeywordManagerTab(tab);
  const buckets = buildCrawlerKeywordManagerBuckets();
  let label = "Active Keywords";
  let items = buckets.activeKeywords;
  let emptyState = "当前没有仍在生效的长期关键词。";
  if (normalizedTab === "disabled") {
    label = "Disabled Keywords";
    items = buckets.disabledRules;
    emptyState = "当前没有禁用关键词规则。";
  } else if (normalizedTab === "roots") {
    label = "Blocked Roots";
    items = buckets.rootRules;
    emptyState = "当前没有 blocked roots。";
  } else if (normalizedTab === "add") {
    label = "Add Keywords";
    items = [];
    emptyState = "";
  }
  const filteredItems = normalizedTab === "add" ? [] : filterCrawlerKeywordManagerItems(items, normalizedTab);
  return {
    tab: normalizedTab,
    label,
    emptyState,
    items,
    filteredItems,
    counts: {
      active: num(buckets.controls.effective_keyword_stats?.active_keyword_count, buckets.activeKeywords.length),
      disabled: num(buckets.controls.effective_keyword_stats?.disabled_keyword_count, buckets.disabledRules.length),
      roots: num(buckets.controls.effective_keyword_stats?.blocked_root_count, buckets.rootRules.length),
      totalBaseline: num(buckets.controls.effective_keyword_stats?.baseline_count, buckets.controls.baseline_keywords.length),
    },
    controls: buckets.controls,
  };
}

function buildCrawlerKeywordManagerKpis(meta, monitorConfig) {
  const controls = meta.controls;
  const activeScopes = getCrawlerKeywordControlScopes();
  return [
    {
      label: "Baseline Seeds",
      value: formatNumber(meta.counts.totalBaseline),
      note: `${controls.baseline_file ? controls.baseline_file.split(/[\\/]/).pop() : "-"} · ${controls.baseline_file_writable ? "writable" : "read-only"}`,
    },
    {
      label: "Active Keywords",
      value: formatNumber(meta.counts.active),
      note: "当前 monitor 视角下仍会参与抓取的有效关键词",
    },
    {
      label: "Disabled Keywords",
      value: formatNumber(meta.counts.disabled),
      note: "彻底禁用 = 移出 baseline + 阻止后续返流",
    },
    {
      label: "Blocked Roots",
      value: formatNumber(meta.counts.roots),
      note: activeScopes.length
        ? `当前作用来源：${activeScopes.join(" / ")}`
        : (monitorConfig ? `Monitor ${monitorConfig.label || monitorConfig.name}` : "先选择 Monitor Config"),
    },
  ];
}

function renderCrawlerKeywordManagerFilters(meta) {
  const sourceSelect = el("crawler-keyword-manager-source-filter");
  if (sourceSelect) {
    const sourceOptions = ["", "baseline", "generated", "tracked", "manual"];
    sourceSelect.innerHTML = sourceOptions.map((source) => `
      <option value="${escapeHtml(source)}">${escapeHtml(source ? source : "全部来源")}</option>
    `).join("");
    if (!sourceOptions.includes(String(state.crawlerKeywordManagerSourceFilter || "").trim().toLowerCase())) {
      state.crawlerKeywordManagerSourceFilter = "";
    }
    sourceSelect.value = state.crawlerKeywordManagerSourceFilter || "";
  }
  const trackingSelect = el("crawler-keyword-manager-tracking-filter");
  if (trackingSelect) {
    const trackingOptions = Array.from(new Set(
      asArray(state.crawlerKeywordActiveKeywords)
        .map((item) => String(item?.tracking_mode || "").trim().toLowerCase())
        .filter(Boolean),
    )).sort((left, right) => left.localeCompare(right, "en"));
    trackingSelect.innerHTML = [
      '<option value="">全部 tracking mode</option>',
      ...trackingOptions.map((mode) => `<option value="${escapeHtml(mode)}">${escapeHtml(mode)}</option>`),
    ].join("");
    if (state.crawlerKeywordManagerTrackingFilter && !trackingOptions.includes(state.crawlerKeywordManagerTrackingFilter)) {
      state.crawlerKeywordManagerTrackingFilter = "";
    }
    trackingSelect.value = state.crawlerKeywordManagerTrackingFilter || "";
    trackingSelect.disabled = meta.tab !== "active";
  }
}

function renderCrawlerKeywordManagerTabs(meta) {
  const node = el("crawler-keyword-manager-tabs");
  if (!node) return;
  const tabs = [
    { key: "active", label: "Active Keywords", count: meta.counts.active },
    { key: "add", label: "Add Keywords", count: null },
    { key: "disabled", label: "Disabled Keywords", count: meta.counts.disabled },
    { key: "roots", label: "Blocked Roots", count: meta.counts.roots },
  ];
  node.innerHTML = tabs.map((item) => `
    <button
      type="button"
      class="tab-pill ${state.crawlerKeywordManagerTab === item.key ? "active" : ""}"
      data-action="set-crawler-keyword-manager-tab"
      data-tab="${escapeHtml(item.key)}"
    >
      ${escapeHtml(item.label)}${item.count === null ? "" : ` · ${escapeHtml(formatNumber(item.count))}`}
    </button>
  `).join("");
}

function renderCrawlerKeywordManagerSelectionSummary(meta) {
  const node = el("crawler-keyword-manager-selection-summary");
  if (!node) return;
  if (meta.tab === "add") {
    const previewKeywords = parseCrawlerKeywordListFromInput(state.crawlerKeywordControlDraft || "");
    const rootModeLabel = state.crawlerKeywordManagerRootMatchMode === "exact" ? "精准词" : "包含词根";
    node.textContent = previewKeywords.length
      ? `待提交 ${formatNumber(previewKeywords.length)} 个关键词，应用范围 ${getCrawlerKeywordControlScopes().join(", ")}，词根模式 ${rootModeLabel}`
      : "这里支持批量加入长期种子、彻底禁用关键词、以及新增禁用词根。";
    return;
  }
  const selectedCount = getCrawlerKeywordManagerSelectionBucket(meta.tab).length;
  node.textContent = selectedCount
    ? `${meta.label} 已选 ${formatNumber(selectedCount)} 个关键词。`
    : `${meta.label} 当前未选择关键词。`;
}

function buildCrawlerKeywordRowMeta(item, tab) {
  if (tab === "active") {
    const parts = [];
    parts.push(`source ${item.source_type || "-"}`);
    parts.push(`tracking ${item.tracking_mode || "-"}`);
    if (Number.isFinite(num(item.priority, Number.NaN))) {
      parts.push(`priority ${formatNumber(num(item.priority, 0))}`);
    }
    if (item.last_crawled_at) parts.push(`last crawl ${formatDate(item.last_crawled_at)}`);
    else if (item.last_expanded_at) parts.push(`last expand ${formatDate(item.last_expanded_at)}`);
    if (item.last_snapshot_id) parts.push(`snapshot ${item.last_snapshot_id}`);
    return parts.join(" | ");
  }
  const sourceLabel = asArray(item.blocked_sources).join(" / ") || "baseline";
  const matchModeLabel = item.match_mode === "contains" ? "包含词根" : "精准词";
  const reasonText = String(item.reason || "").trim() || (tab === "roots" ? "词根拦截" : "未写原因");
  const updatedAt = item.updated_at ? formatDate(item.updated_at) : "-";
  return `${matchModeLabel} | ${sourceLabel} | ${reasonText} | ${updatedAt}`;
}

function renderCrawlerKeywordManagerList(meta) {
  const selectedKeys = new Set(getCrawlerKeywordManagerSelectionBucket(meta.tab));
  if (!meta.filteredItems.length) {
    return `<div class="empty-state crawler-keyword-manager-empty">${escapeHtml(meta.emptyState)}</div>`;
  }
  return `
    <div class="crawler-keyword-manager-list">
      ${meta.filteredItems.map((item) => {
        const itemKey = getCrawlerKeywordManagerItemKey(item, meta.tab);
        const selected = selectedKeys.has(itemKey);
        const keyword = String(item.keyword || "-").trim();
        const rowChips = meta.tab === "active"
          ? [
            item.source_type ? `<span class="filter-chip">${escapeHtml(item.source_type)}</span>` : "",
            item.tracking_mode ? `<span class="filter-chip">${escapeHtml(item.tracking_mode)}</span>` : "",
            num(item.priority, 0) > 0 ? `<span class="filter-chip">P${escapeHtml(formatNumber(num(item.priority, 0)))}</span>` : "",
          ].filter(Boolean).join("")
          : [
            ...asArray(item.blocked_sources).map((source) => `<span class="filter-chip">${escapeHtml(source)}</span>`),
            `<span class="filter-chip ${meta.tab === "roots" ? "accent-chip" : ""}">${escapeHtml(item.match_mode === "contains" ? "包含词根" : "精准词")}</span>`,
          ].join("");
        const actionButton = meta.tab === "active"
          ? `<button type="button" class="mini-button" data-action="disable-crawler-keyword-item" data-keyword="${escapeHtml(keyword)}">彻底禁用</button>`
          : `<button type="button" class="mini-button" data-action="restore-crawler-keyword-item" data-rule-tab="${escapeHtml(meta.tab)}" data-keyword="${escapeHtml(keyword)}" data-blocked-sources="${escapeHtml(asArray(item.blocked_sources).join(","))}" data-match-mode="${escapeHtml(item.match_mode || "exact")}">${meta.tab === "roots" ? "移除词根" : "恢复"}</button>`;
        return `
          <article class="list-card crawler-keyword-manager-row ${selected ? "selected" : ""}">
            <div class="crawler-keyword-row-select">
              <input
                type="checkbox"
                class="crawler-keyword-row-checkbox"
                data-action="toggle-crawler-keyword-manager-selection"
                data-tab="${escapeHtml(meta.tab)}"
                data-key="${escapeHtml(itemKey)}"
                ${selected ? "checked" : ""}
              />
            </div>
            <div class="crawler-keyword-row-main">
              <div class="crawler-keyword-row-head">
                <div class="item-title">${escapeHtml(keyword)}</div>
                <div class="chip-row crawler-keyword-row-chips">
                  ${rowChips}
                </div>
              </div>
              <div class="list-meta">${escapeHtml(buildCrawlerKeywordRowMeta(item, meta.tab))}</div>
            </div>
            <div class="crawler-keyword-row-actions">
              ${actionButton}
            </div>
          </article>
        `;
      }).join("")}
    </div>
  `;
}

function renderCrawlerKeywordManagerSelectionBar(meta) {
  const node = el("crawler-keyword-manager-selection-bar");
  if (!node) return;
  if (meta.tab === "add") {
    node.innerHTML = "";
    node.hidden = true;
    renderCrawlerKeywordManagerSelectionSummary(meta);
    return;
  }
  node.hidden = false;
  const selectedCount = getCrawlerKeywordManagerSelectionBucket(meta.tab).length;
  const primaryAction = meta.tab === "active"
    ? '<button type="button" class="primary-button" data-action="batch-disable-crawler-keywords">批量禁用</button>'
    : '<button type="button" class="primary-button" data-action="batch-restore-crawler-keywords">批量恢复</button>';
  node.innerHTML = `
    <div class="crawler-keyword-manager-selection-copy">
      <strong>${escapeHtml(meta.label)}</strong>
      <span>当前筛选 ${escapeHtml(formatNumber(meta.filteredItems.length))} 项，已选 ${escapeHtml(formatNumber(selectedCount))} 项。</span>
    </div>
    <div class="button-row">
      <button type="button" class="ghost-button" data-action="select-all-crawler-keyword-manager-items" data-tab="${escapeHtml(meta.tab)}">全选当前结果</button>
      <button type="button" class="ghost-button" data-action="clear-crawler-keyword-manager-selection" data-tab="${escapeHtml(meta.tab)}">清空选择</button>
      ${primaryAction}
    </div>
  `;
  renderCrawlerKeywordManagerSelectionSummary(meta);
}

function renderCrawlerKeywordManagerAddPanel(meta) {
  const draftValue = state.crawlerKeywordControlDraft || "";
  const keywords = parseCrawlerKeywordListFromInput(draftValue);
  const activeScopes = getCrawlerKeywordControlScopes();
  return `
    <div class="crawler-keyword-manager-add-grid">
      <article class="detail-card crawler-keyword-add-card">
        <div class="detail-label">新增关键词 / 禁用词根</div>
        <div class="detail-caption">关键词模块的目标不是管词库，而是减少无价值抓取，把爬虫时间集中到更可能帮你找到产品的词上。</div>
        <label class="field">
          <span class="field-label">关键词列表 / One per line</span>
          <textarea id="crawler-keyword-control-input" rows="12" placeholder="dog toy&#10;cat litter&#10;walking pad">${escapeHtml(draftValue)}</textarea>
        </label>
        <div class="crawler-keyword-root-mode-row">
          <label class="field crawler-keyword-root-mode-field">
            <span class="field-label">禁用模式</span>
            <select id="crawler-keyword-root-match-mode">
              <option value="contains" ${state.crawlerKeywordManagerRootMatchMode === "contains" ? "selected" : ""}>排除带有这个词的关键词</option>
              <option value="exact" ${state.crawlerKeywordManagerRootMatchMode === "exact" ? "selected" : ""}>排除精准词</option>
            </select>
          </label>
          <div class="detail-caption">词根禁用默认下一轮 monitor / crawl 生效，不自动回写 baseline。</div>
        </div>
        <div class="button-row crawler-keyword-add-actions">
          <button type="button" class="primary-button" data-action="add-crawler-keyword-baseline">加入长期种子</button>
          <button type="button" class="ghost-button" data-action="add-crawler-keyword-exclusion">批量禁用</button>
          <button type="button" class="ghost-button" data-action="add-crawler-keyword-root-rule">新增禁用词根</button>
          <button type="button" class="ghost-button" data-action="clear-crawler-keyword-control-message">清空状态</button>
        </div>
      </article>
      <article class="detail-card crawler-keyword-preview-card">
        <div class="detail-label">提交预览</div>
        <div class="detail-caption">当前作用范围：${escapeHtml(activeScopes.join(" / ") || "-")}。彻底禁用 = 移出 baseline + 阻止后续返流。</div>
        <div class="crawler-keyword-preview-list" id="crawler-keyword-preview-list">
          ${keywords.length
            ? keywords.map((keyword) => `<span class="filter-chip">${escapeHtml(keyword)}</span>`).join("")
            : '<div class="empty-state">输入关键词后，这里会显示提交预览。</div>'}
        </div>
      </article>
    </div>
  `;
}

function syncCrawlerKeywordInputPreview() {
  if (state.crawlerKeywordManagerTab !== "add") return;
  const previewNode = el("crawler-keyword-preview-list");
  if (previewNode) {
    const keywords = parseCrawlerKeywordListFromInput(state.crawlerKeywordControlDraft || "");
    previewNode.innerHTML = keywords.length
      ? keywords.map((keyword) => `<span class="filter-chip">${escapeHtml(keyword)}</span>`).join("")
      : '<div class="empty-state">输入关键词后，这里会显示提交预览。</div>';
  }
  renderCrawlerKeywordManagerSelectionSummary(getCrawlerKeywordManagerTabData("add"));
}

function renderCrawlerKeywordManagerSummaryCards(meta, monitorConfig) {
  const kpis = buildCrawlerKeywordManagerKpis(meta, monitorConfig);
  renderKpiCards("crawler-keyword-manager-summary", kpis);
  renderKpiCards("crawler-keyword-manager-kpis", kpis);
}

function renderCrawlerKeywordManager() {
  const meta = getCrawlerKeywordManagerTabData(state.crawlerKeywordManagerTab);
  const controls = meta.controls;
  const monitorConfig = getSelectedCrawlerMonitorConfig();
  renderCrawlerKeywordScopeChips();
  renderCrawlerKeywordManagerFilters(meta);
  renderCrawlerKeywordManagerTabs(meta);
  renderCrawlerKeywordManagerSummaryCards(meta, monitorConfig);
  renderCrawlerKeywordManagerSelectionBar(meta);

  const configLabel = el("crawler-keyword-manager-config-label");
  if (configLabel) {
    configLabel.textContent = monitorConfig
      ? `${monitorConfig.label || monitorConfig.name} · ${monitorConfig.name}`
      : (controls.monitor_label || controls.monitor_config || "未选择");
  }
  const configMeta = el("crawler-keyword-manager-config-meta");
  if (configMeta) {
    const updatedAt = controls.updated_at ? formatDate(controls.updated_at) : "尚未同步";
    const baselineFile = controls.baseline_file ? controls.baseline_file.split(/[\\/]/).pop() : "-";
    configMeta.textContent = `baseline file ${baselineFile} · updated ${updatedAt}`;
  }
  const inlineMeta = el("crawler-keyword-manager-inline-meta");
  if (inlineMeta) {
    inlineMeta.textContent = monitorConfig
      ? `当前 ${monitorConfig.label || monitorConfig.name}：Active ${formatNumber(meta.counts.active)} / Disabled ${formatNumber(meta.counts.disabled)} / Roots ${formatNumber(meta.counts.roots)}`
      : "先切到 Keyword Monitor 计划类型，再选择 Monitor Config 打开管理器。";
  }

  const openButton = el("crawler-keyword-manager-open");
  if (openButton) openButton.disabled = !monitorConfig;

  const searchInput = el("crawler-keyword-manager-search");
  if (searchInput && searchInput.value !== state.crawlerKeywordManagerQuery) {
    searchInput.value = state.crawlerKeywordManagerQuery;
  }

  const content = el("crawler-keyword-manager-content");
  if (content) {
    content.innerHTML = meta.tab === "add"
      ? renderCrawlerKeywordManagerAddPanel(meta)
      : renderCrawlerKeywordManagerList(meta);
  }
}

function renderCrawlerKeywordControlList(containerId, items, kind) {
  const node = el(containerId);
  if (!node) return;
  if (!items.length) {
    node.innerHTML = kind === "baseline"
      ? '<div class="empty-state">暂无长期种子。</div>'
      : '<div class="empty-state">暂无排除规则。</div>';
    return;
  }
  node.innerHTML = items.map((item) => {
    if (kind === "baseline") {
      return `
        <div class="list-card crawler-control-list-card">
          <div class="item-title">${escapeHtml(item.keyword || "-")}</div>
          <div class="list-meta">source ${escapeHtml(item.source_scope || "baseline")} | ${escapeHtml(item.origin || "baseline")}</div>
          ${item.created_at ? `<div class="list-meta">${escapeHtml(formatDate(item.created_at))}</div>` : ""}
          <div class="inline-actions">
            <button type="button" class="mini-button" data-action="remove-crawler-keyword-baseline-item" data-keyword="${escapeHtml(item.keyword || "")}" data-source-scope="${escapeHtml(item.source_scope || "baseline")}">移除</button>
          </div>
        </div>
      `;
    }
    const sourcePills = asArray(item.blocked_sources).map((source) => `<span class="filter-chip">${escapeHtml(source)}</span>`).join("");
    return `
      <div class="list-card crawler-control-list-card">
        <div class="item-title">${escapeHtml(item.keyword || "-")}</div>
        <div class="list-meta">${sourcePills || '<span class="filter-chip">baseline</span>'}</div>
        <div class="list-meta">${escapeHtml(item.reason || "no reason")}</div>
        ${item.updated_at ? `<div class="list-meta">${escapeHtml(formatDate(item.updated_at))}</div>` : ""}
        <div class="inline-actions">
          <button type="button" class="mini-button" data-action="remove-crawler-keyword-exclusion-item" data-rule-id="${escapeHtml(item.id || item.keyword || "")}" data-keyword="${escapeHtml(item.keyword || "")}" data-blocked-sources="${escapeHtml((item.blocked_sources || []).join(","))}">移除</button>
        </div>
      </div>
    `;
  }).join("");
}

function renderCrawlerCategorySubcategoryOptionsMarkup(selectedValue = "", filters = []) {
  const selected = String(selectedValue || "").trim();
  const normalizedFilters = asArray(filters).map((item) => String(item || "").trim().toLowerCase()).filter(Boolean);
  const items = asArray(state.crawlerSubcategoryCatalog);
  const visibleItems = items.filter((item) => {
    if (!normalizedFilters.length) return true;
    const haystack = `${item.top_level_category || ""} ${item.breadcrumb_path || ""} ${item.display_name || ""} ${item.config_id || ""}`.toLowerCase();
    return normalizedFilters.some((filter) => haystack.includes(filter));
  });
  if (!visibleItems.length) {
    return '<option value="">No subcategories</option>';
  }
  const grouped = new Map();
  visibleItems.forEach((item) => {
    const group = item.top_level_category || "Other";
    if (!grouped.has(group)) grouped.set(group, []);
    grouped.get(group).push(item);
  });
  return Array.from(grouped.entries()).map(([group, groupItems]) => `
    <optgroup label="${escapeHtml(group)}">
      ${groupItems.map((item) => {
        const optionLabel = `${item.breadcrumb_path || item.display_name || item.config_id} · ${item.config_id}`;
        return `<option value="${escapeHtml(item.config_id)}" ${selected === item.config_id ? "selected" : ""}>${escapeHtml(optionLabel)}</option>`;
      }).join("")}
    </optgroup>
  `).join("");
}

function refreshCrawlerCategorySubcategorySelectors() {
  const selectedTopLevel = parseMultilineList(el("crawler-category-selected")?.value || "");
  const selectedSingle = String(el("crawler-category-target-subcategory")?.value || "").trim();
  const selectedPicker = String(el("crawler-category-subcategory-picker")?.value || "").trim();
  const markup = ['<option value="">Select a subcategory</option>', renderCrawlerCategorySubcategoryOptionsMarkup(selectedSingle, [])].join("");
  const pickerMarkup = ['<option value="">Select a subcategory</option>', renderCrawlerCategorySubcategoryOptionsMarkup(selectedPicker, selectedTopLevel)].join("");
  const targetSelect = el("crawler-category-target-subcategory");
  if (targetSelect) {
    targetSelect.innerHTML = markup;
    if (selectedSingle && Array.from(targetSelect.options).some((option) => option.value === selectedSingle)) {
      targetSelect.value = selectedSingle;
    } else {
      targetSelect.value = "";
    }
  }
  const picker = el("crawler-category-subcategory-picker");
  if (picker) {
    picker.innerHTML = pickerMarkup;
    if (selectedPicker && Array.from(picker.options).some((option) => option.value === selectedPicker)) {
      picker.value = selectedPicker;
    } else {
      picker.value = "";
    }
  }
}

function renderCrawlerKeywordControls() {
  const panel = el("crawler-keyword-control-panel");
  if (!panel) return;
  const isActive = String(el("crawler-plan-type")?.value || "") === "keyword_monitor";
  panel.hidden = !isActive;
  if (!isActive) {
    if (state.crawlerKeywordManagerOpen) closeCrawlerKeywordManager();
    return;
  }

  if (!state.crawlerKeywordControlMessage) {
    const monitorConfig = getSelectedCrawlerMonitorConfig();
    setCrawlerKeywordControlMessage(monitorConfig ? `Loaded ${monitorConfig.label || monitorConfig.name}` : "请选择 Monitor Config。");
  } else {
    setCrawlerKeywordControlMessage(state.crawlerKeywordControlMessage, el("crawler-keyword-control-message")?.dataset.state || "neutral");
  }
  renderCrawlerKeywordManager();
}

function renderCrawlerCategoryControls() {
  const panel = el("crawler-category-control-panel");
  if (!panel) return;
  const activePlanType = String(el("crawler-plan-type")?.value || "");
  const isActive = activePlanType === "category_single" || activePlanType === "category_ready_scan";
  panel.hidden = !isActive;
  if (!isActive) return;
  const isReadyScan = activePlanType === "category_ready_scan";
  refreshCrawlerCategorySubcategorySelectors();

  const selectedSubcategory = getSelectedCrawlerTargetSubcategoryConfig();
  const targetSummary = el("crawler-category-single-summary");
  if (targetSummary) {
    targetSummary.textContent = selectedSubcategory
      ? `${selectedSubcategory.breadcrumb_path || selectedSubcategory.display_name || selectedSubcategory.config_id} · config_id ${selectedSubcategory.config_id}`
      : "请选择目标子类目，优先使用 config_id。";
  }

  const picker = el("crawler-category-subcategory-picker");
  const pickerDepth = el("crawler-category-subcategory-depth");
  const pickerAdd = el("crawler-category-subcategory-add");
  const pickerClear = el("crawler-category-subcategory-clear");
  if (picker) picker.disabled = !isReadyScan;
  if (pickerDepth) pickerDepth.disabled = !isReadyScan;
  if (pickerAdd) pickerAdd.disabled = !isReadyScan;
  if (pickerClear) pickerClear.disabled = !isReadyScan;

  const overrideMeta = el("crawler-category-subcategory-meta");
  if (overrideMeta) {
    const draftCount = asArray(state.crawlerCategorySubcategoryOverridesDraft).length;
    overrideMeta.textContent = isReadyScan
      ? `${formatNumber(draftCount)} subcategory overrides drafted`
      : `${formatNumber(draftCount)} subcategory overrides drafted | ready scan only`;
  }

  const overrideList = el("crawler-category-subcategory-overrides-list");
  if (overrideList) {
    const items = asArray(state.crawlerCategorySubcategoryOverridesDraft);
    overrideList.innerHTML = items.length
      ? items.map((item) => `
        <div class="list-card crawler-control-list-card">
          <div class="item-title">${escapeHtml(item.display_name || item.breadcrumb_path || item.config_id || "-")}</div>
          <div class="list-meta">${escapeHtml(item.breadcrumb_path || item.display_name || item.config_id || "-")}</div>
          <div class="list-meta">config_id ${escapeHtml(item.config_id || "-")} | depth ${escapeHtml(formatNumber(item.product_count || 0))}</div>
          <div class="inline-actions">
            <button type="button" class="mini-button" data-action="remove-crawler-category-subcategory-override" data-config-id="${escapeHtml(item.config_id || "")}">移除</button>
          </div>
        </div>
      `).join("")
      : '<div class="empty-state">暂无子类目深度覆盖。</div>';
  }

  const singlePanel = el("crawler-category-control-message");
  if (singlePanel && !state.crawlerCategoryControlMessage) {
    setCrawlerCategoryControlMessage(isActive ? "Subcategory controls ready." : "请选择 Category 计划类型。");
  } else if (singlePanel) {
    setCrawlerCategoryControlMessage(state.crawlerCategoryControlMessage, singlePanel.dataset.state || "neutral");
  }
}

async function submitCrawlerKeywordControlAction(action, options = {}) {
  const monitorConfig = getSelectedCrawlerMonitorConfig();
  if (!monitorConfig?.name) {
    setCrawlerKeywordControlMessage("请选择 Monitor Config。", "warning");
    return;
  }
  const inputKeywords = options.keywords || parseCrawlerKeywordListFromInput(el("crawler-keyword-control-input")?.value || "");
  const sourceScopes = options.source_scopes || getCrawlerKeywordControlScopes();
  const reason = String((options.reason ?? el("crawler-keyword-control-reason")?.value) || "").trim();
  const normalizedAction = String(action || "").trim().toLowerCase();
  if (!inputKeywords.length) {
    setCrawlerKeywordControlMessage("请输入至少一个关键词。", "warning");
    return;
  }
  const blockedSources = asArray(options.blocked_sources || sourceScopes)
    .map((item) => String(item || "").trim().toLowerCase())
    .filter(Boolean);
  setCrawlerKeywordControlMessage(`Submitting ${normalizedAction}...`, "neutral");
  try {
    if (normalizedAction === "add_baseline" || normalizedAction === "remove_baseline") {
      await postJson(
        "/api/crawler/keyword-controls/baseline",
        {
          monitor_config: monitorConfig.name,
          keywords: inputKeywords,
          mode: normalizedAction === "add_baseline" ? "add" : "remove",
        },
        "POST",
      );
    } else if (normalizedAction === "disable_keywords" || normalizedAction === "add_exclusion") {
      await postJson(
        "/api/crawler/keyword-controls/disable",
        {
          monitor_config: monitorConfig.name,
          keywords: inputKeywords,
          blocked_sources: blockedSources,
          reason,
        },
        "POST",
      );
    } else if (normalizedAction === "restore_keywords" || normalizedAction === "remove_exclusion") {
      await postJson(
        "/api/crawler/keyword-controls/restore",
        {
          monitor_config: monitorConfig.name,
          keywords: inputKeywords,
        },
        "POST",
      );
    } else {
      throw new Error(`Unsupported keyword control action: ${normalizedAction}`);
    }
    setCrawlerKeywordControlMessage(`Applied ${normalizedAction} for ${monitorConfig.label || monitorConfig.name}.`, "success");
    await loadCrawlerKeywordControls();
  } catch (error) {
    setCrawlerKeywordControlMessage(`Keyword control failed: ${error.message}`, "error");
    throw error;
  }
}

async function mutateCrawlerKeywordRootRules(entries = [], mode = "upsert", successMessage = "") {
  const monitorConfig = getSelectedCrawlerMonitorConfig();
  if (!monitorConfig?.name) {
    setCrawlerKeywordControlMessage("请选择 Monitor Config。", "warning");
    return;
  }
  const normalizedMode = String(mode || "upsert").trim().toLowerCase();
  const normalizedEntries = asArray(entries).map((item) => ({
    keyword: String(item?.keyword || "").trim().toLowerCase(),
    match_mode: String(item?.match_mode || "contains").trim().toLowerCase() || "contains",
    blocked_sources: asArray(item?.blocked_sources).map((source) => String(source || "").trim().toLowerCase()).filter(Boolean),
    reason: String(item?.reason || "").trim(),
  })).filter((item) => item.keyword);
  if (!normalizedEntries.length) {
    setCrawlerKeywordControlMessage("当前没有可提交的禁用词根。", "warning");
    return;
  }
  setCrawlerKeywordControlMessage(`${normalizedMode === "delete" ? "恢复" : "提交"} ${normalizedEntries.length} 条词根规则中...`, "neutral");
  try {
    const groupedEntries = new Map();
    normalizedEntries.forEach((entry) => {
      const groupKey = entry.match_mode || "contains";
      if (!groupedEntries.has(groupKey)) groupedEntries.set(groupKey, []);
      groupedEntries.get(groupKey).push(entry);
    });
    for (const [matchMode, entriesForMode] of groupedEntries.entries()) {
      await postJson(
        "/api/crawler/keyword-controls/roots",
        {
          monitor_config: monitorConfig.name,
          keywords: entriesForMode.map((entry) => entry.keyword),
          blocked_sources: entriesForMode[0]?.blocked_sources || [],
          reason: entriesForMode[0]?.reason || "",
          match_mode: matchMode,
          mode: normalizedMode,
        },
        "POST",
      );
    }
    await loadCrawlerKeywordControls();
    setCrawlerKeywordControlMessage(successMessage || "禁用词根已更新。", "success");
  } catch (error) {
    setCrawlerKeywordControlMessage(`Keyword control failed: ${error.message}`, "error");
    throw error;
  }
}

function openCrawlerKeywordManager() {
  const monitorConfig = getSelectedCrawlerMonitorConfig();
  if (!monitorConfig?.name) {
    setCrawlerKeywordControlMessage("先选择 Monitor Config，再打开关键词管理器。", "warning");
    return;
  }
  state.crawlerKeywordManagerOpen = true;
  const modal = el("crawler-keyword-manager-modal");
  if (modal) modal.hidden = false;
  syncWorkbenchOverlayState();
  renderCrawlerKeywordManager();
  if (String(state.crawlerKeywordControls?.monitor_config || "") !== monitorConfig.name) {
    loadCrawlerKeywordControls().catch((error) => console.error("crawler keyword controls load on open failed", error));
  }
  window.setTimeout(() => {
    const focusTarget = state.crawlerKeywordManagerTab === "add"
      ? el("crawler-keyword-control-input")
      : el("crawler-keyword-manager-search");
    focusTarget?.focus();
  }, 0);
  scheduleShellMetrics();
}

function closeCrawlerKeywordManager() {
  state.crawlerKeywordManagerOpen = false;
  const modal = el("crawler-keyword-manager-modal");
  if (modal) modal.hidden = true;
  syncWorkbenchOverlayState();
  scheduleShellMetrics();
}

async function addCrawlerKeywordRootRules() {
  const keywords = parseCrawlerKeywordListFromInput(el("crawler-keyword-control-input")?.value || "");
  if (!keywords.length) {
    setCrawlerKeywordControlMessage("请输入至少一个 root keyword。", "warning");
    return;
  }
  const blockedSources = getCrawlerKeywordControlScopes();
  const reason = String(el("crawler-keyword-control-reason")?.value || "").trim();
  await mutateCrawlerKeywordRootRules(
    keywords.map((keyword) => ({
      keyword,
      match_mode: state.crawlerKeywordManagerRootMatchMode || "contains",
      blocked_sources: blockedSources,
      reason,
    })),
    "upsert",
    `已加入 ${formatNumber(keywords.length)} 条禁用词根规则。`,
  );
  state.crawlerKeywordControlDraft = "";
  clearCrawlerKeywordManagerSelection("roots");
  state.crawlerKeywordManagerTab = "roots";
  renderCrawlerKeywordManager();
}

async function batchDisableCrawlerKeywords() {
  const selectedKeys = getCrawlerKeywordManagerSelectionBucket("active");
  if (!selectedKeys.length) {
    setCrawlerKeywordControlMessage("请先在 Active Keywords 里选择要禁用的词。", "warning");
    return;
  }
  await submitCrawlerKeywordControlAction("disable_keywords", {
    keywords: selectedKeys,
    source_scopes: getCrawlerKeywordControlScopes(),
    reason: el("crawler-keyword-control-reason")?.value || "",
  });
  clearCrawlerKeywordManagerSelection("active");
  state.crawlerKeywordManagerTab = "disabled";
  renderCrawlerKeywordManager();
}

async function batchRestoreCrawlerKeywords(tab = state.crawlerKeywordManagerTab) {
  const meta = getCrawlerKeywordManagerTabData(tab);
  const selectedKeys = new Set(getCrawlerKeywordManagerSelectionBucket(meta.tab));
  const selectedItems = meta.items.filter((item) => selectedKeys.has(getCrawlerKeywordManagerItemKey(item, meta.tab)));
  if (!selectedItems.length) {
    setCrawlerKeywordControlMessage(`请先在 ${meta.label} 里选择要恢复的词。`, "warning");
    return;
  }
  if (meta.tab === "roots") {
    await mutateCrawlerKeywordRootRules(
      selectedItems.map((item) => ({
        keyword: item.keyword,
        match_mode: item.match_mode || "contains",
        blocked_sources: item.blocked_sources,
      })),
      "delete",
      `已恢复 ${formatNumber(selectedItems.length)} 条禁用词根规则。`,
    );
  } else {
    await submitCrawlerKeywordControlAction("restore_keywords", {
      keywords: selectedItems.map((item) => item.keyword),
    });
  }
  clearCrawlerKeywordManagerSelection(meta.tab);
  renderCrawlerKeywordManager();
}

async function disableCrawlerKeywordItem(keyword) {
  const normalizedKeyword = String(keyword || "").trim().toLowerCase();
  if (!normalizedKeyword) return;
  await submitCrawlerKeywordControlAction("disable_keywords", {
    keywords: [normalizedKeyword],
    source_scopes: getCrawlerKeywordControlScopes(),
    reason: el("crawler-keyword-control-reason")?.value || "",
  });
  renderCrawlerKeywordManager();
}

async function restoreCrawlerKeywordItem(keyword, blockedSources = [], matchMode = "exact", ruleTab = "disabled") {
  const normalizedKeyword = String(keyword || "").trim().toLowerCase();
  if (!normalizedKeyword) return;
  if (normalizeCrawlerKeywordManagerTab(ruleTab) === "roots") {
    await mutateCrawlerKeywordRootRules(
      [{
        keyword: normalizedKeyword,
        blocked_sources: blockedSources,
        match_mode: matchMode,
      }],
      "delete",
      `已移除词根 ${normalizedKeyword}。`,
    );
  } else {
    await submitCrawlerKeywordControlAction("restore_keywords", {
      keywords: [normalizedKeyword],
    });
  }
  CRAWLER_KEYWORD_MANAGER_TABS.forEach((tab) => {
    if (tab !== "add") {
      setCrawlerKeywordManagerSelection(
        tab,
        getCrawlerKeywordManagerSelectionBucket(tab).filter((item) => item !== normalizedKeyword),
      );
    }
  });
  renderCrawlerKeywordManager();
}

function addCrawlerCategorySubcategoryOverrideDraft() {
  const planType = String(el("crawler-plan-type")?.value || "");
  if (planType !== "category_ready_scan") {
    setCrawlerCategoryControlMessage("子类目深度覆盖仅适用于 Ready Scan。", "warning");
    return;
  }
  const picker = getSelectedCrawlerCategorySubcategoryPickerConfig();
  if (!picker) {
    setCrawlerCategoryControlMessage("请选择子类目。", "warning");
    return;
  }
  const depth = num(el("crawler-category-subcategory-depth")?.value, 0);
  if (depth <= 0) {
    setCrawlerCategoryControlMessage("请输入有效深度。", "warning");
    return;
  }
  upsertCrawlerCategorySubcategoryOverrideDraft(picker, depth);
  setCrawlerCategoryControlMessage(`Added ${picker.display_name || picker.breadcrumb_path || picker.config_id} @ ${formatNumber(depth)}.`, "success");
  renderCrawlerCategoryControls();
}

function clearCrawlerCategorySubcategoryOverrides() {
  clearCrawlerCategorySubcategoryOverrideDraft();
  setCrawlerCategoryControlMessage("已清空子类目深度覆盖。", "neutral");
  renderCrawlerCategoryControls();
}

async function loadCrawlerKeywordControls() {
  const requestKey = nextModuleRequestKey("crawler-keyword-controls");
  const monitorConfig = getSelectedCrawlerMonitorConfig();
  if (!monitorConfig?.name) {
    state.crawlerKeywordControls = {
      monitor_config: "",
      monitor_label: "",
      baseline_keywords: [],
      disabled_keywords: [],
      blocked_roots: [],
      exclusions: [],
      effective_keyword_stats: {},
      updated_at: "",
      status: "",
      message: "",
    };
    state.crawlerKeywordActiveKeywords = [];
    renderCrawlerKeywordControls();
    return;
  }
  try {
    setCrawlerKeywordControlMessage(`Loading keyword controls for ${monitorConfig.label || monitorConfig.name}...`, "neutral");
    const [controlsResult, activeKeywordsResult] = await Promise.all([
      getJsonSafe(
        `/api/crawler/keyword-controls?monitor_config=${encodeURIComponent(monitorConfig.name)}`,
        { monitor_config: monitorConfig.name, monitor_label: monitorConfig.label, baseline_keywords: [], exclusions: [], disabled_keywords: [], blocked_roots: [] },
        "Crawler keyword controls",
      ),
      getJsonSafe(
        `/api/crawler/keyword-controls/active-keywords?monitor_config=${encodeURIComponent(monitorConfig.name)}&limit=500`,
        { items: [], total: 0 },
        "Crawler active keywords",
      ),
    ]);
    if (!isActiveModuleRequest("crawler-keyword-controls", requestKey)) return;
    state.crawlerKeywordControls = normalizeCrawlerKeywordControls({
      ...(controlsResult.data || {}),
      monitor_config: monitorConfig.name,
      monitor_label: monitorConfig.label,
    });
    state.crawlerKeywordActiveKeywords = asArray(activeKeywordsResult.data?.items)
      .map((item) => normalizeCrawlerKeywordActiveItem(item))
      .filter(Boolean);
    if (controlsResult.error || activeKeywordsResult.error) {
      setCrawlerKeywordControlMessage(controlsResult.error || activeKeywordsResult.error, "warning");
    } else {
      setCrawlerKeywordControlMessage(`Loaded controls for ${monitorConfig.label || monitorConfig.name}`, "success");
    }
    renderCrawlerKeywordControls();
  } finally {
    clearModuleRequest("crawler-keyword-controls", requestKey);
  }
}

function renderCrawlerWorkspace() {
  renderCrawlerOverview();
  renderCrawlerPlans();
  renderCrawlerRuns();
  renderCrawlerHistory();
  renderCrawlerKeywordControls();
  renderCrawlerCategoryControls();
}

async function loadCrawlerView() {
  const requestKey = nextModuleRequestKey("crawler-view");
  setListLoading("crawler-overview-cards", "Loading crawler overview...");
  setListLoading("crawler-plan-list", "Loading crawler plans...");
  setListLoading("crawler-run-list", "Loading crawler runs...");
  setListLoading("crawler-history-list", "Loading crawler history...");
  try {
    const [catalogResult, plansResult, runsResult, workersResult] = await Promise.all([
      getJsonSafe("/api/crawler/catalog", { ready_categories: [], monitor_configs: [], defaults: {} }, "Crawler catalog"),
      getJsonSafe("/api/crawler/plans?limit=200", { items: [] }, "Crawler plans"),
      getJsonSafe("/api/crawler/runs?limit=200", { items: [] }, "Crawler runs"),
      getJsonSafe("/api/workers", { items: [] }, "Workers"),
    ]);
    if (!isActiveModuleRequest("crawler-view", requestKey)) return;
    state.crawlerLoadWarnings = [catalogResult.error, plansResult.error, runsResult.error, workersResult.error].filter(Boolean);
    state.crawlerCatalog = catalogResult.data;
    state.crawlerPlans = plansResult.data;
    state.crawlerRuns = runsResult.data;
    state.workers = workersResult.data;
    populateCrawlerCatalogForm(state.crawlerCatalog);
    renderCrawlerWorkspace();
    loadCrawlerKeywordControls().catch((error) => console.error("crawler keyword controls load failed", error));
  } finally {
    clearModuleRequest("crawler-view", requestKey);
  }
}

async function submitCrawlerPlan(launchNow = false) {
  const request = buildCrawlerPlanRequest();
  setCrawlerPlanBusy(true);
  setCrawlerPlanMessage(launchNow ? "Creating and launching plan..." : "Creating crawler plan...", "neutral");
  try {
    const created = await postJson("/api/crawler/plans", request, "POST");
    if (launchNow) await postJson(`/api/crawler/plans/${created.id}/launch`, {}, "POST");
    resetCrawlerPlanForm();
    setCrawlerPlanMessage(launchNow ? `Created and launched plan #${created.id}` : `Created plan #${created.id}`, "success");
    await loadCrawlerView();
  } catch (error) {
    setCrawlerPlanMessage(`Crawler plan failed: ${error.message}`, "error");
  } finally {
    setCrawlerPlanBusy(false);
    syncCrawlerPlanFormVisibility();
  }
}

async function launchCrawlerPlanFromConsole(planId) {
  const id = num(planId, 0);
  if (!id) return;
  await postJson(`/api/crawler/plans/${id}/launch`, {}, "POST");
  await loadCrawlerView();
}

async function pauseCrawlerPlanFromConsole(planId) {
  const id = num(planId, 0);
  if (!id) return;
  await postJson(`/api/crawler/plans/${id}/pause`, {}, "POST");
  await loadCrawlerView();
}

async function resumeCrawlerPlanFromConsole(planId) {
  const id = num(planId, 0);
  if (!id) return;
  await postJson(`/api/crawler/plans/${id}/resume`, {}, "POST");
  await loadCrawlerView();
}

function setTaskFormMessage(message, level = "neutral") {
  const node = el("task-form-message");
  if (!node) return;
  node.textContent = message || "-";
  node.dataset.state = level;
}

function resetTaskFormFields() {
  if (el("task-type")) el("task-type").value = "category_ready_scan";
  if (el("task-category")) el("task-category").value = "";
  if (el("task-keyword")) el("task-keyword").value = "";
  if (el("task-monitor-config")) el("task-monitor-config").value = "";
  if (el("task-product-count")) el("task-product-count").value = "50";
  if (el("task-priority")) el("task-priority").value = "10";
  if (el("task-persist")) el("task-persist").value = "true";
  if (el("task-export-excel")) el("task-export-excel").value = "false";
  if (el("task-noon-count")) el("task-noon-count").value = "30";
  if (el("task-amazon-count")) el("task-amazon-count").value = "30";
  if (el("task-reason")) el("task-reason").value = "";
  setTaskFormMessage("Tasks are queued for scheduler/worker execution. 任务会进入队列，由 scheduler/worker 执行。");
}

function setTaskFormBusy(busy) {
  state.taskFormBusy = Boolean(busy);
  ["task-type", "task-category", "task-keyword", "task-monitor-config", "task-product-count", "task-priority", "task-persist", "task-export-excel", "task-noon-count", "task-amazon-count", "task-reason", "task-form-reset", "task-form-submit"]
    .forEach((id) => {
      const node = el(id);
      if (node) node.disabled = state.taskFormBusy;
    });
}

function buildTaskCreateRequestFromForm() {
  return runsModule.buildTaskCreateRequestFromForm();
}

function renderRunsTaskCenter() {
  return runsModule.renderRunsTaskCenter();
}

function renderRunsWorkspace() {
  return runsModule.renderRunsWorkspace();
}

function prefillDrawerFromRow(item) {
  if (!item) return;
  state.productDetail = {
    summary: item,
    signals: item.signals || {},
    category_summary: item.category_summary || {},
    category_paths: asArray(item.category_paths),
    keywords: asArray(item.keywords),
    source_coverage: asArray(item.source_coverage),
    keyword_rankings: asArray(item.keyword_rankings),
    keyword_ranking_timeline: asArray(item.keyword_ranking_timeline),
    signal_timeline: asArray(item.signal_timeline),
    category_context_levels: asArray(item.category_context_levels),
    effective_category_context: item.effective_category_context || null,
  };
  state.productHistory = asArray(item.history);
  renderProductDrawer(true);
}

function syncDrawerLayoutState(isOpen) {
  if (drawerModule?.syncDrawerLayoutState) return drawerModule.syncDrawerLayoutState(isOpen);
  document.body.classList.remove("drawer-open");
  document.body.classList.toggle("drawer-visible", Boolean(isOpen));
  el("product-drawer")?.setAttribute("aria-hidden", isOpen ? "false" : "true");
}

function closeProductDrawer(silent = false) {
  if (drawerModule?.closeProductDrawer) return drawerModule.closeProductDrawer(silent);
  hideProductImagePreview(true);
  const drawer = el("product-drawer");
  if (drawer) {
    drawer.classList.remove("active");
    drawer.scrollTop = 0;
  }
  syncDrawerLayoutState(false);
  state.productDetailWarning = "";
  if (!silent) {
    state.route.focus_platform = "";
    state.route.focus_product = "";
    syncRouteToUrl(true);
    syncFocusedProductRows();
  }
}

async function openProductDrawer(platform, productId, silent = false) {
  if (drawerModule?.openProductDrawer) return drawerModule.openProductDrawer(platform, productId, silent);
  if (!platform || !productId) return;
  hideProductImagePreview(true);
  const key = makeProductKey(platform, productId);
  const row = state.rowCache[key];
  state.productDetailWarning = "";
  state.drawerContextLevel = "";
  state.drawerLoading = true;
  if (!silent) {
    state.route.focus_platform = platform;
    state.route.focus_product = productId;
    syncRouteToUrl(true);
    syncFocusedProductRows();
  }
  if (row) {
    prefillDrawerFromRow(row);
  } else {
    state.productDetail = null;
    state.productHistory = null;
    renderProductDrawer();
  }
  const drawer = el("product-drawer");
  if (drawer) {
    drawer.classList.add("active");
    drawer.scrollTop = 0;
  }
  syncDrawerLayoutState(true);

  rememberContext("products", {
    id: key,
    label: row?.title || productId,
    meta: row ? `${formatBsr(row)} | ${formatPrice(row.latest_price, row.latest_currency || "SAR")}` : `${platform} / ${productId}`,
  });
  renderRecentContext();

  const requestKey = `${key}:${Date.now()}`;
  state.drawerRequestKey = requestKey;
  try {
    const detail = await getJson(`/api/products/${encodeURIComponent(platform)}/${encodeURIComponent(productId)}`);
    if (state.drawerRequestKey !== requestKey) return;
    state.drawerLoading = false;
    state.productDetail = detail;
    state.productHistory = null;
    state.productDetailWarning = "";
    renderProductDrawer(false);
    if (drawer) drawer.scrollTop = 0;
  } catch (error) {
    if (state.drawerRequestKey !== requestKey) return;
    state.drawerLoading = false;
    if (row) {
      state.productDetailWarning = `商品详情暂时未能加载，先保留列表里的关键信息。${error?.message ? ` ${error.message}` : ""}`;
      state.productHistory = null;
      renderProductDrawer(true);
      if (drawer) drawer.scrollTop = 0;
      return;
    }
    state.productDetail = null;
    state.productHistory = null;
    state.productDetailWarning = "";
    renderProductDrawer();
    state.route.focus_platform = "";
    state.route.focus_product = "";
    syncRouteToUrl(true);
    syncFocusedProductRows();
    syncDrawerLayoutState(false);
    el("product-drawer")?.classList.remove("active");
  }
}

function buildSignalDetailLine(label, text, lastSeen, isSticky) {
  return `${label}: ${escapeHtml(formatSignalDisplay(text, isSticky ? lastSeen : ""))}`;
}

function renderDrawerKeyfacts(summary = {}, signals = {}) {
  if (drawerModule?.renderDrawerKeyfacts) return drawerModule.renderDrawerKeyfacts(summary, signals);
  const node = el("drawer-keyfacts");
  if (!node) return;
  const cards = [
    { label: "价格", value: formatPrice(summary.latest_price, summary.latest_currency || "SAR"), note: summary.original_price ? `原价 ${formatPrice(summary.original_price, summary.latest_currency || "SAR")}` : "当前价格" },
    { label: "月销量", value: summary.monthly_sales_estimate !== null && summary.monthly_sales_estimate !== undefined ? formatNumber(summary.monthly_sales_estimate) : "-", note: signals.sold_recently_text ? truncate(signals.sold_recently_text, 28) : "暂无销量信号" },
    { label: "库存", value: signals.stock_signal_text ? "有库存信号" : "无库存信号", note: summary.inventory_left_estimate !== null && summary.inventory_left_estimate !== undefined ? `约剩 ${formatNumber(summary.inventory_left_estimate)}` : "仅按公开信号估算" },
    { label: "BSR", value: formatBsr(summary), note: getRankStateLabel(summary) },
    { label: "Reviews", value: formatNumber(summary.latest_review_count), note: `Rating ${formatScore(summary.latest_rating)}` },
    { label: "Rating 增长", value: summary.rating_growth_7d !== null && summary.rating_growth_7d !== undefined ? formatSignedNumber(summary.rating_growth_7d, 2) : "-", note: summary.rating_growth_14d !== null && summary.rating_growth_14d !== undefined ? `14D ${formatSignedNumber(summary.rating_growth_14d, 2)}` : "14D -" },
  ];
  node.innerHTML = cards.map((item) => `
    <div class="fact-card compact">
      <div class="fact-label">${escapeHtml(item.label)}</div>
      <div class="fact-value">${escapeHtml(item.value)}</div>
      <div class="fact-note">${escapeHtml(item.note || "")}</div>
    </div>
  `).join("");
}

function splitCategoryPath(path) {
  return String(path || "")
    .split(">")
    .map((item) => item.trim())
    .filter(Boolean);
}

function getCompactCategoryPath(path, segments = 2) {
  const parts = splitCategoryPath(path || "");
  if (!parts.length) return "-";
  return parts.slice(-Math.max(segments, 1)).join(" › ");
}

function getSignalToneClass(text = "") {
  const normalized = String(text || "").toLowerCase();
  if (normalized.includes("sold") || normalized.includes("销量")) return "success";
  if (normalized.includes("left") || normalized.includes("库存")) return "warning";
  if (normalized.includes("price") || normalized.includes("低价")) return "accent";
  return "neutral";
}

function renderDrawerSignalTimeline(detail = {}) {
  if (drawerModule?.renderDrawerSignalTimeline) return drawerModule.renderDrawerSignalTimeline(detail);
  const node = el("drawer-signals");
  if (!node) return;
  const timeline = asArray(detail.signal_timeline);
  if (!timeline.length) {
    node.innerHTML = state.drawerLoading
      ? '<div class="loading-state">正在加载公开信号时间线...</div>'
      : '<div class="empty-state">最近 30 天没有可用的公开信号时间线。</div>';
    const chart = getChart("drawer-signal-chart");
    if (chart) chart.clear();
    return;
  }
  const latestDays = timeline.slice(-6).reverse();
  node.innerHTML = `
    <div class="drawer-section-copy">按日期聚合最近 30 天的公开信号，hover 可查看当天新增的信号文本。</div>
    <div id="drawer-signal-chart" class="drawer-chart"></div>
    <div class="drawer-timeline-list">
      ${latestDays.map((item) => `
        <div class="timeline-card">
          <div class="timeline-date">${escapeHtml(item.observed_at || "-")}</div>
          <div class="timeline-count">${escapeHtml(formatNumber(item.signal_count || 0))} 个信号</div>
          <div class="timeline-list">
            ${asArray(item.new_signals).slice(0, 4).map((signal) => `<span class="drawer-signal-pill ${getSignalToneClass(signal)}">${escapeHtml(truncate(signal, 22))}</span>`).join("") || '<span class="table-subtitle">无新增信号</span>'}
          </div>
        </div>
      `).join("")}
    </div>
  `;
  const chart = getChart("drawer-signal-chart");
  if (!chart) return;
  chart.setOption({
    grid: { left: 30, right: 18, top: 24, bottom: 30 },
    tooltip: {
      trigger: "axis",
      formatter: (params) => {
        const day = timeline[params?.[0]?.dataIndex || 0];
        const details = asArray(day?.new_signals).slice(0, 6).map((signal) => `• ${escapeHtml(signal)}`).join("<br />");
        return [
          `<strong>${escapeHtml(day?.observed_at || "-")}</strong>`,
          `Signal count: ${escapeHtml(formatNumber(day?.signal_count || 0))}`,
          details || "No new signals",
        ].join("<br />");
      },
    },
    xAxis: {
      type: "category",
      data: timeline.map((item) => item.observed_at),
      axisLabel: { color: "#746c63", fontSize: 10 },
      axisTick: { show: false },
      axisLine: { lineStyle: { color: "rgba(88, 78, 64, 0.18)" } },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#746c63", fontSize: 10 },
      splitLine: { lineStyle: { color: "rgba(88, 78, 64, 0.08)" } },
    },
    series: [{
      name: "公开信号",
      type: "line",
      smooth: true,
      symbolSize: 7,
      lineStyle: { width: 2, color: "#7d9b55" },
      itemStyle: { color: "#7d9b55" },
      areaStyle: { color: "rgba(125, 155, 85, 0.10)" },
      data: timeline.map((item) => num(item.signal_count, 0)),
    }],
  });
}

function renderDrawerKeywordRankingSection(detail = {}) {
  if (drawerModule?.renderDrawerKeywordRankingSection) return drawerModule.renderDrawerKeywordRankingSection(detail);
  const node = el("drawer-keyword-rankings");
  if (!node) return;
  const timeline = asArray(detail.keyword_ranking_timeline);
  const currentRows = asArray(detail.keyword_rankings).slice(0, 8);
  if (!timeline.length && !currentRows.length) {
    node.innerHTML = state.drawerLoading
      ? '<div class="loading-state">正在加载关键词排名趋势...</div>'
      : '<div class="empty-state">暂无关键词排名历史。后续新抓取会逐步补齐。</div>';
    const chart = getChart("drawer-keyword-chart");
    if (chart) chart.clear();
    return;
  }
  node.innerHTML = `
    <div class="drawer-section-copy">默认展示最近命中的关键词排名走势。纵轴越靠上表示排名越好。</div>
    <div id="drawer-keyword-chart" class="drawer-chart"></div>
    <div id="drawer-keyword-ranking-table" class="drawer-ranking-grid"></div>
  `;
  const grouped = new Map();
  timeline.forEach((item) => {
    const key = `${item.keyword || "-"}__${item.rank_type || "organic"}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  });
  const selectedSeries = [...grouped.entries()]
    .sort((left, right) => left[1].length === right[1].length
      ? num(left[1][left[1].length - 1]?.rank_position, 999999) - num(right[1][right[1].length - 1]?.rank_position, 999999)
      : right[1].length - left[1].length)
    .slice(0, 6);
  const days = [...new Set(timeline.map((item) => item.observed_day || item.observed_at))].sort();
  const signalLookup = new Map(asArray(detail.signal_timeline).map((item) => [item.observed_at, item]));
  const series = selectedSeries.map(([key, rows]) => {
    const [keyword, rankType] = key.split("__");
    const dataMap = new Map(rows.map((item) => [item.observed_day || item.observed_at, num(item.rank_position, null)]));
    return {
      name: `${keyword} · ${rankType}`,
      type: "line",
      smooth: true,
      connectNulls: false,
      symbolSize: 6,
      data: days.map((day) => dataMap.has(day) ? dataMap.get(day) : null),
      lineStyle: {
        width: rankType === "organic" ? 2.4 : 1.8,
        type: rankType === "organic" ? "solid" : "dashed",
      },
    };
  });
  const chart = getChart("drawer-keyword-chart");
  if (chart) {
    chart.setOption({
      grid: { left: 36, right: 20, top: 24, bottom: 36 },
      legend: { bottom: 0, left: "center", textStyle: { color: "#746c63", fontSize: 10 } },
      tooltip: {
        trigger: "axis",
        formatter: (params) => {
          const day = days[params?.[0]?.dataIndex || 0];
          const signalDay = signalLookup.get(day);
          const lines = params.map((item) => `${escapeHtml(item.seriesName)}: #${escapeHtml(String(item.value ?? "-"))}`);
          if (signalDay?.new_signals?.length) {
            lines.push("当天信号:");
            lines.push(...signalDay.new_signals.slice(0, 5).map((signal) => `• ${escapeHtml(signal)}`));
          }
          return [`<strong>${escapeHtml(day || "-")}</strong>`, ...lines].join("<br />");
        },
      },
      xAxis: {
        type: "category",
        data: days,
        axisLabel: { color: "#746c63", fontSize: 10 },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: "rgba(88, 78, 64, 0.18)" } },
      },
      yAxis: {
        type: "value",
        inverse: true,
        min: 1,
        minInterval: 1,
        axisLabel: { color: "#746c63", fontSize: 10 },
        splitLine: { lineStyle: { color: "rgba(88, 78, 64, 0.08)" } },
      },
      series,
    });
  }
  const tableNode = el("drawer-keyword-ranking-table");
  if (tableNode) {
    tableNode.innerHTML = currentRows.length
      ? `
        <table class="drawer-mini-table">
          <thead>
            <tr>
              <th>Keyword</th>
              <th>Rank</th>
              <th>Type</th>
              <th>Source</th>
              <th>Observed</th>
            </tr>
          </thead>
          <tbody>
            ${currentRows.map((item) => `
              <tr>
                <td><button type="button" class="drawer-keyword-link" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword || "")}">${escapeHtml(truncate(item.keyword || "-", 28))}</button></td>
                <td>#${escapeHtml(formatNumber(item.rank_position || 0))}</td>
                <td>${escapeHtml(item.rank_type || "-")}</td>
                <td>${escapeHtml((item.source_platform || "").toUpperCase() || "-")}</td>
                <td>${escapeHtml(formatDate(item.observed_at))}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `
      : '<div class="empty-state">暂无当前关键词排名表。</div>';
  }
}

function renderDrawerContextSection(detail = {}) {
  if (drawerModule?.renderDrawerContextSection) return drawerModule.renderDrawerContextSection(detail);
  const node = el("drawer-sources");
  if (!node) return;
  const levels = asArray(detail.category_context_levels);
  if (!levels.length) {
    node.innerHTML = state.drawerLoading
      ? '<div class="loading-state">正在加载类目上下文聚合...</div>'
      : '<div class="empty-state">暂无类目上下文聚合数据。</div>';
    return;
  }
  if (!state.drawerContextLevel || !levels.some((item) => item.level === state.drawerContextLevel)) {
    state.drawerContextLevel = detail.effective_category_context?.level || levels[levels.length - 1]?.level || levels[0]?.level || "";
  }
  const active = levels.find((item) => item.level === state.drawerContextLevel) || levels[levels.length - 1];
  const scope = active.summary || {};
  const summary = scope.summary || {};
  const childRows = asArray(scope.child_categories).slice(0, 5);
  node.innerHTML = `
    <div class="drawer-context-shell">
      <div class="drawer-section-copy">点击不同层级查看当前商品所在类目在该层级的大盘聚合。</div>
      <div class="segmented-control">
        ${levels.map((item) => `
          <button type="button" class="segment-button ${item.level === active.level ? "active" : ""}" data-action="set-drawer-context-level" data-level="${escapeHtml(item.level)}">
            ${escapeHtml(item.level)} · ${escapeHtml(item.label || item.path || "-")}
          </button>
        `).join("")}
      </div>
      <div class="table-subline">${escapeHtml(active.path || "-")}</div>
      <div class="drawer-context-grid">
        <div class="fact-card compact"><div class="fact-label">商品数</div><div class="fact-value">${escapeHtml(formatNumber(summary.product_count || 0))}</div></div>
        <div class="fact-card compact"><div class="fact-label">均价</div><div class="fact-value">${escapeHtml(formatPrice(summary.avg_price || 0))}</div></div>
        <div class="fact-card compact"><div class="fact-label">Express 占比</div><div class="fact-value">${escapeHtml(formatPercent(summary.express_share_pct))}</div></div>
        <div class="fact-card compact"><div class="fact-label">广告占比</div><div class="fact-value">${escapeHtml(formatPercent(summary.ad_share_pct))}</div></div>
        <div class="fact-card compact"><div class="fact-label">BSR 覆盖</div><div class="fact-value">${escapeHtml(formatPercent(summary.bsr_coverage_pct))}</div></div>
        <div class="fact-card compact"><div class="fact-label">信号覆盖</div><div class="fact-value">${escapeHtml(formatPercent(summary.signal_coverage_pct))}</div></div>
      </div>
      <div class="drawer-context-lists">
        <div class="detail-card">
          <div class="detail-label">细分类目大盘</div>
          ${childRows.length ? `
            <div class="drawer-context-mini-table">
              ${childRows.map((item) => `
                <button type="button" class="drawer-context-row" data-action="jump-drawer-category" data-path="${escapeHtml(item.path || item.label || "")}">
                  <span>${escapeHtml(truncate(item.label || item.path || "-", 26))}</span>
                  <strong>${escapeHtml(formatNumber(item.product_count || 0))}</strong>
                </button>
              `).join("")}
            </div>
          ` : '<div class="table-subtitle">当前层级没有更细分类目聚合。</div>'}
        </div>
      </div>
    </div>
  `;
}

function renderProductDrawer(prefillOnly = false) {
  if (drawerModule?.renderProductDrawer) return drawerModule.renderProductDrawer(prefillOnly);
  const detail = state.productDetail;
  const drawer = el("product-drawer");
  const warningNode = el("drawer-status-note");
  if (!drawer) return;

  if (!detail) {
    state.drawerLoading = false;
    el("drawer-title").textContent = "商品详情 / Product Detail";
    el("drawer-summary").textContent = "请选择商品查看详情 / Select a product to inspect details.";
    if (warningNode) {
      warningNode.hidden = true;
      warningNode.textContent = "";
    }
    if (el("drawer-primary-actions")) el("drawer-primary-actions").innerHTML = "";
    if (el("drawer-keyfacts")) el("drawer-keyfacts").innerHTML = "";
    if (el("drawer-category-summary")) el("drawer-category-summary").innerHTML = "";
    if (el("drawer-signals")) el("drawer-signals").innerHTML = "";
    if (el("drawer-keyword-rankings")) el("drawer-keyword-rankings").innerHTML = "";
    if (el("drawer-sources")) el("drawer-sources").innerHTML = "";
    return;
  }

  const summary = detail.summary || {};
  const signals = detail.signals || {};
  const categoryPaths = asArray(detail.category_paths);
  const primaryCategoryPath = detail.primary_category_path || summary.latest_observed_category_path || summary.latest_category_path || "";
  const productPageLabel = summary.platform === "amazon" ? "打开 Amazon / Open Amazon" : "打开 Noon / Open Noon";
  const favoriteActive = isFavoriteProduct(summary.platform, summary.product_id);
  const favoriteLabel = favoriteActive ? "取消收藏" : "加入收藏";

  el("drawer-title").textContent = truncate(summary.title || "商品详情", 82);
  el("drawer-summary").textContent = `${summary.brand || "-"} · ${summary.seller_name || "-"} · ${(summary.platform || "-").toUpperCase()} · ${summary.product_id || "-"}`;
  if (warningNode) {
    if (state.productDetailWarning) {
      warningNode.hidden = false;
      warningNode.textContent = state.productDetailWarning;
    } else {
      warningNode.hidden = true;
      warningNode.textContent = "";
    }
  }
  renderDrawerKeyfacts(summary, signals);

  el("drawer-primary-actions").innerHTML = `
    ${summary.product_url ? `<a class="primary-button" href="${escapeHtml(summary.product_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(productPageLabel)}</a>` : ""}
    <button class="ghost-button ${favoriteActive ? "active" : ""}" data-action="toggle-product-favorite" data-platform="${escapeHtml(summary.platform || "")}" data-product-id="${escapeHtml(summary.product_id || "")}" aria-pressed="${favoriteActive ? "true" : "false"}">${escapeHtml(favoriteLabel)}</button>
    ${primaryCategoryPath ? `<button class="ghost-button" data-action="jump-drawer-category" data-path="${escapeHtml(primaryCategoryPath)}">查看类目分析 / Open Category Analysis</button>` : ""}
  `;
  if (el("drawer-category-summary")) el("drawer-category-summary").innerHTML = "";
  renderDrawerSignalTimeline(detail);
  renderDrawerKeywordRankingSection(detail);
  renderDrawerContextSection(detail);
}

async function loadHealth() {
  state.health = await getJson("/api/health");
  const node = el("warehouse-path");
  if (node) node.textContent = state.health.warehouse_db;
}

async function loadHomeCore() {
  if (homeModule?.loadHomeCore) return homeModule.loadHomeCore();
  const requestKey = nextModuleRequestKey("home-core");
  el("dashboard-kpis").innerHTML = '<div class="loading-state">正在加载首页总览...</div>';
  setTableLoading("dashboard-category-table-body", "dashboard-category-table-meta", 8, "正在加载类目驾驶舱...");
  try {
    const params = new URLSearchParams();
    getHomeScopePaths(state.route).forEach((item) => params.append("selected_category_paths", item));
    const payload = await getJson(`/api/dashboard${params.toString() ? `?${params.toString()}` : ""}`);
    if (!isActiveModuleRequest("home-core", requestKey)) return;
    state.dashboard = payload;
    renderDashboard();
  } catch (error) {
    if (!isActiveModuleRequest("home-core", requestKey)) return;
    throw error;
  } finally {
    clearModuleRequest("home-core", requestKey);
  }
}

async function loadHomeSecondary() {
  if (homeModule?.loadHomeSecondary) return homeModule.loadHomeSecondary();
  await ensureCategoryTree();
  renderDashboard();
}

async function loadHomeView() {
  if (homeModule?.loadHomeView) return homeModule.loadHomeView();
  await loadHomeCore();
  clearModuleTimer("home-secondary");
  state.moduleTimers["home-secondary"] = window.setTimeout(() => {
    loadHomeSecondary().catch((error) => console.error("home secondary load failed", error));
  }, 0);
}

async function ensureCategoryTree() {
  if (state.categoryTree) return;
  state.categoryTree = await getJson("/api/categories/tree");
}

function setKeywordCoreLoading() {
  setListLoading("keyword-list", "正在加载关键词列表 / Loading keyword list...");
  setTableLoading("keyword-products-body", "keyword-products-meta", 11, "正在加载关键词商品 / Loading keyword products...");
}

function setKeywordSecondaryLoading() {
  setListLoading("keyword-opportunity-list", "正在加载机会词 / Loading keyword opportunities...");
  setListLoading("keyword-quality-issues", "正在加载质量问题 / Loading keyword quality issues...");
  setListLoading("keyword-seed-groups", "正在加载种子分组 / Loading seed groups...");
  setListLoading("keyword-runs-list", "正在加载关键词运行记录 / Loading keyword runs...");
  setLoadingText("keyword-opportunity-meta", "正在加载关键词情报 / Loading keyword intelligence...");
  setLoadingText("keyword-quality-meta", "正在加载关键词质量概览 / Loading keyword quality summary...");
  setLoadingText("keyword-graph-meta", "正在加载关键词图谱 / Loading keyword graph...");
}

async function loadKeywordProducts() {
  if (!state.route.keyword) {
    state.keywordProducts = null;
    renderProductTable("keyword-products-body", null, "keyword-products-meta", "选择关键词后加载商品结果 / Select a keyword first.");
    updatePagination("keyword-pagination", "keyword-prev", "keyword-next", null);
    return;
  }
  const requestKey = nextModuleRequestKey("keyword-products");
  setTableLoading("keyword-products-body", "keyword-products-meta", 11, "正在加载关键词商品 / Loading keyword products...");
  try {
    const result = await getJsonSafe(
      `/api/keywords/products?${buildProductQueryParams({
        keyword: state.route.keyword,
        limit: PAGE_SIZE,
        offset: state.route.keyword_offset || 0,
      }).toString()}`,
      { items: [], total_count: 0, limit: PAGE_SIZE, offset: 0 },
      "关键词商品",
    );
    if (!isActiveModuleRequest("keyword-products", requestKey)) return;
    state.keywordProducts = result.data;
    if (result.error) {
      state.keywordLoadWarnings = [...new Set([...(state.keywordLoadWarnings || []), result.error])];
    }
    renderProductTable("keyword-products-body", state.keywordProducts, "keyword-products-meta", "当前关键词下没有商品结果 / No keyword products.");
    updatePagination("keyword-pagination", "keyword-prev", "keyword-next", state.keywordProducts);
  } finally {
    clearModuleRequest("keyword-products", requestKey);
  }
}

async function loadKeywordCore() {
  if (keywordModule?.loadKeywordCore) return keywordModule.loadKeywordCore();
  const requestKey = nextModuleRequestKey("keyword-core");
  state.keywordLoadWarnings = [];
  setKeywordCoreLoading();
  try {
    const summaryResult = await getJsonSafe("/api/keywords/summary?limit=200", { items: [] }, "关键词摘要");
    if (!isActiveModuleRequest("keyword-core", requestKey)) return;
    state.keywordSummary = summaryResult.data;
    if (summaryResult.error) state.keywordLoadWarnings.push(summaryResult.error);

    const selected = asArray(state.keywordSummary.items).find((item) => item.keyword === state.route.keyword) || null;
    if (state.route.keyword) {
      setLoadingText("keyword-headline", "关键词研究 / Keyword Research");
      setLoadingText("keyword-subtitle", "正在加载关键词答案与命中商品。");
      const [benchmarksResult, historyResult] = await Promise.all([
        getJsonSafe(`/api/keywords/benchmarks?keyword=${encodeURIComponent(state.route.keyword)}`, { summary: {}, top_categories: [], delivery_breakdown: [] }, "关键词基准"),
        getJsonSafe(`/api/keyword/${encodeURIComponent(state.route.keyword)}/history?limit=120`, { items: [] }, "关键词历史"),
      ]);
      if (!isActiveModuleRequest("keyword-core", requestKey)) return;
      state.keywordLoadWarnings.push(...[benchmarksResult.error, historyResult.error].filter(Boolean));
      state.keywordBenchmarks = benchmarksResult.data;
      state.keywordHistory = historyResult.data;
      await loadKeywordProducts();
      rememberContext("keywords", {
        id: state.route.keyword,
        label: selected?.display_keyword || state.route.keyword,
        meta: selected ? `${selected.grade || "-"} | score ${formatScore(selected.total_score)}` : state.route.keyword,
      });
      renderRecentContext();
    } else {
      state.keywordBenchmarks = null;
      state.keywordHistory = null;
      state.keywordProducts = null;
      renderProductTable("keyword-products-body", null, "keyword-products-meta", "选择关键词后加载命中商品。");
      updatePagination("keyword-pagination", "keyword-prev", "keyword-next", null);
    }
    renderKeywordWorkspace();
  } finally {
    clearModuleRequest("keyword-core", requestKey);
  }
}

async function loadKeywordSecondary() {
  if (keywordModule?.loadKeywordSecondary) return keywordModule.loadKeywordSecondary();
  const requestKey = nextModuleRequestKey("keyword-secondary");
  const graphRootKeyword = state.route.keyword || asArray(state.keywordSummary?.items)[0]?.keyword || "";
  const opportunityParams = new URLSearchParams({ limit: "8" });
  if (graphRootKeyword) opportunityParams.set("root_keyword", graphRootKeyword);
  if (state.route.opp_priority) opportunityParams.set("priority_band", state.route.opp_priority);
  if (state.route.opp_evidence) opportunityParams.set("evidence_strength", state.route.opp_evidence);
  const opportunityUrl = `/api/keywords/opportunities?${opportunityParams.toString()}`;
  setKeywordSecondaryLoading();
  try {
    const [opportunitiesResult, qualityIssuesResult, seedGroupsResult, graphResult, runsResult, warehouseHealthResult] = await Promise.all([
      getJsonSafe(opportunityUrl, { summary: {}, items: [] }, "关键词机会"),
      getJsonSafe("/api/keywords/quality-issues?limit=80", { summary: {}, items: [] }, "关键词质量问题"),
      getJsonSafe("/api/keywords/seed-groups", { items: [] }, "关键词种子组"),
      graphRootKeyword
        ? getJsonSafe(
            `/api/keywords/graph?root_keyword=${encodeURIComponent(graphRootKeyword)}&depth=2&limit=80`,
            { root_keyword: graphRootKeyword, node_count: 0, edge_count: 0, nodes: [], edges: [] },
            "关键词图谱",
          )
        : Promise.resolve({ data: { root_keyword: "", node_count: 0, edge_count: 0, nodes: [], edges: [] }, error: "" }),
      state.runsSummary
        ? Promise.resolve({ data: state.runsSummary, error: "" })
        : getJsonSafe("/api/runs/summary", { health: {}, recent_imports: [], keyword_runs: [] }, "关键词运行摘要"),
      state.keywordWarehouseHealth
        ? Promise.resolve({ data: state.keywordWarehouseHealth, error: "" })
        : getJsonSafe("/api/keywords/warehouse-health", { summary: {}, recent_imports: [], recent_runs: [] }, "Keyword warehouse health"),
    ]);
    if (!isActiveModuleRequest("keyword-secondary", requestKey)) return;
    state.keywordLoadWarnings = [
      ...new Set([
        ...(state.keywordLoadWarnings || []),
        opportunitiesResult.error,
        qualityIssuesResult.error,
        seedGroupsResult.error,
        graphResult.error,
        runsResult.error,
        warehouseHealthResult.error,
      ].filter(Boolean)),
    ];
    state.keywordOpportunities = opportunitiesResult.data;
    state.keywordQualityIssues = qualityIssuesResult.data;
    state.keywordSeedGroups = seedGroupsResult.data;
    state.keywordGraph = graphResult.data;
    state.runsSummary = runsResult.data;
    state.watchdogSummary = runsResult.data?.watchdog || null;
    state.keywordWarehouseHealth = warehouseHealthResult.data;
    renderKeywordWorkspace();
    renderKeywordIntelligence();
  } finally {
    clearModuleRequest("keyword-secondary", requestKey);
  }
}

async function loadKeywordView() {
  if (keywordModule?.loadKeywordView) return keywordModule.loadKeywordView();
  await loadKeywordCore();
  renderKeywordIntelligence();
  clearModuleTimer("keyword-secondary");
  state.moduleTimers["keyword-secondary"] = window.setTimeout(() => {
    loadKeywordSecondary().catch((error) => console.error("keyword secondary load failed", error));
  }, 0);
}

async function refreshSignalOptionsForView() {
  if (keywordModule?.refreshSignalOptionsForView) return keywordModule.refreshSignalOptionsForView();
  if (["selection", "keyword"].includes(state.route.view)) {
    await loadSignalOptions();
  } else {
    state.productSignalOptions = [];
  }
}

async function loadRunsCore() {
  const requestKey = nextModuleRequestKey("runs-core");
  setListLoading("task-queue-list", "Loading task queue...");
  setListLoading("task-workers-list", "Loading workers...");
  setListLoading("task-runs-list", "Loading task runs...");
  setListLoading("runs-health", "Loading runs health...");
  setListLoading("runs-keyword", "Loading keyword runs...");
  setListLoading("runs-imports", "Loading imports...");
  try {
    const [runsSummaryResult, keywordWarehouseHealthResult] = await Promise.all([
      getJsonSafe("/api/runs/summary", { health: {}, recent_imports: [], keyword_runs: [] }, "运行摘要"),
      getJsonSafe("/api/keywords/warehouse-health", { summary: {}, recent_imports: [], recent_runs: [] }, "关键词仓库健康"),
    ]);
    if (!isActiveModuleRequest("runs-core", requestKey)) return;
    state.runsLoadWarnings = [runsSummaryResult.error, keywordWarehouseHealthResult.error].filter(Boolean);
    state.runsSummary = runsSummaryResult.data;
    state.watchdogSummary = runsSummaryResult.data?.watchdog || null;
    state.keywordWarehouseHealth = keywordWarehouseHealthResult.data;
    renderRunsWorkspace();
  } finally {
    clearModuleRequest("runs-core", requestKey);
  }
}
async function loadRunsTaskCenter() { return runsModule.loadRunsTaskCenter(); }

async function loadRunsCoreEnhanced() { return runsModule.loadRunsCoreEnhanced(); }
async function loadRunsView() { return runsModule.loadRunsView(); }

function renderRuntimeAlertPanel() { return runsModule.renderRuntimeAlertPanel(); }

async function runRoute() {
  if (["runs", "crawler"].includes(state.route.view) && !(state.route.view === "runs" ? canAccessRuns() : canAccessCrawler())) {
    state.route = normalizeRoute({ ...state.route, view: "home" });
    syncRouteToUrl(true);
  }
  syncWorkspaceMeta();
  syncGlobalControls();
  renderGlobalFilterSummary();
  renderSavedViews();
  renderRecentContext();
  renderCompareTray();
  updateSidebarContext();

  try {
    if (!state.health) await loadHealth();
    await refreshSignalOptionsForView();
    renderFilterForms();
    syncOwnerFilters();
    if (state.route.view === "home") await loadHomeView();
    else if (state.route.view === "selection") await loadSelectionView();
    else if (state.route.view === "favorites") await loadFavoritesView();
    else if (state.route.view === "keyword") await loadKeywordView();
    else if (state.route.view === "crawler") await loadCrawlerView();
    else if (state.route.view === "runs") await loadRunsView();
  } catch (error) {
  const message = `加载失败 / Load failed: ${error.message}`;
    if (state.route.view === "keyword") {
    setTableLoading("keyword-products-body", "keyword-products-meta", 11, message);
    } else if (state.route.view === "favorites") {
      setTableLoading("favorites-body", "favorites-meta", 11, message);
    } else if (state.route.view === "crawler") {
      setListLoading("crawler-overview-cards", message);
      setListLoading("crawler-plan-list", message);
      setListLoading("crawler-run-list", message);
      setListLoading("crawler-history-list", message);
    } else if (state.route.view === "runs") {
      setListLoading("runs-health", message);
    } else {
      setListLoading("warning-list", message);
    }
  }
}

async function refreshCurrentView(previousRoute) {
  if (["runs", "crawler"].includes(state.route.view) && !(state.route.view === "runs" ? canAccessRuns() : canAccessCrawler())) {
    state.route = normalizeRoute({ ...state.route, view: "home" });
    syncRouteToUrl(true);
  }
  syncWorkspaceMeta();
  syncGlobalControls();
  renderGlobalFilterSummary();
  renderSavedViews();
  renderRecentContext();
  renderCompareTray();
  updateSidebarContext();

  if (!state.health) await loadHealth();
  await refreshSignalOptionsForView();
  renderFilterForms();
  syncOwnerFilters();
  if ((previousRoute?.view || "home") !== state.route.view) {
    await runRoute();
    return;
  }

  const changedKeys = getChangedRouteKeys(previousRoute || {}, state.route);
  if (!changedKeys.length) return;

  const nonLocalKeys = changedKeys.filter((key) => !LOCAL_ONLY_ROUTE_KEYS.has(key));
  if (!nonLocalKeys.length) {
    if (changedKeys.includes("density")) {
      renderVisibleProductTables();
      renderProductDrawer(Boolean(state.productHistory));
    }
    if (changedKeys.includes("time")) {
      if (state.route.view === "keyword") renderKeywordWorkspace();
      if (state.route.view === "home" && state.dashboard) renderDashboard();
    }
    return;
  }

  if (state.route.view === "home") {
    renderDashboardScopeSelectorOptions();
    const scopeMeta = el("dashboard-scope-meta");
    if (scopeMeta) {
      const scopePaths = getHomeScopePaths(state.route);
      scopeMeta.textContent = scopePaths.length
        ? `${scopePaths.map((item) => item.split(" > ").slice(-1)[0] || item).join(" · ")} · 正在重新聚合大盘...`
        : "全平台大盘 · 正在恢复总览...";
    }
    await loadHomeCore();
    clearModuleTimer("home-secondary");
    state.moduleTimers["home-secondary"] = window.setTimeout(() => {
      loadHomeSecondary().catch((error) => console.error("home secondary load failed", error));
    }, 0);
    return;
  }

  if (state.route.view === "selection") {
    await loadSelectionView();
    scheduleSelectionWorkspaceHydration();
    return;
  }

  if (state.route.view === "favorites") {
    await loadFavoritesView();
    return;
  }

  if (state.route.view === "crawler") {
    await loadCrawlerView();
    return;
  }

  if (state.route.view === "keyword") {
    if (changedKeys.includes("keyword")) {
      await loadKeywordView();
      return;
    }
    if (changedKeys.includes("keyword_mode")) {
      renderKeywordWorkspace();
      return;
    }
    const keywordProductsChanged = changedKeys.some((key) => KEYWORD_PRODUCT_ROUTE_KEYS.has(key));
    const keywordSecondaryChanged = changedKeys.some((key) => ["opp_priority", "opp_evidence"].includes(key));
    if (keywordProductsChanged) await loadKeywordProducts();
    if (keywordSecondaryChanged) {
      clearModuleTimer("keyword-secondary");
      state.moduleTimers["keyword-secondary"] = window.setTimeout(() => {
        loadKeywordSecondary().catch((error) => console.error("keyword secondary load failed", error));
      }, 0);
    }
    if (changedKeys.includes("time")) renderKeywordWorkspace();
    return;
  }

  if (state.route.view === "runs") {
    await loadRunsCoreEnhanced();
  }
}

async function navigateWithPatch(patch, replace = false) {
  const previousRoute = { ...state.route };
  state.route = normalizeRoute({ ...state.route, ...patch });
  syncWorkbenchScopeState();
  document.body.dataset.density = state.route.density;
  syncRouteToUrl(replace);
  await refreshCurrentView(previousRoute);
  if (state.route.focus_platform && state.route.focus_product) {
    await openProductDrawer(state.route.focus_platform, state.route.focus_product, true);
  } else {
    closeProductDrawer(true);
  }
}

function applyProductFilters(owner) {
  state.productSignalSearch[owner] = (el(`${owner}-signal-search`)?.value || "").trim();
  if (owner === "selection") {
    const nextPatch = collectProductPatch(owner);
    const currentSignature = buildSelectionLoadSignature(state.route, state.route.products_offset || 0);
    const nextRoute = normalizeRoute({ ...state.route, ...nextPatch });
    const nextSignature = buildSelectionLoadSignature(nextRoute, nextRoute.products_offset || 0);
    if (state.selectionLoadPromise && nextSignature === currentSignature) {
      return state.selectionLoadPromise.then(() => recordProductFilterHistory());
    }
    return navigateWithPatch(nextPatch).then(async () => {
      await ensureSelectionWorkspaceHydrated(true);
      return recordProductFilterHistory();
    });
  }
  return navigateWithPatch(collectProductPatch(owner)).then(async () => {
    return recordProductFilterHistory();
  });
}

function resetProductFilters(owner) {
  if (owner === "selection") {
    state.productSignalSearch[owner] = "";
    return navigateWithPatch({
      view: "selection",
      q: "",
      market: state.route.market,
      platform: "",
      source: "",
      selected_category_paths: "",
      category_path: "",
      keyword: "",
      delivery: "",
      is_ad: "",
      tab: "all",
      sort: "sales_desc",
      bsr_min: "",
      bsr_max: "",
      review_min: "",
      review_max: "",
      rating_min: "",
      rating_max: "",
      price_min: "",
      price_max: "",
      sales_min: "",
      sales_max: "",
      inventory_min: "",
      inventory_max: "",
      review_growth_7d_min: "",
      review_growth_14d_min: "",
      rating_growth_7d_min: "",
      rating_growth_14d_min: "",
      signal_tags: "",
      signal_text: "",
      has_sold_signal: "",
      has_stock_signal: "",
      has_lowest_price_signal: "",
      products_offset: 0,
      category_offset: 0,
      keyword_offset: 0,
      focus_platform: "",
      focus_product: "",
    }).then(() => ensureSelectionWorkspaceHydrated(true));
  }
  const patch = {
    sort: "bsr_asc",
    delivery: "",
    is_ad: "",
    bsr_min: "",
    bsr_max: "",
    review_min: "",
    review_max: "",
    price_min: "",
    price_max: "",
    signal_tags: "",
    has_sold_signal: "",
    has_stock_signal: "",
    has_lowest_price_signal: "",
    signal_text: "",
    products_offset: 0,
    category_offset: 0,
    keyword_offset: 0,
  };
  if (owner === "products") patch.category_path = "";
  return navigateWithPatch(patch);
}

function applyProductPreset(owner, preset) {
  const patch = {
    products_offset: 0,
    category_offset: 0,
    keyword_offset: 0,
  };
  if (preset === "top100-lowreview") Object.assign(patch, { tab: "ranked", sort: "bsr_asc", bsr_max: "100", review_max: "10" });
  if (preset === "entry-window") Object.assign(patch, { tab: "ranked", sort: "bsr_asc", bsr_max: "100", review_max: "10", has_sold_signal: "true" });
  if (preset === "sold") Object.assign(patch, { has_sold_signal: "true" });
  if (preset === "stock") Object.assign(patch, { has_stock_signal: "true" });
  if (preset === "lowest") Object.assign(patch, { has_lowest_price_signal: "true" });
  if (preset === "express") Object.assign(patch, { delivery: "express" });
  if (preset === "ads") Object.assign(patch, { is_ad: "1" });
  navigateWithPatch(patch);
}

function applyKeywordOpportunityFilters() {
  return navigateWithPatch({
    opp_priority: el("keyword-opportunity-priority")?.value?.trim?.() ?? "",
    opp_evidence: el("keyword-opportunity-evidence")?.value?.trim?.() ?? "",
    keyword_offset: 0,
  });
}

function resetKeywordOpportunityFilters() {
  return navigateWithPatch({
    opp_priority: "",
    opp_evidence: "",
    keyword_offset: 0,
  });
}

function buildKeywordQueryPatch() {
  return {
    view: "keyword",
    keyword: el("keyword-workspace-input")?.value?.trim?.() ?? "",
    market: el("keyword-market")?.value?.trim?.() ?? "",
    platform: el("keyword-platform")?.value?.trim?.() ?? "",
    source: el("keyword-source")?.value?.trim?.() ?? "",
    keyword_offset: 0,
    products_offset: 0,
    focus_platform: "",
    focus_product: "",
  };
}

async function applyProductFilterHistory(historyId) {
  const item = state.productFilterHistory.find((entry) => String(entry.id) === String(historyId));
  if (!item?.route_payload) return;
  const nextRoutePayload = {
    ...buildProductFilterMemoryRoute(item.route_payload || {}),
    products_offset: 0,
    category_offset: 0,
    keyword_offset: 0,
  };
  await navigateWithPatch(nextRoutePayload);
  await recordProductFilterHistory(buildProductFilterMemoryRoute(nextRoutePayload));
}

async function handleActionClick(event) {
  const target = event.target.closest("[data-action], .nav-item[data-view]");
  if (!target) return;

  const fallbackAction = target.classList.contains("nav-item") && target.dataset.view ? "nav-view" : "";
  const action = target.dataset.action || fallbackAction;
  if (!action) return;

  if (action === "nav-view") {
    const nextView = target.dataset.view || "home";
    if ((nextView === "runs" && !canAccessRuns()) || (nextView === "crawler" && !canAccessCrawler())) {
      await navigateWithPatch({ view: "home", focus_platform: "", focus_product: "" });
      return;
    }
    await navigateWithPatch({ view: nextView, focus_platform: "", focus_product: "" });
    return;
  }
  if (action === "set-keyword-mode") {
    await navigateWithPatch({ keyword_mode: target.dataset.mode || "research" }, true);
    return;
  }
  if (action === "open-category-selector") {
    openCategorySelector(target.dataset.mode || "multi", target.dataset.context || state.route.view);
    return;
  }
  if (action === "toggle-category-advanced") {
    state.showCategoryAdvanced = !state.showCategoryAdvanced;
    renderFilterForms();
    scheduleShellMetrics();
    return;
  }
  if (action === "close-category-selector") {
    closeCategorySelector();
    return;
  }
  if (action === "jump-section") {
    const node = el(target.dataset.target || "");
    if (node) node.scrollIntoView({ behavior: "smooth", block: "start" });
    return;
  }
  if (action === "apply-keyword-opportunity-filters") {
    await applyKeywordOpportunityFilters();
    return;
  }
  if (action === "reset-keyword-opportunity-filters") {
    await resetKeywordOpportunityFilters();
    return;
  }
  if (action === "open-category") {
    await navigateWithPatch(buildSelectionCategoryRoute(target.dataset.path || ""));
    return;
  }
  if (action === "pick-category") {
    togglePendingCategoryPath(target.dataset.path || "");
    return;
  }
  if (action === "set-category-selector-mode") {
    setCategorySelectorMode(target.dataset.mode || "multi");
    return;
  }
  if (action === "open-keyword") {
    await navigateWithPatch({ view: "keyword", keyword: target.dataset.keyword || "", keyword_offset: 0, focus_platform: "", focus_product: "" });
    return;
  }
  if (action === "open-product") {
    await openProductDrawer(target.dataset.platform, target.dataset.productId);
    return;
  }
  if (action === "toggle-product-favorite") {
    await toggleProductFavorite(target.dataset.platform, target.dataset.productId);
    return;
  }
  if (action === "open-product-url") {
    const url = target.dataset.url || state.productDetail?.summary?.product_url || "";
    if (url) {
      const opened = window.open(url, "_blank", "noopener,noreferrer");
      if (!opened) window.location.href = url;
    }
    return;
  }
  if (action === "toggle-tree") {
    const path = target.dataset.path || "";
    if (state.expandedCategoryPaths.has(path)) state.expandedCategoryPaths.delete(path);
    else state.expandedCategoryPaths.add(path);
    renderCategoryTree();
    return;
  }
  if (action === "toggle-product-advanced") {
    const owner = target.dataset.owner || "selection";
    state.productAdvancedOpen[owner] = !state.productAdvancedOpen[owner];
    renderFilterForms();
    scheduleShellMetrics();
    return;
  }
  if (action === "apply-product-filters") {
    await applyProductFilters(target.dataset.owner || "products");
    return;
  }
  if (action === "apply-product-filter-history") {
    await applyProductFilterHistory(target.dataset.historyId || "");
    return;
  }
  if (action === "reset-product-filters") {
    await resetProductFilters(target.dataset.owner || "products");
    return;
  }
  if (action === "save-product-filter-preset") {
    await saveCurrentProductFilterPreset(target.dataset.defaultName || "");
    return;
  }
  if (action === "focus-selection-memory") {
    const node = el("selection-summary-strip");
    if (node) {
      node.scrollIntoView({ behavior: "smooth", block: "start" });
      window.setTimeout(() => node.classList.remove("pulse-focus"), 900);
      node.classList.add("pulse-focus");
    }
    return;
  }
  if (action === "apply-product-filter-preset") {
    await applyProductFilterPreset(target.dataset.presetId || "");
    return;
  }
  if (action === "delete-product-filter-preset") {
    await deleteProductFilterPreset(target.dataset.presetId || "");
    return;
  }
  if (action === "set-default-product-filter-preset") {
    await setDefaultProductFilterPreset(target.dataset.presetId || "");
    return;
  }
  if (action === "toggle-keyword-secondary") {
    state.keywordSecondaryOpen = !state.keywordSecondaryOpen;
    renderKeywordWorkspace();
    scheduleShellMetrics();
    return;
  }
  if (action === "apply-keyword-query") {
    await navigateWithPatch(buildKeywordQueryPatch());
    return;
  }
  if (action === "toggle-signal-tag") {
    const owner = target.dataset.owner || "products";
    const tag = target.dataset.tag || "";
    const selected = new Set(parseSignalTagCsv());
    if (selected.has(tag)) selected.delete(tag);
    else selected.add(tag);
    state.route.signal_tags = Array.from(selected).join(",");
    if (!state.productAdvancedOpen[owner]) state.productAdvancedOpen[owner] = true;
    renderFilterForms();
    scheduleShellMetrics();
    return;
  }
  if (action === "apply-preset") {
    applyProductPreset(target.dataset.owner || "products", target.dataset.preset || "");
    return;
  }
  if (action === "clear-home-scope") {
    await navigateWithPatch({ view: "home", home_scope: "", focus_platform: "", focus_product: "" });
    return;
  }
  if (action === "switch-tab") {
    await navigateWithPatch({ tab: target.dataset.tab || "ranked", products_offset: 0, category_offset: 0, keyword_offset: 0 });
    return;
  }
  if (action === "add-compare-product") {
    const key = `${target.dataset.platform}::${target.dataset.productId}`;
    addProductToCompare(state.rowCache[key]);
    return;
  }
  if (action === "add-compare-keyword") {
    const item = asArray(state.keywordSummary?.items).find((row) => row.keyword === target.dataset.keyword);
    addKeywordToCompare(item);
    return;
  }
  if (action === "remove-compare-product") {
    state.compareTray.products = state.compareTray.products.filter((item) => item.id !== target.dataset.id);
    syncCompareRouteState();
    persistCompareTray();
    renderCompareTray();
    syncRouteToUrl(true);
    return;
  }
  if (action === "remove-compare-keyword") {
    state.compareTray.keywords = state.compareTray.keywords.filter((item) => item.id !== target.dataset.id);
    syncCompareRouteState();
    persistCompareTray();
    renderCompareTray();
    syncRouteToUrl(true);
    return;
  }
  if (action === "open-saved-view") {
    const index = num(target.dataset.index, -1);
    const item = state.savedViews[index];
    if (item?.route) await navigateWithPatch({ ...ROUTE_DEFAULTS, ...item.route }, true);
    return;
  }
  if (action === "delete-saved-view") {
    const index = num(target.dataset.index, -1);
    state.savedViews.splice(index, 1);
    persistSavedViews();
    renderSavedViews();
    return;
  }
  if (action === "open-recent") {
    const kind = target.dataset.kind;
    const id = target.dataset.id || "";
    if (kind === "category") await navigateWithPatch(buildSelectionCategoryRoute(id));
    else if (kind === "keyword") await navigateWithPatch({ view: "keyword", keyword: id, keyword_offset: 0 });
    else if (kind === "product") {
      const [platform, productId] = id.split("::");
      if (platform && productId) await openProductDrawer(platform, productId);
    }
    return;
  }
  if (action === "jump-drawer-category") {
    await navigateWithPatch(buildSelectionCategoryRoute(target.dataset.path || ""));
    return;
  }
  if (action === "set-drawer-context-level") {
    state.drawerContextLevel = target.dataset.level || "";
    renderDrawerContextSection(state.productDetail || {});
    return;
  }
  if (action === "cancel-task") {
    await cancelTaskFromRuns(target.dataset.taskId);
    return;
  }
  if (action === "retry-task") {
    await retryTaskFromRuns(target.dataset.taskId);
    return;
  }
  if (action === "launch-crawler-plan") {
    await launchCrawlerPlanFromConsole(target.dataset.planId);
    return;
  }
  if (action === "pause-crawler-plan") {
    await pauseCrawlerPlanFromConsole(target.dataset.planId);
    return;
  }
  if (action === "resume-crawler-plan") {
    await resumeCrawlerPlanFromConsole(target.dataset.planId);
    return;
  }
  if (action === "open-crawler-keyword-manager") {
    openCrawlerKeywordManager();
    return;
  }
  if (action === "close-crawler-keyword-manager") {
    closeCrawlerKeywordManager();
    return;
  }
  if (action === "set-crawler-keyword-manager-tab") {
    state.crawlerKeywordManagerTab = normalizeCrawlerKeywordManagerTab(target.dataset.tab);
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "toggle-crawler-keyword-control-scope") {
    toggleCrawlerKeywordControlScope(target.dataset.scope);
    renderCrawlerKeywordControls();
    return;
  }
  if (action === "refresh-crawler-keyword-controls") {
    await loadCrawlerKeywordControls();
    return;
  }
  if (action === "clear-crawler-keyword-control-message") {
    setCrawlerKeywordControlMessage("", "neutral");
    return;
  }
  if (action === "add-crawler-keyword-baseline") {
    await submitCrawlerKeywordControlAction("add_baseline");
    if (state.crawlerKeywordManagerTab === "add") state.crawlerKeywordControlDraft = "";
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "remove-crawler-keyword-baseline") {
    await submitCrawlerKeywordControlAction("remove_baseline");
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "add-crawler-keyword-exclusion") {
    await submitCrawlerKeywordControlAction("disable_keywords");
    if (state.crawlerKeywordManagerTab === "add") state.crawlerKeywordControlDraft = "";
    state.crawlerKeywordManagerTab = "disabled";
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "remove-crawler-keyword-exclusion") {
    await submitCrawlerKeywordControlAction("remove_exclusion");
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "add-crawler-keyword-root-rule") {
    await addCrawlerKeywordRootRules();
    return;
  }
  if (action === "toggle-crawler-keyword-manager-selection") {
    toggleCrawlerKeywordManagerSelection(target.dataset.tab, target.dataset.key, Boolean(target.checked));
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "select-all-crawler-keyword-manager-items") {
    const meta = getCrawlerKeywordManagerTabData(target.dataset.tab);
    setCrawlerKeywordManagerSelection(meta.tab, meta.filteredItems.map((item) => getCrawlerKeywordManagerItemKey(item, meta.tab)));
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "clear-crawler-keyword-manager-selection") {
    clearCrawlerKeywordManagerSelection(target.dataset.tab || state.crawlerKeywordManagerTab);
    renderCrawlerKeywordManager();
    return;
  }
  if (action === "batch-disable-crawler-keywords") {
    await batchDisableCrawlerKeywords();
    return;
  }
  if (action === "batch-restore-crawler-keywords") {
    await batchRestoreCrawlerKeywords();
    return;
  }
  if (action === "disable-crawler-keyword-item") {
    await disableCrawlerKeywordItem(target.dataset.keyword || "");
    return;
  }
  if (action === "restore-crawler-keyword-item") {
    await restoreCrawlerKeywordItem(
      target.dataset.keyword || "",
      String(target.dataset.blockedSources || "").split(/[\s,]+/).filter(Boolean),
      target.dataset.matchMode || "exact",
      target.dataset.ruleTab || state.crawlerKeywordManagerTab,
    );
    return;
  }
  if (action === "remove-crawler-keyword-baseline-item") {
    await submitCrawlerKeywordControlAction("remove_baseline", {
      keywords: [target.dataset.keyword || ""],
      source_scopes: String(target.dataset.sourceScope || "baseline").split(/[\s,]+/).filter(Boolean),
    });
    return;
  }
  if (action === "remove-crawler-keyword-exclusion-item") {
    await submitCrawlerKeywordControlAction("remove_exclusion", {
      keywords: [target.dataset.keyword || ""],
      rule_id: target.dataset.ruleId || "",
      blocked_sources: String(target.dataset.blockedSources || "").split(/[\s,]+/).filter(Boolean),
    });
    return;
  }
  if (action === "add-crawler-category-subcategory-override") {
    addCrawlerCategorySubcategoryOverrideDraft();
    return;
  }
  if (action === "remove-crawler-category-subcategory-override") {
    removeCrawlerCategorySubcategoryOverrideDraft(target.dataset.configId || "");
    setCrawlerCategoryControlMessage(`Removed ${target.dataset.configId || "-"}.`, "neutral");
    renderCrawlerCategoryControls();
    return;
  }
  if (action === "clear-crawler-category-subcategory-override") {
    clearCrawlerCategorySubcategoryOverrides();
    return;
  }
}

async function refreshRunsTaskCenterIfVisible() { return runsModule.refreshRunsTaskCenterIfVisible(); }

async function createTaskFromRunsForm() { return runsModule.createTaskFromRunsForm(); }

async function cancelTaskFromRuns(taskId) { return runsModule.cancelTaskFromRuns(taskId); }

async function retryTaskFromRuns(taskId) { return runsModule.retryTaskFromRuns(taskId); }

function bindStaticEvents() {
  const bind = (nodeId, eventName, handler) => {
    const node = el(nodeId);
    if (!node) return null;
    node.addEventListener(eventName, handler);
    return node;
  };
  document.addEventListener("click", (event) => {
    handleActionClick(event).catch((error) => {
      console.error("action handler failed", error);
    });
  });

  if (el("beta-role")) {
    el("beta-role").addEventListener("change", (event) => {
      setUserRole(event.target.value, false);
      runRoute().catch((error) => console.error("role switch failed", error));
    });
  }

  if (el("keyword-pool-search")) {
    el("keyword-pool-search").addEventListener("input", (event) => {
      state.keywordPoolQuery = event.target.value || "";
      const subnavSearch = el("keyword-subnav-search");
      if (subnavSearch) subnavSearch.value = state.keywordPoolQuery;
      renderKeywordPoolTree();
      renderKeywordPoolBrief();
    });
  }

  if (el("category-selector-search")) {
    el("category-selector-search").addEventListener("input", (event) => {
      state.treeQuery = event.target.value || "";
      renderCategoryTree();
    });
  }
  if (el("crawler-keyword-manager-search")) {
    el("crawler-keyword-manager-search").addEventListener("input", (event) => {
      state.crawlerKeywordManagerQuery = String(event.target.value || "");
      renderCrawlerKeywordManager();
    });
  }
  if (el("crawler-keyword-manager-source-filter")) {
    el("crawler-keyword-manager-source-filter").addEventListener("change", (event) => {
      state.crawlerKeywordManagerSourceFilter = String(event.target.value || "").trim().toLowerCase();
      renderCrawlerKeywordManager();
    });
  }
  if (el("crawler-keyword-manager-tracking-filter")) {
    el("crawler-keyword-manager-tracking-filter").addEventListener("change", (event) => {
      state.crawlerKeywordManagerTrackingFilter = String(event.target.value || "").trim().toLowerCase();
      renderCrawlerKeywordManager();
    });
  }
  document.addEventListener("input", (event) => {
    if (event.target?.id === "crawler-keyword-control-input") {
      state.crawlerKeywordControlDraft = String(event.target.value || "");
      syncCrawlerKeywordInputPreview();
    }
  });
  document.addEventListener("change", (event) => {
    if (event.target?.id === "crawler-keyword-root-match-mode") {
      const nextMode = String(event.target.value || "").trim().toLowerCase();
      state.crawlerKeywordManagerRootMatchMode = ["exact", "contains"].includes(nextMode) ? nextMode : "contains";
      renderCrawlerKeywordManagerSelectionSummary(getCrawlerKeywordManagerTabData("add"));
    }
  });

  bind("save-view", "click", saveCurrentView);
  bind("export-slice", "click", () => window.open(buildExportUrl(), "_blank"));

  if (el("crawler-plan-type")) {
    el("crawler-plan-type").addEventListener("change", () => {
      syncCrawlerPlanFormVisibility();
    });
  }
  if (el("crawler-schedule-kind")) {
    el("crawler-schedule-kind").addEventListener("change", () => {
      syncCrawlerPlanFormVisibility();
    });
  }
  if (el("crawler-plan-launch-now")) {
    el("crawler-plan-launch-now").addEventListener("click", () => {
      submitCrawlerPlan(true).catch((error) => {
        console.error("crawler launch failed", error);
        setCrawlerPlanMessage(`Crawler plan failed: ${error.message}`, "error");
        setCrawlerPlanBusy(false);
      });
    });
  }
  if (el("crawler-plan-form")) {
    el("crawler-plan-form").addEventListener("submit", (event) => {
      event.preventDefault();
      submitCrawlerPlan(false).catch((error) => {
        console.error("crawler plan create failed", error);
        setCrawlerPlanMessage(`Crawler plan failed: ${error.message}`, "error");
        setCrawlerPlanBusy(false);
      });
    });
  }
  if (el("crawler-monitor-config")) {
    el("crawler-monitor-config").addEventListener("change", () => {
      clearAllCrawlerKeywordManagerSelections();
      loadCrawlerKeywordControls().catch((error) => console.error("crawler keyword controls refresh failed", error));
      renderCrawlerKeywordControls();
    });
  }
  if (el("crawler-category-selected")) {
    el("crawler-category-selected").addEventListener("input", () => {
      refreshCrawlerCategorySubcategorySelectors();
      renderCrawlerCategoryControls();
    });
  }
  if (el("crawler-category-target-subcategory")) {
    el("crawler-category-target-subcategory").addEventListener("change", () => {
      renderCrawlerCategoryControls();
    });
  }
  if (el("crawler-category-subcategory-picker")) {
    el("crawler-category-subcategory-picker").addEventListener("change", () => {
      renderCrawlerCategoryControls();
    });
  }
  if (el("crawler-category-subcategory-depth")) {
    el("crawler-category-subcategory-depth").addEventListener("input", () => {
      renderCrawlerCategoryControls();
    });
  }
  bind("task-form-reset", "click", () => {
    resetTaskFormFields();
  });
  bind("task-create-form", "submit", (event) => {
    event.preventDefault();
    createTaskFromRunsForm().catch((error) => {
      console.error("task create failed", error);
      setTaskFormMessage(`Create failed / 创建失败: ${error.message}`, "error");
      setTaskFormBusy(false);
    });
  });

  bind("compare-clear", "click", () => {
    state.compareTray = { products: [], keywords: [] };
    syncCompareRouteState();
    persistCompareTray();
    renderCompareTray();
    syncRouteToUrl(true);
  });

  if (el("category-selector-clear")) {
    el("category-selector-clear").addEventListener("click", clearPendingCategorySelection);
  }
  if (el("category-selector-confirm")) {
    el("category-selector-confirm").addEventListener("click", () => {
      applyCategorySelectorSelection().catch((error) => console.error("apply category selector failed", error));
    });
  }
  bind("products-prev", "click", () => navigateWithPatch({ products_offset: Math.max(0, num(state.route.products_offset, 0) - PAGE_SIZE) }));
  bind("products-next", "click", () => navigateWithPatch({ products_offset: num(state.route.products_offset, 0) + PAGE_SIZE }));
  bind("keyword-prev", "click", () => navigateWithPatch({ keyword_offset: Math.max(0, num(state.route.keyword_offset, 0) - PAGE_SIZE) }));
  bind("keyword-next", "click", () => navigateWithPatch({ keyword_offset: num(state.route.keyword_offset, 0) + PAGE_SIZE }));
  bind("drawer-close", "click", () => closeProductDrawer());
  if (el("product-image-preview")) {
    el("product-image-preview").addEventListener("pointerenter", () => clearProductImagePreviewTimer());
    el("product-image-preview").addEventListener("pointerleave", () => scheduleProductImagePreviewHide());
  }
  bindProductImagePreviewTargets(document);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideProductImagePreview(true);
      if (state.categorySelectorOpen) {
        closeCategorySelector();
        return;
      }
      if (state.crawlerKeywordManagerOpen) {
        closeCrawlerKeywordManager();
        return;
      }
      if (state.route.focus_platform && state.route.focus_product) closeProductDrawer();
    }
  });

  window.addEventListener("resize", () => {
    Object.values(state.charts).forEach((chart) => chart?.resize());
    hideProductImagePreview(true);
    scheduleShellMetrics();
  });

  window.addEventListener("scroll", () => {
    hideProductImagePreview(true);
  }, true);

  window.addEventListener("popstate", async () => {
    state.route = parseRouteFromUrl();
    syncWorkbenchScopeState();
    document.body.dataset.density = state.route.density;
    if ((state.route.view === "runs" && !canAccessRuns()) || (state.route.view === "crawler" && !canAccessCrawler())) {
      state.route = normalizeRoute({ ...state.route, view: "home" });
      syncRouteToUrl(true);
    }
    await runRoute();
    if (state.route.view === "selection") {
      if (!state.productsPayload) await loadSelectionView();
      scheduleSelectionWorkspaceHydration();
    }
    if (state.route.focus_platform && state.route.focus_product) {
      await openProductDrawer(state.route.focus_platform, state.route.focus_product, true);
    } else {
      closeProductDrawer(true);
    }
  });
}

function getProductSortLabel(sortKey) {
  const labels = {
    bsr_asc: "BSR 优先",
    latest_observed_at: "最近采集",
    price_asc: "价格从低到高",
    price_desc: "价格从高到低",
    review_desc: "评论数优先",
    ad_first: "广告优先",
    signal_count_desc: "信号数量优先",
  };
  return labels[sortKey] || "";
}

function getSignalGroupLabel(group) {
  const labels = {
    sales: "销量信号",
    stock: "库存信号",
    price: "价格信号",
    ranking: "排名信号",
    delivery: "配送时效",
  };
  return labels[group] || group || "其他信号";
}

function groupSignalOptionsForRender(owner) {
  const keyword = (state.productSignalSearch[owner] || "").trim().toLowerCase();
  const groups = new Map();
  asArray(state.productSignalOptions).forEach((item) => {
    const label = String(item?.label || "");
    const example = String(item?.example_text || "");
    const haystack = `${label} ${example}`.toLowerCase();
    if (keyword && !haystack.includes(keyword)) return;
    const group = String(item?.group || "other");
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(item);
  });
  return ["sales", "stock", "price", "ranking", "delivery"]
    .filter((group) => groups.has(group))
    .map((group) => ({
      group,
      label: getSignalGroupLabel(group),
      items: groups.get(group),
    }));
}

function renderDashboardScopeSelectorOptions() {
  const node = el("dashboard-home-scope-current");
  if (!node) return;
  const paths = getHomeScopePaths(state.route);
  node.innerHTML = paths.length
    ? paths
        .slice(0, 4)
        .map((path) => `<span class="filter-chip accent-chip" title="${escapeHtml(path)}">${escapeHtml(path.split(" > ").slice(-1)[0] || path)}</span>`)
        .join("")
    : '<span class="filter-chip accent-chip">全平台大盘</span>';
}

function renderDashboardCategoryTable(items = []) {
  const body = el("dashboard-category-table-body");
  const meta = el("dashboard-category-table-meta");
  if (!body || !meta) return;
  const rows = asArray(items);
  meta.textContent = rows.length
    ? "当前范围内有 " + formatNumber(rows.length) + " 个直属类目"
    : "当前范围内没有可展示的直属类目。";
  body.innerHTML = rows.length
    ? rows.map((item) => `
      <tr>
        <td>
          <button type="button" class="table-link-button" data-action="open-category" data-path="${escapeHtml(item.category_path || "")}">
            ${escapeHtml(item.label || item.category_path || "-")}
          </button>
        </td>
        <td>${formatNumber(item.product_count)}</td>
        <td>${formatPercent(item.express_share_pct)}</td>
        <td>${formatPercent(item.ad_share_pct)}</td>
        <td>${formatPrice(item.avg_price)}</td>
        <td>${formatPercent(item.bsr_coverage_pct)}</td>
        <td>${formatPercent(item.signal_coverage_pct)}</td>
        <td>${escapeHtml(formatDate(item.latest_observed_at))}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="8"><div class="empty-state">当前范围内没有类目表现数据。</div></td></tr>';
}

function bindDynamicProductFilterUi() {
  ["category", "products", "selection", "keyword"].forEach((owner) => {
    const searchNode = el(`${owner}-signal-search`);
    if (searchNode) {
      searchNode.value = state.productSignalSearch[owner] || "";
      searchNode.oninput = (event) => {
        state.productSignalSearch[owner] = event.target.value || "";
        renderFilterForms();
      };
    }
  });
}

function getFilterPresetViewName() {
  return "selection";
}

function formatSignedMetric(value, digits = 0, fallback = "-") {
  if (value === null || value === undefined || value === "") return fallback;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  const formatted = digits > 0 ? numeric.toFixed(digits) : formatNumber(numeric);
  return numeric > 0 ? `+${formatted}` : formatted;
}

function formatSalesEstimate(item) {
  const estimate = num(item?.monthly_sales_estimate, 0);
  if (estimate > 0) return `${formatNumber(estimate)} / 月`;
  const fallback = getSalesSignalSummary(item, 26);
  return fallback || "待估算";
}

function formatInventoryEstimate(item) {
  const estimate = num(item?.inventory_left_estimate, 0);
  if (estimate > 0) return formatNumber(estimate) + " left";
  const sticky = getEffectiveSignalText(item, "stock_signal_text");
  const parsedLeft = String(sticky || "").match(/only\s+(\d+)\s+left/i);
  if (parsedLeft) return `${formatNumber(parsedLeft[1])} left`;
  const bucket = String(item?.inventory_signal_bucket || "").trim();
  if (bucket && bucket.toLowerCase() !== "signaled") return bucket;
  if (sticky) return "Stock signal";
  return sticky || "未识别";
}

function parseScopePathList(rawValue) {
  return String(rawValue || "")
    .split("||")
    .map((item) => item.trim())
    .filter(Boolean);
}

function getHomeScopePaths(routePayload = state.route) {
  return parseScopePathList(routePayload?.home_scope);
}

function getSelectionScopePaths(routePayload = state.route) {
  return getSelectedCategoryPaths(routePayload);
}

function syncWorkbenchScopeState() {
  state.homeScopePaths = getHomeScopePaths(state.route);
  state.selectionScopePaths = getSelectionScopePaths(state.route);
}

function getSelectionPathsFromRoute(routePayload = state.route) {
  if (routePayload?.view === "home") return getHomeScopePaths(routePayload);
  return getSelectionScopePaths(routePayload);
}

function getCurrentPendingCategoryPaths() {
  return asArray(state.pendingCategoryPaths)
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function setPendingCategoryPaths(paths = []) {
  const seen = new Set();
  state.pendingCategoryPaths = asArray(paths)
    .map((item) => String(item || "").trim())
    .filter((item) => item && !seen.has(item) && seen.add(item));
}

function buildSelectionCategoryRoute(path = "") {
  const normalizedPath = String(path || "").trim();
  return {
    view: "selection",
    selected_category_paths: normalizedPath ? encodeSelectedCategoryPaths([normalizedPath]) : "",
    category_path: normalizedPath,
    products_offset: 0,
    category_offset: 0,
    keyword_offset: 0,
    focus_platform: "",
    focus_product: "",
  };
}

function ensureExpandedCategoryAncestors(paths = []) {
  asArray(paths).forEach((path) => {
    const parts = String(path || "").split(" > ").map((item) => item.trim()).filter(Boolean);
    for (let index = 2; index < parts.length; index += 1) {
      state.expandedCategoryPaths.add(parts.slice(0, index).join(" > "));
    }
  });
}

function syncCategorySelectorModeControls() {
  document.querySelectorAll('#category-selector-mode-switch [data-action="set-category-selector-mode"]').forEach((node) => {
    node.classList.toggle("active", (node.dataset.mode || "multi") === state.categorySelectorMode);
  });
}

function setCategorySelectorMode(mode = "multi") {
  state.categorySelectorMode = mode === "single" ? "single" : "multi";
  const currentPaths = getCurrentPendingCategoryPaths();
  if (state.categorySelectorMode === "single" && currentPaths.length > 1) {
    setPendingCategoryPaths(currentPaths.slice(0, 1));
  }
  syncCategorySelectorModeControls();
  syncCategorySelectorCurrent();
  renderCategoryTree();
}

function togglePendingCategoryPath(path = "") {
  const nextPath = String(path || "").trim();
  if (!nextPath) return;
  const currentPaths = getCurrentPendingCategoryPaths();
  if (state.categorySelectorMode === "single") {
    setPendingCategoryPaths([nextPath]);
    ensureExpandedCategoryAncestors([nextPath]);
    syncCategorySelectorCurrent();
    renderCategoryTree();
    return;
  }
  const selected = new Set(currentPaths);
  if (selected.has(nextPath)) selected.delete(nextPath);
  else selected.add(nextPath);
  const nextPaths = Array.from(selected);
  setPendingCategoryPaths(nextPaths);
  ensureExpandedCategoryAncestors(nextPaths);
  syncCategorySelectorCurrent();
  renderCategoryTree();
}

function clearPendingCategorySelection() {
  setPendingCategoryPaths([]);
  syncCategorySelectorCurrent();
  renderCategoryTree();
}

async function applyCategorySelectorSelection() {
  const nextPaths = getCurrentPendingCategoryPaths();
  const context = state.categorySelectorContext === "home" ? "home" : "selection";
  closeCategorySelector();
  if (context === "home") {
    await navigateWithPatch({
      view: "home",
      home_scope: encodeSelectedCategoryPaths(nextPaths),
      focus_platform: "",
      focus_product: "",
    });
    return;
  }
  state.selectionInteractionBusy = true;
  syncSelectionActionButtons();
  try {
    await navigateWithPatch({
      view: "selection",
      selected_category_paths: encodeSelectedCategoryPaths(nextPaths),
      category_path: nextPaths.length === 1 ? nextPaths[0] : "",
      products_offset: 0,
      category_offset: 0,
      keyword_offset: 0,
      focus_platform: "",
      focus_product: "",
    });
  } finally {
    if (!state.selectionLoadPromise) {
      state.selectionInteractionBusy = false;
      syncSelectionActionButtons();
    }
  }
}

async function loadProductFilterMemory() {
  if (isLocalUiMemoryMode()) {
    state.uiFilterMemoryMode = "local";
    setProductFilterMemoryFromPayload({
      presets: parseStoredJson(STORAGE_KEYS.productFilterPresets, []),
      history: parseStoredJson(STORAGE_KEYS.productFilterHistory, []),
    });
    return;
  }
  state.uiFilterMemoryMode = "server";
  const viewName = getFilterPresetViewName();
  const [presetResult, historyResult] = await Promise.all([
    getJsonSafe("/api/ui/filter-presets?view=" + encodeURIComponent(viewName), { items: [] }, "筛选预设"),
    getJsonSafe("/api/ui/filter-history?view=" + encodeURIComponent(viewName), { items: [] }, "筛选历史"),
  ]);
  setProductFilterMemoryFromPayload({
    presets: presetResult.data?.items || [],
    history: historyResult.data?.items || [],
  });
}

async function recordProductFilterHistory(routePayload = buildProductFilterMemoryRoute()) {
  const summary = { label: getProductFilterSummary(routePayload) };
  if (isLocalUiMemoryMode()) {
    const nowIso = new Date().toISOString();
    const nextItem = {
      id: "local:" + btoa(unescape(encodeURIComponent(JSON.stringify(routePayload)))).slice(0, 18),
      route_payload: routePayload,
      summary,
      last_used_at: nowIso,
    };
    state.productFilterHistory = [nextItem, ...state.productFilterHistory.filter((item) => JSON.stringify(item.route_payload || {}) !== JSON.stringify(routePayload))].slice(0, 12);
    persistLocalProductFilterMemory();
    return;
  }
  const result = await postJson("/api/ui/filter-history", {
    view: getFilterPresetViewName(),
    route_payload: routePayload,
    summary,
  });
  state.productFilterHistory = Array.isArray(result?.items) ? result.items : state.productFilterHistory;
}

async function saveCurrentProductFilterPreset(defaultName = "") {
  const presetName = window.prompt("输入筛选名称", defaultName || ("选品筛选 " + new Date().toLocaleDateString("zh-CN")));
  if (!presetName || !presetName.trim()) return;
  const routePayload = buildProductFilterMemoryRoute();
  if (isLocalUiMemoryMode()) {
    const nextItem = {
      id: "local-preset:" + Date.now(),
      preset_name: presetName.trim(),
      route_payload: routePayload,
      is_default: false,
      last_used_at: new Date().toISOString(),
    };
    state.productFilterPresets = [nextItem, ...state.productFilterPresets.filter((item) => item.preset_name !== nextItem.preset_name)].slice(0, 16);
    persistLocalProductFilterMemory();
    renderFilterForms();
    return;
  }
  const result = await postJson("/api/ui/filter-presets", {
    view: getFilterPresetViewName(),
    preset_name: presetName.trim(),
    route_payload: routePayload,
    is_default: false,
  });
  if (result?.item) {
    await loadProductFilterMemory();
    renderFilterForms();
  }
}

async function buildAndApplySavedPreset(presetId, patch = {}) {
  const preset = state.productFilterPresets.find((item) => String(item.id) === String(presetId));
  if (!preset) return;
  if (!isLocalUiMemoryMode()) {
    await postJson("/api/ui/filter-presets/" + encodeURIComponent(presetId), { mark_used: true }, "PATCH");
  }
  const nextRoutePayload = {
    ...buildProductFilterMemoryRoute(preset.route_payload || {}),
    ...patch,
    products_offset: 0,
    category_offset: 0,
    keyword_offset: 0,
  };
  await navigateWithPatch(nextRoutePayload);
  await recordProductFilterHistory(buildProductFilterMemoryRoute(nextRoutePayload));
}

async function applyProductFilterPreset(presetId) {
  await buildAndApplySavedPreset(presetId);
}

function buildProductQueryParams(overrides = {}) {
  const payload = normalizeRoute({ ...state.route, ...overrides });
  const params = new URLSearchParams();
  const selectedPaths = getSelectedCategoryPaths(payload);
  if (payload.q) params.set("q", payload.q);
  if (payload.market) params.set("market", payload.market);
  if (payload.platform) params.set("platform", payload.platform);
  if (payload.source) params.set("source_scope", payload.source);
  if (payload.category_path) params.set("category_path", payload.category_path);
  selectedPaths.forEach((item) => params.append("selected_category_paths", item));
  if (payload.keyword) params.set("keyword", payload.keyword);
  if (payload.delivery) params.set("delivery_type", payload.delivery);
  if (payload.is_ad !== "") params.set("is_ad", payload.is_ad);
  if (payload.tab) params.set("tab", payload.tab);
  if (payload.sort) params.set("sort", payload.sort);
  [
    "bsr_min",
    "bsr_max",
    "review_min",
    "review_max",
    "rating_min",
    "rating_max",
    "price_min",
    "price_max",
    "sales_min",
    "sales_max",
    "inventory_min",
    "inventory_max",
    "review_growth_7d_min",
    "review_growth_14d_min",
    "rating_growth_7d_min",
    "rating_growth_14d_min",
    "signal_text",
    "limit",
    "offset",
  ].forEach((key) => {
    if (payload[key] !== null && payload[key] !== undefined && payload[key] !== "") {
      params.set(key, payload[key]);
    }
  });
  if (payload.signal_tags) params.set("signal_tags", payload.signal_tags);
  if (payload.has_sold_signal !== "") params.set("has_sold_signal", payload.has_sold_signal);
  if (payload.has_stock_signal !== "") params.set("has_stock_signal", payload.has_stock_signal);
  if (payload.has_lowest_price_signal !== "") params.set("has_lowest_price_signal", payload.has_lowest_price_signal);
  return params;
}

async function loadSignalOptions() {
  const result = await getJsonSafe("/api/products/signal-options?" + buildProductQueryParams().toString(), { items: [] }, "信号标签");
  state.productSignalOptions = Array.isArray(result.data?.items) ? result.data.items : [];
}

function saveCurrentView() {
  if (state.route.view === "selection") {
    saveCurrentProductFilterPreset().catch((error) => console.error("save product filter preset failed", error));
    return;
  }
  const defaultName = (WORKSPACE_META[state.route.view]?.title || state.route.view) + " " + new Date().toLocaleDateString("zh-CN");
  const label = window.prompt("输入视图名称 / Enter a name for this view", defaultName);
  if (!label) return;
  const payload = {
    name: label.trim(),
    route: {
      ...state.route,
      focus_platform: "",
      focus_product: "",
      products_offset: 0,
      category_offset: 0,
      keyword_offset: 0,
    },
    touched_at: new Date().toISOString(),
  };
  state.savedViews = [payload, ...state.savedViews.filter((item) => item.name !== payload.name)].slice(0, MAX_SAVED_VIEWS);
  persistSavedViews();
  renderSavedViews();
}

function buildExportUrl() {
  return `/api/export/products.csv?${buildProductQueryParams({ limit: 20000, offset: 0 }).toString()}`;
}

function syncCategorySelectorCurrent() {
  const node = el("category-selector-current");
  if (!node) return;
  const paths = getCurrentPendingCategoryPaths();
  if (!paths.length) {
    node.innerHTML = `
      <div class="selector-selection-summary">
        <div class="detail-caption">当前模式：${state.categorySelectorMode === "single" ? "单选" : "多选"}</div>
        <div class="detail-value">未选择类目</div>
      </div>
    `;
    syncCategorySelectorModeControls();
    return;
  }
  node.innerHTML = `
    <div class="selector-selection-summary">
      <div class="detail-caption">当前模式：${state.categorySelectorMode === "single" ? "单选" : "多选"} · 已选 ${formatNumber(paths.length)} 个类目</div>
    </div>
    <div class="chip-row">
      ${paths.map((path) => `<span class="filter-chip">${escapeHtml(path.split(" > ").slice(-1)[0] || path)}</span>`).join("")}
    </div>
    <div class="detail-caption">确认后将作为首页大盘范围或选品页筛选条件。</div>
  `;
  syncCategorySelectorModeControls();
}

function openCategorySelector(mode = "multi", context = state.route.view) {
  state.categorySelectorOpen = true;
  state.categorySelectorMode = mode === "single" ? "single" : "multi";
  state.categorySelectorContext = context === "home" ? "home" : "selection";
  const scopedPaths = state.categorySelectorContext === "home"
    ? getHomeScopePaths(state.route)
    : getSelectionScopePaths(state.route);
  setPendingCategoryPaths(scopedPaths);
  ensureExpandedCategoryAncestors(scopedPaths);
  state.searchExpandedCategoryPaths = new Set();
  const modal = el("category-selector-modal");
  if (modal) modal.hidden = false;
  syncWorkbenchOverlayState();
  const search = el("category-selector-search");
  if (search) {
    search.value = state.treeQuery || "";
    window.setTimeout(() => search.focus(), 0);
  }
  syncCategorySelectorModeControls();
  syncCategorySelectorCurrent();
  renderCategoryTree();
  scheduleShellMetrics();
}

function renderCategoryTree() {
  const container = el("category-tree");
  if (!container) return;
  const items = asArray(state.categoryTree?.items);
  const query = state.treeQuery.trim().toLowerCase();
  const pendingPaths = state.categorySelectorOpen
    ? getCurrentPendingCategoryPaths()
    : (state.categorySelectorContext === "home" ? getHomeScopePaths(state.route) : getSelectionScopePaths(state.route));
  const selectedSet = new Set(pendingPaths);
  state.searchExpandedCategoryPaths = new Set();

  const filterNodes = (nodes, ancestors = []) => nodes.reduce((acc, node) => {
    const children = filterNodes(asArray(node.children), [...ancestors, String(node.path || "").trim()]);
    const haystack = `${node.label || ""} ${node.path || ""}`.toLowerCase();
    const matches = !query || haystack.includes(query);
    if (matches && query) {
      ancestors.forEach((item) => state.searchExpandedCategoryPaths.add(item));
    }
    if (matches || children.length) acc.push({ ...node, children });
    return acc;
  }, []);

  const visibleNodes = query ? filterNodes(items) : items;
  const renderNodes = (nodes, depth = 0) => nodes.map((node) => {
    const children = asArray(node.children);
    const hasChildren = children.length > 0;
    const path = String(node.path || "").trim();
    const expanded = hasChildren && (query
      ? state.searchExpandedCategoryPaths.has(path) || state.expandedCategoryPaths.has(path)
      : state.expandedCategoryPaths.has(path));
    const active = selectedSet.has(path);
    const label = node.label || node.path || "-";
    return `
      <div class="tree-node tree-depth-${Math.min(depth, 3)}">
        <div class="tree-row">
          ${hasChildren
            ? `<button class="tree-toggle" type="button" data-action="toggle-tree" data-path="${escapeHtml(path)}">${expanded ? "-" : "+"}</button>`
            : '<button class="tree-toggle static" type="button" disabled>·</button>'}
          <button
            class="tree-link ${active ? "active" : ""}"
            type="button"
            data-action="${state.categorySelectorOpen ? "pick-category" : "open-category"}"
            data-path="${escapeHtml(path)}"
            aria-pressed="${active ? "true" : "false"}"
          >
            <div class="tree-title-row">
              <span>${escapeHtml(label)}</span>
              ${active ? `<span class="filter-chip accent">${state.categorySelectorMode === "single" ? "已选中" : "已加入"}</span>` : ""}
            </div>
            <div class="tree-meta">${formatNumber(node.descendant_product_count)} products | ${formatNumber(node.child_count)} children</div>
          </button>
        </div>
        ${hasChildren && expanded ? `<div class="tree-children">${renderNodes(children, depth + 1)}</div>` : ""}
      </div>
    `;
  }).join("");

  container.innerHTML = visibleNodes.length
    ? renderNodes(visibleNodes)
    : '<div class="empty-state">当前搜索下没有匹配的类目节点。</div>';
  syncCategorySelectorCurrent();
}

function buildKeywordToolbarMarkup() {
  const activeKeyword = state.route.keyword || "";
  return `
    <div class="keyword-toolbar-grid">
      <label class="field keyword-query-field">
        <span class="field-label">关键词输入</span>
        <input id="keyword-workspace-input" type="search" placeholder="输入 seed keyword 或选择 root seed" />
      </label>
      <label class="field">
        <span class="field-label">市场</span>
        <select id="keyword-market">
          <option value="">全部市场</option>
          <option value="ksa">KSA / Saudi</option>
          <option value="uae">UAE / Emirates</option>
        </select>
      </label>
      <label class="field"><span class="field-label">平台</span><select id="keyword-platform"><option value="">全部平台</option><option value="noon">Noon</option><option value="amazon">Amazon</option></select></label>
      <label class="field"><span class="field-label">来源</span><select id="keyword-source"><option value="">全部来源</option><option value="category">类目</option><option value="keyword">关键词</option><option value="both">双来源</option></select></label>
      <div class="button-row toolbar-action-row">
        <button type="button" class="ghost-button" id="save-view-keyword" data-action="save-view">保存视图</button>
        <button type="button" class="ghost-button" data-action="toggle-keyword-secondary">${state.keywordSecondaryOpen ? "收起洞察模块" : "展开洞察模块"}</button>
        <button type="button" class="ghost-button" data-action="export-slice">导出</button>
        <button type="button" class="primary-button" data-action="apply-keyword-query">应用</button>
      </div>
    </div>
    <div class="keyword-toolbar-meta keyword-toolbar-meta-compact">
      <div class="filter-summary">
        <span class="filter-chip accent-chip">${escapeHtml(activeKeyword || "当前未选择关键词")}</span>
        <span class="filter-chip">市场 ${escapeHtml(getMarketLabel(state.route.market))}</span>
        <span class="filter-chip">平台 ${escapeHtml(state.route.platform ? state.route.platform.toUpperCase() : "ALL")}</span>
      </div>
      <div class="keyword-toolbar-nav">
        <button type="button" class="ghost-button mini-button" data-action="jump-section" data-target="keyword-answer-panel">机会研究</button>
        <button type="button" class="ghost-button mini-button" data-action="set-keyword-mode" data-mode="pool">关键词池</button>
        <button type="button" class="ghost-button mini-button" data-action="jump-section" data-target="keyword-products-panel">命中商品</button>
      </div>
    </div>
  `;
}

function prepareWorkspaceDom() {
  const shell = el("workspace-shell");
  if (shell) shell.dataset.prepared = "true";
  const inlineContext = el("workspace-context-inline");
  if (inlineContext && !inlineContext.textContent.trim()) {
    inlineContext.textContent = "正在同步当前工作区上下文";
  }
  document.querySelectorAll(".workspace").forEach((node) => {
    node.classList.toggle("active", node.id === `view-${state.route?.view || "home"}`);
  });
  const drawerActions = el("drawer-primary-actions");
  if (drawerActions && !el("drawer-hero")) {
    const hero = document.createElement("div");
    hero.id = "drawer-hero";
    hero.className = "drawer-hero";
    drawerActions.insertAdjacentElement("afterend", hero);
  }
  const keywordPanel = el("drawer-panel-keywords");
  if (keywordPanel) {
    const chartNode = el("drawer-keyword-chart") || document.createElement("div");
    chartNode.id = "drawer-keyword-chart";
    chartNode.className = "drawer-chart";
    const tableNode = el("drawer-keyword-ranking-table") || document.createElement("div");
    tableNode.id = "drawer-keyword-ranking-table";
    tableNode.className = "drawer-ranking-grid";
    keywordPanel.innerHTML = `
      <div class="drawer-section-head"><h3>关键词排名趋势</h3></div>
      <div id="drawer-keyword-chart" class="drawer-chart"></div>
      <div id="drawer-keyword-ranking-table" class="drawer-ranking-grid"></div>
    `;
    void chartNode;
    void tableNode;
  }
  const signalsPanel = el("drawer-panel-signals");
  if (signalsPanel) {
    const heading = signalsPanel.querySelector("h3");
    if (heading) heading.textContent = "公开信号时间线";
  }
  const sourcesPanel = el("drawer-panel-sources");
  if (sourcesPanel) {
    const heading = sourcesPanel.querySelector("h3");
    if (heading) heading.textContent = "类目上下文";
  }
}

async function bootstrap() {
  prepareWorkspaceDom();
  loadWorkbenchMemory();
  await loadUiSession();
  syncEnvironmentBadges();
  await loadProductFilterMemory();
  await loadFavoriteProducts({ silent: true });
  state.route = parseRouteFromUrl();
  syncWorkbenchScopeState();
  setUserRole(state.userRole);
  if ((state.route.view === "runs" && !canAccessRuns()) || (state.route.view === "crawler" && !canAccessCrawler())) {
    state.route = normalizeRoute({ ...state.route, view: "home" });
  }
  document.body.dataset.density = state.route.density;
  syncCompareRouteState();
  renderFilterForms();
  renderSavedViews();
  renderRecentContext();
  renderCompareTray();
  renderProductDrawer();
  resetCrawlerPlanForm();
  resetTaskFormFields();
  bindStaticEvents();
  await runRoute();
  if (state.route.view === "selection") {
    if (!state.productsPayload) await loadSelectionView();
    scheduleSelectionWorkspaceHydration();
  }
  scheduleShellMetrics();
  if (document.fonts?.ready) {
    document.fonts.ready.then(() => scheduleShellMetrics()).catch(() => {});
  }
  if (state.route.focus_platform && state.route.focus_product) {
    await openProductDrawer(state.route.focus_platform, state.route.focus_product, true);
  }
}


bootstrap().catch((error) => {
  console.error("bootstrap failed", error);
});












