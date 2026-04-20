(() => {
  const contract = window.WEB_BETA_CONTRACT;
  if (!contract) {
    throw new Error("WEB_BETA_CONTRACT is missing");
  }

  const {
    ROUTE_DEFAULTS,
    WORKSPACE_META,
  } = contract;

  function asArray(value) {
    if (Array.isArray(value)) return value;
    if (value && typeof value === "object") return [value];
    return [];
  }

  function num(value, fallback = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function normalizeDensity(value) {
    return value === "comfortable" ? "comfortable" : "compact";
  }

  function normalizeTab(value) {
    if (value === "ranked") return "ranked";
    if (value === "unranked") return "unranked";
    return "all";
  }

  function normalizeKeywordMode(value) {
    return value === "pool" ? "pool" : "research";
  }

  function encodeSelectedCategoryPaths(paths = []) {
    return asArray(paths)
      .map((item) => String(item || "").trim())
      .filter(Boolean)
      .join("||");
  }

  function getSelectedCategoryPaths(route = ROUTE_DEFAULTS) {
    const raw = String(route?.selected_category_paths || "").trim();
    const items = raw ? raw.split("||") : [];
    const seen = new Set();
    const normalized = [];

    if (route?.category_path) {
      const legacy = String(route.category_path).trim();
      if (legacy && !seen.has(legacy)) {
        seen.add(legacy);
        normalized.push(legacy);
      }
    }

    items.forEach((item) => {
      const decoded = String(item || "").trim();
      if (!decoded || seen.has(decoded)) return;
      seen.add(decoded);
      normalized.push(decoded);
    });

    return normalized;
  }

  function normalizeView(value) {
    if (value === "category" || value === "products") return "selection";
    return WORKSPACE_META[value] ? value : "home";
  }

  function normalizeRoute(nextRoute = {}) {
    const route = { ...ROUTE_DEFAULTS, ...nextRoute };
    route.view = normalizeView(route.view);
    route.keyword_mode = normalizeKeywordMode(route.keyword_mode);
    route.density = normalizeDensity(route.density);
    route.tab = normalizeTab(route.tab);
    route.selected_category_paths = encodeSelectedCategoryPaths(getSelectedCategoryPaths(route));
    const selectedCategoryPaths = getSelectedCategoryPaths(route);
    if (!route.category_path && selectedCategoryPaths.length === 1) {
      route.category_path = selectedCategoryPaths[0];
    }
    route.products_offset = num(route.products_offset, 0);
    route.category_offset = num(route.category_offset, 0);
    route.keyword_offset = num(route.keyword_offset, 0);
    return route;
  }

  function buildRouteUrl(route) {
    const params = new URLSearchParams();
    Object.entries(route).forEach(([key, value]) => {
      if (value === null || value === undefined || value === "") return;
      if (
        typeof value === "number"
        && value === 0
        && ["products_offset", "category_offset", "keyword_offset"].includes(key)
      ) {
        return;
      }
      params.set(key, String(value));
    });
    const query = params.toString();
    return query ? `${window.location.pathname}?${query}` : window.location.pathname;
  }

  function parseRouteFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const next = { ...ROUTE_DEFAULTS };
    Object.keys(ROUTE_DEFAULTS).forEach((key) => {
      if (params.has(key)) next[key] = params.get(key) ?? ROUTE_DEFAULTS[key];
    });
    return normalizeRoute(next);
  }

  window.WEB_BETA_STATE = Object.freeze({
    normalizeDensity,
    normalizeTab,
    normalizeKeywordMode,
    encodeSelectedCategoryPaths,
    getSelectedCategoryPaths,
    normalizeView,
    normalizeRoute,
    buildRouteUrl,
    parseRouteFromUrl,
  });
})();
