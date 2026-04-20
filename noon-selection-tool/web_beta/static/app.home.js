(() => {
  function createHomeModule(deps) {
    const {
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
    } = deps;

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

    function renderDashboard() {
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

      renderLineChart("dashboard-category-import-chart", asArray(importSeries.days), [{
        name: "Category",
        type: "line",
        smooth: true,
        data: asArray(importSeries.category_products),
        itemStyle: { color: "#8d7a34" },
        areaStyle: { color: "rgba(141,122,52,.12)" },
      }], [{ type: "value" }], "暂无类目导入趋势");

      renderLineChart("dashboard-keyword-import-chart", asArray(importSeries.days), [{
        name: "Keyword",
        type: "line",
        smooth: true,
        data: asArray(importSeries.keyword_products),
        itemStyle: { color: "#215d7a" },
        areaStyle: { color: "rgba(33,93,122,.12)" },
      }], [{ type: "value" }], "暂无关键词导入趋势");

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

      renderBarChart("dashboard-price-band-chart", asArray(scope.price_bands).map((item) => item.label || "-"), asArray(scope.price_bands).map((item) => num(item.count, 0)), "暂无价格带分布");
      renderDonutChart("dashboard-delivery-mix-chart", asArray(scope.delivery_breakdown).map((item) => ({ name: item.label || item.delivery_type || "-", value: num(item.product_count, 0) })), undefined, "暂无配送结构");
      renderDonutChart("dashboard-ad-structure-chart", asArray(scope.ad_structure).map((item) => ({ name: item.label || "-", value: num(item.count, 0) })), ["#d3c3ab", "#2457d6"], "暂无广告结构");
      renderDonutChart("dashboard-signal-coverage-chart", asArray(scope.signal_structure).map((item) => ({ name: item.label || "-", value: num(item.count, 0) })), ["#0f8a5f", "#dd6b20", "#cf9f00"], "暂无信号覆盖");
      renderDashboardCategoryTable(scope.child_categories || []);
    }

    async function loadHomeCore() {
      const requestKey = nextModuleRequestKey("home-core");
      const kpiNode = el("dashboard-kpis");
      if (kpiNode) kpiNode.innerHTML = '<div class="loading-state">正在加载首页总览...</div>';
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
      if (!state.categoryTree) {
        state.categoryTree = await getJson("/api/categories/tree");
      }
      renderDashboard();
    }

    async function loadHomeView() {
      await loadHomeCore();
      clearModuleTimer("home-secondary");
      state.moduleTimers["home-secondary"] = window.setTimeout(() => {
        loadHomeSecondary().catch((error) => console.error("home secondary load failed", error));
      }, 0);
    }

    return Object.freeze({
      renderDashboardScopeSelectorOptions,
      renderDashboardCategoryTable,
      renderDashboard,
      loadHomeCore,
      loadHomeSecondary,
      loadHomeView,
    });
  }

  window.WEB_BETA_HOME = Object.freeze({
    createHomeModule,
  });
})();
