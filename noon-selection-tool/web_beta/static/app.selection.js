(() => {
  function createSelectionModule(deps) {
    const {
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
    } = deps;

    function renderSelectionSummaryStrip(payload, dashboardPayload) {
      const cards = el("selection-summary-cards");
      const meta = el("selection-summary-meta");
      const strip = el("selection-summary-strip");
      if (!cards || !meta) return;
      const items = asArray(payload?.items);
      const selectedPaths = getSelectedCategoryPaths();
      const hitCount = num(payload?.total_count, 0);
      const lowReviewShare = items.length ? (items.filter((item) => num(item.latest_review_count, 0) <= 10).length / items.length) * 100 : 0;
      const adShare = items.length ? (items.filter((item) => item.latest_is_ad).length / items.length) * 100 : 0;
      const signalCoverage = items.length ? (items.filter((item) => num(item.latest_signal_count, 0) > 0).length / items.length) * 100 : 0;
      const marketLabel = getMarketLabel(state.route.market);
      const presetButtons = state.productFilterPresets
        .slice(0, 2)
        .map((item) => `
          <button type="button" class="mini-button subtle" data-action="apply-product-filter-preset" data-preset-id="${escapeHtml(item.id)}">
            ${escapeHtml(truncate(item.name || "载入设置", 18))}
          </button>
        `)
        .join("");

      if (strip) strip.classList.add("selection-summary-compact");
      meta.textContent = selectedPaths.length
        ? `当前已限定 ${selectedPaths.length} 个类目。汇总按当前筛选条件与页内样本计算。`
        : "当前未限定类目，选品工作台正在执行全表扫描；可先找货，再收窄到特定类目。";
      cards.innerHTML = `
        <div class="selection-summary-ribbon">
          <div class="selection-summary-ribbon-main">
            <span class="filter-chip accent-chip">${escapeHtml(marketLabel)}</span>
            <span class="filter-chip">${selectedPaths.length ? `已选类目 ${selectedPaths.length}` : "全部类目"}</span>
            <span class="filter-chip">命中商品 ${formatNumber(hitCount)}</span>
            <span class="filter-chip">低评论占比 ${formatPercent(lowReviewShare)}</span>
            <span class="filter-chip">广告占比 ${formatPercent(adShare)}</span>
            <span class="filter-chip">信号覆盖 ${formatPercent(signalCoverage)}</span>
          </div>
          <div class="selection-summary-ribbon-side">
            <span class="filter-chip">DIY 模式</span>
            ${presetButtons}
          </div>
        </div>
      `;
    }

    function renderSelectionContextPanel(scope = {}) {
      const meta = el("selection-context-meta");
      const body = el("selection-category-table-body");
      if (meta) {
        meta.textContent = scope?.scope_label
          ? "当前类目范围：" + scope.scope_label
          : "选择类目后，这里显示价格带、配送结构和广告占比。";
      }
      renderBarChart(
        "selection-price-band-chart",
        asArray(scope.price_bands).map((item) => item.label || "-"),
        asArray(scope.price_bands).map((item) => num(item.count, 0)),
        "暂无价格带分布"
      );
      renderDonutChart(
        "selection-delivery-mix-chart",
        asArray(scope.delivery_breakdown).map((item) => ({ name: getDeliveryLabel(item.delivery_type), value: num(item.product_count, 0) })),
        undefined,
        "暂无配送结构"
      );
      renderDonutChart(
        "selection-ad-structure-chart",
        asArray(scope.ad_structure).map((item) => ({ name: item.label || "-", value: num(item.count, 0) })),
        ["#d3c3ab", "#2457d6"],
        "暂无广告结构"
      );
      if (!body) return;
      const rows = asArray(scope.child_categories).slice(0, 10);
      body.innerHTML = rows.length
        ? rows.map((item) => `
          <tr>
            <td>${escapeHtml(truncate(item.label || item.path || "-", 28))}</td>
            <td>${escapeHtml(formatNumber(item.product_count || 0))}</td>
            <td>${escapeHtml(formatPercent(item.ad_share_pct))}</td>
            <td>${escapeHtml(formatPercent(item.signal_coverage_pct))}</td>
          </tr>
        `).join("")
        : '<tr><td colspan="4" class="table-subtitle">当前没有更细分类目样本。</td></tr>';
    }

    function renderSelectionFilterSummary() {
      const meta = el("selection-summary-meta");
      const strip = el("selection-summary-strip");
      const selectedPaths = getSelectedCategoryPaths();
      if (strip) strip.classList.add("selection-summary-compact");
      if (meta) {
        meta.textContent = selectedPaths.length
          ? `当前已限定 ${selectedPaths.length} 个类目，选品工作台已按类目范围组织。`
          : "当前未限定类目，可先全表扫描，再收窄到特定类目。";
      }
    }

    function buildSelectionToolbarMarkup() {
      return `
        <div class="selection-command-grid selection-command-grid-primary">
          <label class="field">
            <span class="field-label">搜索</span>
            <input id="selection-q" type="search" placeholder="标题 / 品牌 / 卖家 / product_id" />
          </label>
          <label class="field">
            <span class="field-label">市场</span>
            <select id="selection-market">
              <option value="">KSA / UAE</option>
              <option value="ksa">KSA / Saudi</option>
              <option value="uae">UAE / Emirates</option>
            </select>
          </label>
          <label class="field">
            <span class="field-label">平台</span>
            <select id="selection-platform">
              <option value="">全部平台</option>
              <option value="noon">Noon</option>
              <option value="amazon">Amazon</option>
            </select>
          </label>
          <label class="field">
            <span class="field-label">来源</span>
            <select id="selection-source">
              <option value="">全部来源</option>
              <option value="category">类目</option>
              <option value="keyword">关键词</option>
              <option value="both">双来源</option>
            </select>
          </label>
          <label class="field field-readonly">
            <span class="field-label">我的模式</span>
            <div class="field-static">DIY 设置 / 保存为主</div>
          </label>
          <div class="button-row toolbar-action-row">
            <button type="button" class="ghost-button" data-action="open-category-selector" data-context="selection" data-mode="multi">选择类目</button>
            <button type="button" class="ghost-button" data-action="save-product-filter-preset">保存设置</button>
            <button type="button" class="ghost-button" data-action="focus-selection-memory">载入设置</button>
            <button type="button" class="ghost-button" data-action="export-slice">导出</button>
            <button type="button" class="ghost-button" data-action="reset-product-filters" data-owner="selection">重置</button>
            <button type="button" class="primary-button" data-action="apply-product-filters" data-owner="selection">应用</button>
          </div>
        </div>
        <div class="selection-filter-groups">
          <section class="selection-filter-group selection-filter-group-sales">
            <div class="panel-head compact-head"><div><h3>销售表现</h3></div></div>
            <div class="selection-filter-grid selection-filter-grid-sales">
              <label class="field"><span class="field-label">月销量最小</span><input id="selection-sales-min" type="number" min="0" step="1" placeholder="例如 50" /></label>
              <label class="field"><span class="field-label">月销量最大</span><input id="selection-sales-max" type="number" min="0" step="1" placeholder="例如 2000" /></label>
              <label class="field"><span class="field-label">Review 最小</span><input id="selection-review-min" type="number" min="0" step="1" placeholder="例如 0" /></label>
              <label class="field"><span class="field-label">Review 最大</span><input id="selection-review-max" type="number" min="0" step="1" placeholder="例如 300" /></label>
              <label class="field"><span class="field-label">Rating 最小</span><input id="selection-rating-min" type="number" min="0" max="5" step="0.1" placeholder="例如 4.0" /></label>
              <label class="field"><span class="field-label">Rating 最大</span><input id="selection-rating-max" type="number" min="0" max="5" step="0.1" placeholder="例如 5.0" /></label>
              <label class="field"><span class="field-label">Rating 增长 7D ≥</span><input id="selection-rating-growth-7d-min" type="number" step="0.1" placeholder="例如 0.2" /></label>
              <label class="field"><span class="field-label">Rating 增长 14D ≥</span><input id="selection-rating-growth-14d-min" type="number" step="0.1" placeholder="例如 0.4" /></label>
            </div>
          </section>
          <section class="selection-filter-group selection-filter-group-product">
            <div class="panel-head compact-head"><div><h3>产品信息</h3></div></div>
            <div class="selection-filter-grid selection-filter-grid-product">
              <label class="field"><span class="field-label">价格最小</span><input id="selection-price-min" type="number" min="0" step="0.01" placeholder="例如 30" /></label>
              <label class="field"><span class="field-label">价格最大</span><input id="selection-price-max" type="number" min="0" step="0.01" placeholder="例如 300" /></label>
            </div>
          </section>
          <section class="selection-filter-group selection-filter-group-competition">
            <div class="panel-head compact-head"><div><h3>竞争约束</h3></div></div>
            <div class="selection-filter-grid selection-filter-grid-competition">
              <label class="field"><span class="field-label">BSR 最小</span><input id="selection-bsr-min" type="number" min="1" step="1" placeholder="例如 1" /></label>
              <label class="field"><span class="field-label">BSR 最大</span><input id="selection-bsr-max" type="number" min="1" step="1" placeholder="例如 100" /></label>
              <label class="field"><span class="field-label">库存信号</span>
                <select id="selection-has-stock-signal">
                  <option value="">不限</option>
                  <option value="true">仅显示有库存信号</option>
                </select>
              </label>
            </div>
          </section>
        </div>
      `;
    }

    function syncOwnerFilters() {
      const assign = (id, value) => {
        const node = el(id);
        if (node) node.value = value ?? "";
      };

      assign("selection-q", state.route.q);
      assign("selection-market", state.route.market);
      assign("selection-platform", state.route.platform);
      assign("selection-source", state.route.source);
      assign("selection-price-min", state.route.price_min);
      assign("selection-price-max", state.route.price_max);
      assign("selection-sales-min", state.route.sales_min);
      assign("selection-sales-max", state.route.sales_max);
      assign("selection-review-min", state.route.review_min);
      assign("selection-review-max", state.route.review_max);
      assign("selection-rating-min", state.route.rating_min);
      assign("selection-rating-max", state.route.rating_max);
      assign("selection-rating-growth-7d-min", state.route.rating_growth_7d_min);
      assign("selection-rating-growth-14d-min", state.route.rating_growth_14d_min);
      assign("selection-bsr-min", state.route.bsr_min);
      assign("selection-bsr-max", state.route.bsr_max);
      assign("selection-has-stock-signal", state.route.has_stock_signal);
    }

    function collectProductPatch(owner) {
      const read = (suffix) => el(`${owner}-${suffix}`)?.value?.trim?.() ?? "";
      if (owner === "selection") {
        const selectionPaths = getSelectionScopePaths(state.route);
        return {
          view: "selection",
          q: read("q"),
          market: read("market"),
          platform: read("platform"),
          source: read("source"),
          selected_category_paths: encodeSelectedCategoryPaths(selectionPaths),
          category_path: selectionPaths.length === 1 ? selectionPaths[0] : "",
          tab: "all",
          sort: "sales_desc",
          price_min: read("price-min"),
          price_max: read("price-max"),
          sales_min: read("sales-min"),
          sales_max: read("sales-max"),
          review_min: read("review-min"),
          review_max: read("review-max"),
          rating_min: read("rating-min"),
          rating_max: read("rating-max"),
          rating_growth_7d_min: read("rating-growth-7d-min"),
          rating_growth_14d_min: read("rating-growth-14d-min"),
          bsr_min: read("bsr-min"),
          bsr_max: read("bsr-max"),
          has_stock_signal: read("has-stock-signal"),
          signal_tags: "",
          signal_text: "",
          products_offset: 0,
          category_offset: 0,
          keyword_offset: 0,
          focus_platform: "",
          focus_product: "",
        };
      }
      return {
        q: state.route.q,
        market: read("market") || state.route.market,
        platform: read("platform") || state.route.platform,
        source: read("source") || state.route.source,
        sort: state.route.sort || "sales_desc",
        price_min: state.route.price_min || "",
        price_max: state.route.price_max || "",
        sales_min: state.route.sales_min || "",
        sales_max: state.route.sales_max || "",
        review_min: state.route.review_min || "",
        review_max: state.route.review_max || "",
        rating_min: state.route.rating_min || "",
        rating_max: state.route.rating_max || "",
        rating_growth_7d_min: state.route.rating_growth_7d_min || "",
        rating_growth_14d_min: state.route.rating_growth_14d_min || "",
        bsr_min: state.route.bsr_min || "",
        bsr_max: state.route.bsr_max || "",
        has_stock_signal: state.route.has_stock_signal || "",
        signal_tags: "",
        signal_text: "",
        products_offset: 0,
        category_offset: 0,
        keyword_offset: 0,
      };
    }

    return Object.freeze({
      renderSelectionSummaryStrip,
      renderSelectionContextPanel,
      renderSelectionFilterSummary,
      buildSelectionToolbarMarkup,
      syncOwnerFilters,
      collectProductPatch,
    });
  }

  window.WEB_BETA_SELECTION = Object.freeze({
    createSelectionModule,
  });
})();
