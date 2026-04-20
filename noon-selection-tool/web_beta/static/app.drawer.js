(() => {
  function createDrawerModule(deps) {
    const {
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
    } = deps;

    function syncDrawerLayoutState(isOpen) {
      document.body.classList.remove("drawer-open");
      document.body.classList.toggle("drawer-visible", Boolean(isOpen));
      el("product-drawer")?.setAttribute("aria-hidden", isOpen ? "false" : "true");
    }

    function closeProductDrawer(silent = false) {
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

    function renderDrawerKeyfacts(summary = {}, signals = {}) {
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

    function renderDrawerSignalTimeline(detail = {}) {
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
                ${asArray(item.new_signals).slice(0, 4).map((signal) => `<span class="drawer-signal-pill">${escapeHtml(truncate(signal, 22))}</span>`).join("") || '<span class="table-subtitle">无新增信号</span>'}
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
            return [`<strong>${escapeHtml(day?.observed_at || "-")}</strong>`, `Signal count: ${escapeHtml(formatNumber(day?.signal_count || 0))}`, details || "No new signals"].join("<br />");
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
          lineStyle: { width: rankType === "organic" ? 2.4 : 1.8, type: rankType === "organic" ? "solid" : "dashed" },
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

    function renderProductDrawer() {
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

    return Object.freeze({
      syncDrawerLayoutState,
      closeProductDrawer,
      openProductDrawer,
      renderDrawerKeyfacts,
      renderDrawerSignalTimeline,
      renderDrawerKeywordRankingSection,
      renderDrawerContextSection,
      renderProductDrawer,
    });
  }

  window.WEB_BETA_DRAWER = Object.freeze({
    createDrawerModule,
  });
})();
