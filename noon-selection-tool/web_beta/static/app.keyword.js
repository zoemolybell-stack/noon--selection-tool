(() => {
  function createKeywordModule(deps) {
    const {
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
    } = deps;

    function renderKeywordPoolTree() {
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
      const rootMarkup = Object.entries(hierarchy)
        .map(([opportunityType, priorityGroups]) => `
          <div class="keyword-pool-group">
            <div class="subsection-title">${escapeHtml(getOpportunityTypeLabel(opportunityType))}</div>
            ${Object.entries(priorityGroups).map(([priorityBand, seedGroups]) => `
              <div class="keyword-pool-priority">
                <div class="detail-caption">Priority ${escapeHtml(priorityBand || "-")}</div>
                ${Object.entries(seedGroups).map(([seedKeyword, items]) => `
                  <div class="keyword-pool-seed">
                    <div class="detail-caption">${escapeHtml(seedKeyword || "-")}</div>
                    <div class="chip-row">
                      ${items.map((item) => `
                        <button type="button" class="filter-chip ${item.keyword === selectedKeyword ? "accent-chip" : ""}" data-action="open-keyword" data-keyword="${escapeHtml(item.keyword)}">
                          ${escapeHtml(truncate(item.display_keyword || item.keyword, 24))}
                        </button>
                      `).join("")}
                    </div>
                  </div>
                `).join("")}
              </div>
            `).join("")}
          </div>
        `).join("");
      container.innerHTML = rootMarkup || '<div class="empty-state">当前关键词池没有可展示结果。</div>';
    }

    function renderKeywordPoolBrief() {
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
          <div class="detail-label">关键词</div>
          <div class="detail-value">${escapeHtml(selected.display_keyword || selected.keyword)}</div>
        </div>
        <div class="detail-card">
          <div class="detail-label">运行状态</div>
          <div class="detail-value">${escapeHtml(selected.status || "-")} · ${escapeHtml(selected.tracking_mode || "-")}</div>
        </div>
      `;
    }

    function renderKeywordWorkspace() {
      el("view-keyword")?.classList.toggle("pool-mode", state.route.keyword_mode === "pool");
      const shell = el("keyword-secondary-shell");
      if (shell) shell.hidden = !(state.keywordSecondaryOpen || state.route.keyword_mode === "pool");
      if (el("keyword-pool-search")) el("keyword-pool-search").value = state.keywordPoolQuery || "";
      if (el("keyword-subnav-search")) el("keyword-subnav-search").value = state.keywordPoolQuery || "";
      renderKeywordPoolTree();
      renderKeywordPoolBrief();
      renderTabbar("keyword-tabbar");
      const mergedItems = getKeywordPoolItems();
      if (!state.route.keyword && mergedItems.length) {
        window.setTimeout(() => navigateWithPatch({ keyword: mergedItems[0].keyword, keyword_offset: 0 }, true), 0);
      }
    }

    function renderKeywordIntelligence() {
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
    }

    async function loadKeywordCore() {
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
          const [benchmarksResult, historyResult] = await Promise.all([
            getJsonSafe(`/api/keywords/benchmarks?keyword=${encodeURIComponent(state.route.keyword)}`, { summary: {}, top_categories: [], delivery_breakdown: [] }, "关键词基准"),
            getJsonSafe(`/api/keyword/${encodeURIComponent(state.route.keyword)}/history?limit=120`, { items: [] }, "关键词历史"),
          ]);
          if (!isActiveModuleRequest("keyword-core", requestKey)) return;
          state.keywordBenchmarks = benchmarksResult.data;
          state.keywordHistory = historyResult.data;
          if (benchmarksResult.error) state.keywordLoadWarnings.push(benchmarksResult.error);
          if (historyResult.error) state.keywordLoadWarnings.push(historyResult.error);
        } else {
          state.keywordBenchmarks = null;
          state.keywordHistory = null;
        }

        state.keywordSummary = state.keywordSummary || { items: [] };
        state.keywordSummary.selected = selected;
        renderKeywordWorkspace();
        renderKeywordIntelligence();
      } catch (error) {
        if (!isActiveModuleRequest("keyword-core", requestKey)) return;
        throw error;
      } finally {
        clearModuleRequest("keyword-core", requestKey);
      }
    }

    async function loadKeywordSecondary() {
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
            ? getJsonSafe(`/api/keywords/graph?root_keyword=${encodeURIComponent(graphRootKeyword)}&depth=2&limit=80`, { root_keyword: graphRootKeyword, node_count: 0, edge_count: 0, nodes: [], edges: [] }, "关键词图谱")
            : Promise.resolve({ data: { root_keyword: "", node_count: 0, edge_count: 0, nodes: [], edges: [] }, error: "" }),
          state.runsSummary
            ? Promise.resolve({ data: state.runsSummary, error: "" })
            : getJsonSafe("/api/runs/summary", { health: {}, recent_imports: [], keyword_runs: [] }, "关键词运行摘要"),
          state.keywordWarehouseHealth
            ? Promise.resolve({ data: state.keywordWarehouseHealth, error: "" })
            : getJsonSafe("/api/keywords/warehouse-health", { summary: {}, recent_imports: [], recent_runs: [] }, "Keyword warehouse health"),
        ]);
        if (!isActiveModuleRequest("keyword-secondary", requestKey)) return;
        state.keywordOpportunities = opportunitiesResult.data;
        state.keywordQualityIssues = qualityIssuesResult.data;
        state.keywordSeedGroups = seedGroupsResult.data;
        state.keywordGraph = graphResult.data;
        state.runsSummary = runsResult.data;
        state.keywordWarehouseHealth = warehouseHealthResult.data;
        renderKeywordWorkspace();
        renderKeywordIntelligence();
      } finally {
        clearModuleRequest("keyword-secondary", requestKey);
      }
    }

    async function loadKeywordView() {
      await loadKeywordCore();
      clearModuleTimer("keyword-secondary");
      state.moduleTimers["keyword-secondary"] = window.setTimeout(() => {
        loadKeywordSecondary().catch((error) => console.error("keyword secondary load failed", error));
      }, 0);
    }

    async function refreshSignalOptionsForView() {
      if (["selection", "keyword"].includes(state.route.view)) {
        await loadSignalOptions();
      } else {
        state.productSignalOptions = [];
      }
    }

    return Object.freeze({
      renderKeywordPoolTree,
      renderKeywordPoolBrief,
      renderKeywordWorkspace,
      renderKeywordIntelligence,
      loadKeywordCore,
      loadKeywordSecondary,
      loadKeywordView,
      refreshSignalOptionsForView,
    });
  }

  window.WEB_BETA_KEYWORD = Object.freeze({
    createKeywordModule,
  });
})();
