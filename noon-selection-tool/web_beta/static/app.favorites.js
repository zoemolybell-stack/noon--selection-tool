(() => {
  function createFavoritesModule(deps) {
    const {
      state,
      el,
      asArray,
      num,
      escapeHtml,
      formatNumber,
      formatDate,
    } = deps;

    function renderFavoritesSummaryStrip(payload) {
      const cardsNode = el("favorites-summary-cards");
      const metaNode = el("favorites-summary-meta");
      if (!cardsNode || !metaNode) return;
      const items = asArray(payload?.items);
      const summary = payload?.summary || {};
      const salesSignalCount = num(summary.sales_signal_count, 0);
      const stockSignalCount = num(summary.stock_signal_count, 0);
      const platformCount = num(summary.platform_count, 0);
      const lastFavoritedAt = summary.last_favorited_at ? formatDate(summary.last_favorited_at) : "-";

      metaNode.textContent = items.length
        ? "按最近收藏顺序展示。点击商品可继续打开抽屉复查。"
        : "还没有收藏商品。先在选品或关键词研究里把候选商品加入收藏。";

      cardsNode.innerHTML = `
        <div class="selection-summary-ribbon">
          <div class="selection-summary-ribbon-main">
            <span class="filter-chip accent-chip">收藏商品 ${escapeHtml(formatNumber(summary.favorite_count || 0))}</span>
            <span class="filter-chip">覆盖平台 ${escapeHtml(formatNumber(platformCount))}</span>
            <span class="filter-chip">带销量信号 ${escapeHtml(formatNumber(salesSignalCount))}</span>
            <span class="filter-chip">带库存信号 ${escapeHtml(formatNumber(stockSignalCount))}</span>
            <span class="filter-chip">最近收藏 ${escapeHtml(lastFavoritedAt)}</span>
          </div>
          <div class="selection-summary-ribbon-side">
            <span class="filter-chip">独立收藏工作台</span>
          </div>
        </div>
      `;
    }

    return Object.freeze({
      renderFavoritesSummaryStrip,
    });
  }

  window.WEB_BETA_FAVORITES = Object.freeze({
    createFavoritesModule,
  });
})();
