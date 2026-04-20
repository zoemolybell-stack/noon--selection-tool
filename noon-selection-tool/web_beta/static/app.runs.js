(() => {
  function createRunsModule(deps) {
    const {
      state,
      el,
      asArray,
      num,
      escapeHtml,
      truncate,
      formatNumber,
      formatDate,
      formatAgeSeconds,
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
    } = deps;

    const SKIP_REASON_LABELS = Object.freeze({
      active_monitor: "主动跳过：同类关键词监控批次仍在运行",
      active_category_crawl: "主动跳过：同类类目抓取仍在运行",
      lock_active: "主动跳过：共享锁仍被占用",
    });

    function buildTaskCreateRequestFromForm() {
      const taskType = el("task-type")?.value || "category_ready_scan";
      const categoryValue = (el("task-category")?.value || "").trim();
      const keywordValue = (el("task-keyword")?.value || "").trim();
      const reasonValue = (el("task-reason")?.value || "").trim();
      const monitorConfigValue = (el("task-monitor-config")?.value || "").trim();
      const productCountValue = num(el("task-product-count")?.value, 50);
      const priorityValue = num(el("task-priority")?.value, 10);
      const persistValue = (el("task-persist")?.value || "true") === "true";
      const exportExcelValue = (el("task-export-excel")?.value || "false") === "true";
      const noonCountValue = num(el("task-noon-count")?.value, 30);
      const amazonCountValue = num(el("task-amazon-count")?.value, 30);

      const payload = {};
      if (taskType === "category_ready_scan") {
        payload.product_count = productCountValue;
        payload.persist = persistValue;
        payload.export_excel = exportExcelValue;
        if (categoryValue) {
          payload.categories = categoryValue.split(",").map((item) => item.trim()).filter(Boolean);
        }
        if (reasonValue) payload.reason = reasonValue;
      } else if (taskType === "category_single") {
        if (!categoryValue) throw new Error("Category is required for category_single");
        payload.category = categoryValue;
        payload.product_count = productCountValue;
        payload.persist = persistValue;
        payload.export_excel = exportExcelValue;
        if (reasonValue) payload.reason = reasonValue;
      } else if (taskType === "keyword_once") {
        if (!keywordValue) throw new Error("Keyword is required for keyword_once");
        payload.keyword = keywordValue;
        payload.noon_count = noonCountValue;
        payload.amazon_count = amazonCountValue;
        payload.persist = persistValue;
        if (reasonValue) payload.reason = reasonValue;
      } else if (taskType === "keyword_monitor") {
        if (!monitorConfigValue) throw new Error("Monitor config is required for keyword_monitor");
        payload.monitor_config = monitorConfigValue;
        payload.noon_count = noonCountValue;
        payload.amazon_count = amazonCountValue;
        payload.persist = persistValue;
        if (reasonValue) payload.reason = reasonValue;
      } else if (taskType === "warehouse_sync") {
        payload.reason = reasonValue || "manual_web_task";
        payload.actor = "web";
      } else {
        throw new Error(`Unsupported task type: ${taskType}`);
      }

      return {
        task_type: taskType,
        created_by: "web",
        priority: priorityValue,
        schedule_type: "manual",
        schedule_expr: "",
        payload,
      };
    }

    function getSkipReasonLabel(skipReason) {
      const normalized = String(skipReason || "").trim().toLowerCase();
      return SKIP_REASON_LABELS[normalized] || normalized || "";
    }

    function extractTerminalReason(item) {
      if (!item || typeof item !== "object") return "";
      const result = item.result && typeof item.result === "object" ? item.result : {};
      const progress = item.progress && typeof item.progress === "object" ? item.progress : {};
      const details = progress.details && typeof progress.details === "object" ? progress.details : {};
      return {
        skipReason: String(
          result.skip_reason
          || item.skip_reason
          || details.skip_reason
          || ""
        ).trim(),
        cancelReason: String(
          result.cancel_reason
          || item.cancel_reason
          || ""
        ).trim(),
        errorText: String(item.last_error || item.error_text || "").trim(),
      };
    }

    function buildOutcomeLine(item) {
      const status = String(item?.status || "").trim().toLowerCase();
      const { skipReason, cancelReason, errorText } = extractTerminalReason(item);
      if (status === "skipped") {
        return getSkipReasonLabel(skipReason) || "主动跳过，未创建新的执行链路。";
      }
      if (status === "failed") {
        return errorText || "执行失败，但没有返回更多错误文本。";
      }
      if (status === "cancelled") {
        return cancelReason || errorText || "任务被人工或系统取消。";
      }
      if (status === "completed") {
        return "执行完成，终态已写入任务历史。";
      }
      if (status === "running") {
        return String(item?.progress?.message || "").trim() || "任务正在运行中。";
      }
      return "";
    }

    function buildTaskCountsSummary(counts) {
      if (!counts || typeof counts !== "object" || !Object.keys(counts).length) {
        return "暂无任务统计";
      }
      return Object.entries(counts)
        .map(([key, value]) => `${getTaskStatusLabel(key)}: ${formatNumber(value)}`)
        .join(" | ");
    }

    function buildSourceBreakdownChips(breakdown) {
      return Object.entries(breakdown && typeof breakdown === "object" ? breakdown : {})
        .map(([key, payload]) => {
          const stateText = String(payload?.state || payload?.status || "").trim().toLowerCase();
          return stateText ? `${key} ${stateText}` : key;
        })
        .filter(Boolean)
        .slice(0, 6);
    }

    function renderTaskCard(item) {
      const canManageTasks = state.userRole === "admin";
      const canCancel = canManageTasks && ["pending", "leased", "running"].includes(item.status);
      const canRetry = canManageTasks && ["failed", "cancelled", "skipped"].includes(item.status);
      const outcomeLine = buildOutcomeLine(item);
      const progress = item.progress && typeof item.progress === "object" ? item.progress : {};
      const progressStage = String(progress.stage || "").trim();
      return `
        <div class="list-card task-card">
          <div class="task-card-head">
            <div class="item-title">${escapeHtml(getTaskTypeLabel(item.task_type))}</div>
            ${renderTaskStatusChip(item.status)}
          </div>
          <div class="ops-meta">
            <div>Task ID: ${formatNumber(item.id)} | Worker: ${escapeHtml(item.worker_type || "-")} | Priority: ${formatNumber(item.priority)}</div>
            <div>Created by: ${escapeHtml(item.created_by || "-")} | Attempts: ${formatNumber(item.attempt_count)}</div>
            <div>Updated: ${escapeHtml(formatDate(item.updated_at))} | Last run: ${escapeHtml(formatDate(item.last_run_at))}</div>
            ${progressStage ? `<div>Progress: ${escapeHtml(progressStage)} | ${escapeHtml(String(progress.message || "").trim() || "-")}</div>` : ""}
            <div>Payload: ${escapeHtml(safeJsonPreview(item.payload || item.payload_json))}</div>
            ${outcomeLine ? `<div class="task-error-line">Outcome: ${escapeHtml(truncate(outcomeLine, 260))}</div>` : ""}
          </div>
          <div class="button-row inline-actions">
            ${canCancel ? `<button type="button" class="ghost-button mini-button" data-action="cancel-task" data-task-id="${item.id}">取消任务</button>` : ""}
            ${canRetry ? `<button type="button" class="ghost-button mini-button" data-action="retry-task" data-task-id="${item.id}">重新入队</button>` : ""}
          </div>
        </div>
      `;
    }

    function renderTaskRunCard(item) {
      const outcomeLine = buildOutcomeLine(item);
      return `
        <div class="list-card task-card">
          <div class="task-card-head">
            <div class="item-title">${escapeHtml(getTaskTypeLabel(item.task_type))} #${formatNumber(item.task_id)}</div>
            ${renderTaskStatusChip(item.status)}
          </div>
          <div class="ops-meta">
            <div>Worker: ${escapeHtml(item.worker_type || "-")} | Attempt: ${formatNumber(item.attempt_number)}</div>
            <div>Started: ${escapeHtml(formatDate(item.started_at))}</div>
            <div>Finished: ${escapeHtml(formatDate(item.finished_at))}</div>
            <div>Command: ${escapeHtml(safeJsonPreview(item.command))}</div>
            ${outcomeLine ? `<div class="task-error-line">Outcome: ${escapeHtml(truncate(outcomeLine, 260))}</div>` : ""}
          </div>
        </div>
      `;
    }

    function renderRunsTaskCenter() {
      const tasksPayload = state.opsTasks || { items: [], counts: {} };
      const taskRunsPayload = state.taskRuns || { items: [] };
      const workersPayload = state.workers || { items: [] };
      const queueItems = asArray(tasksPayload.items);
      const runItems = asArray(taskRunsPayload.items);
      const workerItems = asArray(workersPayload.items);

      const metaNode = el("task-form-meta");
      const existingFormState = metaNode?.dataset.state || "neutral";
      setTaskFormMessage(
        state.taskFormBusy
          ? "正在提交任务，等待 API 返回。"
          : existingFormState === "success" || existingFormState === "error"
            ? metaNode?.textContent
            : "任务会进入调度队列，再由 scheduler / worker 执行。",
        existingFormState,
      );

      if (el("task-queue-summary")) {
        el("task-queue-summary").textContent = buildTaskCountsSummary(tasksPayload.counts || {});
      }

      el("task-queue-list").innerHTML = queueItems.length
        ? queueItems.map(renderTaskCard).join("")
        : '<div class="empty-state">暂无队列任务 / No queued tasks yet.</div>';

      el("task-workers-list").innerHTML = workerItems.length
        ? workerItems.map((item) => `
          <div class="list-card task-card">
            <div class="task-card-head">
              <div class="item-title">${escapeHtml(item.worker_name || "-")}</div>
              ${renderTaskStatusChip(item.status)}
            </div>
            <div class="ops-meta">
              <div>Worker type: ${escapeHtml(item.worker_type || "-")}</div>
              <div>Current task: ${item.current_task_id ? formatNumber(item.current_task_id) : "-"}</div>
              <div>Heartbeat: ${escapeHtml(formatDate(item.heartbeat_at))}</div>
              <div>Details: ${escapeHtml(safeJsonPreview(item.details || item.details_json))}</div>
            </div>
          </div>
        `).join("")
        : '<div class="empty-state">暂无 worker 心跳 / No worker heartbeat yet.</div>';

      el("task-runs-list").innerHTML = runItems.length
        ? runItems.map(renderTaskRunCard).join("")
        : '<div class="empty-state">暂无任务运行记录 / No task runs yet.</div>';
    }

    function renderRuntimeAlertPanel() {
      const summaryNode = el("runtime-alert-summary");
      const listNode = el("runtime-alert-list");
      if (!summaryNode || !listNode) return;

      const runsSummary = state.runsSummary || {};
      const freshness = runsSummary.freshness || {};
      const sharedSync = freshness.shared_sync || {};
      const keywordSummary = state.keywordWarehouseHealth?.summary || {};
      const keywordQuality = runsSummary.keyword_quality || {};
      const watchdogSummary = runsSummary.watchdog || state.watchdogSummary || {};
      const currentAlertSummary = watchdogSummary.current_alert_summary || {};
      const historicalMissingDates = asArray(
        watchdogSummary.historical_report_missing_dates || watchdogSummary.report_missing_dates,
      );
      const watchdogIssues = asArray(watchdogSummary.issues)
        .map((issue) => {
          if (!issue || typeof issue !== "object") return "";
          return [issue.severity, issue.check, issue.message].filter(Boolean).join(": ");
        })
        .filter(Boolean);
      const warningTexts = [
        ...(state.runsLoadWarnings || []),
        ...asArray(freshness.warnings),
        ...watchdogIssues,
      ].map(normalizeFreshnessWarningText).filter(Boolean);

      const operatorQualityState = String(
        keywordQuality.operator_quality_state
        || keywordQuality.latest_terminal_batch_state
        || keywordQuality.latest_terminal_quality_state
        || "unknown",
      ).trim();
      const liveBatchState = String(
        keywordQuality.live_batch_state
        || currentAlertSummary.current_batch_state
        || "idle",
      ).trim();
      const terminalQualityState = String(
        keywordQuality.latest_terminal_batch_state
        || keywordQuality.latest_terminal_quality_state
        || "unknown",
      ).trim();
      const operatorFreshnessState = String(
        freshness.operator_freshness_state
        || freshness.overall_state
        || sharedSync.state
        || sharedSync.freshness_state
        || "unknown",
      ).trim();

      summaryNode.textContent = [
        `Keyword quality ${operatorQualityState}`,
        `live batch ${liveBatchState}`,
        `freshness ${operatorFreshnessState}`,
      ].join(" | ");

      const cards = [
        {
          title: "Keyword Batch Truth",
          meta: operatorQualityState,
          chips: [
            liveBatchState ? `live ${liveBatchState}` : "",
            terminalQualityState ? `terminal ${terminalQualityState}` : "",
            keywordQuality.live_batch_snapshot_id ? `live snapshot ${keywordQuality.live_batch_snapshot_id}` : "",
            keywordQuality.latest_terminal_batch_snapshot_id ? `terminal snapshot ${keywordQuality.latest_terminal_batch_snapshot_id}` : "",
            num(keywordQuality.active_snapshot_count, 0) ? `active ${formatNumber(keywordQuality.active_snapshot_count)}` : "active 0",
            num(keywordQuality.stale_running_snapshot_count, 0) ? `stale running ${formatNumber(keywordQuality.stale_running_snapshot_count)}` : "",
          ].filter(Boolean),
          detail: [
            keywordQuality.quality_status_summary || keywordSummary.quality_status_summary || "",
            asArray(keywordQuality.operator_quality_reasons || keywordQuality.batch_quality_reasons).join(" | "),
            keywordQuality.truth_source ? `truth ${keywordQuality.truth_source}` : "",
          ].filter(Boolean).join(" | ") || "Batch truth is waiting for the next keyword terminal snapshot.",
        },
        {
          title: "Live Batch",
          meta: liveBatchState,
          chips: [
            keywordQuality.live_seed_keyword ? `seed ${keywordQuality.live_seed_keyword}` : "",
            keywordQuality.live_run_status ? `run ${keywordQuality.live_run_status}` : "",
            keywordQuality.live_crawl_status ? `crawl ${keywordQuality.live_crawl_status}` : "",
            ...buildSourceBreakdownChips(keywordQuality.live_quality_source_breakdown),
          ].filter(Boolean),
          detail: [
            keywordQuality.live_started_at ? `Started ${formatDate(keywordQuality.live_started_at)}` : "",
            keywordQuality.live_finished_at ? `Last activity ${formatDate(keywordQuality.live_finished_at)}` : "",
            asArray(keywordQuality.live_quality_reasons).join(" | "),
          ].filter(Boolean).join(" | ") || "No active keyword batch is currently running.",
        },
        {
          title: "Shared Sync / Freshness",
          meta: freshness.operator_freshness_state || sharedSync.state || sharedSync.freshness_state || "unknown",
          chips: [
            freshness.warehouse_visible_freshness ? `warehouse ${freshness.warehouse_visible_freshness}` : "",
            freshness.stage_import_freshness ? `stage ${freshness.stage_import_freshness}` : "",
            sharedSync.state ? `sync ${sharedSync.state}` : sharedSync.freshness_state ? `sync ${sharedSync.freshness_state}` : "",
            sharedSync.last_completed_at ? `last ${formatDate(sharedSync.last_completed_at)}` : "",
          ].filter(Boolean),
          detail: [
            sharedSync.reason ? `Reason: ${sharedSync.reason}` : "",
            sharedSync.recommended_action ? `Action: ${sharedSync.recommended_action}` : "",
            freshness.truth_source ? `truth ${freshness.truth_source}` : "",
          ].filter(Boolean).join(" | ") || "Warehouse-visible freshness looks healthy.",
        },
        {
          title: "Watchdog",
          meta: watchdogSummary.state || watchdogSummary.status || "pending",
          chips: [
            watchdogSummary.generated_at ? `run ${formatDate(watchdogSummary.generated_at)}` : "",
            num(watchdogSummary.issue_count, 0) > 0 ? `issues ${formatNumber(watchdogSummary.issue_count)}` : "issues 0",
            historicalMissingDates.length ? `historical backfill ${historicalMissingDates.length}` : "",
          ].filter(Boolean),
          detail: [
            historicalMissingDates.length ? `Historical backfill: ${historicalMissingDates.slice(0, 5).join(", ")}` : "",
            currentAlertSummary.needs_attention ? "Current runtime needs operator attention." : "No current runtime fault is flagged.",
            watchdogIssues.join(" | "),
          ].filter(Boolean).join(" | ") || "Watchdog did not report active issues.",
        },
      ];

      if (warningTexts.length) {
        cards.push({
          title: "Active Alerts",
          meta: `${formatNumber(warningTexts.length)} items`,
          chips: [],
          detail: warningTexts.join(" | "),
        });
      }

      listNode.innerHTML = cards.map((item) => `
        <div class="runtime-alert-card">
          <div class="alert-title">${escapeHtml(item.title)}</div>
          <div class="alert-meta">${escapeHtml(item.meta || "-")}</div>
          <div class="alert-chip-row">
            ${asArray(item.chips).map((chip) => chip ? `<span class="filter-chip accent-chip">${escapeHtml(chip)}</span>` : "").join("")}
          </div>
          <div class="list-meta">${escapeHtml(item.detail || "-")}</div>
        </div>
      `).join("");
    }

    async function loadRunsTaskCenter() {
      const requestKey = nextModuleRequestKey("runs-task-center");
      setListLoading("task-queue-list", "正在加载任务队列...");
      setListLoading("task-workers-list", "正在加载 worker 状态...");
      setListLoading("task-runs-list", "正在加载任务运行记录...");
      try {
        const [tasksResult, taskRunsResult, workersResult] = await Promise.all([
          getJsonSafe("/api/tasks?limit=100", { counts: {}, items: [] }, "Task queue"),
          getJsonSafe("/api/task-runs?limit=40", { items: [] }, "Task runs"),
          getJsonSafe("/api/workers", { items: [] }, "Workers"),
        ]);
        if (!isActiveModuleRequest("runs-task-center", requestKey)) return;
        state.runsLoadWarnings = [
          ...(state.runsLoadWarnings || []),
          tasksResult.error,
          taskRunsResult.error,
          workersResult.error,
        ].filter(Boolean);
        state.opsTasks = tasksResult.data;
        state.taskRuns = taskRunsResult.data;
        state.workers = workersResult.data;
        if (state.runsSummary) renderRunsWorkspace();
      } finally {
        clearModuleRequest("runs-task-center", requestKey);
      }
    }

    async function loadRunsCoreEnhanced() {
      const requestKey = nextModuleRequestKey("runs-core-enhanced");
      setListLoading("runs-health", "正在加载运行真相...");
      setListLoading("runs-keyword", "正在加载关键词批次...");
      setListLoading("runs-imports", "正在加载最近导入...");
      try {
        const [runsSummaryResult, keywordWarehouseHealthResult, tasksResult, taskRunsResult, workersResult] = await Promise.all([
          getJsonSafe("/api/runs/summary", { health: {}, recent_imports: [], keyword_runs: [] }, "运行摘要"),
          getJsonSafe("/api/keywords/warehouse-health", { summary: {}, recent_imports: [], recent_runs: [] }, "关键词仓库健康"),
          getJsonSafe("/api/tasks?limit=100", { counts: {}, items: [] }, "Task queue"),
          getJsonSafe("/api/task-runs?limit=40", { items: [] }, "Task runs"),
          getJsonSafe("/api/workers", { items: [] }, "Workers"),
        ]);
        if (!isActiveModuleRequest("runs-core-enhanced", requestKey)) return;
        state.runsLoadWarnings = [
          runsSummaryResult.error,
          keywordWarehouseHealthResult.error,
          tasksResult.error,
          taskRunsResult.error,
          workersResult.error,
        ].filter(Boolean);
        state.runsSummary = runsSummaryResult.data;
        state.watchdogSummary = runsSummaryResult.data?.watchdog || null;
        state.keywordWarehouseHealth = keywordWarehouseHealthResult.data;
        state.opsTasks = tasksResult.data;
        state.taskRuns = taskRunsResult.data;
        state.workers = workersResult.data;
        renderRunsWorkspace();
      } finally {
        clearModuleRequest("runs-core-enhanced", requestKey);
      }
    }

    async function loadRunsView() {
      await loadRunsCoreEnhanced();
    }

    function renderRunsWorkspace() {
      const payload = state.runsSummary;
      renderRuntimeAlertPanel();
      if (!payload) return;

      const keywordHealth = state.keywordWarehouseHealth || {};
      const freshness = payload.freshness || {};
      const warehouseFreshness = freshness.warehouse || {};
      const categoryFreshness = freshness.category_stage || {};
      const keywordFreshness = freshness.keyword_stage || {};
      const sharedSync = freshness.shared_sync || {};
      const keywordQuality = payload.keyword_quality || {};
      const statusBreakdown = keywordHealth?.summary?.status_breakdown || {};
      const statusText = Object.keys(statusBreakdown).length
        ? Object.entries(statusBreakdown).map(([key, value]) => `${key}:${formatNumber(value)}`).join(" | ")
        : "-";
      const warehouseHealthState = keywordHealth?.summary?.warehouse_health_state || "-";
      const lagHint = keywordHealth?.summary?.lag_hint || "-";
      const syncWarnings = asArray(freshness.warnings).map(normalizeFreshnessWarningText).filter(Boolean);
      const allWarnings = [...new Set([...(state.runsLoadWarnings || []), ...syncWarnings])];
      const warningBlockHtml = renderWarningBlock("部分模块未完成加载", allWarnings);
      const warningCard = allWarnings.length
        ? `<div class="list-card"><div class="item-title">Partial Load / 部分加载失败</div><div class="list-meta">${escapeHtml(allWarnings.join(" | "))}</div></div>`
        : "";

      const opsCards = [
        renderOpsCard(
          "运行真相 / Runtime Truth",
          freshness.visibility_scope_note || "Warehouse-visible state only.",
          [
            renderHealthChip("keyword", keywordQuality.operator_quality_state || "-"),
            renderHealthChip("live", keywordQuality.live_batch_state || "-"),
            renderHealthChip("freshness", freshness.operator_freshness_state || freshness.overall_state || "-"),
          ],
          [
            `Latest terminal snapshot: ${keywordQuality.latest_terminal_batch_snapshot_id || "-"}`,
            `Truth source: ${keywordQuality.truth_source || "-"}`,
            `Freshness source: ${freshness.truth_source || "-"}`,
          ],
        ),
        renderOpsCard(
          "关键词批次 / Keyword Batch",
          keywordQuality.quality_status_summary || keywordHealth?.summary?.quality_status_summary || "-",
          [
            renderHealthChip("operator", keywordQuality.operator_quality_state || "-"),
            renderHealthChip("live", keywordQuality.live_batch_state || "-"),
            renderHealthChip("terminal", keywordQuality.latest_terminal_batch_state || "-"),
          ],
          [
            `Live snapshot: ${keywordQuality.live_batch_snapshot_id || "-"}`,
            `Terminal snapshot: ${keywordQuality.latest_terminal_batch_snapshot_id || "-"}`,
            `Active snapshots: ${formatNumber(keywordQuality.active_snapshot_count)}`,
            `Stale running snapshots: ${formatNumber(keywordQuality.stale_running_snapshot_count)}`,
          ],
        ),
        renderOpsCard(
          "共享同步 / Shared Sync",
          sharedSync.status_summary || "-",
          [
            renderHealthChip("sync", sharedSync.state || sharedSync.freshness_state || "-"),
            renderHealthChip("warehouse", freshness.warehouse_visible_freshness || "-"),
            renderHealthChip("stage", freshness.stage_import_freshness || "-"),
          ],
          [
            `Updated: ${formatDate(sharedSync.updated_at)} | ${formatAgeSeconds(sharedSync.updated_age_seconds)}`,
            `Reason: ${sharedSync.reason || "-"}`,
            `Action: ${sharedSync.recommended_action || "-"}`,
          ],
        ),
        renderOpsCard(
          "统一仓库 / Warehouse",
          warehouseFreshness.status_summary || "-",
          [
            renderHealthChip("import", warehouseFreshness.import_freshness_state || "-"),
            renderHealthChip("kw-health", warehouseHealthState),
            renderHealthChip("kw-import", keywordHealth?.summary?.import_freshness_state),
            renderHealthChip("kw-runs", keywordHealth?.summary?.run_activity_state),
          ],
          [
            `Imported: ${formatDate(warehouseFreshness.last_imported_at)} | ${formatAgeSeconds(warehouseFreshness.last_import_age_seconds)}`,
            `Keyword status: ${statusText}`,
            `Hint: ${lagHint}`,
          ],
        ),
        renderOpsCard(
          "阶段导入 / Stage Imports",
          `Category ${categoryFreshness.import_freshness_state || "-"} | Keyword ${keywordFreshness.import_freshness_state || "-"}`,
          [
            renderHealthChip("category", categoryFreshness.diagnosis_state || "-"),
            renderHealthChip("keyword", keywordFreshness.diagnosis_state || "-"),
          ],
          [
            `Category observed: ${formatDate(categoryFreshness.last_observed_at)} | ${formatAgeSeconds(categoryFreshness.last_observed_age_seconds)}`,
            `Keyword observed: ${formatDate(keywordFreshness.last_observed_at)} | ${formatAgeSeconds(keywordFreshness.last_observed_age_seconds)}`,
            `Keyword action: ${keywordFreshness.recommended_action || "-"}`,
          ],
        ),
      ];

      el("runs-health").innerHTML = `
        ${warningBlockHtml || warningCard}
        ${opsCards.join("")}
        <div class="list-card">
          <div class="item-title">基础计数 / Base Counters</div>
          <div class="ops-meta">
            <div>Category stage DBs: ${formatNumber(payload.health?.category_source_db_count)}</div>
            <div>Keyword stage DBs: ${formatNumber(payload.health?.keyword_source_db_count)}</div>
            <div>Last imported: ${escapeHtml(formatDate(payload.health?.last_imported_at))}</div>
            <div>Incomplete keyword runs: ${formatNumber(payload.health?.incomplete_keyword_run_count)}</div>
            <div>Keyword catalog: ${formatNumber(keywordHealth?.summary?.keyword_catalog_count)}</div>
            <div>Metric snapshots: ${formatNumber(keywordHealth?.summary?.keyword_metric_snapshot_count)}</div>
          </div>
        </div>
      `;

      el("runs-keyword").innerHTML = asArray(payload.keyword_runs).length
        ? asArray(payload.keyword_runs).map((item) => `
          <div class="list-card">
            <div class="item-title">${escapeHtml(item.snapshot_id || "-")}</div>
            <div class="list-meta">${escapeHtml(item.run_type || "-")} | ${escapeHtml(item.status || "-")} | quality ${escapeHtml(item.quality_state || "-")}</div>
            <div class="list-meta">${escapeHtml(formatDate(item.started_at))} -> ${escapeHtml(formatDate(item.finished_at))}</div>
            ${asArray(item.quality_reasons).length ? `<div class="list-meta">${escapeHtml(asArray(item.quality_reasons).join(" | "))}</div>` : ""}
          </div>
        `).join("")
        : '<div class="empty-state">暂无关键词运行记录 / No keyword runs yet.</div>';

      el("runs-imports").innerHTML = asArray(payload.recent_imports).length
        ? asArray(payload.recent_imports).map((item) => `
          <div class="list-card">
            <div class="item-title">${escapeHtml(item.source_label || "-")}</div>
            <div class="list-meta">${escapeHtml(item.source_scope || "-")} | ${escapeHtml(formatDate(item.imported_at))}</div>
            <div class="list-meta">Products ${formatNumber(item.source_product_count)} | Obs ${formatNumber(item.source_observation_count)} | Keywords ${formatNumber(item.source_keyword_count)}</div>
          </div>
        `).join("")
        : '<div class="empty-state">暂无导入记录 / No import records yet.</div>';

      renderRunsTaskCenter();
    }

    async function refreshRunsTaskCenterIfVisible() {
      if (state.route.view !== "runs") return;
      await loadRunsTaskCenter();
    }

    async function createTaskFromRunsForm() {
      if (state.userRole !== "admin") {
        setTaskFormMessage("当前账号没有任务控制权限。", "error");
        throw new Error("admin role required");
      }
      const request = buildTaskCreateRequestFromForm();
      setTaskFormBusy(true);
      setTaskFormMessage("正在提交任务...", "neutral");
      try {
        const created = await postJson("/api/tasks", request, "POST");
        resetTaskFormFields();
        setTaskFormMessage(`已创建任务 #${created.id}`, "success");
        await refreshRunsTaskCenterIfVisible();
      } catch (error) {
        setTaskFormMessage(`创建失败: ${error.message}`, "error");
      } finally {
        setTaskFormBusy(false);
      }
    }

    async function cancelTaskFromRuns(taskId) {
      if (state.userRole !== "admin") {
        throw new Error("admin role required");
      }
      const id = num(taskId, 0);
      if (!id) return;
      await postJson(`/api/tasks/${id}/cancel`, {}, "POST");
      await refreshRunsTaskCenterIfVisible();
    }

    async function retryTaskFromRuns(taskId) {
      if (state.userRole !== "admin") {
        throw new Error("admin role required");
      }
      const id = num(taskId, 0);
      if (!id) return;
      await postJson(`/api/tasks/${id}/retry`, {}, "POST");
      await refreshRunsTaskCenterIfVisible();
    }

    return Object.freeze({
      buildTaskCreateRequestFromForm,
      renderRunsTaskCenter,
      renderRunsWorkspace,
      renderRuntimeAlertPanel,
      loadRunsTaskCenter,
      loadRunsCoreEnhanced,
      loadRunsView,
      refreshRunsTaskCenterIfVisible,
      createTaskFromRunsForm,
      cancelTaskFromRuns,
      retryTaskFromRuns,
    });
  }

  window.WEB_BETA_RUNS = Object.freeze({
    createRunsModule,
  });
})();
