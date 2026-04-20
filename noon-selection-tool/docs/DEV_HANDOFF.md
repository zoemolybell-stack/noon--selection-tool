# DEV HANDOFF

Last Updated: 2026-04-13  
Primary Writer: main window  
Status: active execution handoff

## Purpose

This file is the execution boundary document.

Use it for:
- active ownership
- current boundaries
- release blocking gates
- runtime safety rules

For strategy, use:
- [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)

For chronology, use:
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)

## Current Operating Mode

Current mode:

`single control plane + remote category node steady state`

Rules:
- local beta remains the only development environment
- NAS only accepts explicit release bundles
- NAS stable is now running on retained-data Postgres
- current NAS stable release is `huihaokang-nas-20260414-keyword-control-r68`
- NAS release flow now includes safe post-deploy runtime reconciliation:
  - cancel only demonstrably stranded running tasks
  - delete only demonstrably replaced stale worker rows
  - fall back to in-container reconciliation when NAS host Python lacks `psycopg`
- NAS steady state now targets:
  - `web`
  - `scheduler`
  - `keyword-worker`
  - `sync-worker`
  - `postgres`
  - host watchdog/report/alternating runner
- local steady state now targets:
  - remote Docker `category-worker`
  - SSH local-forward for NAS Postgres
  - SSH/SCP artifact push for runtime category map
- NAS category-worker is no longer part of the intended steady-state path; it remains fallback only
- no schema changes unless explicitly scheduled
- no new UI feature growth unless required to keep release gates green or stabilize NAS runtime
- default project delivery mode is `leader + multi-agent`
- NAS runtime observability now standardizes on:
  - `crawl_plans + scheduler`
  - daily `huihaokang-crawl-report.timer`
  - periodic `huihaokang-nas-watchdog.timer`
  - Web runtime center consuming `/api/runs/summary` + `/api/system/health`

## Current Priority Stack

Execute in this order unless a production incident forces reprioritization:

1. category crawler gradual layering
2. remote category steady-state hardening
3. keyword protection and runtime observability no-regression
4. doc/knowledge-base alignment

Current authoritative runtime files:
- [keyword_runtime_contract.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_contract.py)
- [keyword_monitor_runtime.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_monitor_runtime.py)
- [keyword_batch_summary.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_batch_summary.py)
- [keyword_runtime_io.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_io.py)
- [keyword_runtime_stages.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_stages.py)
- [ops/crawler_runtime_contract.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_runtime_contract.py)
- [scrapers/noon_category_signals.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_category_signals.py)
- [scrapers/noon_category_payloads.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_category_payloads.py)
- [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
- [web_beta/static/app.home.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.home.js)
- [web_beta/static/app.keyword.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.keyword.js)
- [web_beta/static/app.drawer.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.drawer.js)
- [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
- [tools/keyword_runtime_health.py](D:/claude%20noon%20v1/noon-selection-tool/tools/keyword_runtime_health.py)
- [tools/write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
- [tools/run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
- [tools/sync_runtime_category_artifacts.py](D:/claude%20noon%20v1/noon-selection-tool/tools/sync_runtime_category_artifacts.py)
- [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
- [ops/nas_compose_render.py](D:/claude%20noon%20v1/noon-selection-tool/ops/nas_compose_render.py)
- [tools/render_nas_project_compose.py](D:/claude%20noon%20v1/noon-selection-tool/tools/render_nas_project_compose.py)
- [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
- [ops/keyword_control_state.py](D:/claude%20noon%20v1/noon-selection-tool/ops/keyword_control_state.py)
- [ops/crawler_control.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_control.py)
- [docker-compose.category-node.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.category-node.yml)
- [.env.category-node.example](D:/claude%20noon%20v1/noon-selection-tool/.env.category-node.example)

Current retained local green gates:
- stabilization:
  - [2026-04-11T12-07-49-656Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T12-07-49-656Z)
- runtime-center:
  - [2026-04-11T12-07-49-655Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T12-07-49-655Z)

Current retained NAS green gates:
- stabilization:
  - [2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
- runtime-center:
  - [2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)

Current crawler control capabilities on NAS:
- keyword controls are monitor-config scoped, not global
- baseline keyword add/remove persists through the monitor config baseline file + sidecar state
- keyword disable now means:
  - remove from baseline when present
  - write monitor-scoped disabled rules so the keyword does not flow back from `baseline / generated / tracked / manual`
- keyword blocked roots now support:
  - `exact`
  - `contains`
- keyword control runtime exists to reduce low-value crawl and focus runtime on terms that can help find target products
- keyword rules are source-scoped and currently allow:
  - `baseline`
  - `generated`
  - `tracked`
  - `manual`
- keyword control APIs now include:
  - `GET /api/crawler/keyword-controls/active-keywords`
  - `POST /api/crawler/keyword-controls/disable`
  - `POST /api/crawler/keyword-controls/restore`
  - `POST /api/crawler/keyword-controls/roots`
- category controls now support:
  - `category_single.target_subcategory`
  - `category_ready_scan.subcategory_overrides`
- runtime category map fallback now prefers the richest available batch-scan map when `config/runtime_category_map.json` is empty

Current remote category rollout status:
- remote category worker heartbeat is visible on NAS `/api/workers`
- NAS `/api/health` and `/api/system/health` are now reading Postgres correctly during the rollout
- remote category artifact sync now succeeds and writes:
  - `runtime_category_map.json`
  - `runtime_category_map.meta.json`
  into NAS shared batch paths
- keyword protection remained green during the rollout
- remote category node now uses host Chrome over CDP with warm-up navigation for Noon category pages
- host Chrome runtime is now managed through a local controller + scheduled self-heal path:
  - desktop launcher:
    - `C:\Users\Admin\Desktop\Noon Remote Category Controller.cmd`
  - local runtime manager:
    - [tools/manage_remote_category_runtime.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/manage_remote_category_runtime.ps1)
  - host Chrome bootstrap:
    - [tools/start_remote_category_host_browser.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/start_remote_category_host_browser.ps1)
  - scheduled self-heal tasks:
    - `NoonRemoteCategoryEnsureAtLogon`
    - `NoonRemoteCategoryEnsureEvery5Min`
  - task runner wrapper:
    - [tools/ensure_remote_category_runtime.cmd](D:/claude%20noon%20v1/noon-selection-tool/tools/ensure_remote_category_runtime.cmd)
  - desktop controller business workflow now supports:
    - filter top-level categories
    - select one or more categories
    - set per-leaf crawl depth before launch
    - view live task stage, message, persisted subcategory count, and persisted observation count at the bottom panel
    - launch new category tasks without interrupting an already running remote category task
- local category Docker compose now includes a dedicated `nas-db-tunnel` sidecar:
  - category worker no longer depends on a manually maintained host SSH tunnel
  - worker DSNs now point to `nas-db-tunnel:55432`
  - the sidecar copies the mounted SSH key into a writable temp path and fixes key permissions before opening the NAS loopback Postgres tunnel
- canonical shared-sync DB refs now enqueue NAS-consumable `warehouse_sync` tasks:
  - `trigger_db = postgresql://...@postgres:5432/noon_stage`
  - `warehouse_db = postgresql://...@postgres:5432/noon_warehouse`
- acceptance validation completed:
  - `category_single sports/cycling x60`
    - `persisted_observations = 50`
    - queued shared sync completed
  - `category_ready_scan pets x300`
    - `persisted_subcategories = 13`
    - `persisted_observations = 2623`
    - terminal progress stage = `web_visible`
  - `category_ready_scan sports x300`
    - `persisted_subcategories = 18`
    - `persisted_observations = 4748`
    - terminal progress stage = `web_visible`
- current steady state is cut over:
  - NAS `/api/system/health` shows only remote category heartbeat, not a NAS local category-worker
  - keep NAS category-worker disabled in steady state
  - retain it only as manual fallback

## Mandatory Collaboration Mode

Default mode for project development is:

`leader + multi-agent`

This is now the standard execution model for non-trivial repo work.

### Leader Responsibilities

- define the task boundary before implementation
- split work into bounded slices with explicit write ownership
- keep shared-state and main-path contracts consistent
- own final integration into the main working tree
- run mandatory self-checks after integration
- decide whether the work is ready for manual review or NAS release

### Worker Responsibilities

- own only the assigned slice
- avoid overlapping writes unless the leader explicitly reassigns ownership
- do not change release gates, NAS runbooks, or main-path ownership rules unless assigned
- report concrete output back to the leader for integration

### Required Execution Order

1. leader freezes the state or release contract when needed
2. leader assigns write scopes
3. workers implement bounded changes
4. leader integrates and resolves conflicts
5. leader runs self-check and local smoke
6. only after green gates does manual review begin

### Practical Rule

- if a task is too small to benefit from parallel work, the leader may execute it directly
- this does not change ownership, gate, or review rules

## Stop Conditions

Default stop condition is:

`goal complete`

Do not stop only because a local subtask finished.

Stop and escalate only when:
- production deployment or rollback is next
- schema or destructive data change is next
- credentials, payment, or external approvals are required
- there are multiple materially different implementation paths with non-trivial tradeoffs
- the requested goal is fully completed

Do not stop for routine continuation such as:
- the next bug in the same chain
- the next cleanup step in the same agreed objective
- the next verification step after a code change
- the next implied sync step inside an already-approved workstream

## Task Intake Contract

Default user-to-Codex task intake now requires 5 input classes whenever the task is larger than a trivial one-liner:

1. `goal`
   - what business problem this task should solve
2. `scope`
   - what is included and explicitly not included
3. `priority`
   - whether speed, stability, UX, or long-term architecture matters most
4. `constraints`
   - what may not be changed, what must stay compatible, and where the work may run or deploy
5. `acceptance`
   - what the user must see before the work counts as done

Default expectation:
- once these five inputs are sufficiently clear, Codex should not revert to patch-thinking
- Codex should reason at board/module scope, not single-edit scope
- Codex should continue until the whole requested board/module reaches a reviewable state

Recommended user prompt shape:
- goal
- scope
- priority
- constraints
- acceptance
- and the standing instruction:
  - continue by default unless the next step is production deploy, schema change, destructive action, credentials, or a genuine product decision gate

Reference:
- [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)

## Board-Complete Delivery Rule

The default delivery unit is now:

`board-complete / module-complete`

This means:
- do not stop after one small UI patch if the page still cannot be meaningfully reviewed
- do not stop after one crawler fix if the crawl -> persist -> sync -> visibility chain is still incomplete
- do not stop after one data change if directly adjacent validation and regression repair are still outstanding

For current repo work, Codex should usually continue through:
- implementation
- direct regression repair
- required validation
- related doc sync

Only then should work be returned for manual review, unless a true stop condition is reached.

## UI Delivery Rules

For ERP, dashboard, analytics, and workbench UI tasks, use this fixed delivery order:

1. page spec
2. data contract check
3. implementation
4. browser screenshot validation
5. UI critique
6. real-data regression
7. repair if needed
8. manual review

Manual review is for workflow and product judgment, not for discovering basic broken paths.

## Functional Role Expectations

Use these roles by default on non-trivial work:
- `leader`
- `ui-spec`
- `ui-implement`
- `browser-qa`
- `ui-critic`
- `runtime-data`

The leader decides which roles are needed for a task and owns final integration.

## Regression Gates

Repeat bug classes must gain at least one persistent guardrail before the task is considered done:
- test
- smoke script
- browser check
- document rule
- release gate

If a bug is fixed without adding a guardrail, the task remains partially complete.

## Business Sample Regression

Maintain a fixed real-data sample set for high-frequency product logic and UI validation.

Default buckets:
- inventory-signal products
- no-inventory products
- keyword-ranking-history products
- multi-source provenance products
- delivery-marker products
- category edge cases

When a task touches UI, parsing, or runtime product behavior, regression against the relevant sample set is expected before manual review.

## Active Ownership

### Main Window
- local beta Web main path
- NAS runbook and rollback docs
- release gating
- compose/migration rehearsal

Primary files:
- [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
- [web_beta/static/index.html](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/index.html)
- [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
- [web_beta/static/app.contract.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.contract.js)
- [web_beta/static/app.services.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.services.js)
- [web_beta/static/app.state.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.state.js)
- [web_beta/static/app.selection.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.selection.js)
- [tools/run_web_beta_stabilization_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_web_beta_stabilization_check.js)
- [docs/NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
- [docs/NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
- [docs/OPS_RETENTION_POLICY.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OPS_RETENTION_POLICY.md)
- [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)

### Other Windows
- may not edit the above main-path Web files or NAS runbook files without explicit handoff ownership

## Frozen State Contract

The following are the only valid main-path state buckets:

- home scope:
  - `homeScopePaths[]`
- selection scope:
  - `selectionScopePaths[]`
- category selector pending state:
  - `pendingCategoryPaths[]`
  - `selectionMode`
  - `expandedCategoryPaths[]`
  - `searchExpandedCategoryPaths[]`
- selection loading state:
  - `selectionLoadSignature`
  - `selectionLoadPromise`
  - `selectionInteractionBusy`

Behavior rules:
- home scope affects dashboard only
- selection scope affects product table only
- entering selection does not inherit home scope
- selection with no filters shows full product results
- selection with category-only scope shows all products in that category
- during selection refresh:
  - action buttons lock
  - previous table stays visible
  - new results replace old only after fetch returns

## Release Blocking Gates

These must pass before any NAS promotion:

1. `selection` with no filters returns full product results
2. `selection` with category-only scope returns that category's products
3. home `reset to dashboard` truly clears scope
4. top nav active state is unique and correct
5. category selector supports:
   - expand
   - collapse
   - single select
   - multi select
   - `Esc` close
6. drawer stays overlay and does not revert to old tab workflow
7. drawer does not show:
   - `signal count`
   - `category count`
   - `keyword count`
8. primary paths have no visible garbled text
9. `consoleErrors = []`
10. `pageErrors = []`
11. performance gates:
   - `/api/dashboard` cold path `< 2500ms`
   - `/api/products/{platform}/{product_id}` cold path `< 1000ms`
   - warm path stays cache-level
12. local human review is complete
13. release bundle builds cleanly
14. compose smoke passes with NAS-style env
15. cutover and rollback checklists are current

## Mandatory Workflow

Every main-path or release-path change must follow this order:

1. update state, release, or rollback contract if needed
2. leader assigns bounded write ownership
3. implement code or doc change
4. leader integrates changes
5. run fixed self-check and relevant local smoke
6. if checks are not green, continue fixing
7. if the goal is not complete and no real escalation point exists, continue automatically
8. only then hand off for manual review
9. only after manual review + release checks pass, prepare NAS release promotion and smoke

Manual review is not the bug-discovery layer for basic path correctness.

## Fixed Self-Check

Entry:
- [run_web_beta_stabilization_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_web_beta_stabilization_check.js)
- [run_runtime_center_browser_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_runtime_center_browser_check.js)

Scope:
- static checks
- API smoke
- browser main path
- screenshot generation
- performance checks

Latest green run:
- stabilization:
  - [2026-04-11T10-14-59-731Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T10-14-59-731Z)
- runtime-center:
  - [2026-04-11T10-14-17-325Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T10-14-17-325Z)

Handling rule:
- these doc pointers are the repo source of truth for the latest retained green runs
- `scripts/sync_obsidian_context.py` mirrors them into Obsidian
- do not point Vault at ad hoc local runs that have not replaced the retained green run in repo docs

## Artifact Retention

- keep only the latest green stabilization run in `output/playwright/web_beta_stabilization/`
- delete obvious temp files from repo root after they stop being useful
- keep core docs concise; move transient details into the collab log only when they matter

## Runtime Safety

- no ad hoc NAS changes outside the cutover checklist
- no database schema changes from this sprint
- no crawler runtime changes from this sprint
- NAS stable uses a single scheduler entry:
  - `crawl_plans + scheduler`
  - host `huihaokang-alternating-crawl.service` is legacy only and not part of production truth
- NAS release flow performs post-deploy reconciliation through:
  - [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
  - [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - scope is limited to demonstrably stale rows created by pre-deploy worker replacement
- NAS stable observability uses:
  - `huihaokang-crawl-report.timer` for daily reports
  - `huihaokang-nas-watchdog.timer` for read-only runtime inspection

## Current Status

- NAS stable release is live on retained-data Postgres
- current NAS release:
  - `huihaokang-nas-20260413-runtime-quality-r56`
- local beta remains the only development baseline
- fixed self-check is the mandatory entry gate before any new manual review
- latest NAS browser smoke:
  - stabilization:
    - [2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
  - runtime-center:
    - [2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)
- recent NAS cleanup completed:
  - stale worker rows reviewed
  - orphan and smoke terminal task history pruned conservatively
  - obvious local-only probe scripts removed from current NAS release
- runtime governance updated:
  - `crawl_plans + scheduler` is the single auto-dispatch entry for NAS stable
  - task retention policy is now explicit and documented
  - legacy host alternating service no longer participates in production health or automatic dispatch
  - current NAS release/image is now `huihaokang-nas-20260413-runtime-quality-r56`
  - batch truth now comes from snapshot lifecycle, and post-release reconciliation repairs stale warehouse `keyword_runs_log` rows from source truth
  - runtime-center now shows operator truth as `keyword quality full | live batch idle | freshness fresh` after the latest terminal keyword snapshot
  - historical daily report gaps for `20260407-20260409` remain backfilled and excluded from active warnings
  - current steady-state summary is:
    - keyword quality: `full` at operator level, with `live batch idle` after the latest terminal batch
    - freshness: `fresh`
    - shared sync: `completed` and operator-visible
  - current live worker set is back to `3` after post-release stale cleanup
  - `active_snapshot_count = 0`
  - `stale_running_snapshot_count = 0`
  - latest acceptance snapshots in the `sports/pets` priority lane:
    - `2026-04-12_164251`
    - `2026-04-12_171052`
    - `2026-04-12_173213`
  - all three latest acceptance snapshots resolved to `full`
