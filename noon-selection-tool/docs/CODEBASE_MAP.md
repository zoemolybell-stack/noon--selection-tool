# CODEBASE MAP

Last Updated: 2026-04-13

## Purpose

This file is the current code map for the active repo.  
It is intentionally concise and only lists authoritative paths.

## Top-Level Structure

- `analysis/`
  - analytics and comparison helpers
- `config/`
  - runtime config and product store logic
- `db/`
  - database config helpers
- `docs/`
  - strategy, handoff, journal, runbooks
- `ops/`
  - task store, scheduler control, systemd units
- `scrapers/`
  - Playwright crawlers and base scraper
- `tests/`
  - Python regression coverage
- `tools/`
  - local utilities, self-check, migration helpers
- `web_beta/`
  - local beta Web app

## Current Web Main Path

### Backend

- [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - local beta API
  - dashboard/product/detail payloads
  - caching and read-model shaping

### Frontend Entry

- [web_beta/static/index.html](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/index.html)
  - shell DOM, nav, workspace containers
- [web_beta/static/app.css](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.css)
  - current local beta visual system

### Frontend Modules

- [web_beta/static/app.contract.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.contract.js)
  - frozen contract constants and default workbench state factory
- [web_beta/static/app.services.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.services.js)
  - fetch wrappers and JSON helpers
- [web_beta/static/app.state.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.state.js)
  - route normalization, scope encoding, route parsing/building
- [web_beta/static/app.selection.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.selection.js)
  - active selection workbench functions
- [web_beta/static/app.favorites.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.favorites.js)
  - favorites page and product favorite interactions
- [web_beta/static/app.home.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.home.js)
  - home/dashboard rendering and scope interactions
- [web_beta/static/app.keyword.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.keyword.js)
  - keyword page rendering and query-first workspace logic
- [web_beta/static/app.drawer.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.drawer.js)
  - right drawer rendering, charts, context, detail sections
- [web_beta/static/app.runs.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.runs.js)
  - runtime center rendering, task center orchestration, watchdog summary cards
- [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
  - bootstrap, route execution, remaining event glue, module wiring

## Runtime Quality / Observability Tools

- [keyword_runtime_contract.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_contract.py)
  - unified keyword result-file contract, payload loading, and failure-evidence shaping
- [keyword_monitor_runtime.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_monitor_runtime.py)
  - keyword monitor lock, summary, checkpoint, and final warehouse-sync helpers
- [keyword_batch_summary.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_batch_summary.py)
  - batch truth merge, quality precedence, and analysis/runtime summary helpers
- [keyword_runtime_io.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_io.py)
  - keyword snapshot/result/keyword-pool IO helpers and atomic write helpers
- [keyword_runtime_stages.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_stages.py)
  - runtime stage builders, crawl retry/finalize helpers, and scrape summary shaping
- [ops/crawler_runtime_contract.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_runtime_contract.py)
  - shared duplicate-dispatch skip semantics and conflict pattern helpers
- [ops/keyword_control_state.py](D:/claude%20noon%20v1/noon-selection-tool/ops/keyword_control_state.py)
  - monitor-config scoped baseline additions/removals, disabled keywords, blocked roots, and runtime rule matching for `exact / contains`
- [ops/crawler_control.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_control.py)
  - crawler control plan normalization, catalog shaping, subcategory override validation, and monitor keyword-control APIs
- [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - crawler control API surface for active keyword management, disable/restore flows, blocked-root updates, and immediate tracked registration after baseline add
- [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
  - truth-model summarizer for `live_batch_state`, `latest_terminal_batch_state`, and `operator_quality_state`
- [analysis/keyword_warehouse_health.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_warehouse_health.py)
  - keyword quality summary, recent runs, degradation reasons
- [tools/keyword_runtime_health.py](D:/claude%20noon%20v1/noon-selection-tool/tools/keyword_runtime_health.py)
  - task-quality state shaping for runtime governance
- [tools/write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - daily runtime report, keyword quality summary, watchdog summary
- [tools/run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
  - structured runtime warning/critical summary
- [tools/sync_runtime_category_artifacts.py](D:/claude%20noon%20v1/noon-selection-tool/tools/sync_runtime_category_artifacts.py)
  - pushes minimal runtime category control artifacts back to NAS shared storage over SSH/SCP
- [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - conservative ops cleanup and post-deploy runtime reconciliation
- [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
  - NAS compose promotion and automatic safe post-deploy reconciliation
- [ops/nas_compose_render.py](D:/claude%20noon%20v1/noon-selection-tool/ops/nas_compose_render.py)
  - renders NAS compose content with Docker App-safe env interpolation and remote-category service removal
- [tools/render_nas_project_compose.py](D:/claude%20noon%20v1/noon-selection-tool/tools/render_nas_project_compose.py)
  - CLI wrapper for NAS compose rendering during release/update flows
- [docker-compose.category-node.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.category-node.yml)
  - local Docker-only category worker deployment for remote category-node mode
- [.env.category-node.example](D:/claude%20noon%20v1/noon-selection-tool/.env.category-node.example)
  - example tunnel/DB/artifact-sync env for the remote category node

## Fixed Local Beta Self-Check

- [tools/run_web_beta_stabilization_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_web_beta_stabilization_check.js)
  - static checks
  - API smoke
  - browser main-path validation
  - screenshot output
  - performance gates
- [tools/run_runtime_center_browser_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_runtime_center_browser_check.js)
  - dedicated runs/runtime-center and home-scope browser validation
  - runtime alert summary/cards
  - home scoped-dashboard clear behavior

Latest retained green run:
- stabilization:
  - [2026-04-11T12-07-49-656Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T12-07-49-656Z)
- runtime-center:
  - [2026-04-11T12-07-49-655Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T12-07-49-655Z)

Latest retained NAS green run:
- stabilization:
  - [2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
- runtime-center:
  - [2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)

Handling rule:
- these retained green pointers are maintained in repo docs first
- Obsidian mirrors them from repo docs through `scripts/sync_obsidian_context.py`
- local scratch runs do not become authoritative until they replace the retained pointers here

Cleanup entry:
- [tools/cleanup_local_artifacts.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_local_artifacts.ps1)
  - removes local scratch files
  - removes repo `__pycache__`
  - trims Playwright artifacts to the latest retained green run

## Data Flow

Stable local-beta data flow:

`category runtime/stage + keyword runtime/stage -> warehouse.db -> web_beta`

Rules:
- Web reads warehouse only
- runtime/stage is not Web truth
- identity is `(platform, product_id)`

Current production rollout data flow:

`NAS web/scheduler/keyword-worker/sync-worker/postgres + remote local category-worker -> NAS Postgres -> NAS warehouse -> Web`

Rules:
- NAS remains the only control plane and the only truth database
- remote category-worker writes directly into NAS Postgres through SSH tunnel
- remote category-worker does not run shared warehouse sync locally
- NAS `sync-worker` remains the only shared warehouse promotion executor
- only minimal category control artifacts are pushed back to NAS shared storage

## Current Cleanup Status

- duplicated active function layer: removed from main path
- main-path override assignments: removed
- selection/home/keyword/drawer modules: extracted
- runs/runtime-center module: extracted
- runtime center now reads watchdog + keyword quality summaries
- keyword quality operator truth is now batch-based rather than single-run based
- historical report gaps `20260407-20260409` are backfilled and excluded from active watchdog warnings
- deploy path now includes safe post-deploy runtime reconciliation for stranded running tasks and replaced stale workers
- deploy path falls back to in-container reconciliation when NAS host Python lacks `psycopg`
- watchdog/report host services now execute inside the web container while consuming real host systemd state via env injection
- keyword runtime quality recovery now persists structured crawl failure evidence and stabilizes parquet analysis with `pyarrow`
- keyword runtime lifecycle now finalizes from source truth after monitor-final sync, so stale warehouse `running` snapshots no longer override operator truth
- keyword runtime orchestration is now split into contract, monitor-runtime, and batch-summary helpers instead of one larger keyword entrypoint
- keyword runtime orchestration now also routes stage and IO behavior through `keyword_runtime_stages.py` and `keyword_runtime_io.py`, reducing direct responsibility inside `keyword_main.py`
- category signal parsing and evidence shaping now live in `scrapers/noon_category_signals.py`, reducing churn inside the large category crawler file
- category payload shaping and product-card parsing now route through `scrapers/noon_category_payloads.py`, reducing churn inside the large category crawler file
- crawler control now includes persistent monitor keyword controls and subcategory-aware category controls
- current retained NAS control-plane truth is `Keyword quality full | live batch idle | freshness fresh`
- the latest three acceptance terminal snapshots in the `sports/pets` priority lane are all `full`:
  - `2026-04-12_173213`
  - `2026-04-13_023917`
  - `2026-04-13_032000`
- remote category rollout status:
  - remote worker heartbeat is visible in NAS `/api/workers`
  - artifact sync for `runtime_category_map.json` is working
  - remote category node now reaches Noon category pages through host Chrome over CDP
  - queued shared sync from remote category node now uses NAS-canonical DB refs
  - validated terminal category batches:
    - `pets x300` -> `2623` persisted observations, `web_visible`
    - `sports x300` -> `4748` persisted observations, `web_visible`
  - current steady state is remote category execution, with NAS local `category-worker` reserved as manual fallback only
- docs: aligned to runtime hardening phase
