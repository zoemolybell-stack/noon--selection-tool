# Docs Index

Last Updated: 2026-04-13

## Start Here

Do not start by reading every file.

Use this default reading order:

1. [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md)
   - global execution rules, UI rules, default behavior
2. [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
   - strategy, architecture, current phase, environment model
3. [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
   - current ownership, frozen contracts, release gates, active boundaries
4. [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)
   - how the user should brief Codex, and how Codex should deliver board-complete work

Use these only when needed:

- [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
  - current code structure and authoritative files
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
  - chronology and proof log; do not use as the default first-read document

## Task-Based Entry Points

### Starting normal development work

Read in this order:

1. [AGENTS.md](D:/claude%20noon%20v1/AGENTS.md)
2. [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
3. [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
4. [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)

### Understanding repo structure

- [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)

### Checking what changed and when

- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)

### Assessing the quality of the doc system itself

- [DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md)

## Core Docs

- [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - strategy, architecture, roadmap, sprint status
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - active ownership, frozen contracts, release gates
- [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)
  - how to brief Codex, default execution expectations, board-complete delivery model
- [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
  - current code structure and authoritative files
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
  - concise development journal
- [DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md)
  - objective assessment of which docs are positive constraints vs emerging negative constraints

## Deployment Docs

- [NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
- [NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
- [CLOUDFLARE_TUNNEL_ACCESS.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CLOUDFLARE_TUNNEL_ACCESS.md)
- [OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md)
- [REMOTE_CATEGORY_MULTI_SLOT.md](D:/claude%20noon%20v1/noon-selection-tool/docs/REMOTE_CATEGORY_MULTI_SLOT.md)

## Historical / One-off Technical Notes

- [POSTGRES_PHASE2_LOCAL.md](D:/claude%20noon%20v1/noon-selection-tool/docs/POSTGRES_PHASE2_LOCAL.md)
- [PRODUCT_IDENTITY_MIGRATION_PLAN.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PRODUCT_IDENTITY_MIGRATION_PLAN.md)

## Usage Rule

When these docs disagree:
- whitepaper wins for strategy
- handoff wins for current execution
- collab log wins for chronology

Reading rule:
- do not start with the collab log unless you specifically need history
- do not use Obsidian mirrors as the authority source
- use this README as the document entrypoint, not as the strategy source

## Default Execution Model

Project development now defaults to:

`leader + multi-agent`

Use:
- [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md) for the strategic rule
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md) for ownership, gates, and execution order

Related defaults now also include:
- `goal closure first`
- fixed UI delivery pipeline
- fixed regression gates
- real-data business sample regression before manual review on relevant tasks

## Maintenance

- local artifact cleanup script:
  - [cleanup_local_artifacts.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_local_artifacts.ps1)
- latest retained local-beta green self-check:
  - stabilization:
    - [2026-04-11T12-07-49-656Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T12-07-49-656Z)
  - runtime-center:
    - [2026-04-11T12-07-49-655Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T12-07-49-655Z)
- current NAS stable release:
  - `huihaokang-nas-20260414-keyword-control-r68`
- latest retained NAS green self-check:
  - stabilization:
    - [2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
  - runtime-center:
    - [2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)

Current runtime architecture note:
- NAS is the single control plane
- remote local Docker node is the intended category worker
- remote category heartbeat and artifact sync are working
- local remote category runtime now has a direct desktop operator entrypoint:
  - `C:\Users\Admin\Desktop\Noon Remote Category Controller.cmd`
  - use it to:
    - repair host Chrome + compose stack
    - install auto-heal scheduled tasks
    - filter categories and set per-leaf crawl depth before launch
    - launch selected category scans without interrupting the currently running remote category task
    - view bottom-panel runtime progress:
      - active task
      - current stage
      - progress message
      - persisted subcategories
      - persisted observations
- local category compose is now self-contained for DB connectivity:
  - `nas-db-tunnel` keeps the NAS Postgres SSH tunnel alive inside compose
  - category worker talks to `nas-db-tunnel:55432` instead of relying on a host-level manual tunnel
- host Chrome is now part of the intended runtime contract for the remote category node:
  - startup / health-check / recovery are managed by:
    - [manage_remote_category_runtime.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/manage_remote_category_runtime.ps1)
    - [start_remote_category_host_browser.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/start_remote_category_host_browser.ps1)
    - [ensure_remote_category_runtime.cmd](D:/claude%20noon%20v1/noon-selection-tool/tools/ensure_remote_category_runtime.cmd)
  - scheduled self-heal task names:
    - `NoonRemoteCategoryEnsureAtLogon`
    - `NoonRemoteCategoryEnsureEvery5Min`
- remote category steady-state cutover is complete:
  - `category_single sports/cycling x60` closed to `web_visible` with `50` persisted observations
  - `pets x300` closed to `web_visible` with `2623` persisted observations
  - `sports x300` closed to `web_visible` with `4748` persisted observations
- keyword control is now business-oriented rather than purely technical:
  - monitor-config scoped baseline add/remove is persistent
  - disabled keywords mean `remove from baseline + block future return flow`
  - blocked roots support both `精准词 / exact` and `包含词根 / contains`
  - active keyword management is now available in the crawler console modal
