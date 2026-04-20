# PROJECT WHITEPAPER

Last Updated: 2026-04-13  
Status: active  
Audience: all windows

## Purpose

This document is the strategy and phase source of truth for the current repo.

Use it for:
- project north star
- current architecture
- active delivery phase
- release strategy
- roadmap and exit criteria

Use the other two core docs for execution:
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)

Priority rule:
- `PROJECT_WHITEPAPER.md` wins for strategy
- `DEV_HANDOFF.md` wins for active ownership and boundaries
- `DEV_COLLAB_LOG.md` wins for factual chronology

## North Star

The target business loop is:

`multi-source crawling -> unified product intelligence -> Web operations workbench -> scoring/classification -> listing preparation -> AI enrichment -> controlled automation`

This means:
- all crawl sources feed one trusted product intelligence layer
- Web is an operations workbench, not a passive report page
- downstream automation must only consume stable, warehouse-promoted facts

## Stable Architecture

Current stable data flow:

`category runtime/stage + keyword runtime/stage -> warehouse -> Web`

Rules:
- Web reads warehouse only
- runtime/stage is not Web truth
- canonical identity is `(platform, product_id)`
- stable provenance fields remain available:
  - `source_type`
  - `source_value`
  - `has_category`
  - `has_keyword`

## Environments

- `local-beta`
  - the only environment for active development and release validation
- `nas-stable`
  - operator-facing environment
  - receives only deliberate releases
  - keeps its own data volumes
  - live on retained-data Postgres

Implications:
- local beta changes do not auto-promote to NAS
- NAS data and browser/runtime state stay isolated
- local beta data is not auto-promoted into NAS production
- the remote local category node is a production worker, not a local-beta mirror
- the remote category node writes directly into NAS Postgres over SSH tunnel and only pushes minimal control artifacts back to NAS

Current NAS stable release:
- `huihaokang-nas-20260414-keyword-control-r68`

## Current Phase

Current phase:

`single control plane + remote category node steady state`

The cutover phase is complete. NAS is already running on retained-data Postgres.

The active objectives are now:
- keep NAS as the only control plane:
  - `web`
  - `scheduler`
  - `keyword-worker`
  - `sync-worker`
  - `postgres`
- run category execution on a remote local Docker node through SSH-tunneled Postgres access
- keep NAS `sync-worker` as the only shared warehouse promotion executor
- sync only minimal category control artifacts back to NAS:
  - `runtime_category_map.json`
  - minimal batch metadata
- keep remote category crawling stable under the steady-state dual-node contract
- keep NAS local `category-worker` disabled in steady state and reserved only as manual fallback
- keep keyword control business-oriented:
  - keyword control exists to cut low-value crawl and focus runtime on terms that can help find target products
  - keyword disable is monitor-config scoped, not a global keyword blacklist
- align repo docs and Obsidian knowledge base with the live dual-node runtime state

Exit criteria for this phase:
- NAS `/api/workers` shows the remote category worker heartbeat with:
  - `node_role=remote_category`
  - `node_host`
- remote category node can write directly into NAS ops/stage/warehouse Postgres through SSH tunnel
- remote category ready scan can push `runtime_category_map.json` back into NAS shared storage
- category tasks from the remote node enqueue or reuse NAS `warehouse_sync` tasks instead of running local shared sync
- NAS release smoke remains green with NAS category-worker removed from steady-state compose
- keyword terminal batches remain `full` during the rollout
- remote category crawling no longer returns upstream `Access Denied` for Noon category pages

Current factual status:
- remote category heartbeat is visible on NAS
- remote category artifact sync is working
- keyword protection remained green during rollout
- remote category node now reaches Noon category pages through host Chrome over CDP + warm-up navigation
- remote `category_single sports/cycling x60` completed with:
  - `persisted_observations = 50`
  - queued shared sync completed on NAS
- remote validation batches completed with non-zero observations and `web_visible` terminal state:
  - `pets x300`:
    - `persisted_subcategories = 13`
    - `persisted_observations = 2623`
  - `sports x300`:
    - `persisted_subcategories = 18`
    - `persisted_observations = 4748`
- NAS steady state now shows only:
  - `scheduler`
  - `keyword-worker`
  - `sync-worker`
  - `remote_category` worker heartbeat
- steady-state category cutover is complete; NAS local `category-worker` is fallback only

## Default Delivery Model

Default project delivery mode is now:

`leader + multi-agent`

This is the baseline for ongoing project development, not an optional preference.

Rules:
- one `leader` owns task decomposition, integration, release judgment, and final verification
- worker agents own bounded slices only after write scope is declared
- main-path files, release docs, and NAS runtime changes may not be edited by multiple agents without explicit leader ownership
- manual review starts only after leader integration and green self-check gates
- if a task is truly trivial and has no separable slice, the leader may execute it alone, but the task still follows the same ownership and gate rules

Reason for this default:
- reduce patch-on-patch conflicts
- freeze state behavior before implementation
- make self-check mandatory before user review
- keep final responsibility with one integrating owner instead of diffused across parallel edits

## Goal-Closure Workflow

Default execution mode is now:

`goal closure first`

This means work does not stop when one subtask finishes. Work stops only when:
- the requested end goal is complete
- a true decision gate exists
- a destructive or production action needs explicit approval
- credentials, paid resources, or external access are required

It does not stop only because:
- one code change landed
- one bug was fixed
- one page became locally usable
- one round of verification passed

Default execution order:
1. define the end goal
2. decompose the goal into sub-tasks
3. execute one sub-task
4. run the relevant self-check
5. if no real decision gate exists, continue to the next sub-task automatically
6. stop only at goal completion or a true escalation point

The user should not need to re-authorize ordinary forward progress when the next step is already implied by the agreed goal.

## Functional Agent Roles

Default non-trivial work should use functional roles instead of ad hoc parallelism.

Standard roles:
- `leader`
  - owns decomposition, sequencing, integration, verification, and release judgment
- `ui-spec`
  - freezes page goal, workflow, density, and state behavior before implementation
- `ui-implement`
  - implements the assigned UI slice only
- `browser-qa`
  - validates real browser behavior, state transitions, and visible regressions
- `ui-critic`
  - critiques hierarchy, scanability, density, and operator workflow quality
- `runtime-data`
  - validates API behavior, data contracts, parsing logic, and performance

Not every task needs every role, but the role model is the default frame for project work.

## UI Delivery Pipeline

All ERP, dashboard, analytics, and workbench UI work should follow this pipeline:

1. page specification
2. data contract confirmation
3. implementation
4. screenshot validation
5. UI critique
6. real-data regression
7. repair if needed
8. manual review

UI is not complete merely because it renders or because one local click path works.

The implementation must answer:
- what the operator is looking at
- what is abnormal
- what the next action is
- where the operator can drill next

## Regression Strategy

Every recurring class of bug must gain a guardrail:
- unit test
- API smoke
- browser regression
- document rule
- release gate

Fixing without adding a guardrail is not considered complete for repeat issues.

## Business Sample Set

A fixed real-data sample set is now part of the delivery model.

Default buckets to maintain:
- products with inventory signals
- products without inventory signals
- products with keyword ranking history
- products with multiple provenance sources
- products with different delivery-type markers
- category edge cases

UI, parser, and runtime changes should be checked against this sample set before manual review.

## Local Beta Gate

Local beta remains the only development environment.

Before any NAS promotion:
- local UI stabilization self-check must stay green
- local human review must complete
- release bundle must be rebuilt from the frozen baseline

Latest retained green run:
- [2026-04-11T10-14-59-731Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T10-14-59-731Z)
- runtime-center companion check:
  - [2026-04-11T10-14-17-325Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T10-14-17-325Z)

Latest NAS green run:
- stabilization:
  - [2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
- runtime-center:
  - [2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)

Handling rule:
- these latest-green pointers are authoritative in repo docs
- Obsidian mirrors them from repo docs via `scripts/sync_obsidian_context.py`
- when a newer retained green run replaces them, update docs first, then sync the Vault

## Active Sprint

Current sprint:

`NAS Runtime Hardening / Quality Improvement`

Scope:
- keyword task quality classification and reason visibility
- batch truth across `monitor / crawl / analyze / monitor_expand / register`
- runtime center as the operator command center
- shared sync contention reduction and operator-readable state
- Web main-path split beyond the old `app.js`
- repo docs and Obsidian phase alignment

Out of scope for this sprint:
- environment migration
- schema redesign
- external alert channels as the first response path
- business-facing feature expansion unrelated to runtime quality

### Sprint Exit Criteria

All of these must hold:
- keyword tasks expose stable `full / partial / degraded`
- runtime center, watchdog, and daily reports consume the same batch-truth quality result
- runtime center shows actionable current-vs-history warnings
- shared sync state is normalized and operator-readable
- local green run stays green after main-path splits
- NAS smoke shows the same runtime-center behavior
- historical report gaps no longer remain as active warnings after approved backfill
- repo docs and Obsidian no longer describe pre-cutover phases

### Sprint Phase Status

1. Phase A `runtime quality contract`
- status: completed

2. Phase B `runtime center command center`
- status: completed

3. Phase C `shared sync behavior hardening`
- status: completed

4. Phase D `web main-path split`
- status: in progress

5. Phase E `ui, docs, and knowledge-base alignment`
- status: in progress

## Local Beta Web Structure

Current front-end main-path modules:
- [app.contract.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.contract.js)
- [app.services.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.services.js)
- [app.state.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.state.js)
- [app.selection.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.selection.js)
- [app.favorites.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.favorites.js)
- [app.home.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.home.js)
- [app.keyword.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.keyword.js)
- [app.drawer.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.drawer.js)
- [app.runs.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.runs.js)
- [app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)

Current self-check entry:
- [run_web_beta_stabilization_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_web_beta_stabilization_check.js)

## Roadmap After This Sprint

Once Phase E passes:
1. keep NAS on Postgres as the production runtime
2. continue reducing runtime degraded states, especially keyword quality regressions
3. keep `leader + multi-agent` as the default delivery model for project development
4. then return to business-facing improvements and controlled automation

## Artifact Retention

To keep the repo readable:
- keep only the latest green local-beta stabilization run under `output/playwright/web_beta_stabilization/`
- delete obvious scratch files and one-off temp logs from repo root
- keep long-form historical discussion out of core docs

## Non-Goals For This Sprint

- no new crawler schema redesign
- no environment migration
- no local-to-NAS data copy
- no external alerting as the first response path
