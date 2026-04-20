# DEV COLLAB LOG

## Purpose

This file is the concise development journal.

It keeps only high-signal milestones:
- what changed
- what validation passed
- what boundary remained

Detailed chat history is intentionally not preserved here.

## 2026-04-11 18:25 - Runtime control truth corrected on r51

### Theme

Fix the last control-plane drift where a completed keyword batch could still leave `active_snapshot_count = 1`, causing `runs/summary` and runtime center to imply a ghost active snapshot after terminal completion.

### What Changed

- Updated:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - tightened keyword quality overlay logic so `idle_with_summary` no longer preserves a false active snapshot count
  - when no live keyword task exists, `active_snapshot_count` is forced back to `0` and stale residuals move into `stale_running_snapshot_count`
- Updated:
  - [tests/test_web_phase0.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_web_phase0.py)
  - added a focused regression test for the idle-overlay demotion path and isolated the summary tests from the real local ops runtime
- Promoted NAS stable to:
  - `huihaokang-nas-20260411-runtime-control-r51`
- Verified the latest terminal keyword snapshot:
  - `keyword_monitor_plan_10_round_14_seed-0102`
  - operator truth now resolves to `partial`

### Validation

- local retained green runs:
  - stabilization:
    - [2026-04-11T10-14-59-731Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T10-14-59-731Z)
  - runtime-center:
    - [2026-04-11T10-14-17-325Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T10-14-17-325Z)
- NAS retained green runs:
  - stabilization:
    - [2026-04-11T10-13-35-612Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T10-13-35-612Z)
  - runtime-center:
    - [2026-04-11T10-13-35-610Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T10-13-35-610Z)
- key NAS runtime result:
  - `/api/runs/summary` now reports:
    - `live_batch_state = idle`
    - `latest_terminal_batch_state = partial`
    - `operator_quality_state = partial`
    - `active_snapshot_count = 0`
    - `stale_running_snapshot_count = 0`
  - runtime center now shows `Keyword quality partial | live batch idle | freshness fresh`

### Effect Boundary

- control-plane truth only
- no schema change
- no crawler runtime logic change
- no UI workflow change

## 2026-04-11 14:20 - Runtime control truth finalized on r47

### Theme

Close the remaining runtime/control drift by fixing warehouse keyword-run lifecycle ordering, reconciling stale `running` snapshots from source truth, and validating a fresh `sports/pets` terminal batch on NAS.

### What Changed

- Extended:
  - [keyword_main.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_main.py)
  - [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - [tests/test_cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_cleanup_ops_history.py)
- Moved final warehouse sync to run after `finish_keyword_run(...)`, so source truth lands before the final monitor summary and warehouse promotion.
- Taught deploy/runtime reconciliation to repair warehouse `keyword_runs_log` rows from source `keyword_runs` truth instead of blindly cancelling everything old.
- Promoted NAS stable to:
  - `huihaokang-nas-20260411-runtime-control-r47`
- Launched and completed a fresh `sports/pets` keyword batch for post-release validation:
  - task `846`
  - snapshot `keyword_monitor_plan_10_round_14_seed-0099`

### Validation

- Local retained green runs:
  - stabilization:
    - [2026-04-11T05-11-43-658Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T05-11-43-658Z)
  - runtime-center:
    - [2026-04-11T05-11-43-661Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T05-11-43-661Z)
- NAS green runs:
  - stabilization baseline retained:
    - [2026-04-11T03-30-27-651Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T03-30-27-651Z)
  - runtime-center after deploy cleanup:
    - [2026-04-11T05-17-19-691Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T05-17-19-691Z)
  - runtime-center after fresh terminal batch:
    - [2026-04-11T06-14-48-469Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T06-14-48-469Z)
- Key NAS runtime result:
  - runtime center now shows `Keyword quality degraded | live batch idle | freshness fresh`
  - `active_keyword_run_count = 0`
  - `incomplete_keyword_run_count = 0`
  - deploy reconciliation no longer leaves stale live snapshots in warehouse summary truth

### Effect Boundary

- Keyword monitor finalize ordering
- Warehouse keyword run reconciliation
- NAS runtime/control truth only

## 2026-04-11 11:35 - Batch truth, freshness truth, and historical report backfill finalized

### Theme

Make runtime truth consistent across the runs center, watchdog, daily reports, and task history by moving keyword quality from single-run truth to snapshot batch truth, separating operator freshness from stale stage-import diagnostics, and backfilling the historical daily-report gap.

### What Changed

- Extended:
  - [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
  - [tools/keyword_runtime_health.py](D:/claude%20noon%20v1/noon-selection-tool/tools/keyword_runtime_health.py)
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - [ops/crawler_control.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_control.py)
  - [run_task_scheduler.py](D:/claude%20noon%20v1/noon-selection-tool/run_task_scheduler.py)
  - [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - [tools/write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - [tools/run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
- Fixed keyword quality aggregation so one snapshot now resolves to a single batch truth:
  - `degraded > partial > full > unknown`
  - `crawl/analyze` outrank `register/monitor_expand`
- Fixed duplicate keyword monitor dispatch so active-monitor collisions are recorded as skip behavior instead of failed task history.
- Fixed operator freshness so warehouse visibility and shared sync drive the operator state while stale `source_databases.imported_at` remains diagnostic only.
- Backfilled missing daily reports for:
  - `20260407`
  - `20260408`
  - `20260409`
- Promoted NAS stable to:
  - `huihaokang-nas-20260411-runtime-batch-truth-r43`

### Validation

- Local retained green runs:
  - stabilization:
    - [2026-04-11T03-22-13-192Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T03-22-13-192Z)
  - runtime-center:
    - [2026-04-11T03-22-13-192Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T03-22-13-192Z)
- NAS green runs:
  - stabilization:
    - [2026-04-11T03-30-27-651Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T03-30-27-651Z)
  - runtime-center:
    - [2026-04-11T03-32-28-949Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T03-32-28-949Z)
- Key NAS runtime result:
  - runtime center now shows `Keyword quality degraded | live batch full | sync completed`
  - historical report-gap warning is cleared after backfill
  - release reconciliation no longer leaves stranded running tasks or stale workers for manual cleanup

### Effect Boundary

- Runtime truth model
- Scheduler skip semantics
- Operator freshness semantics
- Historical daily report backfill
- NAS release hardening only

## 2026-04-11 09:35 - Keyword quality truth model and post-deploy reconciliation finalized

### Theme

Make the runtime center say the truth about keyword quality, then make NAS release promotion clean up provably stranded runtime state automatically instead of relying on manual cleanup.

### What Changed

- Added:
  - [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
- Extended:
  - [analysis/keyword_warehouse_health.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_warehouse_health.py)
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - [tools/write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - [tools/run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
  - [web_beta/static/app.runs.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.runs.js)
  - [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
- Fixed runtime keyword truth fields so the system now distinguishes:
  - `live_batch_state`
  - `latest_terminal_quality_state`
  - `operator_quality_state`
- Fixed NAS deploy hardening so post-deploy runtime reconciliation now:
  - rechecks health
  - reconciles provably stranded running tasks
  - deletes only demonstrably replaced stale worker rows
  - falls back to container-side reconciliation when NAS host Python lacks `psycopg`
- Promoted NAS stable to:
  - `huihaokang-nas-20260411-094820`

### Validation

- Local retained green runs:
  - stabilization:
    - [2026-04-11T01-15-53-303Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T01-15-53-303Z)
  - runtime-center:
    - [2026-04-11T01-15-15-768Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T01-15-15-768Z)
- NAS green runs:
  - stabilization:
    - [2026-04-11T01-51-27-967Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T01-51-27-967Z)
  - runtime-center:
    - [2026-04-11T01-51-27-974Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T01-51-27-974Z)
- Key NAS runtime result:
  - runtime center now shows `Keyword quality full | live batch unknown | sync completed`
  - post-deploy reconciliation left `3` live workers without manual cleanup

### Effect Boundary

- Runtime governance truth model
- NAS release hardening
- Runtime center wording and operator visibility
- No schema change

## 2026-04-11 10:10 - Post-deploy runtime reconciliation added to NAS release flow

### Theme

Close the gap between successful compose promotion and runtime cleanup by making the deploy path automatically reconcile only records that are demonstrably stranded after worker replacement.

## 2026-04-11 10:55 - Host watchdog truth path fixed and NAS release promoted

### Theme

Make the NAS runtime center tell the truth by letting watchdog/report run inside the web container while still consuming real host systemd state.

### Delivered

- `run_nas_watchdog_agent.py` now accepts host service state overrides instead of blindly calling `systemctl` inside the container
- `huihaokang-nas-watchdog.service` now injects real host states for:
  - `huihaokang-alternating-crawl.service`
  - `huihaokang-crawl-report.service`
  - `huihaokang-crawl-report.timer`
- `deploy_nas_release.py` now updates `/current` after a successful promotion
- NAS stable promoted to:
  - `huihaokang-nas-20260411-094820`

### Validation

- `/api/runs/summary` now reports:
  - `host_alternating_service_active = true`
  - `watchdog.status = warning`
  - only historical report gaps remain
- NAS green runs:
  - stabilization:
    - [2026-04-11T01-51-27-967Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T01-51-27-967Z)
  - runtime-center:
    - [2026-04-11T01-51-27-974Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T01-51-27-974Z)

### What Changed

- Extended:
  - [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
  - [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
- Added a safe `post-deploy reconcile` mode that:
  - cancels only running tasks that provably belong to pre-deploy workers replaced by newer worker rows
  - deletes only stale worker rows that predate the deploy and have same-type replacements after the deploy
  - does not prune terminal history as part of release promotion
- Updated:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
  - [scripts/sync_obsidian_context.py](D:/claude%20noon%20v1/scripts/sync_obsidian_context.py)
- Clarified latest green run handling:
  - repo docs remain the source of truth
  - Obsidian mirrors those pointers instead of inventing new ones

### Validation

- `python -m py_compile` on:
  - `deploy_nas_release.py`
  - `tools/cleanup_ops_history.py`
  - `scripts/sync_obsidian_context.py`
- `cleanup_ops_history.py --help` succeeds with the new reconciliation flags
- `sync_obsidian_context.py --dry-run` succeeds and prints clean target paths

### Effect Boundary

- Deploy/runtime reconciliation path only
- Docs and Obsidian sync alignment
- No backend keyword-quality logic change
- No frontend/UI change

## 2026-04-10 23:20 - Runtime-center gate split finalized and docs realigned

### Theme

Finish the local runtime-governance round by turning runs/home browser checks into a dedicated gate, then align repo docs and knowledge-base inputs to the same runtime-hardening phase.

### What Changed

- Added:
  - [tools/run_runtime_center_browser_check.js](D:/claude%20noon%20v1/noon-selection-tool/tools/run_runtime_center_browser_check.js)
- Kept the main stabilization entry focused on:
  - selection
  - category selector
  - drawer
  - keyword
- Updated:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
  - [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
- Fixed doc references so the latest retained local gates now point to the current runtime-hardening outputs instead of the old cutover-prep era runs.

### Validation

- Main stabilization run is green:
  - [2026-04-10T16-15-45-903Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-10T16-15-45-903Z)
- Dedicated runtime-center browser check is green:
  - [2026-04-10T16-15-45-886Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-10T16-15-45-886Z)
- Key runtime-center assertions:
  - `runtimeAlertCards = 4`
  - `runtimeAlertSummary` present
  - home scoped dashboard clears back to `全平台 / 大盘`
  - `consoleErrors = 0`
  - `pageErrors = 0`

### Effect Boundary

- Local beta gate hardening only
- Documentation and knowledge-base source alignment
- No schema change
- NAS release promotion pending

## 2026-04-10 20:20 - Runtime governance batch released to NAS stable

### Theme

Promote the runtime-governance batch from local beta to NAS stable after local green gates, then verify the same alert/quality behavior against the live retained-data Postgres environment.

### What Changed

- Built and promoted:
  - `huihaokang-nas-20260410-runtime-governance-r39`
- Synced:
  - runtime quality governance backend changes
  - runs center alert summary/cards
  - operator access to `runs`
  - `app.home.js / app.keyword.js / app.drawer.js`-based main path
  - docs and Obsidian sync inputs
- Bumped static build tag to:
  - `ui-v3-workbench-r39`

### Validation

- NAS current release:
  - `/volume1/docker/huihaokang-erp/releases/huihaokang-nas-20260410-runtime-governance-r39`
- NAS smoke:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
- NAS browser stabilization run is green:
  - [2026-04-10T12-13-05-640Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-10T12-13-05-640Z)
- Key runtime UI assertions on NAS:
  - `runtimeAlertSummary = 关键词质量 degraded · 同步 fresh`
  - `runtimeAlertCards = 4`
  - `consoleErrors = 0`
  - `pageErrors = 0`

### Effect Boundary

- Local beta and NAS stable runtime/UI alignment
- No schema change
- No environment migration

## 2026-04-10 12:05 - Runtime governance and runs center closure

### Theme

Close the gap between backend runtime quality governance and the operator-visible runs center so the same warning model is visible in API, report, watchdog, and Web.

### What Changed

- Extended:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - [analysis/keyword_warehouse_health.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_warehouse_health.py)
  - [tools/keyword_runtime_health.py](D:/claude%20noon%20v1/noon-selection-tool/tools/keyword_runtime_health.py)
  - [tools/write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - [tools/run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
  - [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
  - [web_beta/static/index.html](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/index.html)
  - [web_beta/static/app.css](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.css)
- Added/validated:
  - keyword run `quality_state`
  - watchdog summary in `/api/runs/summary` and `/api/system/health`
  - runs center alert summary/cards in Web
  - operator access to `runs` view
- Fixed the remaining runs-route regression:
  - operator no longer gets forced back to `home`
  - `view-runs` is no longer hidden for operator role

### Validation

- Python tests passed:
  - `tests.test_web_phase0`
  - `tests.test_keyword_runtime_health`
  - `tests.test_keyword_warehouse_health`
  - `tests.test_write_crawl_report`
  - `tests.test_runtime_quality_governance`
- Fixed local beta stabilization run is green:
  - [2026-04-10T12-02-38-756Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-10T12-02-38-756Z)
- Key browser gates:
  - `consoleErrors = 0`
  - `pageErrors = 0`
  - `runtimeAlertCards = 4`
  - selection and keyword main paths remain green

### Effect Boundary

- Local beta runtime governance and Web main path
- No schema change
- NAS deploy pending after docs/knowledge-base alignment

## 2026-04-10 21:15 - Leader plus multi-agent made the default delivery mode

### Theme

Turn the recent multi-agent runtime hardening pattern into the repo-wide default execution model.

### What Changed

- Updated:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
- Fixed the default collaboration rules to:
  - require one `leader` on project work
  - use bounded worker slices by default for non-trivial tasks
  - keep final integration, self-check, and release judgment with the leader
  - keep manual review behind green gates instead of using it as the first bug-discovery layer

### Validation

- Docs are now aligned on the same execution rule:
  - strategy says `leader + multi-agent` is the default delivery model
  - handoff says the leader owns decomposition, integration, and gates
  - the execution order now explicitly includes leader ownership before implementation

### Effect Boundary

- Documentation and collaboration policy only
- No runtime change
- No schema change
- No crawler logic change

## 2026-04-10 21:30 - Goal-closure and UI delivery rules hardened

### Theme

Close the gap between local bug fixing and actual goal completion by turning continuation, UI quality, and regression expectations into default rules.

### What Changed

- Extended:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
- Added explicit defaults for:
  - `goal closure first`
  - UI delivery pipeline
  - regression guardrails
  - real-data business sample regression
- Fixed the stop condition model so routine forward progress no longer waits for user confirmation when no true decision gate exists

### Validation

- Strategy, handoff, and docs index now agree on:
  - default `leader + multi-agent`
  - default `goal complete` stop condition
  - UI work requiring spec, screenshot validation, critique, and regression before manual review

### Effect Boundary

- Documentation and workflow policy only
- No runtime change
- No schema change
- No crawler logic change

## 2026-04-06 19:25 - NAS runtime cleanup and residue audit

### Theme

Clean NAS `ops` noise and verify the deployed release no longer carries obvious temporary residue or conflicting active code.

### What Changed

- Added:
  - [cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
- Audited current NAS release:
  - no `__pycache__`
  - no `tmp_*`
  - current `web_beta/static/app.js` matches local baseline hash
  - no duplicate named active functions found in local main-path `app.js`
- Took an `ops` backup before cleanup:
  - `/volume1/docker/huihaokang-erp/backups/ops-cleanup-20260406-191829`
- Pruned conservative `ops` noise from NAS:
  - 19 orphan/smoke terminal tasks
  - 19 matching `task_runs`
  - stale workers reviewed, none deleted
- Removed obvious local-only probe scripts from current NAS release and running container:
  - `explore_category_filter.py`
  - `explore_category_filter2.py`
  - `run_local_compose_smoke.py`
  - `run_ops_postgres_smoke.py`
  - `run_product_store_postgres_smoke.py`
  - `run_warehouse_postgres_smoke.py`
- Updated:
  - [build_release_bundle.py](D:/claude%20noon%20v1/noon-selection-tool/build_release_bundle.py) so those local-only files are excluded from future NAS bundles

### Validation

- Cleanup dry-run before apply:
  - `task_candidates_total = 19`
  - `stale_workers = 0`
- After apply:
  - `tasks_total: 419 -> 400`
  - `completed: 348 -> 339`
  - `failed: 14 -> 10`
  - `cancelled: 56 -> 50`
- Runtime remained healthy:
  - `/api/health = ok`
  - `/api/workers` shows only 4 live workers
  - `/api/system/health = ok`

### Effect Boundary

- No schema change
- No crawler logic change
- No data rollback
- Only conservative ops-history cleanup and release hygiene

## 2026-04-06 12:55 - NAS Postgres cutover preparation started

### Theme

Shift from local-only UI stabilization to formal NAS retained-data cutover preparation.

### What Changed

- Reframed NAS strategy from cold-start deployment to:
  - keep NAS data volumes
  - migrate NAS SQLite to NAS Postgres in place
  - release code and database cutover in one controlled maintenance window
- Marked the new sprint in:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
- Planned companion docs and tooling for:
  - cutover checklist
  - rollback checklist
  - NAS env audit
  - compose smoke against NAS-style env

### Validation

- Latest local beta green run retained:
  - [2026-04-06T04-41-35-872Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-06T04-41-35-872Z)
- Release bundle built locally:
  - `huihaokang-nas-20260406-124135.zip`

### Effect Boundary

- No NAS mutation yet
- No database cutover yet
- Preparation phase only

## 2026-04-06 12:58 - Cutover prep scripts and remote audit

### Theme

Turn the NAS cutover plan into executable local preparation assets and verify current NAS state.

### What Changed

- Rewrote:
  - [NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
  - [NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
- Added:
  - [NAS_POSTGRES_CUTOVER_CHECKLIST.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_POSTGRES_CUTOVER_CHECKLIST.md)
  - [NAS_POSTGRES_ROLLBACK_CHECKLIST.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_POSTGRES_ROLLBACK_CHECKLIST.md)
  - [audit_nas_postgres_cutover.py](D:/claude%20noon%20v1/noon-selection-tool/tools/audit_nas_postgres_cutover.py)
- Updated:
  - [run_local_compose_smoke.py](D:/claude%20noon%20v1/noon-selection-tool/run_local_compose_smoke.py) so an explicit env file is materialized as `.env.nas` during compose config
  - [build_release_bundle.py](D:/claude%20noon%20v1/noon-selection-tool/build_release_bundle.py) manifest notes to retained-data Postgres cutover
  - [docker-compose.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.yml) to remove obsolete compose `version` noise

### Validation

- Local green run refreshed:
  - [2026-04-06T04-49-20-495Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-06T04-49-20-495Z)
- Compose smoke now passes with:
  - `python run_local_compose_smoke.py --env-file .env.nas.example`
- New release bundle built:
  - `huihaokang-nas-20260406-124914.zip`
- Remote read-only NAS audit confirmed:
  - shared data/runtime/browser/log/postgres directories exist
  - production SQLite files exist
  - current release env already contains Postgres DSNs
  - current release env is missing some SQLite rollback source fields such as `NOON_PRODUCT_STORE_DB`

### Effect Boundary

- No NAS mutation yet
- No production cutover yet
- Phase B complete
- Phase C started

## 2026-04-06 11:40 - Project cleanup

### Theme

Remove stale documentation noise, temporary files, and redundant self-check artifacts. Rebuild project docs around current architecture and stabilization status.

### What Changed

- Rewrote core docs into concise current-state versions:
  - [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
- Added:
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
  - [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
- Added repeatable cleanup tooling:
  - [cleanup_local_artifacts.ps1](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_local_artifacts.ps1)
  - [D:\claude noon v1\.gitignore](D:\claude%20noon%20v1\.gitignore)
- Removed obsolete root-level scratch files, old temporary artifacts, local uvicorn logs, repo `__pycache__`, `.playwright-cli`, and `data/tmp_*` probe artifacts after pausing local beta.
- Trimmed local beta self-check artifacts to keep only the latest green run.

### Validation

- Cleanup was limited to docs and reproducible temporary artifacts.
- Active local beta green run retained:
  - [2026-04-06T03-36-01-452Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-06T03-36-01-452Z)

### Effect Boundary

- No NAS change
- No database change
- No crawler change
- No runtime behavior change

## 2026-04-06 11:17 - Phase C complete

### Theme

Finish the main-path structure split so local beta no longer depends on active override layering inside `app.js`.

### What Changed

- Added:
  - [app.selection.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.selection.js)
- Kept existing extracted modules:
  - [app.contract.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.contract.js)
  - [app.services.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.services.js)
  - [app.state.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.state.js)
- Moved active selection main-path functions out of the giant `app.js`
- Removed remaining `= function final...` override assignments from the main path
- Converted the last active override-style functions into normal declarations
- Extended fixed self-check to validate `app.selection.js`

### Validation

- Green result directory:
  - [2026-04-06T03-36-01-452Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-06T03-36-01-452Z)
- Gate result summary:
  - `rowsNoFilters = 50`
  - `visibleRowsNoFilters = 5`
  - `rowsAfterCategory = 50`
  - `visibleRowsAfterCategory = 5`
  - `dashboard_ms = 7`
  - `product_detail_ms = 714`
  - `consoleErrors = 0`
  - `pageErrors = 0`
  - `failures = []`

### Effect Boundary

- No NAS change
- No database change
- No crawler change
- Phase C complete

## 2026-04-06 20:10 - NAS scheduler unification and retention policy

### Theme

Finish the remaining NAS runtime governance work: one scheduler entry, explicit task retention rules, and release-time enforcement.

### What Changed

- Scheduler runtime policy fixed:
  - NAS stable now treats host `huihaokang-alternating-crawl.service` as the single scheduler entry
  - container `huihaokang-scheduler` is no longer part of the target steady state
- Updated:
  - [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
  - [NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
  - [.env.nas.example](D:/claude%20noon%20v1/noon-selection-tool/.env.nas.example)
  - [validate_nas_env.py](D:/claude%20noon%20v1/noon-selection-tool/validate_nas_env.py)
- Added:
  - [OPS_RETENTION_POLICY.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OPS_RETENTION_POLICY.md)
- Extended:
  - [cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - now supports explicit plan-linked terminal cleanup under a separate flag and cutoff

### Validation

- NAS health remained `ok` after removing the container `huihaokang-scheduler`
- Current runtime continues under host alternating service
- Latest keyword task kept running while container scheduler stayed absent

### Effect Boundary

- NAS runtime governance changed
- No schema change
- No crawler logic change
- Future NAS deploys now honor host scheduler mode

## 2026-04-06 20:45 - NAS steady-state rebuild after missing output package

### Theme

Close the last release-path residue bug: the release bundle excluded the Python package `output/`, which broke category runtime on NAS after redeploy.

### What Changed

- Fixed:
  - [build_release_bundle.py](D:/claude%20noon%20v1/noon-selection-tool/build_release_bundle.py)
  - top-level `output/` Python modules are now included in release bundles
  - only generated artifact subpaths under `output/` stay excluded
- Synced current release files to NAS stable
- Rebuilt the current NAS stable image from the corrected release source
- Verified:
  - container scheduler remains absent
  - host alternating service remains the scheduler entry
  - category worker can import `output.crawl_data_exporter`

### Validation

- NAS `/api/health` returned `ok`
- NAS `/api/system/health` returned `ok`
- NAS `/api/workers` showed only 3 live workers:
  - `category`
  - `keyword`
  - `sync`
- Manual category smoke progressed past import failure and wrote subcategory files under:
  - `/app/data/batch_scans/manual_smoke_20260406_electronics`

### Effect Boundary

- NAS release/runtime changed
- No schema change
- No DB cutover change
- Release packaging for future NAS promotions is now corrected

## 2026-04-06 21:00 - NAS runtime governance close-out and product drawer fix

### Theme

Finish the remaining NAS governance tasks and fix the product-detail regression that made the selection drawer look incomplete on NAS.

### What Changed

- Unified NAS scheduling to a single runtime entry:
  - removed the container scheduler
  - kept the host service `huihaokang-alternating-crawl.service` as the only active scheduler path
- Applied the new retention policy to current orphan/smoke terminal history in `ops`
- Fixed a Postgres-only product detail failure in:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - the detail endpoint used a literal `%` inside SQL and raised `psycopg.ProgrammingError`
  - category-path filtering now uses a parameterized `LIKE ?`
  - visible-BSR parsing no longer depends on `%` literals inside SQL fragments
- Synced the fixed `web_beta/app.py` into the current NAS release and redeployed stable

### Validation

- NAS `/api/health` returned `ok`
- NAS `/api/workers` now shows only 3 live workers:
  - `category`
  - `keyword`
  - `sync`
- Host alternating service stayed `active`
- Product detail API for `noon/N12519777A` now returns:
  - `keyword_rankings`
  - `keyword_ranking_timeline`
  - `signal_timeline`
  - `category_context_levels`
  - `effective_category_context`

### Effect Boundary

- NAS runtime governance changed
- No schema change
- No DB cutover change
- Product drawer data parity on NAS was restored at the API layer

## 2026-04-10 15:20 - NAS runtime observability hardening

### Theme

Finish the three remaining runtime-governance tasks as one batch:
- stale worker / stale running task cleanup
- reliable daily report generation
- a real read-only NAS watchdog

### What Changed

- Extended:
  - [cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - added explicit cleanup modes:
    - `--workers-only`
    - `--running-only`
    - `--history-only`
  - stale running task detection now also covers:
    - missing lease owner
    - missing worker row
    - expired lease
    - lease/current_task mismatch
- Updated:
  - [OPS_RETENTION_POLICY.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OPS_RETENTION_POLICY.md)
  - retention policy now matches the expanded cleanup modes
- Updated:
  - [write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - report requests now retry
  - `24h delta` now uses the last snapshot before the 24h boundary instead of falling back to current totals
  - release label prefers the release manifest when available
- Added the canonical NAS watchdog:
  - [run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
  - [huihaokang-nas-watchdog.service](D:/claude%20noon%20v1/noon-selection-tool/ops/systemd/huihaokang-nas-watchdog.service)
  - [huihaokang-nas-watchdog.timer](D:/claude%20noon%20v1/noon-selection-tool/ops/systemd/huihaokang-nas-watchdog.timer)
- Added regression tests:
  - [test_cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_cleanup_ops_history.py)
  - [test_write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_write_crawl_report.py)

### Validation

- local unit tests and `py_compile` passed
- NAS stale task `711` was detected by `running-only` cleanup and cancelled safely
- NAS daily report/service path was redeployed with retry and timeout hardening
- NAS watchdog timer and service were installed as the single canonical watchdog path

### Effect Boundary

- NAS runtime governance changed
- No schema change
- No crawler logic change
- NAS now has a distinct read-only watchdog in addition to the runner and daily report timer

## 2026-04-10 16:40 - NAS runtime follow-up r42

### Theme

Finish the remaining runtime-center gap after the governance release:
- remove `watchdog unknown` from Web
- make watchdog/report outputs readable
- make report/watchdog files land on a path visible to the Web container
- re-clean stale workers created by release redeploys

### What Changed

- Updated:
  - [run_nas_watchdog_agent.py](D:/claude%20noon%20v1/noon-selection-tool/tools/run_nas_watchdog_agent.py)
  - watchdog now forces readable file modes for latest/daily/jsonl outputs
- Updated:
  - [write_crawl_report.py](D:/claude%20noon%20v1/noon-selection-tool/tools/write_crawl_report.py)
  - daily/latest report files now force readable file modes
- Updated:
  - [huihaokang-nas-watchdog.service](D:/claude%20noon%20v1/noon-selection-tool/ops/systemd/huihaokang-nas-watchdog.service)
  - [huihaokang-crawl-report.service](D:/claude%20noon%20v1/noon-selection-tool/ops/systemd/huihaokang-crawl-report.service)
  - added `UMask=0022`
  - moved report root to host runtime data path
- Updated:
  - [docker-compose.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.yml)
  - web now reads `NOON_REPORT_ROOT=/app/runtime_data/crawl_reports`
- Promoted NAS release:
  - `huihaokang-nas-20260410-runtime-followup-r42`
- Re-ran:
  - watchdog service
  - report service
  - browser stabilization check
  - runtime-center browser check
- Cleaned post-release stale records again:
  - cancelled stranded task `727`
  - removed 6 stale worker rows

### Validation

- NAS `/api/health = ok`
- NAS `/api/system/health = ok`
- `/api/runs/summary` now exposes watchdog payload directly instead of `unknown`
- current watchdog state is `warning`
  - reason: historical report gaps only
  - current chain problem: `false`
- current live workers are back to `3`
- latest NAS browser runs:
  - [stabilization 2026-04-10T16-37-42-238Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-10T16-37-42-238Z)
  - [runtime-center 2026-04-10T16-37-42-247Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-10T16-37-42-247Z)

### Effect Boundary

- NAS runtime-center observability is now readable from the live Web container
- No schema change
- No environment migration
- Remaining warning is historical-only, not a current runtime break

## 2026-04-11 10:55 - Keyword runtime quality recovery release

### Theme

Stabilize keyword runtime quality evidence and remove release-time manual cleanup from the normal NAS promotion path.

### What Changed

- Updated:
  - [scrapers/base_scraper.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/base_scraper.py)
  - final retry exhaustion now persists a structured failure payload instead of leaving a missing result file
- Updated:
  - [scrapers/amazon_scraper.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/amazon_scraper.py)
  - Amazon scrape failures now emit structured `failure_details`, `page_state`, and root-cause evidence
- Updated:
  - [scrapers/noon_scraper.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_scraper.py)
  - Noon partial/degraded keyword runs now emit page-level evidence and explicit failure detail payloads
- Updated:
  - [keyword_main.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_main.py)
  - quality classification now distinguishes:
    - `runtime_import_error`
    - `amazon_parse_failure`
    - `amazon_upstream_blocked`
    - `page_recognition_failed`
    - `partial_results`
  - platform snapshots now preserve `error_evidence` and `failure_details`
- Updated:
  - [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
  - runtime summaries now retain the same structured failure evidence used by the task pipeline
- Updated:
  - [requirements.txt](D:/claude%20noon%20v1/noon-selection-tool/requirements.txt)
  - added `pyarrow` so analyze no longer fails only because parquet support is missing in the runtime image
- Updated:
  - [deploy_nas_release.py](D:/claude%20noon%20v1/noon-selection-tool/deploy_nas_release.py)
  - reused existing post-deploy runtime reconciliation and switched current release cleanly after successful health recheck
- Promoted NAS release:
  - `huihaokang-nas-20260411-102604`

### Validation

- local:
  - `python -m py_compile` on edited runtime files and tests passed
  - `python -m unittest tests.test_keyword_runtime` passed
  - `python -m unittest tests.test_runtime_quality_governance` passed
  - `python -m unittest tests.test_keyword_runtime_health` passed
  - `python -m unittest tests.test_keyword_warehouse_health` passed
- NAS:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - `docker exec huihaokang-web python -c "import pyarrow"` returned `23.0.1`
  - post-deploy runtime reconciliation auto-cancelled stranded task `809`
  - post-deploy runtime reconciliation auto-deleted 3 stale worker rows
  - latest NAS browser runs:
    - [stabilization 2026-04-11T02-41-42-271Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T02-41-42-271Z)
    - [runtime-center 2026-04-11T02-41-42-278Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T02-41-42-278Z)

### Effect Boundary

- keyword quality truth model is now backed by structured scrape evidence instead of opaque missing files
- analyze no longer depends on host luck for parquet support
- NAS release promotion no longer requires a manual stale-worker / stranded-task cleanup pass after success
- no schema change

## 2026-04-11 12:20 - Runtime structure r52

### Theme

Reduce crawler structure load without changing the external CLI, and promote the new runtime contract/helpers into the live NAS release.

### What Changed

- Updated:
  - [keyword_runtime_io.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_runtime_io.py)
  - restored keyword-pool path and atomic-write behavior to match the existing runtime contract
- Updated:
  - [keyword_main.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_main.py)
  - rewired runtime wrappers so the live path now goes through:
    - `keyword_runtime_io.py`
    - `keyword_runtime_stages.py`
    - `keyword_batch_summary.py`
  - kept the current CLI stable while moving stage/IO responsibility out of the entrypoint
- Updated:
  - [run_task_scheduler.py](D:/claude%20noon%20v1/noon-selection-tool/run_task_scheduler.py)
  - [ops/crawler_control.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_control.py)
  - [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)
  - duplicate-dispatch skip semantics now share the same operator-facing contract:
    - `active_monitor`
    - `active_category_crawl`
- Updated:
  - [scrapers/noon_category_crawler.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_category_crawler.py)
  - category payload shaping and product-card parsing now route through:
    - [scrapers/noon_category_payloads.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_category_payloads.py)
- Promoted NAS release:
  - `huihaokang-nas-20260411-runtime-structure-r52`

### Validation

- local:
  - `python -m py_compile` passed on runtime/scheduler/category files touched in this round
  - `python -m unittest tests.test_run_task_scheduler tests.test_runtime_quality_governance` passed
  - `python -m unittest tests.test_keyword_runtime tests.test_base_scraper_result_safety` passed
  - `python -m unittest tests.test_category_runtime` passed
  - latest retained local browser runs:
    - [stabilization 2026-04-11T12-07-49-656Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T12-07-49-656Z)
    - [runtime-center 2026-04-11T12-07-49-655Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T12-07-49-655Z)
- NAS:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - post-deploy reconciliation completed
  - latest retained NAS browser runs:
    - [stabilization 2026-04-11T12-14-29-634Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-11T12-14-29-634Z)
    - [runtime-center 2026-04-11T12-14-29-622Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-11T12-14-29-622Z)
  - retained runtime truth after release:
    - `Keyword quality partial | live batch idle | freshness delayed`

### Effect Boundary

- crawler runtime structure changed
- duplicate-dispatch contract changed
- category parser/payload coupling decreased
- no schema change

## 2026-04-13 01:55 - Runtime quality r56

### Theme

Close the loop on real keyword quality recovery and promote the LAN-deployed NAS release with fresh terminal-batch validation instead of relying on stale history.

### What Changed

- Updated:
  - [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
  - batch truth now treats pure `zero_results` as quality-neutral, prefers the best analysis payload across a batch, and stops stale `analysis_empty` from dominating operator truth
- Updated:
  - [analysis/keyword_warehouse_health.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_warehouse_health.py)
  - operator-facing active/incomplete keyword counts now come from batch truth, while raw row counts remain diagnostic only
- Updated:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - `/api/runs/summary` now surfaces operator-facing active counts from batch truth rather than ghost `keyword_runs_log` rows
- Updated:
  - [scrapers/noon_scraper.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_scraper.py)
  - timeout handling now salvages pages that visibly loaded results instead of falsely classifying them as `partial`
- Promoted NAS release:
  - `huihaokang-nas-20260413-runtime-quality-r56`

### Validation

- local:
  - targeted runtime/control suites passed after the `zero_results` contract fix
  - a fresh local keyword smoke batch for `foam roller` resolved to `full`
- NAS:
  - LAN SSH deployment completed through `192.168.100.20:22`
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - latest NAS browser runs:
    - [stabilization 2026-04-12T17-54-24-721Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_stabilization/2026-04-12T17-54-24-721Z)
    - [runtime-center 2026-04-12T17-54-07-017Z](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/web_beta_runtime_center/2026-04-12T17-54-07-017Z)
  - watchdog returned to `ok`
  - three new acceptance terminal snapshots in the `sports/pets` priority lane all resolved to `full`:
    - `2026-04-12_164251`
    - `2026-04-12_171052`
    - `2026-04-12_173213`

### Effect Boundary

- operator truth is now based on fresh terminal snapshots, not stale ghost active rows
- `zero_results` no longer inflates `partial` when the batch is otherwise healthy
- timeout salvage reduced false Noon `partial` classifications
- no schema change

## 2026-04-13 10:40 - Crawler control r59

### Theme

Promote crawler control from partial backend wiring to usable runtime control on NAS, while keeping the keyword runtime truth model stable.

### What Changed

- Updated:
  - [ops/keyword_control_state.py](D:/claude%20noon%20v1/noon-selection-tool/ops/keyword_control_state.py)
  - monitor-config scoped keyword control state now persists under shared runtime data and supports:
    - baseline additions/removals
    - source-scoped exclusions
- Updated:
  - [keyword_main.py](D:/claude%20noon%20v1/noon-selection-tool/keyword_main.py)
  - keyword monitor runtime now applies exclusion rules before baseline registration and crawl expansion
- Updated:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - added crawler control APIs for:
    - `GET /api/crawler/keyword-controls`
    - `POST /api/crawler/keyword-controls/baseline`
    - `POST /api/crawler/keyword-controls/exclusions`
- Updated:
  - [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
  - crawler control panel now posts to the correct keyword-control endpoints
  - `category_single` now derives its top-level category from the selected subcategory instead of falling back to breadcrumb text
- Updated:
  - [ops/crawler_control.py](D:/claude%20noon%20v1/noon-selection-tool/ops/crawler_control.py)
  - `target_subcategory` now validates against the selected top-level category
  - `subcategory_overrides` now reject config IDs outside the selected categories
  - runtime category map fallback now prefers the richest available batch-scan map instead of the newest non-empty map
- Updated:
  - [run_task_scheduler.py](D:/claude%20noon%20v1/noon-selection-tool/run_task_scheduler.py)
  - `category_ready_scan` dispatch now forwards `--subcategory-overrides-json`
- Promoted NAS release:
  - `huihaokang-nas-20260413-crawler-control-r59`

### Validation

- local:
  - `python -m unittest tests.test_crawler_control tests.test_web_crawler_api` passed
  - `python -m unittest tests.test_run_task_scheduler tests.test_ready_category_scan tests.test_runtime_quality_governance` passed
  - local crawler catalog fallback now resolves to:
    - `ready_scan_20260403_134641/runtime_category_map.json`
    - `12` categories
    - `186` subcategories
- NAS:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - `/api/crawler/catalog` now exposes:
    - `12` categories
    - `186` subcategories
  - keyword control acceptance passed:
    - baseline add/remove persisted
    - exclusion upsert/delete persisted
  - category validation acceptance passed:
    - mismatched `target_subcategory` is rejected at API level
    - mismatched `subcategory_overrides` is rejected at API level
  - fresh post-deploy keyword terminal snapshot:
    - `2026-04-13_023917`
    - `operator_quality_state = full`

### Effect Boundary

- crawler control is now usable for long-term keyword seed governance
- category control is now subcategory-aware rather than top-level only
- runtime truth remains batch-based and stable after deploy
- a residual summary tail still exists:

## 2026-04-13 11:35 - Crawler control r60

### Theme

Close the last crawler-control gap on NAS by making the subcategory catalog render in the browser, not only exist in the API.

### What Changed

- Updated:
  - [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
  - crawler control subcategory extraction now consumes `subcategory_catalog` from `/api/crawler/catalog`
- Promoted NAS release:
  - `huihaokang-nas-20260413-crawler-control-r60`

### Validation

- local:
  - `python -m unittest tests.test_web_crawler_api tests.test_web_phase0` passed
  - `python run_post_deploy_smoke.py --base-url http://127.0.0.1:8865` passed
- NAS:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - `/api/crawler/catalog` now still exposes:
    - `12` categories
    - `186` subcategories
  - crawler control browser acceptance passed:
    - `#crawler-category-target-subcategory` rendered `186` options
    - `#crawler-category-subcategory-picker` rendered `186` options
    - screenshot:
      - [crawler-control-nas-r60.png](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/crawler_control_nas_r60/crawler-control-nas-r60.png)
  - keyword control acceptance still passed:
    - baseline add/remove roundtrip
    - exclusion upsert/delete roundtrip
  - fresh post-deploy keyword terminal snapshot:
    - `2026-04-13_032000`
    - `operator_quality_state = full`

### Effect Boundary

- crawler control is now usable end-to-end in the browser, not only via API
- subcategory-aware category control now has both backend validation and frontend rendering
- current retained NAS truth remains:
  - `Keyword quality full`
  - `live batch idle`
  - `freshness fresh`

## 2026-04-13 11:50 - Summary reason cleanup r61

### Theme

Remove stale operator-facing quality reasons from `full` keyword terminal batches so `/api/runs/summary` no longer leaks old analysis noise.

### What Changed

- Updated:
  - [analysis/keyword_quality_summary.py](D:/claude%20noon%20v1/noon-selection-tool/analysis/keyword_quality_summary.py)
  - `full` batches now clear operator-facing `quality_reasons` and `quality_evidence`
  - `full` source payloads now clear `reason_codes`, `primary_reason`, and per-source `evidence`
- Updated:
  - [tests/test_runtime_quality_governance.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_runtime_quality_governance.py)
  - added regression coverage for stale reason leakage on `full` batches
- Promoted NAS release:
  - `huihaokang-nas-20260413-summary-reason-r61`

### Validation

- local:
  - `python -m unittest tests.test_runtime_quality_governance tests.test_keyword_warehouse_health` passed
  - `python run_post_deploy_smoke.py --base-url http://127.0.0.1:8865` passed
- NAS:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary` now reports for latest `full` terminal batch:
    - `operator_quality_reasons = []`
    - `operator_quality_evidence = []`
    - `latest_terminal_quality_reasons = []`
    - `latest_terminal_quality_evidence = []`
    - `analysis.reason_codes = []`
    - `crawl.reason_codes = []`
  - latest terminal snapshot remains:
    - `2026-04-13_032000`
    - `operator_quality_state = full`
  - runtime center browser check:
    - [runtime-home-r61.png](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/runtime_center_nas_r61/runtime-home-r61.png)

### Effect Boundary

- operator-facing summary truth is now clean on `full` batches
- stale analysis hints no longer leak into runtime-center quality summaries
  - `operator_quality_reasons` may still leak `analysis_empty` even when the latest batch is `full`
- no schema change

## 2026-04-13 22:10 - Collaboration guide formalized

### Theme

Turn the user/Codex working style into an explicit project contract instead of leaving it as ad hoc chat behavior.

### What Changed

- Added:
  - [CODEX_COLLAB_GUIDE.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEX_COLLAB_GUIDE.md)
  - formalized the 5-part task intake model:
    - goal
    - scope
    - priority
    - constraints
    - acceptance
  - formalized the default expectation that Codex should continue to a board-complete/module-complete review state rather than stopping after one small patch
- Updated:
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - added `Task Intake Contract`
  - added `Board-Complete Delivery Rule`
- Updated:
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
  - linked the new collaboration guide from the core docs index
- Mirrored to Obsidian reusable-method notes:
  - `06-Codex项目开发协作指南.md`

### Validation

- new repo guide created successfully
- handoff and docs index updated successfully
- Obsidian mirror note written under the reusable-methodology directory

### Effect Boundary

- no runtime logic changed
- no schema change
- no NAS deployment impact
- collaboration expectations are now documented in repo docs instead of only chat history

## 2026-04-13 23:05 - Development doc health report and reading-order cleanup

### Theme

Assess the current documentation system as an engineering tool, then reduce entry friction by making the docs index an actual reading-order guide instead of just a file list.

### What Changed

- Added:
  - [DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEVELOPMENT_DOC_HEALTH_REPORT_20260413.md)
  - documented:
    - which docs are currently strong positive constraints
    - which docs are starting to create negative-constraint risk
    - recommended reading order
    - next-round doc-system simplification recommendations
- Updated:
  - [README.md](D:/claude%20noon%20v1/noon-selection-tool/docs/README.md)
  - converted from a flat docs list into:
    - `Start Here`
    - task-based entry points
    - explicit reading-order guidance
    - guidance to avoid treating the collab log as the default first-read document

### Validation

- new report file created successfully
- docs index updated successfully
- report file linked from the docs index

### Effect Boundary

- no runtime logic changed
- no schema change
- no NAS deployment impact
- doc system now has a lighter default entry path for future work

## 2026-04-13 20:20 - Remote category node rollout: Postgres cutover, artifact sync, and upstream block discovery

### Theme

Advance the dual-node architecture from plan to runtime reality:
- NAS remains the single control plane
- local Docker becomes the remote category worker
- category writes directly into NAS Postgres
- runtime category artifacts are pushed back into NAS shared storage

### What Changed

- Added:
  - [ops/nas_compose_render.py](D:/claude%20noon%20v1/noon-selection-tool/ops/nas_compose_render.py)
  - [tools/render_nas_project_compose.py](D:/claude%20noon%20v1/noon-selection-tool/tools/render_nas_project_compose.py)
  - [docker-compose.category-node.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.category-node.yml)
  - [tools/sync_runtime_category_artifacts.py](D:/claude%20noon%20v1/noon-selection-tool/tools/sync_runtime_category_artifacts.py)
- Updated:
  - [tools/sync_runtime_category_artifacts.py](D:/claude%20noon%20v1/noon-selection-tool/tools/sync_runtime_category_artifacts.py)
  - hardened SSH artifact sync with:
    - temporary private-key copy at `0600`
    - writable temporary `known_hosts`
    - legacy `scp -O` for UGREEN volume-path compatibility
  - [docker-compose.category-node.yml](D:/claude%20noon%20v1/noon-selection-tool/docker-compose.category-node.yml)
  - [main.py](D:/claude%20noon%20v1/noon-selection-tool/main.py)
  - category single/runtime flows now align with remote-node rollout contracts already landed earlier in the cycle
- NAS runtime promotion:
  - stable release moved to `huihaokang-nas-20260413-remote-category-r67`
  - Docker App project was re-rendered so NAS `web/scheduler/keyword/sync` actually read Postgres DSNs instead of falling back to SQLite

### Validation

- remote category worker heartbeat is visible on NAS `/api/workers`
- NAS `/api/health` and `/api/system/health` now read Postgres correctly after compose re-render
- remote category `ready_scan` now successfully pushes:
  - `runtime_category_map.json`
  - `runtime_category_map.meta.json`
  into `/volume1/docker/huihaokang-erp/shared/data/batch_scans/garden`
- keyword protection rerun stayed green:
  - new keyword batch completed with `quality_state = full`
- unresolved acceptance blocker discovered:
  - remote category crawl requests from the local node currently hit Noon `Access Denied`
  - both `sports/cycling` and `sports/basketball` returned `0` observations with upstream denial evidence
  - this means the category sync contract is implemented, but full steady-state cutover is not yet production-ready

### Effect Boundary

- no schema change
- NAS single control plane contract is now real, not just planned
- remote category artifact sync is working
- final remote category crawl promotion remains blocked by upstream anti-bot behavior and still needs mitigation before permanent NAS category-worker removal

## 2026-04-13 23:20 - Remote category cutover completed after crawl-quality and shared-sync contract fixes

### Theme

Close the last production blocker for the dual-node architecture:
- remote category crawl quality on Noon category pages
- NAS-consumable shared-sync task payloads from the remote node
- formal x300 validation before declaring steady-state cutover complete

### What Changed

- Updated:
  - [scrapers/browser_runtime.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/browser_runtime.py)
  - [scrapers/base_scraper.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/base_scraper.py)
  - [scrapers/noon_category_crawler.py](D:/claude%20noon%20v1/noon-selection-tool/scrapers/noon_category_crawler.py)
  - remote category crawling now uses host Chrome over CDP plus warm-up navigation, which removed the local-node Noon `Access Denied` blocker
- Updated:
  - [ops/shared_sync_queue.py](D:/claude%20noon%20v1/noon-selection-tool/ops/shared_sync_queue.py)
  - [.env.category-node](D:/claude%20noon%20v1/noon-selection-tool/.env.category-node)
  - [.env.category-node.example](D:/claude%20noon%20v1/noon-selection-tool/.env.category-node.example)
  - remote category node now enqueues canonical NAS-side DB refs for `warehouse_sync`:
    - `postgresql://...@postgres:5432/noon_stage`
    - `postgresql://...@postgres:5432/noon_warehouse`
  - this removed the previous sync failure caused by `host.docker.internal` leaking into NAS-consumed task payloads
- Added / Updated tests:
  - [tests/test_browser_runtime.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_browser_runtime.py)
  - [tests/test_shared_sync_queue.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_shared_sync_queue.py)

### Validation

- remote smoke:
  - `category_single sports/cycling x60`
  - `persisted_observations = 50`
  - queued shared sync completed on NAS
- formal remote validation:
  - task `897` / `pets x300`
    - `persisted_subcategories = 13`
    - `persisted_observations = 2623`
    - terminal progress stage = `web_visible`
  - task `898` / `sports x300`
    - `persisted_subcategories = 18`
    - `persisted_observations = 4748`
    - terminal progress stage = `web_visible`
- NAS control plane after validation:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - NAS workers now show only:
    - `scheduler`
    - `keyword-worker`
    - `sync-worker`
    - remote `category` worker with `node_role=remote_category`
- keyword protection remained green:
  - `/api/runs/summary` still reports `operator_quality_state = full`
  - `live_batch_state = idle`
  - `active_keyword_run_count = 0`

### Effect Boundary

- no schema change
- NAS local `category-worker` is no longer needed for steady-state execution
- remote category node is now the authoritative category executor in the live dual-node architecture
- NAS local `category-worker` remains available only as manual fallback

## 2026-04-14 05:05 - Keyword control r68 promoted as a business-oriented operator workflow

### Theme

Promote keyword control from source-scoped exclusion plumbing into an operator-facing keyword manager:
- disabled keywords now mean `remove from baseline + block future return flow`
- blocked roots now support both `exact` and `contains`
- crawler console now exposes active keyword search, batch disable, batch restore, and blocked-root management in a modal workflow

### What Changed

- Updated:
  - [ops/keyword_control_state.py](D:/claude%20noon%20v1/noon-selection-tool/ops/keyword_control_state.py)
  - keyword control state now persists:
    - `disabled_keywords`
    - `blocked_roots`
  - legacy `exclusions` remain compatible as exact disabled keywords
- Updated:
  - [web_beta/app.py](D:/claude%20noon%20v1/noon-selection-tool/web_beta/app.py)
  - added crawler-control APIs for:
    - `GET /api/crawler/keyword-controls/active-keywords`
    - `POST /api/crawler/keyword-controls/disable`
    - `POST /api/crawler/keyword-controls/restore`
    - `POST /api/crawler/keyword-controls/roots`
  - baseline add now immediately registers tracked keywords for the selected monitor config
- Updated:
  - [web_beta/static/app.js](D:/claude%20noon%20v1/noon-selection-tool/web_beta/static/app.js)
  - crawler control now exposes a modal keyword manager with:
    - `活跃关键词`
    - `新增关键词`
    - `禁用词`
    - `禁用词根`
  - the main operator actions are now:
    - `加入长期种子`
    - `批量禁用`
    - `新增禁用词根`
    - `批量恢复`
- Updated:
  - [tests/test_keyword_control_state.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_keyword_control_state.py)
  - [tests/test_keyword_runtime.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_keyword_runtime.py)
  - [tests/test_web_crawler_api.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_web_crawler_api.py)
  - [tests/test_web_phase0.py](D:/claude%20noon%20v1/noon-selection-tool/tests/test_web_phase0.py)
- Promoted NAS release:
  - `huihaokang-nas-20260414-keyword-control-r68`
- Deployment note:
  - NAS Container Manager initially accepted the project update while still running with unresolved DSN placeholders
  - the final production fix was to redeploy `huihaokang-stable` with a fully rendered compose payload so the live stack stayed on Postgres instead of falling back to SQLite

### Validation

- local:
  - `python -m unittest tests.test_keyword_control_state tests.test_keyword_runtime tests.test_web_crawler_api tests.test_web_phase0`
  - `python run_post_deploy_smoke.py --base-url http://127.0.0.1:8870`
- NAS API:
  - `/api/health = ok`
  - `/api/system/health = ok`
  - `/api/runs/summary = ok`
  - `/api/crawler/keyword-controls/active-keywords = 200`
  - `/api/crawler/keyword-controls` now returns:
    - `disabled_keywords`
    - `blocked_roots`
    - `effective_keyword_stats`
- NAS business-flow roundtrip:
  - baseline add/remove works
  - disabled keyword add/restore works
  - blocked-root `contains` add/delete works
  - temporary validation keywords were removed from `noon_stage.keywords` after the roundtrip to keep the live pool clean
- NAS browser screenshots:
  - [crawler-workspace.png](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/keyword_control_nas_r68/crawler-workspace.png)
  - [keyword-manager-overview.png](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/keyword_control_nas_r68/keyword-manager-overview.png)
  - [keyword-manager-roots.png](D:/claude%20noon%20v1/noon-selection-tool/output/playwright/keyword_control_nas_r68/keyword-manager-roots.png)

### Effect Boundary

- no schema change
- keyword control remains monitor-config scoped, not global
- the product decision principle is now explicit:
  - keyword control is not for managing a lexicon as an end in itself
  - it is for reducing low-value crawl and focusing runtime on terms more likely to help find target products
- NAS remained on the retained-data Postgres control plane after deployment
## 2026-04-20 18:30 - Git and Obsidian knowledge sync switched to single-source junction mode

### Theme

Remove duplicate knowledge copies and make Git + Obsidian read the same files:
- repo `knowledge/` becomes the only maintained knowledge source
- Vault no longer keeps a second physical copy of the six shared knowledge folders
- legacy duplicate files in the Vault root are removed

### What Changed

- Updated:
  - [scripts/link_repo_knowledge_into_obsidian.ps1](D:/claude%20noon%20v1/scripts/link_repo_knowledge_into_obsidian.ps1)
  - added support for:
    - replacing old Vault folders with directory junctions
    - removing legacy Vault root duplicates
    - removing deprecated Vault `workspace/` when not held open
    - removing temporary cutover backup folders after success
- Updated:
  - [OBSIDIAN_SYNC.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OBSIDIAN_SYNC.md)
  - now documents the active rule:
    - repo `knowledge/` is canonical
    - Vault shared folders should be junctions
    - do not maintain two physical copies
- Updated:
  - [GITHUB_OBSIDIAN_KNOWLEDGE_PLAN.md](D:/claude%20noon%20v1/noon-selection-tool/docs/GITHUB_OBSIDIAN_KNOWLEDGE_PLAN.md)
  - records the activated cutover mode and the post-cutover boundary
- Updated:
  - [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
  - current status now includes the Git/Obsidian single-source rule
- Executed cutover in:
  - `C:\Users\Admin\Documents\Obsidian Vault\noon`
  - the following folders were switched to junctions:
    - `系统架构知识库`
    - `可复用方法论`
    - `爬虫开发需求笔记`
    - `项目推进与调度`
    - `noon开发日记`
    - `平台佣金`
  - duplicate Vault root files removed:
    - `AGENTS.md`
    - `FBN模式费用&尺寸级别.md`
    - `FBP & FBN 的差异.md`
    - `noon-pricing-v6 拆解说明.md`

### Validation

- verified all six shared Vault folders are now `Junction`
- verified each junction points to repo `knowledge/`
- verified repo/vault file counts align for:
  - `architecture`
  - `methods`
  - `requirements`
  - `project-ops`
  - `dev-journal`
  - `pricing`
- verified hash-level alignment before cutover so the repo copy was the latest source

### Effect Boundary

- no code-runtime, crawler-runtime, or database contract changed
- this cutover only changes how shared docs/knowledge are stored and read
- legacy Vault `workspace/` remains deprecated; if it is held open by a running local process, deletion must wait until the handle is released
