# NAS Deployment Runbook

## Goal

Deploy the current `local-beta` codebase to `nas-stable` while keeping NAS data in place and cutting NAS from SQLite to Postgres during the maintenance window.

## Operating Model

- `local-beta`
  - development, validation, and release build source
  - no production data import target
- `nas-stable`
  - operator environment
  - keeps existing NAS data volumes
  - receives only explicit release bundles
  - migrates existing NAS SQLite data to NAS Postgres in place

## Hard Rules

- Do not copy local beta SQLite databases into NAS stable.
- Do not cold-start NAS when the target strategy is retained-data cutover.
- Do not expose NAS ports directly to the public internet.
- Web reads the promoted warehouse only.
- `ops` remains control-plane only.
- Category and keyword workers must use isolated browser profiles.
- All secrets stay outside the repository.
- Cloudflare traffic is reopened only after post-deploy smoke passes.

## Release Gates Before Any NAS Action

All must hold before a release bundle is promoted:

1. local beta self-check is green
2. local human review is complete
3. release bundle is built from the frozen local baseline
4. release bundle excludes:
   - local databases
   - local runtime data
   - local browser profiles
5. migration and validation scripts are present in the bundle

## Required NAS Environment Configuration

Audit `.env.nas` before the cutover window:

- host data paths
  - `NOON_HOST_DATA_DIR`
  - `NOON_HOST_RUNTIME_DATA_DIR`
  - `NOON_HOST_BROWSER_PROFILES_DIR`
  - `NOON_HOST_LOGS_DIR`
  - `NOON_HOST_POSTGRES_DIR`
- Postgres service
  - `NOON_POSTGRES_DB`
  - `NOON_POSTGRES_USER`
  - `NOON_POSTGRES_PASSWORD`
- runtime DB wiring
  - `NOON_OPS_DB`
  - `NOON_PRODUCT_STORE_DB`
  - `NOON_WAREHOUSE_DB`
  - `NOON_OPS_DATABASE_URL`
  - `NOON_PRODUCT_STORE_DATABASE_URL`
  - `NOON_WAREHOUSE_DATABASE_URL`
- scheduler mode
  - `NOON_SCHEDULER_RUNTIME`

Rules:
- the three `NOON_*_DATABASE_URL` values are the final runtime targets
- the three SQLite path values remain present for migration and rollback
- all final DSNs must point to Postgres, not SQLite
- `NOON_SCHEDULER_RUNTIME=host` is the NAS stable default

## Migration Sources

The migration source is NAS current SQLite data only:

- `ops.db`
- `product_store.db`
- `warehouse.db`

Use file snapshots as the source during the window. Do not migrate directly from live SQLite files while workers are still writing.

## Maintenance Window Flow

### 1. Prepare release directory

1. Build release bundle on local beta:
   - `python build_release_bundle.py`
2. Copy release zip and manifest to NAS
3. Extract to a new release directory, for example:
   - `/volume1/apps/huihaokang-erp/releases/<version>/`

### 2. Freeze NAS writes

1. Stop:
   - `scheduler`
   - `keyword-worker`
   - `category-worker`
   - `sync-worker`
2. Web may remain up briefly for operator notice, then move to maintenance or stop before migration
3. Confirm no active task is still writing

### 3. Snapshot SQLite

1. Copy current SQLite files into a timestamped backup directory
2. Do not overwrite the live files
3. Record backup paths for rollback

### 4. Start Postgres

1. Confirm `NOON_HOST_POSTGRES_DIR` exists
2. Start only the `postgres` service first
3. Verify health:
   - `docker compose --env-file .env.nas ps`
   - Postgres healthcheck must be healthy

### 5. Run migration

Run in this order:

1. `ops`
2. `product_store`
3. `warehouse`

Each migration must use:
- SQLite snapshot file as source
- Postgres DSN as target

### 6. Run validation

Validation must cover:
- row count parity
- primary/unique key parity
- key table spot-check
- worker/task/control-plane reads
- warehouse read-path smoke

### 7. Cut runtime to Postgres

1. Update `.env.nas`:
   - `NOON_OPS_DATABASE_URL`
   - `NOON_PRODUCT_STORE_DATABASE_URL`
   - `NOON_WAREHOUSE_DATABASE_URL`
2. Keep SQLite path variables populated for rollback reference
3. Do not delete SQLite files

### 8. Deploy new stable

Use only:
- `python deploy_nas_release.py --env-file .env.nas`

Do not start production with ad hoc `docker compose up` from arbitrary directories.

If `NOON_SCHEDULER_RUNTIME=host`:
- `deploy_nas_release.py` will not start the container `scheduler`
- any old `huihaokang-scheduler` container is removed during deployment

### 9. Post-deploy smoke

Verify:
- `GET /api/health`
- `GET /api/system/health`
- `GET /api/tasks`
- one manual `warehouse_sync`
- one manual `keyword_once`
- one manual `category_single` or `category_ready_scan`

### 10. Restore operator traffic

Only after smoke is green:

1. start scheduler and workers
2. confirm worker heartbeat
3. open Cloudflare traffic
4. confirm canonical `crawl_plans` auto-dispatch is enabled

## Service Order

1. `postgres`
2. `web`
3. `scheduler`
4. `sync-worker`
5. `keyword-worker`
6. `category-worker`
7. `cloudflared` only after internal validation

## Cloudflare Reopen Rules

After internal NAS validation:

1. confirm tunnel token in `.env.nas`
2. start `cloudflared` through the stable deployment entrypoint
3. verify authenticated web access
4. reopen public operator traffic last

## Stable Deployment Identity

- compose project name:
  - `huihaokang-stable`
- Docker network name:
  - `huihaokang-stable-net`
- deployment entrypoint:
  - `python deploy_nas_release.py --env-file .env.nas`

## Companion Docs

- [NAS_POSTGRES_CUTOVER_CHECKLIST.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_POSTGRES_CUTOVER_CHECKLIST.md)
- [NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
- [NAS_POSTGRES_ROLLBACK_CHECKLIST.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_POSTGRES_ROLLBACK_CHECKLIST.md)
