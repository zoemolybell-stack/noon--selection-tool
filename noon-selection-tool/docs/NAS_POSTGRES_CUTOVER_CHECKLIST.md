# NAS Postgres Cutover Checklist

## Preconditions

- local UI stabilization self-check is green
- local human review is complete
- release bundle and manifest are built
- `.env.nas` has been audited
- maintenance window is approved

## Preflight

1. Confirm release version and target release directory
2. Confirm SQLite source files:
   - `ops.db`
   - `product_store.db`
   - `warehouse.db`
3. Confirm Postgres data directory exists
4. Confirm the three Postgres DSNs are prepared
5. Confirm rollback target release directory
6. Confirm Cloudflare traffic can stay closed until smoke passes
7. Confirm `NOON_SCHEDULER_RUNTIME=host` for NAS stable

## Freeze

1. Stop `scheduler`
2. Stop `keyword-worker`
3. Stop `category-worker`
4. Stop `sync-worker`
5. Confirm no active write task remains

## Backup

1. Create timestamped snapshot directory
2. Copy SQLite files into snapshot directory
3. Record snapshot paths
4. Do not overwrite live SQLite files

## Migration

1. Start `postgres`
2. Confirm Postgres health is green
3. Run:
   - `ops` migration
   - `product_store` migration
   - `warehouse` migration
4. Run:
   - `ops` validation
   - `product_store` validation
   - `warehouse` validation

## Runtime Cutover

1. Update `.env.nas`:
   - `NOON_OPS_DATABASE_URL`
   - `NOON_PRODUCT_STORE_DATABASE_URL`
   - `NOON_WAREHOUSE_DATABASE_URL`
2. Keep SQLite path variables in place
3. Deploy release with:
   - `python deploy_nas_release.py --env-file .env.nas`

## Smoke

1. `GET /api/health`
2. `GET /api/system/health`
3. `GET /api/tasks`
4. Manual `warehouse_sync`
5. Manual `keyword_once`
6. Manual `category_single` or `category_ready_scan`
7. Confirm worker heartbeat

## Restore Traffic

1. Start `scheduler`, `keyword-worker`, `category-worker`, `sync-worker`
2. Reopen Cloudflare
3. Confirm canonical `crawl_plans` auto-dispatch is enabled
