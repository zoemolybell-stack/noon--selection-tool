# Postgres Phase 2 Local

## Goal

Validate the `ops` control plane on Postgres before buying the new compute host.

## Scope

This phase covers only:

- `tasks`
- `task_runs`
- `workers`
- `crawl_plans`
- `crawl_rounds`

`product_store` and `warehouse` remain SQLite in this phase.

## Prerequisites

1. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

2. Start a local Postgres instance.

If Docker is available:

```powershell
docker compose -f docker-compose.postgres-local.yml up -d
```

Default DSN:

```text
postgresql://noon:noon_local_dev@localhost:5433/noon_ops
```

If native PostgreSQL is already installed on `5432`, keep Docker Compose on `5433`.

## Smoke test

```powershell
python run_ops_postgres_smoke.py --postgres-dsn postgresql://noon:noon_local_dev@localhost:5433/noon_ops
```

Expected result:

- backend = `postgres`
- smoke task can be created, leased, run, and completed
- crawl plan can be created

## SQLite to Postgres migration

```powershell
python tools/migrate_ops_sqlite_to_postgres.py ^
  --sqlite-db data\\ops\\ops.db ^
  --postgres-dsn postgresql://noon:noon_local_dev@localhost:5433/noon_ops ^
  --truncate
```

This copies current `ops.db` state into Postgres.

## Environment switch

To run the control plane on Postgres:

```powershell
$env:NOON_OPS_DATABASE_URL="postgresql://noon:noon_local_dev@localhost:5433/noon_ops"
```

Keep these on SQLite for now:

- `NOON_WAREHOUSE_DB`
- `NOON_PRODUCT_STORE_DB`

## Exit criteria

- `run_ops_postgres_smoke.py` passes
- existing `ops.db` can be migrated
- local Web/task API still works with `NOON_OPS_DATABASE_URL` set
