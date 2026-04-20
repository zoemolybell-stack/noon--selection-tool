# Ops Retention Policy

## Goal

Keep NAS control-plane history useful without letting `ops` fill up with stale worker rows, stranded running tasks, or low-value terminal noise.

This policy is intentionally conservative:
- preserve plan-linked history by default
- separate worker cleanup from task cleanup
- treat report/watchdog as read-only observers, not auto-healers

## Cleanup Modes

The cleanup tool supports four explicit modes:

- `full`
  - delete stale worker rows
  - cancel stranded running tasks whose lease owner is stale, missing, expired, or mismatched
  - prune eligible terminal history
- `workers`
  - delete stale worker rows only
- `running`
  - cancel stranded running tasks only
- `history`
  - prune terminal history only

Use `history` when you want retention cleanup without touching live worker state.
Use `workers` when you only want to remove stale worker rows.
Use `running` when you only want to clear stranded running tasks, including missing-worker and lease-mismatch cases.

## Default Retention Tiers

### 1. Live worker rows

- keep active workers
- remove only worker rows whose heartbeat is older than `6h`

### 2. Orphan terminal tasks

Definition:
- terminal task
- not linked to `plan_id`
- not linked to `round_id`
- not linked to `round_item_id`

Retention:
- keep for `7d`
- then prune

### 3. Smoke or dev terminal tasks

Definition:
- `created_by` contains one of:
  - `smoke`
  - `predeploy`
  - `main_window_sleep_plan`

Retention:
- keep for `6h`
- then prune

### 4. Plan-linked failed/cancelled/skipped tasks

Definition:
- linked to a plan or round
- terminal status in:
  - `failed`
  - `cancelled`
  - `skipped`

Retention:
- keep for `5d`
- only prune when `--include-plan-linked-terminal` is explicitly enabled
- archive the candidate list to JSON before apply

### 5. Plan-linked completed tasks

- keep by default
- do not auto-prune in routine cleanup
- if later cleanup is required, make it a dedicated archival task, not part of normal ops cleanup

## Cleanup Tool

Tool:
- [tools/cleanup_ops_history.py](D:/claude%20noon%20v1/noon-selection-tool/tools/cleanup_ops_history.py)

Routine dry-run:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas
```

Workers only:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --workers-only
```

Stranded running tasks only:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --running-only
```

History only:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --history-only
```

Routine apply:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --apply
```

Plan-linked cleanup dry-run:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --history-only --include-plan-linked-terminal --output-json cleanup_plan.json
```

Plan-linked cleanup apply:

```powershell
python tools/cleanup_ops_history.py --env-file .env.nas --history-only --include-plan-linked-terminal --output-json cleanup_plan.json --apply
```

## Safety Rules

- always back up `ops` before `--apply`
- use `--output-json` when deleting plan-linked history
- do not delete running or queued tasks unless you intentionally chose `--running-only` or `--full`
- do not delete completed plan-linked history as part of routine cleanup
- do not mix cleanup with schema changes or release cutover

## Watchdog Boundary

The watchdog is read-only.

It should:
- inspect `/api/health`
- inspect `/api/system/health`
- flag stale workers
- flag stranded running tasks
- flag missing or stale daily reports

It should not delete or mutate records.
