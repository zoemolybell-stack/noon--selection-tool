# NAS Postgres Rollback Checklist

## Preconditions

- rollback is justified by failed cutover or confirmed data corruption
- previous stable release directory is known
- SQLite snapshot path is known
- operator approves rollback

## Code Rollback First

1. Stop current stack
2. Switch to previous stable release directory
3. Repoint runtime DB config from Postgres DSNs back to SQLite paths
4. Start previous stable with:
   - `python deploy_nas_release.py --env-file .env.nas`

## Validation

1. `GET /api/health`
2. `GET /api/system/health`
3. `GET /api/tasks`
4. Confirm workers can heartbeat
5. Confirm warehouse read path is healthy

## Database Rollback Conditions

Only proceed with full SQLite runtime rollback when all are true:

- Postgres runtime is unusable
- issue cannot be repaired by rerun or fresh sync
- SQLite snapshot is known good
- operator explicitly approves SQLite runtime fallback

## After Rollback

1. Keep failed Postgres data directory intact for investigation
2. Keep cutover release directory intact
3. Record:
   - failed release version
   - rollback target release
   - snapshot used
   - reason for rollback
