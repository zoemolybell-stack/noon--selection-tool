# NAS Release and Rollback

## Release Model

- all releases are built on `local-beta`
- all releases are manually promoted to `nas-stable`
- NAS never auto-follows local source changes
- NAS keeps its existing data volumes
- database cutover is performed on NAS in place:
  - NAS SQLite -> NAS Postgres

## Release Steps

1. Confirm local release gates are green
   - local UI stabilization self-check
   - local human review
   - Python compile
   - compose smoke
   - release bundle build
2. Build release bundle:
   - `python build_release_bundle.py`
3. Record release version and manifest
4. Upload release zip and manifest to NAS
5. Extract to a new release directory
6. Snapshot NAS SQLite files
7. Stop NAS writers
8. Start Postgres
9. Run migration + validation
10. Update `.env.nas` to Postgres DSNs
11. Deploy:
   - `python deploy_nas_release.py --env-file .env.nas`
12. Run post-deploy smoke
13. Reopen Cloudflare only after smoke passes

## Release Artefacts

Each release must include:

- release zip
- release manifest JSON
- version tag
- migration scripts
- validation scripts
- current runbook and rollback docs

## Rollback Principle

- roll back code first
- keep runtime data volumes unless there is confirmed data corruption
- keep SQLite snapshots from the cutover window
- do not delete Postgres or SQLite volumes during incident response

## Default Rollback Path

1. Identify previous stable release directory
2. Stop the current stack
3. Switch to previous release directory
4. Repoint runtime DB configuration from Postgres DSNs back to SQLite paths
5. Start previous version:
   - `python deploy_nas_release.py --env-file .env.nas`
6. Re-run smoke:
   - `/api/health`
   - `/api/system/health`
   - `/api/tasks`
7. Reopen Cloudflare after verification

## When Database Rollback Is Allowed

Database rollback is allowed only when all are true:

- issue is confirmed as data corruption, not just code regression
- current warehouse cannot be repaired by rerun or fresh sync
- a known-good SQLite snapshot exists
- operator explicitly approves reverting runtime DB config to SQLite

## SQLite Snapshot Rule

Every cutover window must create and retain:

- SQLite snapshot directory
- release version used for cutover
- Postgres DSNs used for cutover
- rollback target release

Without a recorded snapshot, no production cutover is valid.

## Minimum Release Record

For every NAS release, record:

- version
- date/time
- release owner
- changed modules
- validation completed
- rollback target version
- SQLite snapshot location
- Postgres data directory
