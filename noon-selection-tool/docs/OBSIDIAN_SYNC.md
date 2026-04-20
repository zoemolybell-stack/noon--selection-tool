# OBSIDIAN SYNC

Last Updated: 2026-04-10

## Purpose

Keep the Obsidian Vault copy of the Noon project aligned with the repo without turning the Vault into a second source of truth.

The repo remains authoritative.

The Vault is for:
- fast architecture recall
- current project context
- reusable crawler / ERP / ops patterns

## Source of Truth

The sync script reads from:
- [PROJECT_WHITEPAPER.md](D:/claude%20noon%20v1/noon-selection-tool/docs/PROJECT_WHITEPAPER.md)
- [DEV_HANDOFF.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_HANDOFF.md)
- [DEV_COLLAB_LOG.md](D:/claude%20noon%20v1/noon-selection-tool/docs/DEV_COLLAB_LOG.md)
- [CODEBASE_MAP.md](D:/claude%20noon%20v1/noon-selection-tool/docs/CODEBASE_MAP.md)
- [NAS_DEPLOYMENT_RUNBOOK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_DEPLOYMENT_RUNBOOK.md)
- [NAS_RELEASE_ROLLBACK.md](D:/claude%20noon%20v1/noon-selection-tool/docs/NAS_RELEASE_ROLLBACK.md)
- [OPS_RETENTION_POLICY.md](D:/claude%20noon%20v1/noon-selection-tool/docs/OPS_RETENTION_POLICY.md)

## Generated Areas

The script generates and overwrites only managed notes:

- `C:\Users\Admin\Documents\Obsidian Vault\Noon\系统架构知识库\`
- `C:\Users\Admin\Documents\Obsidian Vault\Noon\可复用方法论\`

It does not touch:
- `C:\Users\Admin\Documents\Obsidian Vault\Noon\爬虫开发需求笔记\`
- `C:\Users\Admin\Documents\Obsidian Vault\Noon\noon开发日记\`

## Manual Run

From repo root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_obsidian_sync.ps1
```

Dry run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_obsidian_sync.ps1 --dry-run
```

## Automatic Sync

Register Windows scheduled tasks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_obsidian_sync_task.ps1
```

This creates:
- one task at user logon
- one daily task at `09:00`

Remove the tasks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\unregister_obsidian_sync_task.ps1
```

## Recommended Workflow

1. Update repo docs first
2. Run the sync script
3. Review generated Vault notes
4. Keep manual notes only for operator thoughts, open questions, and daily journaling

## Design Rule

Do not edit generated Obsidian notes as if they were authoritative design docs.

If the project truth changes:
- update repo docs
- rerun sync
