# OBSIDIAN SYNC

Last Updated: 2026-04-20

## Purpose

Keep the Obsidian knowledge view aligned with the Git-managed knowledge base without turning the Vault into a second source of truth.

The canonical sources are:

- Git code and repo docs
- repo-managed `knowledge/` Markdown

Obsidian is a knowledge UI, not the primary storage for the full repo.

## Canonical Structure

The current architecture is:

- repo docs: `D:\claude noon v1\noon-selection-tool\docs\`
- repo knowledge: `D:\claude noon v1\knowledge\`
- optional Vault mirror: `C:\Users\Admin\Documents\Obsidian Vault\noon\`

## Managed Notes

`scripts/sync_obsidian_context.py` now does two things:

1. Generate managed notes into repo knowledge:
   - `knowledge\architecture\`
   - `knowledge\methods\`
2. Optionally mirror the same managed notes into legacy Vault folders:
   - `系统架构知识库\`
   - `可复用方法论\`

The repo copy is authoritative.

## What Obsidian Should Contain

Recommended:

- architecture knowledge
- reusable methods
- requirements notes
- project ops notes
- dev journal
- pricing / reference notes

Not recommended:

- full repo workspace copies
- `runtime_data/`
- `data/`
- `logs/`
- `tmp/`
- `venv/`
- database files

## Manual Run

From repo root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_obsidian_sync.ps1
```

Repo-only refresh:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_obsidian_sync.ps1 --skip-vault-mirror
```

Dry run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_obsidian_sync.ps1 --dry-run
```

## Automatic Refresh

Register Windows scheduled tasks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_obsidian_sync_task.ps1
```

This still keeps the old task name for compatibility, but its role is now:

- refresh repo-managed knowledge notes
- optionally mirror managed notes to the legacy Vault folders

## Recommended Workflow

1. Update repo docs or repo knowledge first
2. Run the knowledge refresh script
3. Open the repo `knowledge/` directly in Obsidian, or mirror / link it into the Vault
4. Keep personal drafts outside the Git-managed paths until they are ready to share

## Deprecated Path

Do not continue using full repo migration into the Vault.

`scripts/migrate_repo_to_obsidian_workspace.ps1` is deprecated. Use:

- `scripts/run_obsidian_sync.ps1`
- `scripts/link_repo_knowledge_into_obsidian.ps1`
- [GITHUB_OBSIDIAN_KNOWLEDGE_PLAN.md](D:/claude%20noon%20v1/noon-selection-tool/docs/GITHUB_OBSIDIAN_KNOWLEDGE_PLAN.md)
