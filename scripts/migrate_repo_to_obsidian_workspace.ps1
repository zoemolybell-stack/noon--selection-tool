param()

$ErrorActionPreference = "Stop"

$message = @"
This script is deprecated.

The project no longer moves the full Git repository into the Obsidian Vault.

Current architecture:
- Git / GitHub worktree is the canonical source for code and knowledge files
- Obsidian is a knowledge UI over repo-managed Markdown
- runtime_data / data / logs / tmp / venv remain outside the Vault

Use these entry points instead:
- scripts\run_obsidian_sync.ps1
- scripts\link_repo_knowledge_into_obsidian.ps1
- noon-selection-tool\docs\GITHUB_OBSIDIAN_KNOWLEDGE_PLAN.md
"@

throw $message
