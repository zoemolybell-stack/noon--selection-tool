param(
    [string]$RepoRoot = "D:\claude noon v1",
    [string]$VaultRoot = "C:\Users\Admin\Documents\Obsidian Vault\noon",
    [switch]$ReplaceExisting
)

$ErrorActionPreference = "Stop"

function New-KnowledgeJunction {
    param(
        [string]$Source,
        [string]$Target,
        [string]$BackupRoot,
        [switch]$AllowReplace
    )

    if (-not (Test-Path -LiteralPath $Source)) {
        throw "source path not found: $Source"
    }

    if (Test-Path -LiteralPath $Target) {
        $item = Get-Item -LiteralPath $Target -Force
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            Write-Host "Link already exists, skipping: $Target"
            return
        }

        if (-not $AllowReplace) {
            throw "target already exists: $Target ; rerun with -ReplaceExisting after backup confirmation"
        }

        New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null
        $backupTarget = Join-Path $BackupRoot ([IO.Path]::GetFileName($Target))
        Move-Item -LiteralPath $Target -Destination $backupTarget
        Write-Host "Moved existing target to backup: $backupTarget"
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Target) | Out-Null
    & cmd.exe /c "mklink /J ""$Target"" ""$Source""" | Out-Null
    Write-Host "Created junction: $Target -> $Source"
}

$backupRoot = Join-Path $VaultRoot ("_repo_knowledge_backup_" + (Get-Date -Format "yyyyMMdd-HHmmss"))

$mapping = @(
    @{ Source = (Join-Path $RepoRoot "knowledge\architecture"); Target = (Join-Path $VaultRoot "系统架构知识库") },
    @{ Source = (Join-Path $RepoRoot "knowledge\methods"); Target = (Join-Path $VaultRoot "可复用方法论") },
    @{ Source = (Join-Path $RepoRoot "knowledge\requirements"); Target = (Join-Path $VaultRoot "爬虫开发需求笔记") },
    @{ Source = (Join-Path $RepoRoot "knowledge\project-ops"); Target = (Join-Path $VaultRoot "项目推进与调度") },
    @{ Source = (Join-Path $RepoRoot "knowledge\dev-journal"); Target = (Join-Path $VaultRoot "noon开发日记") },
    @{ Source = (Join-Path $RepoRoot "knowledge\reference\pricing"); Target = (Join-Path $VaultRoot "平台佣金") }
)

foreach ($entry in $mapping) {
    New-KnowledgeJunction -Source $entry.Source -Target $entry.Target -BackupRoot $backupRoot -AllowReplace:$ReplaceExisting
}

[pscustomobject]@{
    repo_root = $RepoRoot
    vault_root = $VaultRoot
    replace_existing = [bool]$ReplaceExisting
    backup_root = $(if ($ReplaceExisting) { $backupRoot } else { "" })
} | ConvertTo-Json -Depth 4
