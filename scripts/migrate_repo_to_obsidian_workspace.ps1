param(
    [string]$SourceRoot = "D:\claude noon v1",
    [string]$TargetRoot = "C:\Users\Admin\Documents\Obsidian Vault\noon\workspace\claude-noon-v1",
    [string]$NasBaseUrl = "http://192.168.100.20:8865",
    [switch]$SkipCategoryIdleCheck,
    [switch]$SkipJunction,
    [switch]$SkipDesktopLauncherRewrite,
    [switch]$SkipDocRewrite
)

$ErrorActionPreference = "Stop"

function Get-CategoryTaskSnapshot {
    param([string]$BaseUrl)
    $resp = Invoke-RestMethod -UseBasicParsing -Uri ($BaseUrl.TrimEnd("/") + "/api/tasks?worker_type=category&limit=50") -TimeoutSec 20
    $items = @($resp.items)
    $active = @($items | Where-Object { $_.status -in @("pending", "running", "leased", "retrying") })
    return [pscustomobject]@{
        Items = $items
        Active = $active
    }
}

function Invoke-RobocopyMirror {
    param(
        [string]$From,
        [string]$To
    )
    New-Item -ItemType Directory -Force -Path $To | Out-Null
    $args = @(
        $From,
        $To,
        "/MIR",
        "/COPY:DAT",
        "/DCOPY:DAT",
        "/R:1",
        "/W:1",
        "/NFL",
        "/NDL",
        "/NP",
        "/XJ"
    )
    & robocopy.exe @args | Out-Null
    $exitCode = $LASTEXITCODE
    if ($exitCode -ge 8) {
        throw "robocopy failed with exit code $exitCode"
    }
}

function Rewrite-DesktopLauncher {
    param([string]$TargetRootPath)
    $launcherPath = "C:\Users\Admin\Desktop\Noon Remote Category Controller.cmd"
    if (-not (Test-Path -LiteralPath $launcherPath)) {
        return
    }
    $content = @(
        "@echo off",
        "setlocal",
        "set PYTHONW=$TargetRootPath\venv\Scripts\pythonw.exe",
        "if exist ""%PYTHONW%"" (",
        "  ""%PYTHONW%"" ""$TargetRootPath\codex_remote_category_controller.py""",
        ") else (",
        "  pythonw ""$TargetRootPath\codex_remote_category_controller.py""",
        ")",
        "endlocal"
    )
    Set-Content -LiteralPath $launcherPath -Value $content -Encoding UTF8
}

function Rewrite-Docs {
    param(
        [string]$OldRoot,
        [string]$NewRoot
    )
    $docsRoots = @(
        (Join-Path $NewRoot "noon-selection-tool\docs"),
        (Join-Path $NewRoot "AGENTS.md")
    )
    $oldUrlRoot = ($OldRoot -replace "\\", "/") -replace " ", "%20"
    $newUrlRoot = ($NewRoot -replace "\\", "/") -replace " ", "%20"
    foreach ($path in $docsRoots) {
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            $text = Get-Content -LiteralPath $path -Raw
            $text = $text.Replace($OldRoot, $NewRoot).Replace($oldUrlRoot, $newUrlRoot)
            Set-Content -LiteralPath $path -Value $text -Encoding UTF8
            continue
        }
        if (Test-Path -LiteralPath $path -PathType Container) {
            Get-ChildItem -LiteralPath $path -Recurse -File -Include *.md | ForEach-Object {
                $text = Get-Content -LiteralPath $_.FullName -Raw
                $updated = $text.Replace($OldRoot, $NewRoot).Replace($oldUrlRoot, $newUrlRoot)
                if ($updated -ne $text) {
                    Set-Content -LiteralPath $_.FullName -Value $updated -Encoding UTF8
                }
            }
        }
    }
}

if (-not (Test-Path -LiteralPath $SourceRoot)) {
    throw "source root not found: $SourceRoot"
}
if (Test-Path -LiteralPath $TargetRoot) {
    throw "target root already exists: $TargetRoot"
}

if (-not $SkipCategoryIdleCheck) {
    $taskSnapshot = Get-CategoryTaskSnapshot -BaseUrl $NasBaseUrl
    if ($taskSnapshot.Active.Count -gt 0) {
        $taskList = ($taskSnapshot.Active | Select-Object id, display_name, status | ConvertTo-Json -Depth 4)
        throw "category tasks still active; aborting migration: $taskList"
    }
}

$targetParent = Split-Path -Parent $TargetRoot
New-Item -ItemType Directory -Force -Path $targetParent | Out-Null

$backupRoot = "$SourceRoot.pre-migration-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss")

Write-Host "Copying repository to target..."
Invoke-RobocopyMirror -From $SourceRoot -To $TargetRoot

if (-not $SkipDocRewrite) {
    Write-Host "Rewriting absolute doc paths..."
    Rewrite-Docs -OldRoot $SourceRoot -NewRoot $TargetRoot
}

if (-not $SkipDesktopLauncherRewrite) {
    Write-Host "Rewriting desktop launcher..."
    Rewrite-DesktopLauncher -TargetRootPath $TargetRoot
}

Write-Host "Stopping local remote category runtime..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $TargetRoot "noon-selection-tool\tools\manage_remote_category_runtime.ps1") -Action stop | Out-Null

Write-Host "Moving old source to backup path..."
Move-Item -LiteralPath $SourceRoot -Destination $backupRoot

if (-not $SkipJunction) {
    Write-Host "Creating compatibility junction..."
    & cmd.exe /c "mklink /J ""$SourceRoot"" ""$TargetRoot""" | Out-Null
}

Write-Host "Re-installing local auto-heal tasks from new root..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $TargetRoot "noon-selection-tool\tools\manage_remote_category_runtime.ps1") -Action install-autostart | Out-Null

Write-Host "Starting runtime from new root..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $TargetRoot "noon-selection-tool\tools\manage_remote_category_runtime.ps1") -Action ensure | Out-Null

[pscustomobject]@{
    source_root = $SourceRoot
    target_root = $TargetRoot
    backup_root = $backupRoot
    junction_created = (-not $SkipJunction)
    doc_rewrite = (-not $SkipDocRewrite)
    desktop_launcher_rewritten = (-not $SkipDesktopLauncherRewrite)
} | ConvertTo-Json -Depth 4
