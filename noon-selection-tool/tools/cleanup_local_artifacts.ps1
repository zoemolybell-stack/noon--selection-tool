param(
    [switch]$KeepLatestGreenStabilizationRun = $true
)

$ErrorActionPreference = 'Stop'

$repoRoot = 'D:\claude noon v1'
$toolRoot = Join-Path $repoRoot 'noon-selection-tool'

function Remove-IfExists {
    param(
        [string]$Path
    )
    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
}

function Remove-ChildrenByPattern {
    param(
        [string]$Base,
        [string[]]$Patterns
    )
    foreach ($pattern in $Patterns) {
        Get-ChildItem -LiteralPath $Base -Force -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like $pattern } |
            ForEach-Object {
                if ($_.PSIsContainer) {
                    Remove-Item -LiteralPath $_.FullName -Recurse -Force
                } else {
                    Remove-Item -LiteralPath $_.FullName -Force
                }
            }
    }
}

function Remove-RepoPyCache {
    param(
        [string]$Base
    )
    Get-ChildItem -LiteralPath $Base -Recurse -Force -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -eq '__pycache__' } |
        Sort-Object FullName -Descending |
        ForEach-Object {
            if (Test-Path -LiteralPath $_.FullName) {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            }
        }
}

# Root scratch
Remove-ChildrenByPattern -Base $repoRoot -Patterns @('tmp_*', 'tmp_chrome_*', '__pycache__')

# Repo-local scratch
Remove-IfExists (Join-Path $toolRoot '.playwright-cli')

Get-ChildItem -LiteralPath $toolRoot -Recurse -Force -Directory -ErrorAction SilentlyContinue |
    Out-Null
Remove-RepoPyCache -Base $toolRoot

$toolRootScratchNames = @(
    '!!document.querySelector(.product-thumb-shell.placeholder)',
    '((e',
    '(element.setAttribute(data-preview-image',
    '({tag',
    'document.querySelectorAll(.product-thumb-shell[data-preview-image]).length',
    'element.setAttribute(data-test',
    'getComputedStyle(document.querySelector(#view-products)).paddingRight'
)
foreach ($name in $toolRootScratchNames) {
    $path = Join-Path $toolRoot $name
    if (Test-Path -LiteralPath $path) {
        Remove-Item -LiteralPath $path -Force
    }
}

# Data scratch only; do not touch stable data or report history.
Remove-ChildrenByPattern -Base (Join-Path $toolRoot 'data') -Patterns @('tmp_*')

# Playwright output retention
$playwrightRoot = Join-Path $toolRoot 'output\playwright'
if (Test-Path -LiteralPath $playwrightRoot) {
    Get-ChildItem -LiteralPath $playwrightRoot -Force |
        Where-Object { $_.Name -ne 'web_beta_stabilization' } |
        ForEach-Object {
            if ($_.PSIsContainer) {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            } else {
                Remove-Item -LiteralPath $_.FullName -Force
            }
        }
}

$stabilizationRoot = Join-Path $playwrightRoot 'web_beta_stabilization'
if ((Test-Path -LiteralPath $stabilizationRoot) -and $KeepLatestGreenStabilizationRun) {
    $latest = Get-ChildItem -LiteralPath $stabilizationRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
    if ($latest) {
        Get-ChildItem -LiteralPath $stabilizationRoot -Directory |
            Where-Object { $_.FullName -ne $latest.FullName } |
            ForEach-Object { Remove-Item -LiteralPath $_.FullName -Recurse -Force }
    }
}

Write-Host "cleanup_local_artifacts.ps1 complete"
