param(
    [string]$TaskPrefix = "Noon-Obsidian-Sync",
    [string]$DailyStart = "09:00"
)

$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$runner = Join-Path $scriptRoot "run_obsidian_sync.cmd"

if (-not (Test-Path $runner)) {
    throw "Runner script not found: $runner"
}

$logonTask = "$TaskPrefix-AtLogon"
$dailyTask = "$TaskPrefix-Daily"
$dailyStartTime = [datetime]$DailyStart

$action = New-ScheduledTaskAction -Execute $runner
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn
$dailyTrigger = New-ScheduledTaskTrigger -Daily -At $dailyStartTime

Register-ScheduledTask -TaskName $logonTask -Action $action -Trigger $logonTrigger -Force | Out-Null
Register-ScheduledTask -TaskName $dailyTask -Action $action -Trigger $dailyTrigger -Force | Out-Null

Write-Host "Registered task: $logonTask"
Write-Host "Registered task: $dailyTask"
Write-Host "Runner: $runner"
