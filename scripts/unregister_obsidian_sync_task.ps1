param(
    [string]$TaskPrefix = "Noon-Obsidian-Sync"
)

$ErrorActionPreference = "Stop"

$logonTask = "$TaskPrefix-AtLogon"
$dailyTask = "$TaskPrefix-Daily"

foreach ($taskName in @($logonTask, $dailyTask)) {
    try {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction Stop
        Write-Host "Removed task: $taskName"
    } catch {
        Write-Host "Task not found: $taskName"
    }
}
