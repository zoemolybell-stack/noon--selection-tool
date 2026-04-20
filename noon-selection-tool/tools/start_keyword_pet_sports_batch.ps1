$ErrorActionPreference = "Stop"

$root = "D:\claude noon v1\noon-selection-tool"
$python = (Get-Command python).Source
$seedFile = Join-Path $root "config\keyword_baseline_pet_sports_batch.txt"
$monitorConfig = Join-Path $root "config\keyword_monitor_pet_sports_batch.json"

Set-Location $root

Write-Host "[keyword-batch] register seeds from $seedFile"
& $python (Join-Path $root "keyword_main.py") `
    --step register `
    --keywords-file $seedFile `
    --tracking-mode tracked `
    --priority 25

Write-Host "[keyword-batch] start monitor with $monitorConfig"
& $python (Join-Path $root "run_keyword_monitor.py") `
    --monitor-config $monitorConfig `
    --noon-count 30 `
    --amazon-count 30
