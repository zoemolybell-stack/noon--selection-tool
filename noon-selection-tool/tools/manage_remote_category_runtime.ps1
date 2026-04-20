param(
    [ValidateSet("status", "ensure", "install-autostart", "launch-deep-scan", "launch-all-categories-scan", "stop")]
    [string]$Action = "status"
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "docker-compose.category-node.yml"
$envFile = Join-Path $projectRoot ".env.category-node"
$chromeScript = Join-Path $PSScriptRoot "start_remote_category_host_browser.ps1"
$logDir = Join-Path $projectRoot "logs"
$logPath = Join-Path $logDir "remote_category_runtime.log"
$runtimeDir = Join-Path $env:LOCALAPPDATA "NoonRuntime"
$defaultNasBaseUrl = "http://192.168.100.20:8865"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $logPath -Value $line
}

function Get-EnvMap {
    $map = @{}
    if (-not (Test-Path -LiteralPath $envFile)) {
        return $map
    }
    foreach ($line in Get-Content -LiteralPath $envFile) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }
        $idx = $trimmed.IndexOf("=")
        if ($idx -lt 1) {
            continue
        }
        $key = $trimmed.Substring(0, $idx).Trim()
        $value = $trimmed.Substring($idx + 1).Trim()
        $map[$key] = $value
    }
    return $map
}

function Get-NasBaseUrl {
    $envMap = Get-EnvMap
    if ($envMap.ContainsKey("NOON_REMOTE_CATEGORY_NAS_BASE_URL")) {
        return $envMap["NOON_REMOTE_CATEGORY_NAS_BASE_URL"]
    }
    return $defaultNasBaseUrl
}

function Get-DockerComposeArgs {
    return @(
        "compose",
        "-f", $composeFile,
        "--env-file", $envFile
    )
}

function Invoke-DockerCompose {
    param(
        [string[]]$ExtraArgs,
        [switch]$IgnoreErrors
    )
    $args = @()
    $args += Get-DockerComposeArgs
    $args += $ExtraArgs
    Write-Log ("docker " + ($args -join " "))
    $previousPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & docker @args 2>&1
    } finally {
        $ErrorActionPreference = $previousPref
    }
    $exitCode = $LASTEXITCODE
    if (($exitCode -ne 0) -and (-not $IgnoreErrors)) {
        throw ("docker compose failed ({0}): {1}" -f $exitCode, ($output -join [Environment]::NewLine))
    }
    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = ($output -join [Environment]::NewLine)
    }
}

function Invoke-JsonGet {
    param([string]$Url)
    try {
        return Invoke-RestMethod -UseBasicParsing -Uri $Url -TimeoutSec 10
    } catch {
        return $null
    }
}

function Test-ChromeReady {
    $port = ${env:NOON_REMOTE_CATEGORY_CDP_PORT}
    if (-not $port) {
        $port = "9222"
    }
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri ("http://127.0.0.1:{0}/json/version" -f $port) -TimeoutSec 3
        return ($resp.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Get-ComposeStatus {
    $result = Invoke-DockerCompose -ExtraArgs @("ps", "--format", "json") -IgnoreErrors
    if ($result.ExitCode -ne 0 -or -not $result.Output.Trim()) {
        return @()
    }
    $items = @()
    foreach ($line in ($result.Output -split "`r?`n")) {
        if (-not $line.Trim()) {
            continue
        }
        try {
            $items += ($line | ConvertFrom-Json)
        } catch {
        }
    }
    return $items
}

function Get-RuntimeStatus {
    $compose = Get-ComposeStatus
    $composeMap = @{}
    foreach ($item in $compose) {
        if ($item.Name) {
            $composeMap[$item.Name] = $item
        }
    }

    $nasBaseUrl = Get-NasBaseUrl
    $systemHealth = Invoke-JsonGet -Url ($nasBaseUrl.TrimEnd("/") + "/api/system/health")
    $workers = @()
    if ($systemHealth -and $systemHealth.ops -and $systemHealth.ops.workers) {
        $workers = @($systemHealth.ops.workers)
    }
    $remoteWorker = $workers | Where-Object { $_.node_role -eq "remote_category" } | Select-Object -First 1

    $status = [ordered]@{
        chrome_ready = Test-ChromeReady
        compose_services = $compose
        tunnel_status = if ($composeMap.Contains("huihaokang-remote-category-db-tunnel")) { $composeMap["huihaokang-remote-category-db-tunnel"].State } else { "" }
        worker_status = if ($composeMap.Contains("huihaokang-remote-category-worker")) { $composeMap["huihaokang-remote-category-worker"].State } else { "" }
        nas_base_url = $nasBaseUrl
        nas_api_ok = ($null -ne $systemHealth -and $systemHealth.status -eq "ok")
        remote_worker_heartbeat_present = ($null -ne $remoteWorker)
        remote_worker_status = if ($remoteWorker) { $remoteWorker.status } else { "" }
        remote_worker_name = if ($remoteWorker) { $remoteWorker.worker_name } else { "" }
        remote_worker_host = if ($remoteWorker) { $remoteWorker.node_host } else { "" }
        stack_ready = $false
    }
    $status.stack_ready = [bool](
        $status.chrome_ready -and
        ($status.tunnel_status -match "running") -and
        ($status.worker_status -match "running") -and
        $status.nas_api_ok -and
        $status.remote_worker_heartbeat_present
    )
    return [pscustomobject]$status
}

function Ensure-ComposeUp {
    $upResult = Invoke-DockerCompose -ExtraArgs @("up", "-d")
    Write-Log $upResult.Output
}

function Ensure-Runtime {
    Write-Log "ensure remote category runtime"
    & $chromeScript | Out-Null
    Ensure-ComposeUp
    for ($i = 0; $i -lt 24; $i++) {
        Start-Sleep -Seconds 5
        $status = Get-RuntimeStatus
        if ($status.stack_ready) {
            return $status
        }
    }
    throw "remote category runtime did not become ready in time"
}

function Stop-Runtime {
    Write-Log "stop remote category runtime"
    Invoke-DockerCompose -ExtraArgs @("down") -IgnoreErrors | Out-Null
}

function Install-Autostart {
    $wrapperPath = Join-Path $runtimeDir "ensure_remote_category_runtime.vbs"
    Write-HiddenVbsWrapper -Path $wrapperPath -CommandLine (Get-HiddenPowerShellCommand -ActionName "ensure")
    $tasks = @(
        @{ Name = "NoonRemoteCategoryEnsureAtLogon"; Schedule = @("/SC","ONLOGON") },
        @{ Name = "NoonRemoteCategoryEnsureEvery5Min"; Schedule = @("/SC","MINUTE","/MO","5") }
    )
    foreach ($task in $tasks) {
        $previousPref = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & schtasks.exe /Delete /F /TN $task.Name 2>&1 | Out-Null
        } finally {
            $ErrorActionPreference = $previousPref
        }
        $args = @(
            "/Create","/F",
            "/TN",$task.Name,
            "/TR",('wscript.exe //B //Nologo "{0}"' -f $wrapperPath),
            "/RL","HIGHEST"
        )
        $args += $task.Schedule
        $previousPref = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $taskOutput = & schtasks.exe @args 2>&1
        } finally {
            $ErrorActionPreference = $previousPref
        }
        if ($LASTEXITCODE -ne 0) {
            throw ("schtasks create failed for {0}: {1}" -f $task.Name, ($taskOutput -join [Environment]::NewLine))
        }
    }

    Write-Log "installed autostart tasks"
    return [pscustomobject]@{
        installed = $true
        tasks = @("NoonRemoteCategoryEnsureAtLogon","NoonRemoteCategoryEnsureEvery5Min")
    }
}

function Invoke-NasApi {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null
    )
    $url = (Get-NasBaseUrl).TrimEnd("/") + $Path
    if ($null -eq $Body) {
        return Invoke-RestMethod -UseBasicParsing -Method $Method -Uri $url -TimeoutSec 20
    }
    $json = $Body | ConvertTo-Json -Depth 8
    return Invoke-RestMethod -UseBasicParsing -Method $Method -Uri $url -ContentType "application/json" -Body $json -TimeoutSec 20
}

function Get-HiddenPowerShellCommand {
    param(
        [string]$ActionName
    )
    return ('powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}" -Action {1}' -f $PSCommandPath, $ActionName)
}

function Write-HiddenVbsWrapper {
    param(
        [string]$Path,
        [string]$CommandLine
    )
    $content = @(
        'Dim shell'
        'Set shell = CreateObject("WScript.Shell")'
        ('shell.Run "{0}", 0, False' -f ($CommandLine -replace '"', '""'))
    ) -join "`r`n"
    Set-Content -LiteralPath $Path -Value $content -Encoding ASCII
}

function Launch-DeepScan {
    $planName = "remote-category-pets-sports-x1000"
    $payload = @{
        categories = @("pets", "sports")
        category_overrides = @{}
        subcategory_overrides = @{}
        default_product_count_per_leaf = 1000
        export_excel = $false
        persist = $true
        reason = "desktop controller deep scan pets and sports x1000"
    }

    $plans = Invoke-NasApi -Method "GET" -Path "/api/crawler/plans"
    $existing = @($plans.items) | Where-Object { $_.name -eq $planName } | Select-Object -First 1

    if ($existing) {
        Invoke-NasApi -Method "PATCH" -Path ("/api/crawler/plans/{0}" -f $existing.id) -Body @{
            enabled = $false
            payload = $payload
        } | Out-Null
        $planId = $existing.id
    } else {
        $created = Invoke-NasApi -Method "POST" -Path "/api/crawler/plans" -Body @{
            plan_type = "category_ready_scan"
            name = $planName
            created_by = "desktop_controller"
            enabled = $false
            schedule_kind = "manual"
            schedule_json = @{}
            payload = $payload
        }
        $planId = $created.id
    }

    $task = Invoke-NasApi -Method "POST" -Path ("/api/crawler/plans/{0}/launch" -f $planId)
    Write-Log ("launched deep scan task {0} from plan {1}" -f $task.id, $planId)
    return [pscustomobject]@{
        plan_id = $planId
        task_id = $task.id
        status = $task.status
        display_name = $task.display_name
    }
}

function Launch-AllCategoriesScan {
    $catalog = Invoke-NasApi -Method "GET" -Path "/api/crawler/catalog"
    $readyCategories = @($catalog.ready_categories)
    if (-not $readyCategories -or $readyCategories.Count -eq 0) {
        throw "no ready categories available from NAS catalog"
    }

    $task = Invoke-NasApi -Method "POST" -Path "/api/tasks" -Body @{
        task_type = "category_ready_scan"
        priority = 20
        created_by = "desktop_controller_daily"
        schedule_type = "manual"
        payload = @{
            categories = $readyCategories
            category_overrides = @{}
            subcategory_overrides = @{}
            default_product_count_per_leaf = 1000
            export_excel = $false
            persist = $true
            reason = "desktop controller daily all categories x1000"
        }
    }

    Write-Log ("launched all categories scan task {0}" -f $task.id)
    return [pscustomobject]@{
        task_id = $task.id
        status = $task.status
        category_count = $readyCategories.Count
    }
}

switch ($Action) {
    "status" {
        Get-RuntimeStatus | ConvertTo-Json -Depth 6
    }
    "ensure" {
        Ensure-Runtime | ConvertTo-Json -Depth 6
    }
    "install-autostart" {
        Install-Autostart | ConvertTo-Json -Depth 4
    }
    "launch-deep-scan" {
        Ensure-Runtime | Out-Null
        Launch-DeepScan | ConvertTo-Json -Depth 4
    }
    "launch-all-categories-scan" {
        Ensure-Runtime | Out-Null
        Launch-AllCategoriesScan | ConvertTo-Json -Depth 4
    }
    "stop" {
        Stop-Runtime
        [pscustomobject]@{ stopped = $true } | ConvertTo-Json -Depth 2
    }
}
