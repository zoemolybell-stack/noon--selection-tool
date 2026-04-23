param(
    [ValidateSet("status", "ensure", "stop", "test")]
    [string]$Action = "status",
    [string]$EnvFile = "",
    [switch]$LeaveRunning
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
if (-not $EnvFile) {
    $EnvFile = Join-Path $projectRoot ".env.category-slots"
}
$categoryNodeEnvFile = Join-Path $projectRoot ".env.category-node"
$slotComposeFile = Join-Path $projectRoot "docker-compose.category-slots.yml"
$renderScript = Join-Path $PSScriptRoot "render_v2ray_slot_xray_config.py"
$proxyScript = Join-Path $PSScriptRoot "start_v2ray_slot_proxy.ps1"
$browserScript = Join-Path $PSScriptRoot "start_remote_category_slot_browser.ps1"
$stopBrowserScript = Join-Path $PSScriptRoot "stop_remote_category_slot_browser.ps1"
$testScript = Join-Path $PSScriptRoot "test_dual_category_slots.ps1"
$runtimeDir = Join-Path $env:LOCALAPPDATA "NoonRuntime\dual-category-slots"
$configDir = Join-Path $runtimeDir "xray"

function Get-EnvMap {
    param([string]$Path)
    $map = @{}
    if (-not $Path -or -not (Test-Path -LiteralPath $Path)) {
        return $map
    }
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
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

function Resolve-Setting {
    param(
        [hashtable]$Map,
        [string]$Name,
        [object]$DefaultValue = ""
    )
    if ($Map.ContainsKey($Name) -and $Map[$Name] -ne "") {
        return $Map[$Name]
    }
    return $DefaultValue
}

function Get-SlotSettings {
    $slotEnv = Get-EnvMap -Path $EnvFile
    if (-not $slotEnv.Count) {
        throw "slot env file not found or empty: $EnvFile"
    }
    $settings = [ordered]@{
        v2rayn_db_path = [string](Resolve-Setting -Map $slotEnv -Name "NOON_V2RAYN_DB_PATH")
        xray_path = [string](Resolve-Setting -Map $slotEnv -Name "NOON_V2RAYN_XRAY_PATH" -DefaultValue "D:\v2rayN-windows-64\bin\xray\xray.exe")
        workers_api_url = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_WORKERS_API_URL" -DefaultValue "http://192.168.1.3:8865/api/workers")
        profile_root = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_SLOT_PROFILE_ROOT" -DefaultValue "D:\tmp\noon-browser-slots")
        compose_project_name = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_SLOT_COMPOSE_PROJECT" -DefaultValue "huihaokang-category-slots")
        slot_a_profile_index_id = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_A_PROFILE_INDEX_ID")
        slot_a_profile_remarks = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_A_PROFILE_REMARKS")
        slot_b_profile_index_id = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_B_PROFILE_INDEX_ID")
        slot_b_profile_remarks = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_B_PROFILE_REMARKS")
        slot_a_socks_port = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_A_SOCKS_PORT" -DefaultValue 11081)
        slot_b_socks_port = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_B_SOCKS_PORT" -DefaultValue 11082)
        slot_a_cdp_port = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_A_CDP_PORT" -DefaultValue 9224)
        slot_b_cdp_port = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_B_CDP_PORT" -DefaultValue 9225)
    }
    if (-not $settings.slot_a_profile_index_id -and -not $settings.slot_a_profile_remarks) {
        throw "slot A selector missing in $EnvFile"
    }
    if (-not $settings.slot_b_profile_index_id -and -not $settings.slot_b_profile_remarks) {
        throw "slot B selector missing in $EnvFile"
    }
    return [pscustomobject]$settings
}

function Invoke-PythonRender {
    param(
        [pscustomobject]$Settings,
        [string]$SlotName,
        [string]$ProfileIndexId,
        [string]$ProfileRemarks,
        [int]$SocksPort,
        [string]$OutputPath
    )
    $args = @($renderScript, "--socks-port", $SocksPort, "--output", $OutputPath)
    if ($Settings.v2rayn_db_path) {
        $args += @("--db", $Settings.v2rayn_db_path)
    }
    if ($ProfileRemarks) {
        $args += @("--profile-remarks", $ProfileRemarks)
    } else {
        $args += @("--profile-index-id", $ProfileIndexId)
    }
    $output = & python @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("slot config render failed: {0}" -f ($output -join [Environment]::NewLine))
    }
    return ($output -join [Environment]::NewLine) | ConvertFrom-Json
}

function Test-CdpReady {
    param([int]$Port)
    try {
        return Invoke-RestMethod -UseBasicParsing -Uri ("http://127.0.0.1:{0}/json/version" -f $Port) -TimeoutSec 5
    } catch {
        return $null
    }
}

function Test-SlotProxyReady {
    param([int]$Port)
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $async = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(2000)) {
            $client.Close()
            return $false
        }
        $client.EndConnect($async)
        $client.Close()
        return $true
    } catch {
        return $false
    }
}

function Get-ProxyIp {
    param([int]$SocksPort)
    for ($i = 0; $i -lt 3; $i++) {
        try {
            $raw = & curl.exe --socks5-hostname ("127.0.0.1:{0}" -f $SocksPort) https://api.ipify.org -m 20 -s
            $value = ([string]$raw).Trim()
            if ($value) {
                return $value
            }
        } catch {
        }
        Start-Sleep -Seconds 1
    }
    return ""
}

function Ensure-Slots {
    param([pscustomobject]$Settings)
    New-Item -ItemType Directory -Force -Path $configDir | Out-Null
    New-Item -ItemType Directory -Force -Path $Settings.profile_root | Out-Null

    $slotAConfig = Join-Path $configDir "slot-a.json"
    $slotBConfig = Join-Path $configDir "slot-b.json"
    $slotAProfileDir = Join-Path $Settings.profile_root "slot-a"
    $slotBProfileDir = Join-Path $Settings.profile_root "slot-b"

    $slotAMeta = Invoke-PythonRender -Settings $Settings -SlotName "slot-a" -ProfileIndexId $Settings.slot_a_profile_index_id -ProfileRemarks $Settings.slot_a_profile_remarks -SocksPort $Settings.slot_a_socks_port -OutputPath $slotAConfig
    $slotBMeta = Invoke-PythonRender -Settings $Settings -SlotName "slot-b" -ProfileIndexId $Settings.slot_b_profile_index_id -ProfileRemarks $Settings.slot_b_profile_remarks -SocksPort $Settings.slot_b_socks_port -OutputPath $slotBConfig

    $null = & $proxyScript -Action ensure -SlotName "slot-a" -ConfigPath $slotAConfig -LocalSocksPort $Settings.slot_a_socks_port -XrayPath $Settings.xray_path
    $null = & $proxyScript -Action ensure -SlotName "slot-b" -ConfigPath $slotBConfig -LocalSocksPort $Settings.slot_b_socks_port -XrayPath $Settings.xray_path
    $null = & $browserScript -SlotName "slot-a" -CdpPort $Settings.slot_a_cdp_port -ProxyServer ("socks5://127.0.0.1:{0}" -f $Settings.slot_a_socks_port) -ProfileDir $slotAProfileDir -ChromeMode hidden
    $null = & $browserScript -SlotName "slot-b" -CdpPort $Settings.slot_b_cdp_port -ProxyServer ("socks5://127.0.0.1:{0}" -f $Settings.slot_b_socks_port) -ProfileDir $slotBProfileDir -ChromeMode hidden

    $env:COMPOSE_PROJECT_NAME = $Settings.compose_project_name
    $env:SLOT_A_BROWSER_CDP_ENDPOINT = "http://host.docker.internal:$($Settings.slot_a_cdp_port)"
    $env:SLOT_B_BROWSER_CDP_ENDPOINT = "http://host.docker.internal:$($Settings.slot_b_cdp_port)"
    $env:SLOT_A_NODE_HOST = "remote-category-slot-a"
    $env:SLOT_B_NODE_HOST = "remote-category-slot-b"
    $env:SLOT_A_WORKER_NAME = "remote-category-slot-a"
    $env:SLOT_B_WORKER_NAME = "remote-category-slot-b"

    & docker compose -f $slotComposeFile --env-file $categoryNodeEnvFile up -d | Out-Null
    Start-Sleep -Seconds 15

    return [pscustomobject]@{
        slot_a = $slotAMeta
        slot_b = $slotBMeta
        slot_a_proxy_ready = (Test-SlotProxyReady -Port $Settings.slot_a_socks_port)
        slot_b_proxy_ready = (Test-SlotProxyReady -Port $Settings.slot_b_socks_port)
        slot_a_proxy_ip = (Get-ProxyIp -SocksPort $Settings.slot_a_socks_port)
        slot_b_proxy_ip = (Get-ProxyIp -SocksPort $Settings.slot_b_socks_port)
        slot_a_cdp = Test-CdpReady -Port $Settings.slot_a_cdp_port
        slot_b_cdp = Test-CdpReady -Port $Settings.slot_b_cdp_port
    }
}

function Stop-Slots {
    param([pscustomobject]$Settings)
    $env:COMPOSE_PROJECT_NAME = $Settings.compose_project_name
    & docker compose -f $slotComposeFile --env-file $categoryNodeEnvFile down | Out-Null
    & $stopBrowserScript -CdpPort $Settings.slot_a_cdp_port -ProfileDir (Join-Path $Settings.profile_root "slot-a") | Out-Null
    & $stopBrowserScript -CdpPort $Settings.slot_b_cdp_port -ProfileDir (Join-Path $Settings.profile_root "slot-b") | Out-Null
    & $proxyScript -Action stop -SlotName "slot-a" -ConfigPath (Join-Path $configDir "slot-a.json") -LocalSocksPort $Settings.slot_a_socks_port -XrayPath $Settings.xray_path | Out-Null
    & $proxyScript -Action stop -SlotName "slot-b" -ConfigPath (Join-Path $configDir "slot-b.json") -LocalSocksPort $Settings.slot_b_socks_port -XrayPath $Settings.xray_path | Out-Null
}

function Get-ComposeStatus {
    param([pscustomobject]$Settings)
    $env:COMPOSE_PROJECT_NAME = $Settings.compose_project_name
    $lines = @(& docker compose -f $slotComposeFile --env-file $categoryNodeEnvFile ps --format "{{json .}}" 2>$null)
    return @(
        $lines |
        Where-Object { $_ } |
        ForEach-Object { $_ | ConvertFrom-Json }
    )
}

function Get-WorkersApiStatus {
    param([pscustomobject]$Settings)
    try {
        $payload = Invoke-RestMethod -UseBasicParsing -Uri $Settings.workers_api_url -TimeoutSec 10
        return @(
            $payload.items |
            Where-Object { $_.worker_name -in @("remote-category-slot-a", "remote-category-slot-b") } |
            Select-Object worker_name, status, node_role, node_host, heartbeat_at
        )
    } catch {
        return @()
    }
}

function Get-StatusPayload {
    param([pscustomobject]$Settings)
    $composeItems = @(Get-ComposeStatus -Settings $Settings)
    $heartbeatItems = @(Get-WorkersApiStatus -Settings $Settings)
    return [ordered]@{
        env_file = $EnvFile
        compose_project_name = $Settings.compose_project_name
        slot_a = [ordered]@{
            socks_port = $Settings.slot_a_socks_port
            proxy_ready = (Test-SlotProxyReady -Port $Settings.slot_a_socks_port)
            proxy_ip = (Get-ProxyIp -SocksPort $Settings.slot_a_socks_port)
            cdp_port = $Settings.slot_a_cdp_port
            cdp_ready = [bool](Test-CdpReady -Port $Settings.slot_a_cdp_port)
            profile_selector = if ($Settings.slot_a_profile_remarks) { $Settings.slot_a_profile_remarks } else { $Settings.slot_a_profile_index_id }
        }
        slot_b = [ordered]@{
            socks_port = $Settings.slot_b_socks_port
            proxy_ready = (Test-SlotProxyReady -Port $Settings.slot_b_socks_port)
            proxy_ip = (Get-ProxyIp -SocksPort $Settings.slot_b_socks_port)
            cdp_port = $Settings.slot_b_cdp_port
            cdp_ready = [bool](Test-CdpReady -Port $Settings.slot_b_cdp_port)
            profile_selector = if ($Settings.slot_b_profile_remarks) { $Settings.slot_b_profile_remarks } else { $Settings.slot_b_profile_index_id }
        }
        compose_count = $composeItems.Count
        compose = $composeItems
        worker_heartbeat_count = $heartbeatItems.Count
        worker_heartbeats = $heartbeatItems
    }
}

$settings = Get-SlotSettings

switch ($Action) {
    "status" {
        Write-Output ((Get-StatusPayload -Settings $settings) | ConvertTo-Json -Depth 8)
    }
    "ensure" {
        $result = Ensure-Slots -Settings $settings
        $status = Get-StatusPayload -Settings $settings
        Write-Output (([ordered]@{ ensured = $result; status = $status }) | ConvertTo-Json -Depth 8)
    }
    "stop" {
        Stop-Slots -Settings $settings
        Write-Output ((Get-StatusPayload -Settings $settings) | ConvertTo-Json -Depth 8)
    }
    "test" {
        $testArgs = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $testScript, "-EnvFile", $EnvFile)
        if ($LeaveRunning) {
            $testArgs += "-LeaveRunning"
        }
        & powershell.exe @testArgs
    }
}
