param(
    [string]$EnvFile = "",
    [string]$V2rayNDbPath = "",
    [string]$WorkersApiUrl = "",
    [string]$ProfileRoot = "",
    [string]$SlotAProfileIndexId = "",
    [string]$SlotAProfileRemarks = "",
    [string]$SlotBProfileIndexId = "",
    [string]$SlotBProfileRemarks = "",
    [int]$SlotASocksPort = 0,
    [int]$SlotBSocksPort = 0,
    [int]$SlotACdpPort = 0,
    [int]$SlotBCdpPort = 0,
    [switch]$StartWorkers,
    [switch]$LeaveRunning
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
if (-not $EnvFile) {
    $EnvFile = Join-Path $projectRoot ".env.category-slots"
}
$renderScript = Join-Path $PSScriptRoot "render_v2ray_slot_xray_config.py"
$proxyScript = Join-Path $PSScriptRoot "start_v2ray_slot_proxy.ps1"
$browserScript = Join-Path $PSScriptRoot "start_remote_category_slot_browser.ps1"
$stopBrowserScript = Join-Path $PSScriptRoot "stop_remote_category_slot_browser.ps1"
$composeFile = Join-Path $projectRoot "docker-compose.category-slots.yml"
$categoryNodeEnvFile = Join-Path $projectRoot ".env.category-node"
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
        [object]$CurrentValue,
        [object]$DefaultValue = ""
    )
    if ($null -ne $CurrentValue -and [string]$CurrentValue -ne "" -and [string]$CurrentValue -ne "0") {
        return $CurrentValue
    }
    if ($Map.ContainsKey($Name) -and $Map[$Name] -ne "") {
        return $Map[$Name]
    }
    return $DefaultValue
}

function Get-ProxyIp {
    param([int]$SocksPort)
    try {
        $raw = & curl.exe --socks5-hostname ("127.0.0.1:{0}" -f $SocksPort) https://api.ipify.org -m 20 -s
        $value = [string]$raw
        if ($value) {
            return $value.Trim()
        }
    } catch {
    }
    return $null
}

function Test-CdpReady {
    param([int]$Port)
    return Invoke-RestMethod -UseBasicParsing -Uri ("http://127.0.0.1:{0}/json/version" -f $Port) -TimeoutSec 5
}

function Invoke-PythonRender {
    param(
        [string]$ProfileIndexId,
        [string]$ProfileRemarks,
        [int]$SocksPort,
        [string]$OutputPath,
        [string]$DbPath
    )
    $args = @($renderScript, "--socks-port", $SocksPort, "--output", $OutputPath)
    if ($DbPath) {
        $args += @("--db", $DbPath)
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

function Stop-ComposeWorkers {
    param([string]$ComposeProjectName)
    $env:COMPOSE_PROJECT_NAME = $ComposeProjectName
    & docker compose -f $composeFile --env-file $categoryNodeEnvFile down | Out-Null
}

function Stop-SlotArtifacts {
    param(
        [int]$SlotACdpPort,
        [int]$SlotBCdpPort,
        [string]$SlotAProfileDir,
        [string]$SlotBProfileDir,
        [string]$SlotAConfig,
        [string]$SlotBConfig,
        [int]$SlotASocksPort,
        [int]$SlotBSocksPort
    )
    & $stopBrowserScript -CdpPort $SlotACdpPort -ProfileDir $SlotAProfileDir | Out-Null
    & $stopBrowserScript -CdpPort $SlotBCdpPort -ProfileDir $SlotBProfileDir | Out-Null
    & $proxyScript -Action stop -SlotName "slot-a" -ConfigPath $SlotAConfig -LocalSocksPort $SlotASocksPort | Out-Null
    & $proxyScript -Action stop -SlotName "slot-b" -ConfigPath $SlotBConfig -LocalSocksPort $SlotBSocksPort | Out-Null
}

$slotEnv = Get-EnvMap -Path $EnvFile

$V2rayNDbPath = [string](Resolve-Setting -Map $slotEnv -Name "NOON_V2RAYN_DB_PATH" -CurrentValue $V2rayNDbPath -DefaultValue "")
$WorkersApiUrl = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_WORKERS_API_URL" -CurrentValue $WorkersApiUrl -DefaultValue "http://192.168.1.3:8865/api/workers")
$ProfileRoot = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_SLOT_PROFILE_ROOT" -CurrentValue $ProfileRoot -DefaultValue "D:\tmp\noon-browser-slots")
$SlotAProfileIndexId = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_A_PROFILE_INDEX_ID" -CurrentValue $SlotAProfileIndexId -DefaultValue "")
$SlotAProfileRemarks = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_A_PROFILE_REMARKS" -CurrentValue $SlotAProfileRemarks -DefaultValue "")
$SlotBProfileIndexId = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_B_PROFILE_INDEX_ID" -CurrentValue $SlotBProfileIndexId -DefaultValue "")
$SlotBProfileRemarks = [string](Resolve-Setting -Map $slotEnv -Name "SLOT_B_PROFILE_REMARKS" -CurrentValue $SlotBProfileRemarks -DefaultValue "")
$SlotASocksPort = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_A_SOCKS_PORT" -CurrentValue $SlotASocksPort -DefaultValue 11081)
$SlotBSocksPort = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_B_SOCKS_PORT" -CurrentValue $SlotBSocksPort -DefaultValue 11082)
$SlotACdpPort = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_A_CDP_PORT" -CurrentValue $SlotACdpPort -DefaultValue 9224)
$SlotBCdpPort = [int](Resolve-Setting -Map $slotEnv -Name "SLOT_B_CDP_PORT" -CurrentValue $SlotBCdpPort -DefaultValue 9225)
$composeProjectName = [string](Resolve-Setting -Map $slotEnv -Name "NOON_REMOTE_CATEGORY_SLOT_COMPOSE_PROJECT" -CurrentValue "" -DefaultValue "huihaokang-category-slots")

if (-not $SlotAProfileIndexId -and -not $SlotAProfileRemarks) {
    throw "slot A profile selector missing: set SLOT_A_PROFILE_INDEX_ID or SLOT_A_PROFILE_REMARKS"
}
if (-not $SlotBProfileIndexId -and -not $SlotBProfileRemarks) {
    throw "slot B profile selector missing: set SLOT_B_PROFILE_INDEX_ID or SLOT_B_PROFILE_REMARKS"
}

New-Item -ItemType Directory -Force -Path $configDir | Out-Null
New-Item -ItemType Directory -Force -Path $ProfileRoot | Out-Null

$slotAConfig = Join-Path $configDir "slot-a.json"
$slotBConfig = Join-Path $configDir "slot-b.json"
$slotAProfileDir = Join-Path $ProfileRoot "slot-a"
$slotBProfileDir = Join-Path $ProfileRoot "slot-b"

$slotAMeta = Invoke-PythonRender -ProfileIndexId $SlotAProfileIndexId -ProfileRemarks $SlotAProfileRemarks -SocksPort $SlotASocksPort -OutputPath $slotAConfig -DbPath $V2rayNDbPath
$slotBMeta = Invoke-PythonRender -ProfileIndexId $SlotBProfileIndexId -ProfileRemarks $SlotBProfileRemarks -SocksPort $SlotBSocksPort -OutputPath $slotBConfig -DbPath $V2rayNDbPath

$slotAProxy = & $proxyScript -Action ensure -SlotName "slot-a" -ConfigPath $slotAConfig -LocalSocksPort $SlotASocksPort | ConvertFrom-Json
$slotBProxy = & $proxyScript -Action ensure -SlotName "slot-b" -ConfigPath $slotBConfig -LocalSocksPort $SlotBSocksPort | ConvertFrom-Json

$null = & $browserScript -SlotName "slot-a" -CdpPort $SlotACdpPort -ProxyServer ("socks5://127.0.0.1:{0}" -f $SlotASocksPort) -ProfileDir $slotAProfileDir -ChromeMode hidden
$null = & $browserScript -SlotName "slot-b" -CdpPort $SlotBCdpPort -ProxyServer ("socks5://127.0.0.1:{0}" -f $SlotBSocksPort) -ProfileDir $slotBProfileDir -ChromeMode hidden

$slotACdp = Test-CdpReady -Port $SlotACdpPort
$slotBCdp = Test-CdpReady -Port $SlotBCdpPort

$result = [ordered]@{
    slot_a = [ordered]@{
        profile_index_id = $slotAMeta.profile_index_id
        remarks = $slotAMeta.remarks
        socks_port = $SlotASocksPort
        proxy_ready = $slotAProxy.ready
        proxy_ip = (Get-ProxyIp -SocksPort $SlotASocksPort)
        cdp_port = $SlotACdpPort
        cdp_browser = $slotACdp.Browser
        cdp_websocket = $slotACdp.webSocketDebuggerUrl
        profile_dir = $slotAProfileDir
    }
    slot_b = [ordered]@{
        profile_index_id = $slotBMeta.profile_index_id
        remarks = $slotBMeta.remarks
        socks_port = $SlotBSocksPort
        proxy_ready = $slotBProxy.ready
        proxy_ip = (Get-ProxyIp -SocksPort $SlotBSocksPort)
        cdp_port = $SlotBCdpPort
        cdp_browser = $slotBCdp.Browser
        cdp_websocket = $slotBCdp.webSocketDebuggerUrl
        profile_dir = $slotBProfileDir
    }
}

if ($StartWorkers) {
    $env:COMPOSE_PROJECT_NAME = $composeProjectName
    $env:SLOT_A_BROWSER_CDP_ENDPOINT = "http://host.docker.internal:$SlotACdpPort"
    $env:SLOT_B_BROWSER_CDP_ENDPOINT = "http://host.docker.internal:$SlotBCdpPort"
    $env:SLOT_A_NODE_HOST = "remote-category-slot-a"
    $env:SLOT_B_NODE_HOST = "remote-category-slot-b"
    $env:SLOT_A_WORKER_NAME = "remote-category-slot-a"
    $env:SLOT_B_WORKER_NAME = "remote-category-slot-b"

    & docker compose -f $composeFile --env-file $categoryNodeEnvFile up -d | Out-Null
    Start-Sleep -Seconds 18

    $workerPsLines = @(& docker compose -f $composeFile --env-file $categoryNodeEnvFile ps --format "{{json .}}")
    $result["workers"] = @(
        $workerPsLines |
        Where-Object { $_ } |
        ForEach-Object { $_ | ConvertFrom-Json }
    )

    try {
        $workersApi = Invoke-RestMethod -UseBasicParsing -Uri $WorkersApiUrl -TimeoutSec 15
        $result["worker_heartbeat_matches"] = @(
            $workersApi.items |
            Where-Object { $_.worker_name -in @("remote-category-slot-a", "remote-category-slot-b") } |
            Select-Object worker_name, status, node_role, node_host, heartbeat_at
        )
    } catch {
        $result["worker_heartbeat_matches"] = @()
        $result["worker_api_error"] = $_.Exception.Message
    }
}

Write-Output ($result | ConvertTo-Json -Depth 8)

if (-not $LeaveRunning) {
    if ($StartWorkers) {
        Stop-ComposeWorkers -ComposeProjectName $composeProjectName
    }
    Stop-SlotArtifacts `
        -SlotACdpPort $SlotACdpPort `
        -SlotBCdpPort $SlotBCdpPort `
        -SlotAProfileDir $slotAProfileDir `
        -SlotBProfileDir $slotBProfileDir `
        -SlotAConfig $slotAConfig `
        -SlotBConfig $slotBConfig `
        -SlotASocksPort $SlotASocksPort `
        -SlotBSocksPort $SlotBSocksPort
}
