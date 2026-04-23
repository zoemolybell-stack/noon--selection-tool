param(
    [ValidateSet("status", "ensure", "stop")]
    [string]$Action = "status",
    [Parameter(Mandatory = $true)]
    [string]$SlotName,
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,
    [Parameter(Mandatory = $true)]
    [int]$LocalSocksPort,
    [string]$XrayPath = ""
)

$ErrorActionPreference = "Stop"

$resolvedConfigPath = (Resolve-Path -LiteralPath $ConfigPath).Path
$runtimeDir = Join-Path $env:LOCALAPPDATA "NoonRuntime\slot-proxies"
$logDir = Join-Path $runtimeDir "logs"
$stdoutLog = Join-Path $logDir ("{0}.stdout.log" -f $SlotName)
$stderrLog = Join-Path $logDir ("{0}.stderr.log" -f $SlotName)

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

if (-not $XrayPath) {
    $XrayPath = ${env:NOON_V2RAYN_XRAY_PATH}
}
if (-not $XrayPath) {
    $XrayPath = "D:\v2rayN-windows-64\bin\xray\xray.exe"
}

function Get-SlotProxyProcesses {
    $escapedConfig = [Regex]::Escape($resolvedConfigPath)
    return @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.Name -match '^xray(\.exe)?$' -and
        $_.ExecutablePath -eq $XrayPath -and
        $_.CommandLine -match $escapedConfig
    })
}

function Test-SlotProxyReady {
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $async = $client.BeginConnect("127.0.0.1", $LocalSocksPort, $null, $null)
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

function Get-SlotProxyStatus {
    $processes = Get-SlotProxyProcesses
    return [pscustomobject]@{
        slot = $SlotName
        config_path = $resolvedConfigPath
        local_socks_port = $LocalSocksPort
        ready = (Test-SlotProxyReady)
        process_ids = @($processes | ForEach-Object { $_.ProcessId })
    }
}

function Stop-SlotProxy {
    $processes = Get-SlotProxyProcesses
    foreach ($process in $processes) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
        } catch {
        }
    }
    Start-Sleep -Seconds 1
}

function Ensure-SlotProxy {
    if (Test-SlotProxyReady) {
        return Get-SlotProxyStatus
    }

    Stop-SlotProxy

    if (-not (Test-Path -LiteralPath $XrayPath)) {
        throw "xray executable not found: $XrayPath"
    }

    Start-Process -FilePath $XrayPath `
        -ArgumentList @("run", "-c", $resolvedConfigPath) `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog | Out-Null

    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Seconds 1
        if (Test-SlotProxyReady) {
            return Get-SlotProxyStatus
        }
    }

    throw "slot proxy failed to become ready: $SlotName"
}

switch ($Action) {
    "status" {
        Get-SlotProxyStatus | ConvertTo-Json -Depth 6
    }
    "ensure" {
        Ensure-SlotProxy | ConvertTo-Json -Depth 6
    }
    "stop" {
        Stop-SlotProxy
        Get-SlotProxyStatus | ConvertTo-Json -Depth 6
    }
}
