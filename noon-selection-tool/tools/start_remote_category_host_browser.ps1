$ErrorActionPreference = "Stop"

$chromePath = ${env:NOON_REMOTE_CATEGORY_CHROME_PATH}
if (-not $chromePath) {
    $chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
}

$debugPort = ${env:NOON_REMOTE_CATEGORY_CDP_PORT}
if (-not $debugPort) {
    $debugPort = "9222"
}

$proxyServer = ${env:NOON_REMOTE_CATEGORY_PROXY_SERVER}
if (-not $proxyServer) {
    $proxyServer = "socks5://127.0.0.1:10808"
}

$profileDir = ${env:NOON_REMOTE_CATEGORY_PROFILE_DIR}
if (-not $profileDir) {
    $profileDir = "D:\tmp\remote-category-chrome-profile"
}

$chromeMode = ${env:NOON_REMOTE_CATEGORY_CHROME_MODE}
if (-not $chromeMode) {
    $chromeMode = "hidden"
}
$chromeMode = $chromeMode.Trim().ToLowerInvariant()
if ($chromeMode -notin @("hidden", "visible", "headless")) {
    $chromeMode = "hidden"
}

function Ensure-RemoteCategoryChromeWin32 {
    if (-not ("RemoteCategoryChromeWin32" -as [type])) {
        Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class RemoteCategoryChromeWin32
{
    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool ShowWindowAsync(IntPtr hWnd, int nCmdShow);
}
"@
    }
}

function Test-RemoteCategoryChromeReady {
    param(
        [string]$Port
    )
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri ("http://127.0.0.1:{0}/json/version" -f $Port) -TimeoutSec 3
        return ($resp.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Get-RemoteCategoryChromeProcesses {
    param(
        [string]$ChromeExecutable,
        [string]$Port,
        [string]$ProfileDir
    )
    $escapedProfile = [Regex]::Escape($ProfileDir)
    $escapedPort = [Regex]::Escape($Port)
    return @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.Name -match '^chrome(\.exe)?$' -and
        $_.ExecutablePath -eq $ChromeExecutable -and
        $_.CommandLine -match "--remote-debugging-port=$escapedPort" -and
        $_.CommandLine -match $escapedProfile
    })
}

function Test-RemoteCategoryChromeProcessMatchesMode {
    param(
        $Process,
        [string]$ChromeMode
    )
    $commandLine = [string]($Process.CommandLine)
    $isHeadless = $commandLine -match "--headless(?:=new)?"
    if ($ChromeMode -eq "headless") {
        return $isHeadless
    }
    return -not $isHeadless
}

function Hide-RemoteCategoryChromeWindows {
    param(
        [array]$Processes
    )
    if (-not $Processes) {
        return
    }
    Ensure-RemoteCategoryChromeWin32
    foreach ($proc in $Processes) {
        try {
            $processHandle = Get-Process -Id $proc.ProcessId -ErrorAction Stop
            if ($processHandle.MainWindowHandle -ne 0) {
                [RemoteCategoryChromeWin32]::ShowWindowAsync($processHandle.MainWindowHandle, 0) | Out-Null
            }
        } catch {
        }
    }
}

if (-not (Test-Path -LiteralPath $chromePath)) {
    throw "Chrome executable not found: $chromePath"
}

New-Item -ItemType Directory -Force -Path $profileDir | Out-Null

$existing = Get-RemoteCategoryChromeProcesses -ChromeExecutable $chromePath -Port $debugPort -ProfileDir $profileDir
$existingMatchingMode = @($existing | Where-Object {
    Test-RemoteCategoryChromeProcessMatchesMode -Process $_ -ChromeMode $chromeMode
})
if ($existingMatchingMode -and (Test-RemoteCategoryChromeReady -Port $debugPort)) {
    if ($chromeMode -eq "hidden") {
        Hide-RemoteCategoryChromeWindows -Processes $existingMatchingMode
    }
    Write-Output "remote-category host browser already running on port $debugPort"
    exit 0
}

if ($existing) {
    foreach ($proc in $existing) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
        } catch {
        }
    }
    Start-Sleep -Seconds 2
}

$arguments = @(
    "--remote-debugging-port=$debugPort"
    "--remote-debugging-address=127.0.0.1"
    "--user-data-dir=""$profileDir"""
    "--proxy-server=$proxyServer"
    "--no-first-run"
    "--no-default-browser-check"
    "about:blank"
)

if ($chromeMode -eq "headless") {
    $arguments += @(
        "--headless=new"
        "--disable-gpu"
    )
} elseif ($chromeMode -eq "hidden") {
    $arguments += @(
        "--window-position=-32000,-32000"
        "--window-size=1200,900"
        "--start-minimized"
    )
}

Start-Process -FilePath $chromePath -ArgumentList $arguments -WindowStyle Minimized | Out-Null

for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Seconds 1
    if (Test-RemoteCategoryChromeReady -Port $debugPort) {
        if ($chromeMode -eq "hidden") {
            $started = Get-RemoteCategoryChromeProcesses -ChromeExecutable $chromePath -Port $debugPort -ProfileDir $profileDir
            Hide-RemoteCategoryChromeWindows -Processes $started
        }
        Write-Output "started remote-category host browser on port $debugPort"
        exit 0
    }
}

throw "remote-category host browser failed to become ready on port $debugPort"
