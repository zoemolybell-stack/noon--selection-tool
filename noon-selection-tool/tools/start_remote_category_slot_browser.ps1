param(
    [Parameter(Mandatory = $true)]
    [string]$SlotName,
    [Parameter(Mandatory = $true)]
    [int]$CdpPort,
    [Parameter(Mandatory = $true)]
    [string]$ProxyServer,
    [Parameter(Mandatory = $true)]
    [string]$ProfileDir,
    [ValidateSet("hidden", "visible", "headless")]
    [string]$ChromeMode = "hidden",
    [string]$ChromePath = ""
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $PSCommandPath
$baseScript = Join-Path $scriptDir "start_remote_category_host_browser.ps1"

if (-not (Test-Path -LiteralPath $baseScript)) {
    throw "base browser launcher not found: $baseScript"
}

$env:NOON_REMOTE_CATEGORY_CDP_PORT = [string]$CdpPort
$env:NOON_REMOTE_CATEGORY_PROXY_SERVER = $ProxyServer
$env:NOON_REMOTE_CATEGORY_PROFILE_DIR = $ProfileDir
$env:NOON_REMOTE_CATEGORY_CHROME_MODE = $ChromeMode
if ($ChromePath) {
    $env:NOON_REMOTE_CATEGORY_CHROME_PATH = $ChromePath
}

& $baseScript | ForEach-Object {
    "{0}: {1}" -f $SlotName, $_
}
