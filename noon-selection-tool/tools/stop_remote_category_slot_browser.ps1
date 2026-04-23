param(
    [Parameter(Mandatory = $true)]
    [int]$CdpPort,
    [string]$ProfileDir = ""
)

$ErrorActionPreference = "Stop"

$profilePattern = if ($ProfileDir) { [Regex]::Escape($ProfileDir) } else { "" }
$portPattern = [Regex]::Escape("--remote-debugging-port=$CdpPort")

$processes = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.Name -match '^chrome(\.exe)?$' -and
    $_.CommandLine -match $portPattern -and
    (
        -not $profilePattern -or
        $_.CommandLine -match $profilePattern
    )
})

foreach ($process in $processes) {
    try {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    } catch {
    }
}

Start-Sleep -Seconds 1

[pscustomobject]@{
    cdp_port = $CdpPort
    profile_dir = $ProfileDir
    stopped_process_ids = @($processes | ForEach-Object { $_.ProcessId })
} | ConvertTo-Json -Depth 4
