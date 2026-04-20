$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptRoot "sync_obsidian_context.py"

if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3 $pythonScript @args
    exit $LASTEXITCODE
}

if (Get-Command python -ErrorAction SilentlyContinue) {
    & python $pythonScript @args
    exit $LASTEXITCODE
}

throw "Python interpreter not found. Ensure py.exe or python.exe is available in PATH."
