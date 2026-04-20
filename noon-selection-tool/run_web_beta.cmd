@echo off
cd /d "%~dp0"
for /f "tokens=5" %%p in ('netstat -ano ^| findstr "127.0.0.1:8865" ^| findstr "LISTENING"') do (
  taskkill /PID %%p /F >nul 2>nul
)
start "" powershell -WindowStyle Hidden -Command "Start-Sleep -Seconds 2; Start-Process 'http://127.0.0.1:8865/'"
python -m uvicorn web_beta.app:app --host 127.0.0.1 --port 8865
