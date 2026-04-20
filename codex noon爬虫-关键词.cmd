@echo off
setlocal
cd /d "%~dp0"

if exist "%~dp0venv\Scripts\pythonw.exe" (
    start "" "%~dp0venv\Scripts\pythonw.exe" "%~dp0codex_noon_keyword_gui.py"
    exit /b 0
)

if exist "%~dp0venv\Scripts\python.exe" (
    start "" "%~dp0venv\Scripts\python.exe" "%~dp0codex_noon_keyword_gui.py"
    exit /b 0
)

echo Python runtime not found: venv\Scripts\python.exe
pause
exit /b 1
