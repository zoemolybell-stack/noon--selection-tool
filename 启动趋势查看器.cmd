@echo off
chcp 65001 >nul
color 0A
title NOON Product Trend Viewer
cd /d "%~dp0"

echo ==============================================
echo     NOON 产品趋势查看器
echo ==============================================
echo.

if not exist "venv\Scripts\python.exe" (
    echo [错误] 虚拟环境未找到!
    echo 请先运行：python -m venv venv
    echo.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
python product_trend_viewer.py

pause
