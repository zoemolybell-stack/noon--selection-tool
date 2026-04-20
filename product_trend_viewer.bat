@echo off
chcp 65001 >nul
color 0A
title NOON Product Trend Viewer

echo ==============================================
echo     NOON Product Trend Viewer
echo ==============================================
echo.

call venv\Scripts\activate.bat

python product_trend_viewer.py

pause
