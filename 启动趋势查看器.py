#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
NOON 产品趋势查看器 - 快捷启动脚本
双击此文件即可启动
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
VENV_PYTHON = PROJECT_ROOT / 'venv' / 'Scripts' / 'python.exe'

def main():
    # 检查 venv 是否存在
    if not VENV_PYTHON.exists():
        print("错误：虚拟环境未找到，请先运行：python -m venv venv")
        print("按任意键退出...")
        input()
        return

    # 启动趋势查看器
    cmd = [str(VENV_PYTHON), str(PROJECT_ROOT / 'product_trend_viewer.py')]
    print("正在启动 NOON 产品趋势查看器...")
    subprocess.run(cmd)

if __name__ == '__main__':
    main()
