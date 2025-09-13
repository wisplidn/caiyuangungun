#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw Data Service CLI Runner

便捷的CLI运行脚本，可以直接执行来启动CLI工具

使用方法:
    python run_cli.py --help
    python run_cli.py status
    python run_cli.py list
    python run_cli.py fetch -s tushare -d stock_basic
    python run_cli.py update --storage-type SNAPSHOT
    python run_cli.py backfill --source tushare --storage-type DAILY
    python run_cli.py fetch-period -s 20241201 -e 20241210 --storage-type DAILY
"""

if __name__ == '__main__':
    from .main import main
    main()