#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw Data Service CLI Main Entry Point

原始数据服务的命令行主入口，提供统一的CLI接口
"""

import click
import sys
from pathlib import Path

# 导入所有命令
from .commands import (
    historical_backfill,
    standard_update,
    update_with_lookback,
    update_with_triple_lookback,
    fetch_period,
    fetch_single,
    status,
    list_sources
)


@click.group()
@click.version_option(version='1.0.0', prog_name='raw-data-cli')
def cli():
    """Raw Data Service CLI
    
    原始数据服务命令行工具，提供数据获取、更新和管理功能。
    
    支持的功能：
    - 历史数据回填
    - 标准数据更新
    - 数据更新含回溯
    - 指定期间数据获取
    - 服务状态查询
    """
    pass


# 添加所有命令到主命令组
cli.add_command(historical_backfill, name='backfill')
cli.add_command(standard_update, name='update')
cli.add_command(update_with_lookback, name='update-lookback')
cli.add_command(update_with_triple_lookback, name='update-triple')
cli.add_command(fetch_period, name='fetch-period')
cli.add_command(fetch_single, name='fetch')
cli.add_command(status, name='status')
cli.add_command(list_sources, name='list')


def main():
    """主入口函数"""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\n操作已取消", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"未预期的错误: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()