#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw Data Service CLI Commands

提供原始数据服务的命令行接口，支持：
- 历史数据回填
- 标准数据更新
- 数据更新含回溯
- 指定期间数据获取
- 服务状态查询
"""

import click
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
import sys

# 添加项目根路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from caiyuangungun.data.raw.services.raw_data_service import RawDataService
from caiyuangungun.data.raw.utils.data_service_utils import RunMode

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def get_service() -> RawDataService:
    """获取RawDataService实例"""
    return RawDataService()


def format_results(results: List[Dict[str, Any]]) -> None:
    """格式化并显示结果"""
    if not results:
        click.echo("没有处理任何任务")
        return
    
    success_count = sum(1 for r in results if r.get('status') == 'success')
    skipped_count = sum(1 for r in results if r.get('status') == 'skipped')
    error_count = sum(1 for r in results if r.get('status') == 'error')
    
    click.echo(f"\n处理结果:")
    click.echo(f"  成功: {success_count}")
    click.echo(f"  跳过: {skipped_count}")
    click.echo(f"  错误: {error_count}")
    click.echo(f"  总计: {len(results)}")
    
    # 显示错误详情
    errors = [r for r in results if r.get('status') == 'error']
    if errors:
        click.echo("\n错误详情:")
        for error in errors[:5]:  # 只显示前5个错误
            source = error.get('source_name', 'unknown')
            data_type = error.get('data_type', 'unknown')
            date_param = error.get('date_param', '')
            error_msg = error.get('error_message', 'Unknown error')
            click.echo(f"  {source}.{data_type} {date_param}: {error_msg}")
        
        if len(errors) > 5:
            click.echo(f"  ... 还有 {len(errors) - 5} 个错误")


@click.command()
@click.option('--source', '-s', help='数据源名称')
@click.option('--storage-type', '-t', type=click.Choice(['SNAPSHOT', 'DAILY', 'MONTHLY']), help='存储类型')
@click.option('--data-type', '-d', help='数据类型')
def historical_backfill(source: Optional[str], storage_type: Optional[str], data_type: Optional[str]):
    """历史数据回填
    
    根据配置的start_date作为起点，自动跳过已存在的数据文件
    """
    click.echo("开始历史数据回填...")
    
    try:
        service = get_service()
        results = service.execute_unified_run(
            run_mode=RunMode.HISTORICAL_BACKFILL,
            source_name=source,
            storage_type=storage_type,
            data_type=data_type
        )
        format_results(results)
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--source', '-s', help='数据源名称')
@click.option('--storage-type', '-t', type=click.Choice(['SNAPSHOT', 'DAILY', 'MONTHLY']), help='存储类型')
@click.option('--data-type', '-d', help='数据类型')
def standard_update(source: Optional[str], storage_type: Optional[str], data_type: Optional[str]):
    """标准数据更新
    
    仅更新最新的一份数据，不跳过已存在文件
    """
    click.echo("开始标准数据更新...")
    
    try:
        service = get_service()
        results = service.execute_unified_run(
            run_mode=RunMode.STANDARD_UPDATE,
            source_name=source,
            storage_type=storage_type,
            data_type=data_type
        )
        format_results(results)
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--source', '-s', help='数据源名称')
@click.option('--storage-type', '-t', type=click.Choice(['SNAPSHOT', 'DAILY', 'MONTHLY']), help='存储类型')
@click.option('--data-type', '-d', help='数据类型')
@click.option('--multiplier', '-m', type=int, default=1, help='回溯倍数')
def update_with_lookback(source: Optional[str], storage_type: Optional[str], 
                        data_type: Optional[str], multiplier: int):
    """数据更新含回溯
    
    最新数据 + lookback_periods 的数据份数
    """
    click.echo(f"开始数据更新含回溯 (倍数: {multiplier})...")
    
    try:
        service = get_service()
        results = service.execute_unified_run(
            run_mode=RunMode.UPDATE_WITH_LOOKBACK,
            source_name=source,
            storage_type=storage_type,
            data_type=data_type,
            multiplier=multiplier
        )
        format_results(results)
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--source', '-s', help='数据源名称')
@click.option('--storage-type', '-t', type=click.Choice(['SNAPSHOT', 'DAILY', 'MONTHLY']), help='存储类型')
@click.option('--data-type', '-d', help='数据类型')
def update_with_triple_lookback(source: Optional[str], storage_type: Optional[str], data_type: Optional[str]):
    """数据更新含三倍回溯
    
    最新数据 + lookback_periods*3 的数据份数
    """
    click.echo("开始数据更新含三倍回溯...")
    
    try:
        service = get_service()
        results = service.execute_unified_run(
            run_mode=RunMode.UPDATE_WITH_TRIPLE_LOOKBACK,
            source_name=source,
            storage_type=storage_type,
            data_type=data_type
        )
        format_results(results)
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--start-date', '-s', required=True, help='开始日期 (格式: YYYYMMDD 或 YYYYMM)')
@click.option('--end-date', '-e', required=True, help='结束日期 (格式: YYYYMMDD 或 YYYYMM)')
@click.option('--source', help='数据源名称')
@click.option('--storage-type', '-t', type=click.Choice(['SNAPSHOT', 'DAILY', 'MONTHLY']), help='存储类型')
@click.option('--data-type', '-d', help='数据类型')
def fetch_period(start_date: str, end_date: str, source: Optional[str], 
                storage_type: Optional[str], data_type: Optional[str]):
    """指定期间数据获取
    
    获取指定时间范围的数据，不跳过已存在文件
    """
    click.echo(f"开始获取期间数据: {start_date} 到 {end_date}...")
    
    try:
        service = get_service()
        results = service.fetch_period_data(
            start_date=start_date,
            end_date=end_date,
            source_name=source,
            storage_type=storage_type,
            data_type=data_type
        )
        format_results(results)
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
@click.option('--source', '-s', required=True, help='数据源名称')
@click.option('--data-type', '-d', required=True, help='数据类型')
@click.option('--date-param', help='日期参数')
@click.option('--skip-existing/--no-skip-existing', default=True, help='是否跳过已存在文件')
def fetch_single(source: str, data_type: str, date_param: Optional[str], skip_existing: bool):
    """获取单个数据
    
    获取指定数据源和数据类型的单个数据
    """
    click.echo(f"开始获取数据: {source}.{data_type} {date_param or ''}...")
    
    try:
        service = get_service()
        result = service.fetch_and_archive_data(
            source_name=source,
            data_type=data_type,
            date_param=date_param,
            skip_existing=skip_existing
        )
        
        status = result.get('status', 'unknown')
        if status == 'success':
            click.echo(f"✓ 成功: {result.get('data_shape', 'unknown shape')}")
        elif status == 'skipped':
            reason = result.get('reason', 'unknown')
            click.echo(f"- 跳过: {reason}")
        elif status == 'error':
            error_msg = result.get('error_message', 'Unknown error')
            click.echo(f"✗ 错误: {error_msg}")
        else:
            click.echo(f"状态: {status}")
            
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
def status():
    """显示服务状态
    
    显示数据源、数据定义等服务状态信息
    """
    try:
        service = get_service()
        status_info = service.get_service_status()
        
        click.echo("=== Raw Data Service 状态 ===")
        click.echo(f"服务名称: {status_info['service_name']}")
        click.echo(f"状态: {status_info['status']}")
        
        click.echo("\n数据源:")
        ds = status_info['data_sources']
        click.echo(f"  总数: {ds['total']}")
        click.echo(f"  已启用: {ds['enabled']}")
        click.echo(f"  已创建实例: {ds['with_instances']}")
        
        click.echo("\n数据定义:")
        dd = status_info['data_definitions']
        click.echo(f"  快照数据: {dd['snapshot']}")
        click.echo(f"  日频数据: {dd['daily']}")
        click.echo(f"  月频数据: {dd['monthly']}")
        click.echo(f"  总计: {dd['total']}")
        
        click.echo("\nDTO验证器:")
        dto = status_info['dto_validator']
        click.echo(f"  启用: {dto['enabled']}")
        click.echo(f"  规则数: {dto['rules_count']}")
        
        click.echo("\n归档器:")
        archivers = status_info['archivers']
        if archivers:
            for archiver in archivers:
                click.echo(f"  - {archiver}")
        else:
            click.echo("  无")
            
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


@click.command()
def list_sources():
    """列出所有数据源
    
    显示所有可用的数据源及其配置信息
    """
    try:
        service = get_service()
        
        # 获取所有存储类型的数据定义
        snapshot_defs = service.get_data_definitions_by_storage_type('SNAPSHOT')
        daily_defs = service.get_data_definitions_by_storage_type('DAILY')
        monthly_defs = service.get_data_definitions_by_storage_type('MONTHLY')
        
        # 按数据源分组
        sources = {}
        for defs, storage_type in [(snapshot_defs, 'SNAPSHOT'), (daily_defs, 'DAILY'), (monthly_defs, 'MONTHLY')]:
            for key, definition in defs.items():
                source_name = definition['source_name']
                data_type = key.replace(f"{source_name}_", "")
                
                if source_name not in sources:
                    sources[source_name] = {'SNAPSHOT': [], 'DAILY': [], 'MONTHLY': []}
                sources[source_name][storage_type].append(data_type)
        
        click.echo("=== 数据源列表 ===")
        for source_name, types in sources.items():
            click.echo(f"\n{source_name}:")
            for storage_type, data_types in types.items():
                if data_types:
                    click.echo(f"  {storage_type}: {', '.join(data_types)}")
                    
    except Exception as e:
        click.echo(f"错误: {e}", err=True)
        sys.exit(1)


# 导出所有命令
__all__ = [
    'historical_backfill',
    'standard_update', 
    'update_with_lookback',
    'update_with_triple_lookback',
    'fetch_period',
    'fetch_single',
    'status',
    'list_sources'
]