#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw数据服务命令行主入口
提供数据采集、任务生成、数据保存等功能的命令行接口

使用方法:
    python cli/main.py <子命令> [选项]

支持的子命令:
    collect     - 数据采集：从指定数据源获取数据并保存
    generate    - 任务生成：生成数据采集任务但不执行
    config      - 配置查看：显示当前配置信息

配置查看示例:
    # 查看所有配置
    python cli/main.py config
    
    # 查看特定数据源配置
    python cli/main.py config --source tushare

参数说明:
    --sources: 数据源列表，用逗号分隔，如 tushare,akshare
    --methods: 数据方法列表，用逗号分隔，如 stock_basic,daily
    --storage-types: 存储类型列表，用逗号分隔，SNAPSHOT(快照)或PERIOD(周期)
    --start-date: 开始日期，格式YYYYMMDD，默认为昨天
    --end-date: 结束日期，格式YYYYMMDD，默认为昨天
    --verbose: 启用详细日志输出
    --dry-run: 仅显示将要执行的操作，不实际执行

注意事项:
    1. 确保在项目根目录下运行此脚本
    2. 首次使用前请确保配置文件已正确设置
    3. 数据源需要相应的API密钥或访问权限
    4. 大量数据采集可能需要较长时间，建议使用--verbose查看进度
"""

import argparse
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# 添加项目路径 - cli的父目录是raw目录
project_root = Path(__file__).resolve().parent.parent  # 先resolve再获取父目录
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'services'))
sys.path.insert(0, str(project_root / 'core'))

try:
    # 使用importlib直接导入
    import importlib.util
    
    # 导入RawDataService
    service_path = project_root / 'services' / 'raw_data_service.py'
    service_spec = importlib.util.spec_from_file_location("raw_data_service", service_path)
    service_module = importlib.util.module_from_spec(service_spec)
    service_spec.loader.exec_module(service_module)
    RawDataService = service_module.RawDataService
    
    # 导入get_config_manager
    config_path = project_root / 'core' / 'config_manager.py'
    config_spec = importlib.util.spec_from_file_location("config_manager", config_path)
    config_module = importlib.util.module_from_spec(config_spec)
    config_spec.loader.exec_module(config_module)
    get_config_manager = config_module.get_config_manager
    
except Exception as e:
    print(f"导入模块失败: {e}")
    print(f"项目根目录: {project_root}")
    print(f"服务文件路径: {project_root / 'services' / 'raw_data_service.py'}")
    print(f"配置文件路径: {project_root / 'core' / 'config_manager.py'}")
    print("请确保在正确的项目目录下运行此脚本")
    sys.exit(1)


def setup_logging(verbose: bool = False):
    """设置日志配置"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_list_argument(arg_value: str) -> List[str]:
    """解析列表参数，支持逗号分隔"""
    if not arg_value:
        return []
    return [item.strip() for item in arg_value.split(',') if item.strip()]


def validate_date_format(date_str: str) -> str:
    """验证日期格式 YYYYMMDD"""
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"日期格式错误: {date_str}，应为 YYYYMMDD 格式")


def get_default_dates() -> tuple:
    """获取默认的开始和结束日期"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # 默认最近7天
    return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')


def cmd_collect_data(args):
    """执行数据采集命令"""
    print("开始执行数据采集任务...")
    
    # 解析参数，如果未提供则使用默认值
    if args.sources:
        data_sources = parse_list_argument(args.sources)
    else:
        # 使用默认数据源
        data_sources = ''  # 从配置中获取的默认值
        print(f"使用默认数据源: {data_sources}")
    
    if args.methods:
        methods = parse_list_argument(args.methods)
    else:
        # 使用默认方法
        methods = ''  # 从配置中获取的默认值
        print(f"使用默认方法: {methods}")
    
    if args.storage_types:
        storage_types = parse_list_argument(args.storage_types)
    else:
        # 使用默认存储类型
        storage_types = ''  # 从配置中获取的默认值
        print(f"使用默认存储类型: {storage_types}")
    
    # 处理日期参数
    start_date = args.start_date
    end_date = args.end_date
    
    if not start_date or not end_date:
        default_start, default_end = get_default_dates()
        start_date = start_date or default_start
        end_date = end_date or default_end
        print(f"使用默认日期范围: {start_date} - {end_date}")
    
    # 构建额外参数
    kwargs = {}
    if args.force_update is not None:
        kwargs['force_update'] = args.force_update
    if args.lookback_multiplier:
        kwargs['lookback_multiplier'] = args.lookback_multiplier
    if args.max_tasks:
        kwargs['max_tasks'] = args.max_tasks
    
    try:
        # 初始化服务
        service = RawDataService()
        
        # 执行数据采集
        result = service.collect_data(
            data_sources=data_sources,
            methods=methods,
            storage_types=storage_types,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        # 输出结果
        print("\n=== 数据采集结果 ===")
        print(f"总任务数: {result.get('total_tasks', 0)}")
        print(f"成功任务: {result.get('successful_tasks', 0)}")
        print(f"失败任务: {result.get('failed_tasks', 0)}")
        print(f"整体状态: {'成功' if result.get('success', False) else '失败'}")
        
        if result.get('errors'):
            print("\n错误信息:")
            for error in result['errors']:
                print(f"  - {error}")
        
        if args.output:
            # 保存详细结果到文件
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n详细结果已保存到: {args.output}")
        
        return 0 if result.get('success', False) else 1
        
    except Exception as e:
        print(f"执行失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def cmd_generate_tasks(args):
    """执行任务生成命令"""
    print("开始生成任务...")
    
    # 解析参数，如果未提供则使用默认值
    if args.sources:
        data_sources = parse_list_argument(args.sources)
    else:
        # 使用默认数据源
        data_sources = ['tushare']  # 从配置中获取的默认值
        print(f"使用默认数据源: {data_sources}")
    
    if args.methods:
        methods = parse_list_argument(args.methods)
    else:
        # 使用默认方法
        methods = ['daily']  # 从配置中获取的默认值
        print(f"使用默认方法: {methods}")
    
    if args.storage_types:
        storage_types = parse_list_argument(args.storage_types)
    else:
        # 使用默认存储类型
        storage_types = ['DAILY']  # 从配置中获取的默认值
        print(f"使用默认存储类型: {storage_types}")
    
    # 处理日期参数
    start_date = args.start_date
    end_date = args.end_date
    
    if not start_date or not end_date:
        default_start, default_end = get_default_dates()
        start_date = start_date or default_start
        end_date = end_date or default_end
        print(f"使用默认日期范围: {start_date} - {end_date}")
    
    # 构建额外参数
    kwargs = {}
    if args.force_update is not None:
        kwargs['force_update'] = args.force_update
    if args.lookback_multiplier:
        kwargs['lookback_multiplier'] = args.lookback_multiplier
    
    try:
        # 初始化服务
        service = RawDataService()
        
        # 生成任务
        result = service.generate_tasks_with_validation(
            data_sources=data_sources,
            methods=methods,
            storage_types=storage_types,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        # 输出结果
        print("\n=== 任务生成结果 ===")
        print(f"生成状态: {'成功' if result.get('success', False) else '失败'}")
        
        if result.get('success'):
            task_blocks = result.get('task_blocks', [])
            print(f"生成任务块数量: {len(task_blocks)}")
            
            if args.show_tasks and task_blocks:
                print("\n任务块详情:")
                for i, task in enumerate(task_blocks[:10]):  # 最多显示10个
                    print(f"  {i+1}. {task.get('data_source', 'unknown')}.{task.get('endpoint', 'unknown')}")
                    if task.get('required_params'):
                        print(f"     参数: {task['required_params']}")
                if len(task_blocks) > 10:
                    print(f"     ... 还有 {len(task_blocks) - 10} 个任务块")
        else:
            print(f"生成失败: {result.get('message', '未知错误')}")
        
        if args.output:
            # 保存结果到文件
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n结果已保存到: {args.output}")
        
        return 0 if result.get('success', False) else 1
        
    except Exception as e:
        print(f"执行失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def cmd_list_config(args):
    """列出配置信息"""
    try:
        config_manager = get_config_manager()
        
        print("=== 配置信息 ===")
        
        if args.sources:
            print("\n可用数据源:")
            # 这里需要根据实际的config_manager API来获取数据源列表
            # 暂时使用示例数据
            sources = ['tushare', 'akshare']  # 从配置中获取
            for source in sources:
                print(f"  - {source}")
        
        if args.methods:
            print("\n可用方法:")
            # 这里需要根据实际的config_manager API来获取方法列表
            methods = ['stock_basic', 'daily', 'trade_cal']  # 从配置中获取
            for method in methods:
                print(f"  - {method}")
        
        if args.storage_types:
            print("\n可用存储类型:")
            storage_types = ['SNAPSHOT', 'PERIOD', 'INCREMENTAL']  # 从配置中获取
            for storage_type in storage_types:
                print(f"  - {storage_type}")
        
        return 0
        
    except Exception as e:
        print(f"获取配置失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='Raw数据服务命令行工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 采集tushare股票基础数据
  python -m cli.main collect --sources tushare --methods stock_basic --storage-types SNAPSHOT
  
  # 采集多个数据源的数据
  python -m cli.main collect --sources tushare,akshare --methods daily --storage-types PERIOD --start-date 20240101 --end-date 20240131
  
  # 生成任务但不执行
  python -m cli.main generate --sources tushare --methods stock_basic --storage-types SNAPSHOT --show-tasks
  
  # 查看可用配置
  python -m cli.main config --sources --methods --storage-types
        """
    )
    
    parser.add_argument('-v', '--verbose', action='store_true', help='详细输出')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # collect 子命令
    collect_parser = subparsers.add_parser('collect', help='执行数据采集')
    collect_parser.add_argument('--sources', help='数据源列表，逗号分隔 (如: tushare,akshare)，默认: tushare')
    collect_parser.add_argument('--methods', help='方法列表，逗号分隔 (如: stock_basic,daily)，默认: daily')
    collect_parser.add_argument('--storage-types', help='存储类型列表，逗号分隔 (如: SNAPSHOT,PERIOD)，默认: DAILY')
    collect_parser.add_argument('--start-date', type=validate_date_format, help='开始日期 (YYYYMMDD格式)')
    collect_parser.add_argument('--end-date', type=validate_date_format, help='结束日期 (YYYYMMDD格式)')
    collect_parser.add_argument('--force-update', type=bool, help='是否强制更新 (true/false)')
    collect_parser.add_argument('--lookback-multiplier', type=int, help='回看周期扩展倍数')
    collect_parser.add_argument('--max-tasks', type=int, default=999999, help='任务数上限，默认: 999999')
    collect_parser.add_argument('--output', help='输出结果到文件 (JSON格式)')
    collect_parser.set_defaults(func=cmd_collect_data)
    
    # generate 子命令
    generate_parser = subparsers.add_parser('generate', help='生成任务但不执行')
    generate_parser.add_argument('--sources', help='数据源列表，逗号分隔，默认: tushare')
    generate_parser.add_argument('--methods', help='方法列表，逗号分隔，默认: daily')
    generate_parser.add_argument('--storage-types', help='存储类型列表，逗号分隔，默认: DAILY')
    generate_parser.add_argument('--start-date', type=validate_date_format, help='开始日期 (YYYYMMDD格式)')
    generate_parser.add_argument('--end-date', type=validate_date_format, help='结束日期 (YYYYMMDD格式)')
    generate_parser.add_argument('--force-update', type=bool, help='是否强制更新 (true/false)')
    generate_parser.add_argument('--lookback-multiplier', type=int, help='回看周期扩展倍数')
    generate_parser.add_argument('--show-tasks', action='store_true', help='显示生成的任务详情')
    generate_parser.add_argument('--output', help='输出结果到文件 (JSON格式)')
    generate_parser.set_defaults(func=cmd_generate_tasks)
    
    # config 子命令
    config_parser = subparsers.add_parser('config', help='查看配置信息')
    config_parser.add_argument('--sources', action='store_true', help='显示可用数据源')
    config_parser.add_argument('--methods', action='store_true', help='显示可用方法')
    config_parser.add_argument('--storage-types', action='store_true', help='显示可用存储类型')
    config_parser.set_defaults(func=cmd_list_config)
    
    return parser


def main():
    """主入口函数"""
    parser = create_parser()
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.verbose)
    
    # 如果没有指定命令，显示帮助
    if not args.command:
        parser.print_help()
        return 1
    
    # 执行对应的命令函数
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\n用户中断操作")
        return 130
    except Exception as e:
        print(f"执行出错: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())