#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
行情数据域CLI处理器

专门处理行情数据的命令行接口。
"""

import argparse
import sys
from datetime import datetime, date
from pathlib import Path

from .manager import BaseDomainCLI
from ..processors.quotes.manager import QlibReadyDataManager
from ..processors.quotes.processor import QlibDataProcessor
from ..core.validator import QlibFormatValidator
from ...contracts import InterfaceType


class QuotesCLI(BaseDomainCLI):
    """行情数据CLI处理器"""
    
    @property
    def domain_name(self) -> str:
        return "quotes"
    
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """添加行情数据相关的命令行参数"""
        subparsers = parser.add_subparsers(
            dest='action',
            help='操作类型'
        )
        
        # 处理数据命令
        process_parser = subparsers.add_parser(
            'process',
            help='处理行情数据'
        )
        process_parser.add_argument(
            '--start-date',
            type=str,
            help='开始日期 (YYYY-MM-DD)'
        )
        process_parser.add_argument(
            '--end-date', 
            type=str,
            help='结束日期 (YYYY-MM-DD)'
        )
        process_parser.add_argument(
            '--symbols',
            nargs='*',
            help='股票代码列表 (可选)'
        )
        process_parser.add_argument(
            '--force',
            action='store_true',
            help='强制重新处理已存在的数据'
        )
        
        # 验证数据命令
        validate_parser = subparsers.add_parser(
            'validate',
            help='验证行情数据格式'
        )
        validate_parser.add_argument(
            '--symbols',
            nargs='*',
            help='要验证的股票代码列表'
        )
        
        # 列出股票命令
        list_parser = subparsers.add_parser(
            'list',
            help='列出可用的股票代码'
        )
        list_parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='显示数量限制 (默认: 50)'
        )
        
        # 清理数据命令
        clean_parser = subparsers.add_parser(
            'clean',
            help='清理行情数据'
        )
        clean_parser.add_argument(
            '--confirm',
            action='store_true',
            help='确认删除操作'
        )
    
    def handle_command(self, args: argparse.Namespace) -> int:
        """处理行情数据命令"""
        if not args.action:
            print("错误: 请指定操作类型", file=sys.stderr)
            return 1
        
        # 初始化管理器
        data_root = args.data_root or str(Path.cwd())
        manager = QlibReadyDataManager(data_root)
        
        try:
            if args.action == 'process':
                return self._handle_process(args, manager)
            elif args.action == 'validate':
                return self._handle_validate(args, manager)
            elif args.action == 'list':
                return self._handle_list(args, manager)
            elif args.action == 'clean':
                return self._handle_clean(args, manager)
            else:
                print(f"错误: 未知的操作 '{args.action}'", file=sys.stderr)
                return 1
        except Exception as e:
            print(f"错误: {e}", file=sys.stderr)
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def _handle_process(self, args: argparse.Namespace, manager: QlibReadyDataManager) -> int:
        """处理数据处理命令"""
        print("开始处理行情数据...")
        
        # 解析日期参数
        start_date = None
        end_date = None
        
        if args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"错误: 无效的开始日期格式 '{args.start_date}'", file=sys.stderr)
                return 1
        
        if args.end_date:
            try:
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"错误: 无效的结束日期格式 '{args.end_date}'", file=sys.stderr)
                return 1
        
        # 处理数据
        result = manager.process_data(
            interface_type=InterfaceType.QUOTES_DAILY,
            start_date=start_date,
            end_date=end_date,
            symbols=args.symbols,
            force_update=args.force
        )
        
        if result:
            print("行情数据处理完成")
            return 0
        else:
            print("行情数据处理失败", file=sys.stderr)
            return 1
    
    def _handle_validate(self, args: argparse.Namespace, manager: QlibReadyDataManager) -> int:
        """处理数据验证命令"""
        print("开始验证行情数据...")
        
        validator = QlibFormatValidator()
        symbols = args.symbols or manager.list_symbols(InterfaceType.QUOTES_DAILY)
        
        valid_count = 0
        total_count = len(symbols)
        
        for symbol in symbols:
            try:
                data_path = manager.get_data_path(InterfaceType.QUOTES_DAILY, symbol)
                if data_path.exists():
                    is_valid = validator.validate_qlib_format(data_path)
                    if is_valid:
                        valid_count += 1
                        if args.verbose:
                            print(f"✓ {symbol}: 格式正确")
                    else:
                        print(f"✗ {symbol}: 格式错误")
                else:
                    print(f"- {symbol}: 数据文件不存在")
            except Exception as e:
                print(f"✗ {symbol}: 验证失败 - {e}")
        
        print(f"\n验证完成: {valid_count}/{total_count} 个文件格式正确")
        return 0 if valid_count == total_count else 1
    
    def _handle_list(self, args: argparse.Namespace, manager: QlibReadyDataManager) -> int:
        """处理列表命令"""
        symbols = manager.list_symbols(InterfaceType.QUOTES_DAILY)
        
        if not symbols:
            print("没有找到可用的股票代码")
            return 0
        
        print(f"找到 {len(symbols)} 个股票代码:")
        for i, symbol in enumerate(symbols[:args.limit]):
            print(f"  {symbol}")
        
        if len(symbols) > args.limit:
            print(f"  ... 还有 {len(symbols) - args.limit} 个股票代码")
        
        return 0
    
    def _handle_clean(self, args: argparse.Namespace, manager: QlibReadyDataManager) -> int:
        """处理清理命令"""
        if not args.confirm:
            print("警告: 此操作将删除所有行情数据")
            print("请使用 --confirm 参数确认删除操作")
            return 1
        
        print("开始清理行情数据...")
        result = manager.clean_data(InterfaceType.QUOTES_DAILY)
        
        if result:
            print("行情数据清理完成")
            return 0
        else:
            print("行情数据清理失败", file=sys.stderr)
            return 1