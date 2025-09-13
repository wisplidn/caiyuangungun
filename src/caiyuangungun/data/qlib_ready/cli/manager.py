#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QLIB-READY层CLI管理器

统一管理多个数据域的CLI接口，提供扩展性和一致性。
"""

import argparse
import sys
from typing import Dict, Type, Optional
from abc import ABC, abstractmethod

from ..core.base_processor import BaseQlibProcessor
from ..core.base_manager import BaseQlibManager


class BaseDomainCLI(ABC):
    """数据域CLI基类"""
    
    @property
    @abstractmethod
    def domain_name(self) -> str:
        """数据域名称"""
        pass
    
    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """添加特定于域的命令行参数"""
        pass
    
    @abstractmethod
    def handle_command(self, args: argparse.Namespace) -> int:
        """处理命令"""
        pass


class QlibReadyCLIManager:
    """QLIB-READY层CLI管理器
    
    统一管理所有数据域的CLI接口，支持动态注册和扩展。
    """
    
    def __init__(self):
        self.domain_handlers: Dict[str, BaseDomainCLI] = {}
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """注册默认的数据域处理器"""
        # 导入并注册行情数据处理器
        try:
            from .quotes_cli import QuotesCLI
            self.register_domain_handler(QuotesCLI())
        except ImportError:
            pass  # 如果模块不存在，跳过
    
    def register_domain_handler(self, handler: BaseDomainCLI):
        """注册数据域处理器"""
        self.domain_handlers[handler.domain_name] = handler
    
    def create_parser(self) -> argparse.ArgumentParser:
        """创建命令行解析器"""
        parser = argparse.ArgumentParser(
            description='QLIB-READY层数据处理工具',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        # 添加全局参数
        parser.add_argument(
            '--data-root',
            type=str,
            help='数据根目录路径'
        )
        
        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='启用详细输出'
        )
        
        # 创建子命令
        subparsers = parser.add_subparsers(
            dest='domain',
            help='数据域类型',
            metavar='DOMAIN'
        )
        
        # 为每个数据域创建子解析器
        for domain_name, handler in self.domain_handlers.items():
            domain_parser = subparsers.add_parser(
                domain_name,
                help=f'{domain_name}数据处理'
            )
            handler.add_arguments(domain_parser)
        
        return parser
    
    def run(self, args: Optional[list] = None) -> int:
        """运行CLI"""
        parser = self.create_parser()
        parsed_args = parser.parse_args(args)
        
        if not parsed_args.domain:
            parser.print_help()
            return 1
        
        if parsed_args.domain not in self.domain_handlers:
            print(f"错误: 未知的数据域 '{parsed_args.domain}'", file=sys.stderr)
            print(f"可用的数据域: {', '.join(self.domain_handlers.keys())}", file=sys.stderr)
            return 1
        
        handler = self.domain_handlers[parsed_args.domain]
        return handler.handle_command(parsed_args)


def main():
    """主入口函数"""
    manager = QlibReadyCLIManager()
    return manager.run()


if __name__ == '__main__':
    sys.exit(main())