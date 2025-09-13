"""QLIB-READY层CLI模块

提供统一的命令行接口，支持多数据域处理。
"""

from .manager import QlibReadyCLIManager, BaseDomainCLI
from .quotes_cli import QuotesCLI
from .cli import QlibDataProcessorCLI  # 保持向后兼容

__all__ = [
    'QlibReadyCLIManager',
    'BaseDomainCLI', 
    'QuotesCLI',
    'QlibDataProcessorCLI'  # 向后兼容
]