"""QLIB-READY层数据处理模块

本模块负责生成符合Qlib格式要求的数据，包括：
- 从daily_quotes提取日频行情数据
- 与复权因子和基础数据进行左连接
- 数据清洗和质量检查
- 按feature分组生成CSV文件
"""

# 核心组件
from .core.base_processor import BaseQlibProcessor
from .core.base_manager import BaseQlibManager
from .core.validator import QlibFormatValidator

# 行情数据处理器
from .processors.quotes.manager import QlibReadyDataManager
from .processors.quotes.processor import QlibDataProcessor

# CLI接口
from .cli.manager import QlibReadyCLIManager
from .cli.quotes_cli import QuotesCLI
from .cli.cli import QlibDataProcessorCLI  # 向后兼容

__all__ = [
    # 核心抽象类
    'BaseQlibProcessor',
    'BaseQlibManager',
    'QlibFormatValidator',
    
    # 行情数据处理
    'QlibReadyDataManager',
    'QlibDataProcessor',
    
    # CLI接口
    'QlibReadyCLIManager',
    'QuotesCLI',
    'QlibDataProcessorCLI'  # 向后兼容
]