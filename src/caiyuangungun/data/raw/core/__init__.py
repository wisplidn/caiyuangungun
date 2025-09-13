"""Raw数据层核心模块

本模块包含Raw数据层的核心抽象类和基础组件：
- BaseDataSource: 数据源抽象基类
- BaseArchiver: 归档器抽象基类  
- DataSourceManager: 数据源管理器
- ArchiverFactory: 归档器工厂
- ConfigManager: 配置管理器
"""

# 核心抽象类
from .base_data_source import BaseDataSource, DataSourceConfig
from .universal_archiver import UniversalArchiver

# 管理器和工厂类
from .data_source_manager import DataSourceManager
from .config_manager import ConfigManager

__all__ = [
    # 抽象基类
    'BaseDataSource',
    'DataSourceConfig', 
    'UniversalArchiver',
    
    # 管理器
    'DataSourceManager',
    'ConfigManager',
]