"""数据源实现模块

提供各种具体数据源的实现，包括：
- Tushare数据源：股票、基金、期货等金融数据
- Wind数据源：专业金融数据服务
- 本地文件数据源：CSV、Excel等文件格式
- 数据库数据源：MySQL、PostgreSQL等数据库

每个数据源都继承自BaseDataSource，实现统一的接口规范。
"""

from .tushare_source import TushareDataSource

# 导出所有数据源实现
__all__ = [
    'TushareDataSource',
]

# 数据源注册信息
DATA_SOURCE_REGISTRY = {
    'tushare': {
        'class': TushareDataSource,
        'name': 'Tushare数据源',
        'description': '提供股票、基金、期货等金融数据',
        'supported_assets': ['stock', 'fund', 'future', 'option'],
        'requires_token': True,
    },
}


def get_available_sources():
    """获取所有可用的数据源
    
    Returns:
        Dict[str, Dict]: 数据源注册信息
    """
    return DATA_SOURCE_REGISTRY.copy()


def get_source_class(source_type: str):
    """根据类型获取数据源类
    
    Args:
        source_type: 数据源类型
        
    Returns:
        Type[BaseDataSource]: 数据源类
        
    Raises:
        ValueError: 不支持的数据源类型
    """
    if source_type not in DATA_SOURCE_REGISTRY:
        available = list(DATA_SOURCE_REGISTRY.keys())
        raise ValueError(f"不支持的数据源类型: {source_type}，可用类型: {available}")
        
    return DATA_SOURCE_REGISTRY[source_type]['class']