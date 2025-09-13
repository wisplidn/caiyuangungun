# 从main和commands模块导入主要函数
from .main import main, cli
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

# CLI版本信息
__version__ = '1.0.0'

# 命令注册表
COMMAND_REGISTRY = {
    'backfill': {
        'function': historical_backfill,
        'description': '历史数据回填，根据配置的start_date作为起点，自动跳过已存在的数据文件',
        'category': 'data_operations'
    },
    'update': {
        'function': standard_update,
        'description': '标准数据更新，仅更新最新的一份数据，不跳过已存在文件',
        'category': 'data_operations'
    },
    'update-lookback': {
        'function': update_with_lookback,
        'description': '数据更新含回溯，最新数据 + lookback_periods 的数据份数',
        'category': 'data_operations'
    },
    'update-triple': {
        'function': update_with_triple_lookback,
        'description': '数据更新含三倍回溯，最新数据 + lookback_periods*3 的数据份数',
        'category': 'data_operations'
    },
    'fetch-period': {
        'function': fetch_period,
        'description': '指定期间数据获取，获取指定时间范围的数据',
        'category': 'data_operations'
    },
    'fetch': {
        'function': fetch_single,
        'description': '获取单个数据，获取指定数据源和数据类型的单个数据',
        'category': 'data_operations'
    },
    'status': {
        'function': status,
        'description': '显示服务状态，显示数据源、数据定义等服务状态信息',
        'category': 'information'
    },
    'list': {
        'function': list_sources,
        'description': '列出所有数据源，显示所有可用的数据源及其配置信息',
        'category': 'information'
    }
}

# 导出的主要接口
__all__ = [
    'main', 'cli',
    'historical_backfill', 'standard_update', 'update_with_lookback',
    'update_with_triple_lookback', 'fetch_period', 'fetch_single',
    'status', 'list_sources',
    'get_available_commands', 'get_commands_by_category'
]