"""
ConfigManager独立测试模块
测试ConfigManager的配置读取和管理功能
"""

import os
import sys
import tempfile
import json
from pathlib import Path

# 添加项目根目录到Python路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir.parent))

from core.config_manager import ConfigManager


def get_real_config_dir():
    """获取真实配置文件目录"""
    return "/Users/daishun/个人文档/caiyuangungun/data/config"


def create_test_configs(temp_config_dir):
    """创建测试配置文件"""
    # 创建unified_data_config.json
    unified_config = {
        "data_sources": {
            "tushare": {
                "name": "tushare",
                "source_type": "tushare",
                "enabled": True,
                "methods": {
                    "daily": {
                        "data_source": "tushare",
                        "endpoint": "daily",
                        "description": "获取A股日线行情",
                        "storage_type": "DAILY",
                        "source_name": "tushare",
                        "enable": True
                    },
                    "trade_cal": {
                        "data_source": "tushare",
                        "endpoint": "trade_cal",
                        "description": "获取交易日历",
                        "storage_type": "MONTHLY",
                        "source_name": "tushare",
                        "enable": True
                    },
                    "stock_basic": {
                        "data_source": "tushare",
                        "endpoint": "stock_basic",
                        "description": "获取基础信息数据",
                        "storage_type": "SNAPSHOT",
                        "source_name": "tushare",
                        "enable": False
                    }
                }
            }
        },
        "archive_types": {
            "DAILY": {
                "name": "DAILY",
                "description": "日级别数据",
                "path_pattern": "{year_month}/{day}",
                "enabled": True
            },
            "MONTHLY": {
                "name": "MONTHLY", 
                "description": "月级别数据",
                "path_pattern": "{year_month}",
                "enabled": True
            },
            "SNAPSHOT": {
                "name": "SNAPSHOT",
                "description": "快照数据",
                "path_pattern": "",
                "enabled": False
            }
        }
    }
    
    with open(os.path.join(temp_config_dir, "unified_data_config.json"), 'w', encoding='utf-8') as f:
        json.dump(unified_config, f, ensure_ascii=False, indent=2)
    
    # 创建path_generator_config.json
    path_config = {
        "path_generator": {
            "base_path": "/test/data/path",
            "paths": {
                "landing_subpath": "landing",
                "archive_subpath": "archive",
                "norm_subpath": "norm"
            },
            "file_config": {
                "supported_formats": ["parquet", "json", "csv"],
                "default_format": "parquet"
            }
        }
    }
    
    with open(os.path.join(temp_config_dir, "path_generator_config.json"), 'w', encoding='utf-8') as f:
        json.dump(path_config, f, ensure_ascii=False, indent=2)
    
    # 创建tushare_limit_config.json
    tushare_config = {
        "tushare_limits": {
            "daily_limit": 1000,
            "monthly_limit": 500,
            "api_key": "test_api_key"
        }
    }
    
    with open(os.path.join(temp_config_dir, "tushare_limit_config.json"), 'w', encoding='utf-8') as f:
        json.dump(tushare_config, f, ensure_ascii=False, indent=2)


def test_config_manager_standalone():
    """测试ConfigManager独立功能"""
    print("=== ConfigManager独立测试（使用真实配置文件）===")
    
    # 使用真实配置目录
    config_dir = get_real_config_dir()
    print(f"\n配置目录: {config_dir}")
    
    # 创建ConfigManager实例
    config_manager = ConfigManager(config_dir=config_dir)
    
    # 测试1: 获取方法列表
    print("【测试1】获取方法列表")
    print("输入: 无")
    methods_list = config_manager.get_methods_list()
    print(f"输出: 共{len(methods_list)}个方法")
    for i, method in enumerate(methods_list, 1):
        print(f"  [{i}] {method}")
    print()
    
    # 测试2: 获取归档类型列表
    print("【测试2】获取归档类型列表")
    print("输入: 无")
    archive_types = config_manager.get_archive_types_list()
    print(f"输出: 共{len(archive_types)}个归档类型")
    for i, archive_type in enumerate(archive_types, 1):
        print(f"  [{i}] {archive_type}")
    print()
    
    # 测试3: 获取方法信息
    print("【测试3】获取方法信息")
    print("输入: method_name='daily'")
    daily_info = config_manager.get_method_info("daily")
    print(f"输出: {daily_info}")
    print()
    
    print("输入: method_name='trade_cal'")
    trade_cal_info = config_manager.get_method_info("trade_cal")
    print(f"输出: {trade_cal_info}")
    print()
    
    # 测试4: 获取归档类型信息
    print("【测试4】获取归档类型信息")
    print("输入: archive_type='DAILY'")
    daily_archive_info = config_manager.get_archive_type_info("DAILY")
    print(f"输出: {daily_archive_info}")
    print()
    
    # 测试5: 获取基础路径
    print("【测试5】获取基础路径")
    print("输入: 无")
    base_path = config_manager.get_base_path()
    print(f"输出: {base_path}")
    print()
    
    # 测试6: 获取支持的格式
    print("【测试6】获取支持的格式")
    print("输入: 无")
    supported_formats = config_manager.get_supported_formats()
    print(f"输出: {supported_formats}")
    print()
    
    # 测试7: 获取默认格式
    print("9. 测试获取默认格式:")
    default_format = config_manager.get_default_format()
    print(f"   默认格式: {default_format}")
    
    # 测试北交所代码映射功能
    print("\n10. 测试北交所代码映射功能:")
    bse_mapping = config_manager.get_bse_code_mapping()
    print(f"   映射表条目数: {len(bse_mapping)}")
    if bse_mapping:
        # 显示前3个映射关系作为示例
        sample_items = list(bse_mapping.items())[:3]
        for old_code, new_code in sample_items:
            print(f"   {old_code} -> {new_code}")
    else:
        print("   未找到北交所代码映射数据")
    print()
    
    # 测试股票基础信息读取功能
    print("\n11. 测试股票基础信息读取功能:")
    try:
        stock_basic_df = config_manager.get_stock_basic_info()
        print(f"   股票基础信息DataFrame形状: {stock_basic_df.shape}")
        print(f"   列名: {list(stock_basic_df.columns)}")
        if not stock_basic_df.empty:
            print(f"   前3行数据:")
            print(stock_basic_df.head(3).to_string(index=False))
    except Exception as e:
        print(f"   读取股票基础信息失败: {e}")
    print()
    
    # 测试BSE映射字典获取功能
    print("\n12. 测试BSE映射字典获取功能:")
    try:
        bse_mapping_dict = config_manager.get_bse_mapping_dict()
        print(f"   BSE映射字典条目数: {len(bse_mapping_dict)}")
        if bse_mapping_dict:
            sample_items = list(bse_mapping_dict.items())[:3]
            for old_code, new_code in sample_items:
                print(f"   {old_code} -> {new_code}")
    except Exception as e:
        print(f"   获取BSE映射字典失败: {e}")
    print()
    
    # 测试8: 检查方法是否启用
    print("【测试8】检查方法是否启用")
    print("输入: method_name='daily'")
    daily_enabled = config_manager.is_method_enabled("daily")
    print(f"输出: {daily_enabled}")
    print()
    
    print("输入: method_name='stock_basic'")
    stock_basic_enabled = config_manager.is_method_enabled("stock_basic")
    print(f"输出: {stock_basic_enabled}")
    print()
    
    # 测试9: 检查归档类型是否启用
    print("【测试9】检查归档类型是否启用")
    print("输入: archive_type='DAILY'")
    daily_archive_enabled = config_manager.is_archive_type_enabled("DAILY")
    print(f"输出: {daily_archive_enabled}")
    print()
    
    print("输入: archive_type='SNAPSHOT'")
    snapshot_enabled = config_manager.is_archive_type_enabled("SNAPSHOT")
    print(f"输出: {snapshot_enabled}")
    print()
    
    print("=== ConfigManager独立测试完成 ===")


if __name__ == "__main__":
    test_config_manager_standalone()