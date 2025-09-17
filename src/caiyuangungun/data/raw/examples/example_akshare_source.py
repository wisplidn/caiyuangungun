#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试重构后的AkShare数据源
"""

import sys
import os
from pathlib import Path

# 添加必要的路径到Python路径
raw_data_path = Path(__file__).parent.parent
sys.path.insert(0, str(raw_data_path))
sys.path.insert(0, str(raw_data_path / 'sources'))
sys.path.insert(0, str(raw_data_path / 'core'))

from akshare_source import AkshareDataSource
from base_data_source import DataSourceConfig

def test_akshare_basic():
    """测试基本功能"""
    print("=== 测试AkShare数据源基本功能 ===")
    
    # 测试1: 使用默认配置
    print("\n1. 测试默认配置初始化")
    default_config = DataSourceConfig(
        name="akshare",
        source_type="akshare",
        connection_params={}
    )
    source = AkshareDataSource(default_config)
    print(f"初始化成功，连接状态: {source.is_connected()}")
    
    # 测试2: 连接
    print("\n2. 测试连接")
    connected = source.connect()
    print(f"连接结果: {connected}")
    print(f"连接状态: {source.is_connected()}")
    
    # 测试3: 使用DataSourceConfig配置
    print("\n3. 测试DataSourceConfig配置")
    config = DataSourceConfig(
        name="akshare",
        source_type="akshare",
        connection_params={
            "timeout": 60,
            "max_requests_per_minute": 30,
            "retry_count": 2,
            "retry_delay": 2.0
        }
    )
    source2 = AkshareDataSource(config)
    source2.connect()
    print(f"DataSourceConfig初始化成功，连接状态: {source2.is_connected()}")
    
    # 测试4: 使用自定义DataSourceConfig配置
    print("\n4. 测试自定义DataSourceConfig配置")
    custom_config = DataSourceConfig(
        name="akshare_custom",
        source_type="akshare",
        connection_params={
            "timeout": 45,
            "max_requests_per_minute": 40,
            "retry_count": 1,
            "retry_delay": 1.5
        }
    )
    source3 = AkshareDataSource(custom_config)
    source3.connect()
    print(f"AkshareConfig初始化成功，连接状态: {source3.is_connected()}")
    
    return source

def test_akshare_data_fetch(source):
    """测试数据获取功能"""
    print("\n=== 测试数据获取功能 ===")
    
    try:
        # 测试1: 获取A股实时行情
        print("\n1. 测试获取A股实时行情 (stock_zh_a_spot_em)")
        data1 = source.fetch_data('stock_zh_a_spot_em')
        print(f"获取成功，数据形状: {data1.shape}")
        print(f"列名: {list(data1.columns)[:5]}...")
        
        # 测试2: 获取股票历史数据
        print("\n2. 测试获取股票历史数据 (stock_zh_a_hist)")
        data2 = source.fetch_data('stock_zh_a_hist', 
                                 symbol='000001', 
                                 period='daily', 
                                 start_date='20240101', 
                                 end_date='20240131')
        print(f"获取成功，数据形状: {data2.shape}")
        print(f"列名: {list(data2.columns)}")
        
        # 测试3: 获取交易日历
        print("\n3. 测试获取交易日历 (tool_trade_date_hist_sina)")
        data3 = source.fetch_data('tool_trade_date_hist_sina')
        print(f"获取成功，数据形状: {data3.shape}")
        print(f"最近几个交易日: {data3.tail(3).values.flatten()}")
        
        print("\n所有数据获取测试通过！")
        
    except Exception as e:
        print(f"数据获取测试失败: {e}")
        raise

def test_akshare_disconnect(source):
    """测试断开连接"""
    print("\n=== 测试断开连接 ===")
    
    print(f"断开前连接状态: {source.is_connected()}")
    source.disconnect()
    print(f"断开后连接状态: {source.is_connected()}")
    print("断开连接测试通过！")

def main():
    """主测试函数"""
    try:
        # 基本功能测试
        source = test_akshare_basic()
        
        # 数据获取测试
        test_akshare_data_fetch(source)
        
        # 断开连接测试
        test_akshare_disconnect(source)
        
        print("\n=== 所有测试通过！ ===")
        
    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)