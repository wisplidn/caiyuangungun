#!/usr/bin/env python3
"""Tushare数据源测试脚本

测试TushareDataSource的基础功能，包括连接、注册和四个API请求：
1. stock_basic - 股票基础信息
2. trade_cal - 交易日历
3. daily - 日行情数据
4. cashflow_vip - 现金流量表
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
raw_data_path = Path(__file__).parent.parent
sys.path.insert(0, str(raw_data_path))
sys.path.insert(0, str(raw_data_path / 'sources'))
sys.path.insert(0, str(raw_data_path / 'core'))

import pandas as pd
from tushare_source import TushareDataSource
from base_data_source import DataSourceConfig
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_tushare_connection():
    """测试Tushare连接功能"""
    print("\n=== 测试Tushare连接功能 ===")
    
    # 创建配置
    from base_data_source import DataSourceConfig
    config = DataSourceConfig(
        name="tushare_test",
        source_type="tushare",
        connection_params={
            "token": "q90f4bdab293fe80b426887ee2afa2d3182",
            "max_requests_per_minute": 300,
            "timeout": 30,
            "retry_count": 3
        }
    )
    
    # 创建数据源实例
    tushare_source = TushareDataSource(config)
    
    # 测试连接
    print("正在连接Tushare API...")
    success = tushare_source.connect()
    
    if success:
        print("✅ Tushare API连接成功")
        print(f"连接状态: {tushare_source.is_connected()}")
        print(f"数据源信息: {tushare_source.get_source_info()}")
        return tushare_source
    else:
        print("❌ Tushare API连接失败")
        return None

def test_stock_basic(tushare_source):
    """测试stock_basic方法"""
    print("\n=== 测试stock_basic方法 ===")
    
    try:
        # 调用stock_basic接口
        result = tushare_source.fetch_data("stock_basic")
        
        print(f"✅ stock_basic调用成功")
        print(f"数据行数: {len(result)}")
        print(f"数据列数: {len(result.columns)}")
        if not result.empty:
            print(f"列名: {list(result.columns)}")
            print("前5行数据:")
            print(result.head())
        
        return True
        
    except Exception as e:
        print(f"❌ stock_basic调用失败: {e}")
        return False

def test_trade_cal(tushare_source):
    """测试trade_cal方法"""
    print("\n=== 测试trade_cal方法 ===")
    
    try:
        # 调用trade_cal接口
        result = tushare_source.fetch_data("trade_cal", 
                                         start_date="20000101",
                                         end_date="20990101")
        
        print(f"✅ trade_cal调用成功")
        print(f"数据行数: {len(result)}")
        print(f"数据列数: {len(result.columns)}")
        if not result.empty:
            print(f"列名: {list(result.columns)}")
            print("前5行数据:")
            print(result.head())
        
        return True
        
    except Exception as e:
        print(f"❌ trade_cal调用失败: {e}")
        return False

def test_daily(tushare_source):
    """测试daily方法"""
    print("\n=== 测试daily方法 ===")
    
    try:
        # 调用daily接口
        result = tushare_source.fetch_data("daily", trade_date="20250912")
        
        print(f"✅ daily调用成功")
        print(f"数据行数: {len(result)}")
        print(f"数据列数: {len(result.columns)}")
        if not result.empty:
            print(f"列名: {list(result.columns)}")
            print("前5行数据:")
            print(result.head())
        
        return True
        
    except Exception as e:
        print(f"❌ daily调用失败: {e}")
        return False

def test_cashflow_vip(tushare_source):
    """测试cashflow_vip方法"""
    print("\n=== 测试cashflow_vip方法 ===")
    
    try:
        # 调用cashflow_vip接口
        result = tushare_source.fetch_data("cashflow_vip", 
                                         start_date="20240401",
                                         end_date="20240430",
                                         report_type="1")
        
        print(f"✅ cashflow_vip调用成功")
        print(f"数据行数: {len(result)}")
        print(f"数据列数: {len(result.columns)}")
        if not result.empty:
            print(f"列名: {list(result.columns)}")
            print("前5行数据:")
            print(result.head())
        
        return True
        
    except Exception as e:
        print(f"❌ cashflow_vip调用失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始测试Tushare数据源基础功能")
    print("=" * 50)
    
    # 测试连接
    tushare_source = test_tushare_connection()
    if not tushare_source:
        print("\n❌ 连接失败，无法继续测试")
        return
    
    # 测试结果统计
    test_results = []
    
    # 测试各个API方法
    test_results.append(("stock_basic", test_stock_basic(tushare_source)))
    test_results.append(("trade_cal", test_trade_cal(tushare_source)))
    test_results.append(("daily", test_daily(tushare_source)))
    test_results.append(("cashflow_vip", test_cashflow_vip(tushare_source)))
    
    # 断开连接
    print("\n=== 断开连接 ===")
    tushare_source.disconnect()
    print(f"连接状态: {tushare_source.is_connected()}")
    
    # 输出测试结果汇总
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    success_count = 0
    for method_name, success in test_results:
        status = "✅ 成功" if success else "❌ 失败"
        print(f"  {method_name}: {status}")
        if success:
            success_count += 1
    
    print(f"\n总计: {success_count}/{len(test_results)} 个测试通过")
    
    if success_count == len(test_results):
        print("🎉 所有测试通过！")
    else:
        print("⚠️  部分测试失败，请检查日志")

if __name__ == "__main__":
    main()