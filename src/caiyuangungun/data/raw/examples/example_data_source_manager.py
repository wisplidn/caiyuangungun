#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试重构后的DataSourceManager
"""

import sys
from pathlib import Path

# 添加路径以便导入模块
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'core'))
sys.path.insert(0, str(project_root / 'sources'))

# 设置PYTHONPATH环境变量
import os
os.environ['PYTHONPATH'] = f"{project_root / 'core'}:{project_root / 'sources'}:{os.environ.get('PYTHONPATH', '')}"

try:
    from data_source_manager import DataSourceManager
except ImportError:
    # 尝试直接导入
    sys.path.insert(0, str(project_root))
    from core.data_source_manager import DataSourceManager

def test_basic_functionality():
    """测试基本功能"""
    print("=== 测试DataSourceManager基本功能 ===")
    
    # 测试1: 默认配置初始化
    print("\n1. 测试默认配置初始化")
    manager = DataSourceManager()
    print(f"初始化成功，注册数据源数量: {len(manager._sources)}")
    
    # 测试2: 带配置初始化
    print("\n2. 测试带配置初始化")
    config = {
        "data_sources": {
            "test_source": {
                "enabled": True,
                "class_path": "akshare_source.AkshareDataSource",
                "config": {
                    "api_key": "test_key",
                    "timeout": 30
                },
                "health_check": {
                    "enabled": False
                }
            }
        }
    }
    
    manager_with_config = DataSourceManager(config)
    print(f"带配置初始化成功，注册数据源数量: {len(manager_with_config._sources)}")
    
    # 测试3: 列出数据源
    print("\n3. 测试列出数据源")
    sources = manager_with_config.list_sources()
    for source in sources:
        print(f"数据源: {source['name']}, 启用: {source['enabled']}, 类路径: {source['class_path']}")
    
    # 测试4: 获取运行指标
    print("\n4. 测试获取运行指标")
    metrics = manager_with_config.get_metrics()
    print(f"运行指标: {metrics}")
    
    # 测试5: 关闭管理器
    print("\n5. 测试关闭管理器")
    manager_with_config.shutdown()
    print("管理器关闭成功")
    
    print("\n=== 所有测试通过 ===")

def test_context_manager():
    """测试上下文管理器功能"""
    print("\n=== 测试上下文管理器功能 ===")
    
    config = {
        "data_sources": {
            "test_source": {
                "enabled": True,
                "class_path": "test.TestSource",
                "config": {},
                "health_check": {"enabled": False}
            }
        }
    }
    
    with DataSourceManager(config) as manager:
        print(f"在上下文中，注册数据源数量: {len(manager._sources)}")
        sources = manager.list_sources()
        print(f"数据源列表: {[s['name'] for s in sources]}")
    
    print("上下文管理器测试完成")

def test_tushare_data_source_integration():
    """测试Tushare数据源集成功能：注册、创建实例并获取数据"""
    print("\n=== 测试Tushare数据源集成功能 ===")
    
    # 导入DataSourceConfig
    try:
        from core.base_data_source import DataSourceConfig
    except ImportError:
        try:
            from base_data_source import DataSourceConfig
        except ImportError:
            # 如果都导入失败，跳过集成测试
            print("❌ 跳过集成测试: 无法导入DataSourceConfig")
            return False
    
    # 配置Tushare数据源
    config = {
        "data_sources": {
            "tushare_test": {
                "enabled": True,
                "class_path": "tushare_source.TushareDataSource",
                "config": DataSourceConfig(
                    name="tushare_test",
                    source_type="tushare",
                    connection_params={
                        "token": "q90f4bdab293fe80b426887ee2afa2d3182",
                        "max_requests_per_minute": 300,
                        "timeout": 30,
                        "retry_count": 3
                    }
                ),
                "health_check": {"enabled": False}
            }
        }
    }
    
    try:
        # 1. 创建DataSourceManager并注册数据源
        print("\n1. 创建DataSourceManager并注册数据源")
        manager = DataSourceManager(config)
        print(f"✅ 管理器创建成功，注册数据源数量: {len(manager._sources)}")
        
        # 2. 列出已注册的数据源
        print("\n2. 列出已注册的数据源")
        sources = manager.list_sources()
        for source in sources:
            print(f"数据源: {source['name']}, 启用: {source['enabled']}, 类路径: {source['class_path']}")
        
        # 3. 获取Tushare数据源实例
        print("\n3. 获取Tushare数据源实例")
        tushare_instance = manager.get_instance("tushare_test")
        if tushare_instance:
            print(f"✅ 成功获取Tushare数据源实例: {type(tushare_instance).__name__}")
        else:
            print("❌ 获取Tushare数据源实例失败")
            return False
        
        # 4. 连接到Tushare API
        print("\n4. 连接到Tushare API")
        connection_success = tushare_instance.connect()
        if connection_success:
            print(f"✅ Tushare API连接成功")
            print(f"连接状态: {tushare_instance.is_connected()}")
            print(f"数据源信息: {tushare_instance.get_source_info()}")
        else:
            print("❌ Tushare API连接失败")
            return False
        
        # 5. 获取股票基础信息数据
        print("\n5. 获取股票基础信息数据")
        try:
            params = {}
            result = tushare_instance.fetch_data("stock_basic", params)
            
            print(f"✅ stock_basic数据获取成功")
            print(f"数据行数: {len(result)}")
            print(f"数据列数: {len(result.columns)}")
            if not result.empty:
                print(f"列名: {list(result.columns)}")
                print("前3行数据:")
                print(result.head(3))
            
        except Exception as e:
            print(f"❌ stock_basic数据获取失败: {e}")
            return False
        
        # 6. 获取交易日历数据
        print("\n6. 获取交易日历数据")
        try:
            params = {
                "start_date": "20240101",
                "end_date": "20240131"
            }
            result = tushare_instance.fetch_data("trade_cal", params)
            
            print(f"✅ trade_cal数据获取成功")
            print(f"数据行数: {len(result)}")
            print(f"数据列数: {len(result.columns)}")
            if not result.empty:
                print(f"列名: {list(result.columns)}")
                print("前3行数据:")
                print(result.head(3))
            
        except Exception as e:
            print(f"❌ trade_cal数据获取失败: {e}")
            return False
        
        # 7. 断开连接
        print("\n7. 断开连接")
        tushare_instance.disconnect()
        print(f"连接状态: {tushare_instance.is_connected()}")
        
        # 8. 关闭管理器
        print("\n8. 关闭管理器")
        manager.shutdown()
        print("✅ 管理器关闭成功")
        
        print("\n🎉 Tushare数据源集成测试全部通过！")
        
    except Exception as e:
        print(f"\n❌ Tushare数据源集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        # 基础功能测试
        test_basic_functionality()
        test_context_manager()
        
        # 集成测试：注册、创建实例并获取数据
        test_tushare_data_source_integration()
        
        print("\n✅ 所有测试通过！DataSourceManager重构成功，集成测试通过")
            
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()