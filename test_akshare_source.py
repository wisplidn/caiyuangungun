#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试akshare数据源注册和运行
"""

import sys
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from caiyuangungun.data.raw.core.data_source_manager import DataSourceManager

def test_akshare_registration():
    """测试akshare数据源注册"""
    print("=== 测试akshare数据源注册 ===")
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 列出所有注册的数据源
        sources = manager.list_sources()
        print(f"注册的数据源数量: {len(sources)}")
        
        for source in sources:
            print(f"- {source['name']}: 启用={source['enabled']}, 类路径={source['class_path']}")
        
        # 检查akshare是否已注册
        akshare_found = any(s['name'] == 'akshare' for s in sources)
        print(f"\nakshare数据源已注册: {akshare_found}")
        
        return akshare_found
        
    except Exception as e:
        print(f"测试注册失败: {e}")
        return False

def test_akshare_instance():
    """测试akshare数据源实例创建"""
    print("\n=== 测试akshare数据源实例创建 ===")
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 获取akshare实例
        print("获取akshare实例...")
        instance = manager.get_instance('akshare')
        
        if instance is None:
            print("❌ 获取akshare实例失败")
            return False
        
        print(f"✅ 实例创建成功: {type(instance)}")
        print(f"连接状态: {instance.is_connected()}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试实例创建失败: {e}")
        return False

def test_akshare_data_fetch():
    """测试akshare数据获取"""
    print("\n=== 测试akshare数据获取 ===")
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 获取akshare实例
        instance = manager.get_instance('akshare')
        
        if instance is None:
            print("❌ 无法获取akshare实例")
            return False
        
        # 测试数据获取
        print("测试获取stock_lrb_em数据...")
        data = instance.fetch_data('stock_lrb_em', date='20240331')
        
        if data is not None and hasattr(data, '__len__'):
            print(f"✅ 数据获取成功，行数: {len(data)}")
            if hasattr(data, 'columns'):
                print(f"列数: {len(data.columns)}")
                print(f"前5列: {list(data.columns[:5])}")
            return True
        else:
            print("❌ 获取到空数据")
            return False
        
    except Exception as e:
        print(f"❌ 测试数据获取失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_health_check():
    """测试健康检查"""
    print("\n=== 测试健康检查 ===")
    
    try:
        # 创建数据源管理器
        manager = DataSourceManager()
        
        # 执行健康检查
        print("执行akshare健康检查...")
        health_result = manager.health_check('akshare')
        
        print(f"健康检查结果: {health_result}")
        
        if 'akshare' in health_result:
            status = health_result['akshare'].get('status')
            print(f"akshare状态: {status}")
            return status in ['healthy', 'warning']
        
        return False
        
    except Exception as e:
        print(f"❌ 健康检查失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始测试akshare数据源...\n")
    
    # 测试步骤
    tests = [
        ("注册测试", test_akshare_registration),
        ("实例创建测试", test_akshare_instance),
        ("数据获取测试", test_akshare_data_fetch),
        ("健康检查测试", test_health_check)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name}执行异常: {e}")
            results.append((test_name, False))
    
    # 汇总结果
    print("\n=== 测试结果汇总 ===")
    all_passed = True
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    print(f"\n总体结果: {'✅ 所有测试通过' if all_passed else '❌ 部分测试失败'}")
    return all_passed

if __name__ == "__main__":
    main()