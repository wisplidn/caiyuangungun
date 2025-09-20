#!/usr/bin/env python3
"""
Debug文件：测试--force-update false参数的问题

问题分析：
1. main.py中定义了--force-update参数，传递给RawDataService
2. RawDataService传递给TaskGenerator，但TaskGenerator没有传递给数据保存流程
3. UniversalArchiver.archive_data_simple()方法没有force_update参数
4. 文件跳过逻辑完全基于MD5比较，忽略了force_update设置

测试目标：
- 验证tushare daily数据（9月8-15日）的跳过逻辑
- 确认force_update参数没有生效的原因
- 提供修复方案
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'core'))
sys.path.insert(0, str(project_root / 'services'))

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_force_update_parameter_flow():
    """测试force_update参数的传递流程"""
    print("=" * 60)
    print("测试1: force_update参数传递流程")
    print("=" * 60)
    
    try:
        from services.raw_data_service import RawDataService, TaskGenerator
        from core.config_manager import get_config_manager
        
        # 1. 测试TaskGenerator是否接收force_update参数
        config_manager = get_config_manager()
        
        # 测试force_update=False
        task_gen_false = TaskGenerator(config_manager, force_update=False)
        print(f"TaskGenerator with force_update=False: {task_gen_false.force_update}")
        
        # 测试force_update=True
        task_gen_true = TaskGenerator(config_manager, force_update=True)
        print(f"TaskGenerator with force_update=True: {task_gen_true.force_update}")
        
        # 2. 检查TaskGenerator的generate_tasks_with_validation方法
        print("\n检查generate_tasks_with_validation方法签名...")
        import inspect
        sig = inspect.signature(task_gen_false.generate_tasks_with_validation)
        print(f"方法参数: {list(sig.parameters.keys())}")
        
        return True
        
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def test_data_saver_force_update():
    """测试DataSaver是否支持force_update参数"""
    print("\n" + "=" * 60)
    print("测试2: DataSaver的force_update支持")
    print("=" * 60)
    
    try:
        from services.raw_data_service import DataSaver
        from core.config_manager import get_config_manager
        
        config_manager = get_config_manager()
        data_saver = DataSaver(config_manager)
        
        # 检查save_data_auto方法签名
        import inspect
        sig = inspect.signature(data_saver.save_data_auto)
        params = list(sig.parameters.keys())
        print(f"save_data_auto方法参数: {params}")
        
        if 'force_update' in params:
            print("✓ save_data_auto方法支持force_update参数")
        else:
            print("✗ save_data_auto方法不支持force_update参数")
            
        return 'force_update' in params
        
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def test_universal_archiver_force_update():
    """测试UniversalArchiver是否支持force_update参数"""
    print("\n" + "=" * 60)
    print("测试3: UniversalArchiver的force_update支持")
    print("=" * 60)
    
    try:
        from core.universal_archiver import UniversalArchiver
        
        archiver = UniversalArchiver()
        
        # 检查archive_data_simple方法签名
        import inspect
        sig = inspect.signature(archiver.archive_data_simple)
        params = list(sig.parameters.keys())
        print(f"archive_data_simple方法参数: {params}")
        
        if 'force_update' in params:
            print("✓ archive_data_simple方法支持force_update参数")
        else:
            print("✗ archive_data_simple方法不支持force_update参数 - 这是问题所在！")
            
        return 'force_update' in params
        
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def test_tushare_daily_skip_logic():
    """测试tushare daily数据的跳过逻辑"""
    print("\n" + "=" * 60)
    print("测试4: tushare daily数据跳过逻辑")
    print("=" * 60)
    
    try:
        # 创建测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ'],
            'trade_date': ['20240908', '20240908'],
            'open': [10.0, 20.0],
            'high': [11.0, 21.0],
            'low': [9.0, 19.0],
            'close': [10.5, 20.5],
            'vol': [1000, 2000]
        })
        
        from core.universal_archiver import UniversalArchiver
        
        archiver = UniversalArchiver()
        
        # 测试数据的MD5计算
        md5_1 = archiver.calculate_dataframe_md5(test_data)
        print(f"测试数据MD5: {md5_1}")
        
        # 创建相同数据，验证MD5一致性
        test_data_2 = test_data.copy()
        md5_2 = archiver.calculate_dataframe_md5(test_data_2)
        print(f"相同数据MD5: {md5_2}")
        print(f"MD5是否相同: {md5_1 == md5_2}")
        
        # 创建不同数据
        test_data_3 = test_data.copy()
        test_data_3.loc[0, 'close'] = 10.6  # 修改一个值
        md5_3 = archiver.calculate_dataframe_md5(test_data_3)
        print(f"修改后数据MD5: {md5_3}")
        print(f"MD5是否不同: {md5_1 != md5_3}")
        
        return True
        
    except Exception as e:
        print(f"测试失败: {e}")
        return False

def analyze_problem_and_solution():
    """分析问题并提供解决方案"""
    print("\n" + "=" * 60)
    print("问题分析和解决方案")
    print("=" * 60)
    
    print("问题根因：")
    print("1. --force-update参数在main.py中正确定义和传递")
    print("2. RawDataService.collect_data()接收force_update参数")
    print("3. TaskGenerator保存force_update参数，但没有传递给数据保存流程")
    print("4. DataSaver.save_data_auto()没有force_update参数")
    print("5. UniversalArchiver.archive_data_simple()没有force_update参数")
    print("6. 文件跳过逻辑完全基于MD5比较，忽略force_update设置")
    
    print("\n解决方案：")
    print("方案1: 修改UniversalArchiver.archive_data_simple()方法")
    print("  - 添加force_update参数")
    print("  - 当force_update=False且文件存在时，直接跳过MD5比较")
    
    print("\n方案2: 修改DataSaver.save_data_auto()方法")
    print("  - 添加force_update参数")
    print("  - 传递给UniversalArchiver")
    
    print("\n方案3: 修改RawDataService中的数据保存调用")
    print("  - 在调用save_data_auto时传递force_update参数")
    
    print("\n推荐实施顺序：")
    print("1. 先修改UniversalArchiver.archive_data_simple()添加force_update参数")
    print("2. 修改DataSaver.save_data_auto()添加force_update参数并传递")
    print("3. 修改RawDataService中的调用，传递force_update参数")
    print("4. 测试验证修复效果")

def main():
    """主测试函数"""
    print("开始调试--force-update false参数问题")
    print("测试时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # 执行所有测试
    test1_result = test_force_update_parameter_flow()
    test2_result = test_data_saver_force_update()
    test3_result = test_universal_archiver_force_update()
    test4_result = test_tushare_daily_skip_logic()
    
    # 分析问题
    analyze_problem_and_solution()
    
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"TaskGenerator参数传递: {'✓' if test1_result else '✗'}")
    print(f"DataSaver force_update支持: {'✓' if test2_result else '✗'}")
    print(f"UniversalArchiver force_update支持: {'✓' if test3_result else '✗'}")
    print(f"MD5跳过逻辑测试: {'✓' if test4_result else '✗'}")
    
    if not test3_result:
        print("\n关键问题确认：UniversalArchiver.archive_data_simple()方法缺少force_update参数！")
        print("这就是--force-update false无法生效的根本原因。")

if __name__ == "__main__":
    main()