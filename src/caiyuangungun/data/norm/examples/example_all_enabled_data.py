#!/usr/bin/env python3
"""
测试所有enabled=true的数据清洗
支持运行所有启用的数据源的指定数量或全部文件进行测试
"""

import sys
import os
from pathlib import Path

# 动态添加项目路径到sys.path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.norm.services.basic_data_cleaning_service import DataCleaningService
import pandas as pd
from typing import Optional, Dict, List

def get_enabled_data_sources() -> Dict[str, List[str]]:
    """
    获取所有enabled=true的数据源
    
    Returns:
        Dict[str, List[str]]: 按数据类型分组的启用数据源
    """
    service = DataCleaningService()
    config = service.config_manager.get_basic_cleaning_config()
    
    enabled_sources = {}
    
    for data_type, type_config in config.items():
        if 'cleaning_pipelines' in type_config:
            enabled_list = []
            for source_name, source_config in type_config['cleaning_pipelines'].items():
                if source_config.get('enabled', True):  # 默认为True
                    enabled_list.append(source_name)
            
            if enabled_list:
                enabled_sources[data_type] = enabled_list
    
    return enabled_sources

def test_all_enabled_data(max_files: Optional[int] = None):
    """
    测试所有enabled=true的数据清洗
    
    Args:
        max_files: 最大处理文件数，如果为None则处理全部文件
    """
    
    # 初始化服务
    service = DataCleaningService()
    
    # 获取所有启用的数据源
    enabled_sources = get_enabled_data_sources()
    
    if not enabled_sources:
        print("❌ 没有找到启用的数据源")
        return
    
    # 显示处理信息
    if max_files:
        print(f"\n{'='*60}")
        print(f"开始测试所有启用的数据清洗 - 处理前{max_files}个文件")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print(f"开始测试所有启用的数据清洗 - 处理全部文件")
        print(f"{'='*60}")
    
    # 显示将要处理的数据源
    total_sources = 0
    for data_type, sources in enabled_sources.items():
        print(f"\n📊 {data_type}:")
        for source in sources:
            print(f"  ✓ {source}")
            total_sources += 1
    
    print(f"\n总计: {total_sources} 个数据源")
    print(f"{'='*60}")
    
    all_results = {}
    
    # 按数据类型处理
    for data_type, sources in enabled_sources.items():
        print(f"\n🔄 处理数据类型: {data_type}")
        print(f"{'='*40}")
        
        type_results = {}
        
        for data_source in sources:
            print(f"\n--- 处理数据源: {data_source} ---")
            
            try:
                # 执行清洗
                result_df = service.clean_data_by_pipeline(
                    pipeline_name=data_type,
                    data_source=data_source,
                    max_files=max_files
                )
                
                if result_df is not None and not result_df.empty:
                    # 保存数据
                    file_path = service.save_cleaned_data(
                        df=result_df,
                        pipeline_name=data_type,
                        data_source=data_source
                    )
                    
                    if file_path:
                        print(f"✓ 数据保存成功: {file_path}")
                        print(f"数据形状: {result_df.shape}")
                        print(f"列名: {list(result_df.columns)[:10]}{'...' if len(result_df.columns) > 10 else ''}")
                        
                        type_results[data_source] = {
                            'status': 'success',
                            'shape': result_df.shape,
                            'columns': len(result_df.columns),
                            'file_path': file_path
                        }
                    else:
                        print(f"✗ 数据保存失败")
                        type_results[data_source] = {
                            'status': 'save_failed',
                            'error': '保存失败'
                        }
                else:
                    print(f"✗ {data_source}: failed - 清洗后数据为空")
                    type_results[data_source] = {
                        'status': 'failed',
                        'error': '清洗后数据为空'
                    }
                    
            except Exception as e:
                print(f"✗ {data_source}: error - {str(e)}")
                type_results[data_source] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        all_results[data_type] = type_results
    
    # 打印汇总结果
    print(f"\n{'='*60}")
    print("测试结果汇总:")
    print(f"{'='*60}")
    
    total_success = 0
    total_failed = 0
    
    for data_type, type_results in all_results.items():
        print(f"\n📊 {data_type}:")
        for source, result in type_results.items():
            if result['status'] == 'success':
                print(f"✓ {source}: 成功 - 形状{result['shape']}, {result['columns']}列")
                total_success += 1
            else:
                print(f"✗ {source}: {result['status']} - {result.get('error', '')}")
                total_failed += 1
    
    print(f"\n{'='*60}")
    print(f"总计: 成功 {total_success} 个, 失败 {total_failed} 个")
    print(f"{'='*60}")

def test_all_enabled_data_first_10():
    """测试前10个文件"""
    test_all_enabled_data(max_files=10)

def test_all_enabled_data_all():
    """测试全部文件"""
    test_all_enabled_data()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        try:
            max_files = int(sys.argv[1])
            test_all_enabled_data(max_files=max_files)
        except ValueError:
            if sys.argv[1].lower() == 'all':
                test_all_enabled_data_all()
            else:
                print("参数错误：请输入数字或'all'")
                print("用法: python example_all_enabled_data.py [数量|all]")
                print("示例: python example_all_enabled_data.py 10")
                print("示例: python example_all_enabled_data.py all")
                sys.exit(1)
    else:
        test_all_enabled_data_first_10()