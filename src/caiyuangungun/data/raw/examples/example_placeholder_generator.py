#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PlaceholderGenerator集成测试
详细打印输入参数、输出结果和错误信息
"""

import sys
import os
from pathlib import Path

# 添加utils目录到Python路径
utils_path = '/Users/daishun/个人文档/caiyuangungun/src/caiyuangungun/data/raw/utils'
sys.path.append(utils_path)

from placeholder_generator import PlaceholderGenerator

def format_list_output(data, max_items=10):
    """格式化列表输出，只显示前后max_items个元素"""
    if not isinstance(data, list):
        return str(data)
    
    if len(data) <= max_items * 2:
        return str(data)
    
    front = data[:max_items]
    back = data[-max_items:]
    return f"{front} ... (省略{len(data) - max_items * 2}个元素) ... {back}"

def format_dict_output(data, max_items=10):
    """格式化字典输出，对列表值进行截断"""
    if not isinstance(data, dict):
        return str(data)
    
    result = {}
    for key, value in data.items():
        if isinstance(value, list):
            result[key] = format_list_output(value, max_items)
        else:
            result[key] = value
    return result

def test_with_detailed_logging(generator, test_name, **kwargs):
    """执行测试并打印详细日志"""
    print(f"\n{'='*60}")
    print(f"测试: {test_name}")
    print(f"{'='*60}")
    
    # 打印输入参数
    print("📥 输入参数:")
    for key, value in kwargs.items():
        print(f"  {key}: {value}")
    
    try:
        # 执行测试
        result = generator.generate_placeholder_values(**kwargs)
        
        # 打印输出结果
        print("\n📤 输出结果:")
        formatted_result = format_dict_output(result)
        for key, value in formatted_result.items():
            print(f"  {key}: {value}")
            
        # 打印统计信息
        print("\n📊 统计信息:")
        for key, value in result.items():
            if isinstance(value, list):
                print(f"  {key} 数量: {len(value)}")
            elif isinstance(value, str) and '-' in str(value):
                print(f"  {key} 范围: {value}")
        
        print("\n✅ 测试成功")
        return result
        
    except Exception as e:
        print(f"\n❌ 报错结果:")
        print(f"  错误类型: {type(e).__name__}")
        print(f"  错误信息: {str(e)}")
        print(f"\n❌ 测试失败")
        return None

def test_process_params_dict(generator):
    """测试新的process_params_dict方法"""
    print(f"\n{'='*60}")
    print("测试新的字典处理方法 - process_params_dict")
    print(f"{'='*60}")
    
    # 测试用例1: 包含占位符的参数字典
    test_params = {
        'symbol': 'AAPL',
        'trade_date': '<TRADE_DATE>',
        'period': 'daily',
        'limit': 100
    }
    
    print("📥 输入参数字典:")
    for key, value in test_params.items():
        print(f"  {key}: {value}")
    
    print("\n📥 其他参数:")
    print(f"  start_date: 20250107")
    print(f"  end_date: 20250115")
    print(f"  lookback_periods: 3")
    print(f"  lookback_multiplier: 1.0")
    print(f"  truncate_mode: True")
    
    try:
        result = generator.process_params_dict(
            params_dict=test_params,
            start_date='20250107',
            end_date='20250115',
            lookback_periods=3,
            lookback_multiplier=1.0,
            truncate_mode=True
        )
        
        print("\n📤 处理后的参数字典:")
        for key, value in result.items():
            if isinstance(value, list):
                print(f"  {key}: {format_list_output(value, 5)} (共{len(value)}个)")
            else:
                print(f"  {key}: {value}")
        
        print("\n✅ 字典处理测试成功")
        
    except Exception as e:
        print(f"\n❌ 字典处理测试失败:")
        print(f"  错误类型: {type(e).__name__}")
        print(f"  错误信息: {str(e)}")
    
    # 测试用例2: 包含MONTHLY_DATE_RANGE的参数字典
    print(f"\n{'-'*40}")
    print("测试MONTHLY_DATE_RANGE特殊处理")
    print(f"{'-'*40}")
    
    monthly_params = {
        'symbol': 'TSLA',
        'date_range': '<MONTHLY_DATE_RANGE>',
        'freq': 'M'
    }
    
    print("📥 输入参数字典:")
    for key, value in monthly_params.items():
        print(f"  {key}: {value}")
    
    try:
        result = generator.process_params_dict(
            params_dict=monthly_params,
            start_date='202501',
            end_date='202503',
            lookback_periods=2,
            lookback_multiplier=1.0,
            truncate_mode=True
        )
        
        print("\n📤 处理后的参数字典:")
        for key, value in result.items():
            if isinstance(value, list):
                print(f"  {key}: {format_list_output(value, 5)} (共{len(value)}个)")
            else:
                print(f"  {key}: {value}")
        
        print("\n✅ MONTHLY_DATE_RANGE处理测试成功")
        
    except Exception as e:
        print(f"\n❌ MONTHLY_DATE_RANGE处理测试失败:")
        print(f"  错误类型: {type(e).__name__}")
        print(f"  错误信息: {str(e)}")

def main():
    print("开始PlaceholderGenerator详细集成测试...")
    print(f"{'='*80}")
    
    # 初始化生成器
    generator = PlaceholderGenerator()
    
    # 首先测试新的字典处理方法
    test_process_params_dict(generator)
    
    # 测试1: 交易日期生成
    test_with_detailed_logging(
        generator,
        "交易日期生成",
        placeholder='<TRADE_DATE>',
        start_date='20250107',
        end_date='20250115',
        lookback_periods=5,
        lookback_multiplier=1
    )
    
    # 测试2: 月度日期范围生成
    test_with_detailed_logging(
        generator,
        "月度日期范围生成",
        placeholder='<MONTHLY_DATE_RANGE>',
        start_date='202401',
        end_date='202503',
        lookback_periods=2,
        lookback_multiplier=3
    )
    
    # 测试3: 季度日期生成
    test_with_detailed_logging(
        generator,
        "季度日期生成",
        placeholder='<QUARTERLY_DATE>',
        start_date='20240101',
        end_date='20251231',
        lookback_periods=3,
        lookback_multiplier=2
    )
    
    # 测试4: 大量数据生成（测试截断功能）
    test_with_detailed_logging(
        generator,
        "大量交易日期生成（测试截断）",
        placeholder='<TRADE_DATE>',
        start_date='20240101',
        end_date='20241231',
        lookback_periods=50,
        lookback_multiplier=3
    )
    
    # 测试5: 边界情况 - 单日范围（交易日）
    test_with_detailed_logging(
        generator,
        "边界情况 - 单日范围（交易日）",
        placeholder='<TRADE_DATE>',
        start_date='20250107',  # 2025年1月7日，周二，应该是交易日
        end_date='20250107',
        lookback_periods=1,
        lookback_multiplier=0
    )
    
    # 测试6: 边界情况 - 单日范围（非交易日）
    test_with_detailed_logging(
        generator,
        "边界情况 - 单日范围（非交易日）",
        placeholder='<TRADE_DATE>',
        start_date='20250101',  # 2025年1月1日，元旦，非交易日
        end_date='20250101',
        lookback_periods=1,
        lookback_multiplier=0
    )
    
    # 测试7: 错误处理 - 无效占位符
    test_with_detailed_logging(
        generator,
        "错误处理 - 无效占位符",
        placeholder='<INVALID_PLACEHOLDER>',
        start_date='20250101',
        end_date='20250131',
        lookback_periods=1,
        lookback_multiplier=0
    )
    
    # 测试8: 错误处理 - 无效日期格式
    test_with_detailed_logging(
        generator,
        "错误处理 - 无效日期格式",
        placeholder='<TRADE_DATE>',
        start_date='2025',  # 无效格式
        end_date='20250131',
        lookback_periods=1,
        lookback_multiplier=0
    )
    
    # 测试9: 回看期数计算验证
    test_with_detailed_logging(
        generator,
        "回看期数计算验证",
        placeholder='<TRADE_DATE>',
        start_date='20250107',
        end_date='20250110',
        lookback_periods=3,
        lookback_multiplier=2
    )
    
    # 测试10: 混合格式测试
    test_with_detailed_logging(
        generator,
        "混合格式测试 - 6位日期",
        placeholder='<MONTHLY_DATE_RANGE>',
        start_date='202501',  # 6位格式
        end_date='202502',    # 6位格式
        lookback_periods=1,
        lookback_multiplier=1
    )
    
    print(f"\n{'='*80}")
    print("PlaceholderGenerator详细集成测试完成!")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
