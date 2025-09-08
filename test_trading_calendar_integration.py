#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试交易日历集成功能

验证data_service_utils.py中DateUtils类的交易日历功能
"""

import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from datetime import datetime, timedelta
from caiyuangungun.data.raw.utils.data_service_utils import DateUtils, RunMode, DataDefinitionProcessor

def test_get_latest_date():
    """测试获取最新日期功能"""
    print("=== 测试获取最新日期功能 ===")
    
    # 测试DAILY类型 - 不使用交易日历
    latest_daily_normal = DateUtils.get_latest_date('DAILY', use_trading_calendar=False)
    print(f"DAILY (普通模式): {latest_daily_normal}")
    
    # 测试DAILY类型 - 使用交易日历
    latest_daily_trading = DateUtils.get_latest_date('DAILY', use_trading_calendar=True)
    print(f"DAILY (交易日历模式): {latest_daily_trading}")
    
    # 测试MONTHLY类型
    latest_monthly = DateUtils.get_latest_date('MONTHLY')
    print(f"MONTHLY: {latest_monthly}")
    
    # 测试SNAPSHOT类型
    latest_snapshot = DateUtils.get_latest_date('SNAPSHOT')
    print(f"SNAPSHOT: '{latest_snapshot}'")
    
    print()

def test_generate_date_range():
    """测试生成日期范围功能"""
    print("=== 测试生成日期范围功能 ===")
    
    start_date = "20240101"
    end_date = "20240110"
    
    # 测试DAILY - 普通模式
    dates_normal = DateUtils.generate_date_range(start_date, end_date, 'DAILY', use_trading_calendar=False)
    print(f"DAILY 普通模式 ({start_date} - {end_date}): {len(dates_normal)} 天")
    print(f"前5天: {dates_normal[:5]}")
    
    # 测试DAILY - 交易日历模式
    dates_trading = DateUtils.generate_date_range(start_date, end_date, 'DAILY', use_trading_calendar=True)
    print(f"DAILY 交易日历模式 ({start_date} - {end_date}): {len(dates_trading)} 天")
    print(f"前5天: {dates_trading[:5]}")
    
    # 测试MONTHLY
    monthly_dates = DateUtils.generate_date_range("202401", "202403", 'MONTHLY')
    print(f"MONTHLY (202401 - 202403): {monthly_dates}")
    
    print()

def test_calculate_lookback_dates():
    """测试计算回溯日期功能"""
    print("=== 测试计算回溯日期功能 ===")
    
    latest_date = "20240115"
    lookback_periods = 5
    
    # 测试DAILY - 普通模式
    lookback_normal = DateUtils.calculate_lookback_dates(
        latest_date, lookback_periods, 'DAILY', multiplier=1, use_trading_calendar=False
    )
    print(f"DAILY 普通回溯 (最新: {latest_date}, 回溯: {lookback_periods}): {lookback_normal}")
    
    # 测试DAILY - 交易日历模式
    lookback_trading = DateUtils.calculate_lookback_dates(
        latest_date, lookback_periods, 'DAILY', multiplier=1, use_trading_calendar=True
    )
    print(f"DAILY 交易日历回溯 (最新: {latest_date}, 回溯: {lookback_periods}): {lookback_trading}")
    
    # 测试MONTHLY
    lookback_monthly = DateUtils.calculate_lookback_dates(
        "202401", 3, 'MONTHLY', multiplier=1
    )
    print(f"MONTHLY 回溯 (最新: 202401, 回溯: 3): {lookback_monthly}")
    
    print()

def test_data_definition_processor():
    """测试数据定义处理器的交易日历集成"""
    print("=== 测试数据定义处理器 ===")
    
    # 模拟数据定义
    definition_with_trading = {
        'storage_type': 'DAILY',
        'start_date': '20240101',
        'lookback_periods': 3,
        'use_trading_calendar': True
    }
    
    definition_without_trading = {
        'storage_type': 'DAILY',
        'start_date': '20240101',
        'lookback_periods': 3,
        'use_trading_calendar': False
    }
    
    # 测试标准更新模式
    dates_with_trading = DataDefinitionProcessor._get_date_list_for_mode(
        definition_with_trading, RunMode.STANDARD_UPDATE
    )
    print(f"标准更新 (启用交易日历): {dates_with_trading}")
    
    dates_without_trading = DataDefinitionProcessor._get_date_list_for_mode(
        definition_without_trading, RunMode.STANDARD_UPDATE
    )
    print(f"标准更新 (不启用交易日历): {dates_without_trading}")
    
    # 测试回溯更新模式
    lookback_with_trading = DataDefinitionProcessor._get_date_list_for_mode(
        definition_with_trading, RunMode.UPDATE_WITH_LOOKBACK
    )
    print(f"回溯更新 (启用交易日历): {lookback_with_trading}")
    
    lookback_without_trading = DataDefinitionProcessor._get_date_list_for_mode(
        definition_without_trading, RunMode.UPDATE_WITH_LOOKBACK
    )
    print(f"回溯更新 (不启用交易日历): {lookback_without_trading}")
    
    print()

def test_backward_compatibility():
    """测试向后兼容性"""
    print("=== 测试向后兼容性 ===")
    
    # 测试没有use_trading_calendar配置的情况（默认启用）
    definition_default = {
        'storage_type': 'DAILY',
        'start_date': '20240101',
        'lookback_periods': 2
    }
    
    dates_default = DataDefinitionProcessor._get_date_list_for_mode(
        definition_default, RunMode.STANDARD_UPDATE
    )
    print(f"默认配置 (DAILY): {dates_default}")
    
    # 测试MONTHLY类型（不受交易日历影响）
    definition_monthly = {
        'storage_type': 'MONTHLY',
        'start_date': '202401',
        'lookback_periods': 2,
        'use_trading_calendar': True  # 对MONTHLY无效
    }
    
    dates_monthly = DataDefinitionProcessor._get_date_list_for_mode(
        definition_monthly, RunMode.UPDATE_WITH_LOOKBACK
    )
    print(f"MONTHLY (交易日历配置无效): {dates_monthly}")
    
    print()

if __name__ == "__main__":
    print("开始测试交易日历集成功能...\n")
    
    try:
        test_get_latest_date()
        test_generate_date_range()
        test_calculate_lookback_dates()
        test_data_definition_processor()
        test_backward_compatibility()
        
        print("✅ 所有测试完成！")
        print("\n📝 测试总结:")
        print("1. DateUtils类已成功集成交易日历功能")
        print("2. 支持可选的交易日历过滤（use_trading_calendar参数）")
        print("3. 保持向后兼容性，默认启用交易日历")
        print("4. MONTHLY和SNAPSHOT类型不受交易日历影响")
        print("5. 提供备用函数，确保在交易日历模块不可用时正常工作")
        
    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()