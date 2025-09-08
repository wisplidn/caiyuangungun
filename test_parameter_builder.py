#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试参数构建器的新逻辑
验证方案1：默认值优先，特殊标记使用生成器
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.raw.utils.data_service_utils import ParameterBuilder

def test_parameter_builder():
    """测试参数构建器的新逻辑"""
    print("=== 测试参数构建器 - 方案1 ===")
    
    # 测试1: 特殊标记 <TRADE_DATE>
    print("\n测试1: 特殊标记 <TRADE_DATE>")
    required_params = {"trade_date": "<TRADE_DATE>"}
    result = ParameterBuilder.build_fetch_params(
        data_type="daily",
        date_param="20241231",
        required_params=required_params
    )
    print(f"输入: {required_params}")
    print(f"结果: {result}")
    expected = {'endpoint_name': 'daily', 'trade_date': '20241231'}
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    # 测试2: 特殊标记 <MONTHLY_DATE>
    print("\n测试2: 特殊标记 <MONTHLY_DATE>")
    required_params = {"monthly_date": "<MONTHLY_DATE>"}
    result = ParameterBuilder.build_fetch_params(
        data_type="monthly_income",
        date_param="202412",
        required_params=required_params
    )
    print(f"输入: {required_params}")
    print(f"结果: {result}")
    expected = {'endpoint_name': 'monthly_income', 'monthly_date': '202412'}
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    # 测试3: 默认值
    print("\n测试3: 默认值")
    required_params = {
        "start_date": "20000101",
        "end_date": "20990101"
    }
    result = ParameterBuilder.build_fetch_params(
        data_type="trade_cal",
        date_param=None,
        required_params=required_params
    )
    print(f"输入: {required_params}")
    print(f"结果: {result}")
    expected = {
        'endpoint_name': 'trade_cal',
        'start_date': '20000101',
        'end_date': '20990101'
    }
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    # 测试4: 混合情况（默认值 + 特殊标记）
    print("\n测试4: 混合情况（默认值 + 特殊标记）")
    required_params = {
        "trade_date": "<TRADE_DATE>",
        "ts_code": "000001.SZ",
        "limit": 5000
    }
    result = ParameterBuilder.build_fetch_params(
        data_type="daily",
        date_param="20241231",
        required_params=required_params
    )
    print(f"输入: {required_params}")
    print(f"结果: {result}")
    expected = {
        'endpoint_name': 'daily',
        'trade_date': '20241231',
        'ts_code': '000001.SZ',
        'limit': 5000
    }
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    # 测试5: 向后兼容 - 列表格式
    print("\n测试5: 向后兼容 - 列表格式")
    required_params = ["trade_date"]
    result = ParameterBuilder.build_fetch_params(
        data_type="daily",
        date_param="20241231",
        required_params=required_params
    )
    print(f"输入: {required_params}")
    print(f"结果: {result}")
    expected = {'endpoint_name': 'daily', 'trade_date': '20241231'}
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    # 测试6: 空参数
    print("\n测试6: 空参数")
    result = ParameterBuilder.build_fetch_params(
        data_type="stock_basic",
        date_param=None,
        required_params=[]
    )
    print(f"输入: []")
    print(f"结果: {result}")
    expected = {'endpoint_name': 'stock_basic'}
    assert result == expected, f"期望 {expected}, 实际 {result}"
    print("✓ 通过")
    
    print("\n=== 所有测试通过! ===")

if __name__ == "__main__":
    test_parameter_builder()