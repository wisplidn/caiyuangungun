#!/usr/bin/env python3
"""测试DTO验证模块

测试dto_validation模块的各种验证功能
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

from caiyuangungun.data.raw.dto.dto_validation import (
    validate,
    validate_data,
    is_valid_data,
    get_validation_errors,
    validate_monthly_date,
    validate_trade_date,
    validate_archive_types,
    validate_lookback_periods,
    get_validator
)


def test_individual_validations():
    """测试单个字段验证"""
    print("=== 测试单个字段验证 ===")
    
    # 测试月度日期
    result = validate_monthly_date("202412")
    print(f"月度日期 '202412': {result.is_valid}, {result.error_message}")
    
    result = validate_monthly_date("20241")
    print(f"月度日期 '20241': {result.is_valid}, {result.error_message}")
    
    # 测试交易日期
    result = validate_trade_date("20241231")
    print(f"交易日期 '20241231': {result.is_valid}, {result.error_message}")
    
    result = validate_trade_date("2024123")
    print(f"交易日期 '2024123': {result.is_valid}, {result.error_message}")
    
    # 测试归档类型
    result = validate_archive_types("DAILY")
    print(f"归档类型 'DAILY': {result.is_valid}, {result.error_message}")
    
    result = validate_archive_types("INVALID")
    print(f"归档类型 'INVALID': {result.is_valid}, {result.error_message}")
    
    # 测试回溯周期
    result = validate_lookback_periods(30)
    print(f"回溯周期 30: {result.is_valid}, {result.error_message}")
    
    result = validate_lookback_periods(20000)
    print(f"回溯周期 20000: {result.is_valid}, {result.error_message}")
    
    print()


def test_batch_validation():
    """测试批量验证"""
    print("=== 测试批量验证 ===")
    
    # 有效数据
    valid_data = {
        "monthly_date": "202412",
        "trade_date": "20241231",
        "archive_types": "DAILY",
        "lookback_periods": 30,
        "required_params": ["trade_date"]
    }
    
    print(f"有效数据验证结果: {is_valid_data(valid_data)}")
    errors = get_validation_errors(valid_data)
    print(f"错误信息: {errors}")
    
    # 无效数据
    invalid_data = {
        "monthly_date": "20241",  # 格式错误
        "trade_date": "20241231",
        "archive_types": "INVALID",  # 枚举值错误
        "lookback_periods": 20000,  # 超出范围
        "required_params": ["trade_date"]
    }
    
    print(f"\n无效数据验证结果: {is_valid_data(invalid_data)}")
    errors = get_validation_errors(invalid_data)
    print(f"错误信息: {errors}")
    
    # 详细验证结果
    results = validate_data(invalid_data)
    print("\n详细验证结果:")
    for result in results:
        status = "✓" if result.is_valid else "✗"
        print(f"  {status} {result.field_name}: {result.value} - {result.error_message or 'OK'}")
    
    print()


def test_validator_info():
    """测试验证器信息获取"""
    print("=== 测试验证器信息 ===")
    
    validator = get_validator()
    
    # 获取特定字段规则
    rule = validator.get_field_rule("monthly_date")
    if rule:
        print(f"monthly_date规则: {rule}")
    
    # 获取所有规则
    all_rules = validator.get_all_rules()
    print(f"\n总共有 {len(all_rules)} 个验证规则:")
    for field_name in all_rules.keys():
        print(f"  - {field_name}")
    
    print()


def main():
    """主测试函数"""
    print("DTO验证模块测试")
    print("=" * 50)
    
    try:
        test_individual_validations()
        test_batch_validation()
        test_validator_info()
        
        print("所有测试完成！")
        
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()