#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Qlib数据转换服务使用示例

演示如何使用QlibConversionService进行数据转换
"""

import sys
from pathlib import Path

# 设置项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.caiyuangungun.data.norm.services.qlib_conversion_service import (
    QlibConversionService,
    create_daily_converter_service,
    create_pit_converter_service,
    create_full_converter_service
)


def example_1_daily_conversion():
    """
    示例1: 转换日频数据
    """
    print("="*80)
    print("示例1: 日频数据转换")
    print("="*80)
    
    # 创建服务（测试模式：只处理5支股票）
    # ConfigManager会自动加载配置文件
    service = create_daily_converter_service(limit_symbols=5)
    
    # 运行转换
    result = service.run_converter('daily_quotes', validate=True)
    
    # 打印结果
    print("\n转换结果:")
    print(f"  成功: {result['success']}")
    if result['success']:
        print(f"  加载: {result['stages']['load']['rows']:,} 行")
        print(f"  转换: {result['stages']['convert']['rows']:,} 行")
        print(f"  验证: {result.get('validation', 'N/A')}")


def example_2_pit_conversion():
    """
    示例2: 转换财务PIT数据
    """
    print("\n" + "="*80)
    print("示例2: 财务PIT数据转换")
    print("="*80)
    
    # 创建服务，指定要转换的数据类型
    service = create_pit_converter_service(
        data_types=['income_statement', 'balance_sheet', 'dividend'],
        limit_symbols=5
    )
    
    # 批量运行所有转换器
    results = service.run_all_converters(validate=True)
    
    # 打印结果
    print("\n批量转换结果:")
    print(f"  总数: {results['total']}")
    print(f"  成功: {results['success_count']}")
    print(f"  失败: {results['failed_count']}")


def example_3_custom_registration():
    """
    示例3: 自定义注册转换器
    """
    print("\n" + "="*80)
    print("示例3: 自定义注册转换器")
    print("="*80)
    
    # 创建服务
    service = QlibConversionService()
    
    # 查看可用转换
    available = service.get_available_conversions()
    print(f"\n可用转换配置: {available}")
    
    # 手动注册转换器
    service.register_from_config('daily_quotes', 'daily', limit_symbols=3)
    service.register_from_config('income_statement', 'pit', limit_symbols=3)
    
    # 运行所有转换器
    results = service.run_all_converters(validate=False)
    
    # 生成报告
    report_path = PROJECT_ROOT / 'qlib_conversion_report.md'
    service.generate_report(results, report_path)
    print(f"\n✅ 报告已生成: {report_path}")


def example_4_full_conversion():
    """
    示例4: 完整转换流程（日频+所有财务数据）
    """
    print("\n" + "="*80)
    print("示例4: 完整转换流程")
    print("="*80)
    
    # 创建完整服务
    service = create_full_converter_service(limit_symbols=5)  # 测试模式
    
    # 运行所有转换器
    results = service.run_all_converters(validate=True)
    
    # 生成报告
    report_path = PROJECT_ROOT / 'qlib_full_conversion_report.md'
    report = service.generate_report(results, report_path)
    
    print("\n" + report)


def example_5_production_mode():
    """
    示例5: 生产模式（处理全部股票）
    """
    print("\n" + "="*80)
    print("示例5: 生产模式（全量转换）")
    print("="*80)
    print("⚠️  这将处理所有股票，耗时较长！")
    
    # 取消注释以下代码以运行全量转换
    """
    # limit_symbols=None 表示处理全部
    service = create_full_converter_service(limit_symbols=None)
    
    results = service.run_all_converters(validate=True)
    
    report_path = PROJECT_ROOT / 'qlib_production_report.md'
    service.generate_report(results, report_path)
    
    print(f"✅ 全量转换完成，报告: {report_path}")
    """
    print("（代码已注释，取消注释以运行）")


def main():
    """
    主函数 - 运行所有示例
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Qlib转换服务示例')
    parser.add_argument('--example', type=int, choices=[1, 2, 3, 4, 5],
                       help='运行指定示例（1-5）')
    parser.add_argument('--all', action='store_true',
                       help='运行所有示例')
    
    args = parser.parse_args()
    
    if args.example:
        # 运行指定示例
        examples = {
            1: example_1_daily_conversion,
            2: example_2_pit_conversion,
            3: example_3_custom_registration,
            4: example_4_full_conversion,
            5: example_5_production_mode,
        }
        examples[args.example]()
    
    elif args.all:
        # 运行所有示例（跳过生产模式）
        example_1_daily_conversion()
        example_2_pit_conversion()
        example_3_custom_registration()
        example_4_full_conversion()
        example_5_production_mode()
    
    else:
        # 默认运行示例1
        print("提示: 使用 --example N 运行指定示例，--all 运行所有示例\n")
        example_1_daily_conversion()


if __name__ == '__main__':
    main()

