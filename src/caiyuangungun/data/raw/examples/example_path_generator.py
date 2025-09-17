#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PathGenerator使用示例
演示如何使用路径生成器生成不同类型的数据路径
"""

import json
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.raw.core.path_generator import (
    PathGenerator, ConfigDTO, ArchiveTypeConfig, FileConfig, PathsConfig
)


def main():
    """PathGenerator使用示例主函数"""
    # 创建示例配置
    config_dto = ConfigDTO(
        base_path="/Users/daishun/个人文档/caiyuangungun/data/raw",
        archive_types={
            "DAILY": ArchiveTypeConfig(
                value="DAILY",
                description="日频数据",
                path_pattern="{source_name}/{data_type}/{year_month}",
                enabled=True,
                validation_rules={
                    "required_fields": ["date"],
                    "optional_fields": []
                }
            ),
            "SNAPSHOT": ArchiveTypeConfig(
                value="SNAPSHOT",
                description="快照数据",
                path_pattern="{source_name}/{data_type}",
                enabled=True,
                validation_rules={
                    "required_fields": [],
                    "optional_fields": []
                }
            ),
            "MONTHLY": ArchiveTypeConfig(
                value="MONTHLY",
                description="月频数据",
                path_pattern="{source_name}/{data_type}/{year_month}",
                enabled=True,
                validation_rules={
                    "required_fields": ["date"],
                    "optional_fields": []
                }
            ),
            "QUARTERLY": ArchiveTypeConfig(
                value="QUARTERLY",
                description="季报数据",
                path_pattern="{source_name}/{data_type}/{year_quarter}",
                enabled=True,
                validation_rules={
                    "required_fields": ["date"],
                    "optional_fields": []
                }
            ),
            "symbol": ArchiveTypeConfig(
                value="symbol",
                description="股票代码归档",
                path_pattern="{source_name}/{data_type}/{symbol}",
                enabled=True,
                validation_rules={
                    "required_fields": ["symbol"],
                    "optional_fields": []
                }
            )
        },
        file_config=FileConfig(),
        paths=PathsConfig()
    )
    
    # 创建路径生成器实例
    generator = PathGenerator(config_dto)
    
    print("=== PathGenerator 使用示例 ===")
    
    # 示例1: SNAPSHOT类型（不需要日期参数）
    result1 = generator.get_path_info(
        source_name="example_source",
        data_type="stock_basic",
        archive_type="SNAPSHOT"
    )
    print("\n1. SNAPSHOT示例:")
    print(json.dumps(result1, indent=2, ensure_ascii=False))
    
    # 示例2: DAILY类型（需要8位日期）
    result2 = generator.get_path_info(
        source_name="example_source",
        data_type="daily",
        archive_type="DAILY",
        date="20241231"
    )
    print("\n2. DAILY示例:")
    print(json.dumps(result2, indent=2, ensure_ascii=False))
    
    # 示例3: MONTHLY类型（支持6位年月格式）
    result3 = generator.get_path_info(
        source_name="example_source",
        data_type="monthly_return",
        archive_type="MONTHLY",
        date="202412"
    )
    print("\n3. MONTHLY示例（6位年月）:")
    print(json.dumps(result3, indent=2, ensure_ascii=False))
    
    # 示例4: MONTHLY类型（支持8位日期格式）
    result4 = generator.get_path_info(
        source_name="example_source",
        data_type="monthly_return",
        archive_type="MONTHLY",
        date="20241231"
    )
    print("\n4. MONTHLY示例（8位日期）:")
    print(json.dumps(result4, indent=2, ensure_ascii=False))
    
    # 示例5: 错误情况 - 空data_type
    result5 = generator.get_path_info(
        source_name="example_source",
        data_type="",  # 空字符串，应该失败
        archive_type="DAILY",
        date="20241231"
    )
    print("\n5. 错误示例 - 空data_type:")
    print(json.dumps(result5, indent=2, ensure_ascii=False))
    
    # 示例6: 错误情况 - DAILY类型缺少日期参数
    result6 = generator.get_path_info(
        source_name="example_source",
        data_type="daily",
        archive_type="DAILY"
        # 缺少date参数
    )
    print("\n6. 错误示例 - DAILY类型缺少日期参数:")
    print(json.dumps(result6, indent=2, ensure_ascii=False))
    
    # 示例7: 错误情况 - DAILY类型日期格式错误
    result7 = generator.get_path_info(
        source_name="example_source",
        data_type="daily",
        archive_type="DAILY",
        date="202412"  # 6位格式，DAILY要求8位
    )
    print("\n7. 错误示例 - DAILY类型日期格式错误:")
    print(json.dumps(result7, indent=2, ensure_ascii=False))
    
    # 示例8: QUARTERLY类型（Q1财报日期）
    result8 = generator.get_path_info(
        source_name="example_source",
        data_type="financial_report",
        archive_type="QUARTERLY",
        date="20240331"
    )
    print("\n8. QUARTERLY示例（Q1财报）:")
    print(json.dumps(result8, indent=2, ensure_ascii=False))
    
    # 示例9: QUARTERLY类型（Q4财报日期）
    result9 = generator.get_path_info(
        source_name="example_source",
        data_type="financial_report",
        archive_type="QUARTERLY",
        date="20241231"
    )
    print("\n9. QUARTERLY示例（Q4财报）:")
    print(json.dumps(result9, indent=2, ensure_ascii=False))
    
    # 示例10: 错误情况 - MONTHLY类型日期格式错误
    result10 = generator.get_path_info(
        source_name="example_source",
        data_type="monthly_return",
        archive_type="MONTHLY",
        date="2024"  # 4位格式，不符合要求
    )
    print("\n10. 错误示例 - MONTHLY类型日期格式错误:")
    print(json.dumps(result10, indent=2, ensure_ascii=False))
    
    # 示例11: 错误情况 - QUARTERLY类型缺少日期
    result11 = generator.get_path_info(
        source_name="example_source",
        data_type="financial_report",
        archive_type="QUARTERLY"
        # 缺少date参数
    )
    print("\n11. 错误示例 - QUARTERLY类型缺少日期:")
    print(json.dumps(result11, indent=2, ensure_ascii=False))
    
    # 示例12: 错误情况 - QUARTERLY类型非财报日期
    result12 = generator.get_path_info(
        source_name="example_source",
        data_type="financial_report",
        archive_type="QUARTERLY",
        date="20240415"  # 不是财报日期
    )
    print("\n12. 错误示例 - QUARTERLY类型非财报日期:")
    print(json.dumps(result12, indent=2, ensure_ascii=False))
    
    # 示例13: symbol类型（股票代码）
    result13 = generator.get_path_info(
        source_name="akshare",
        data_type="stock_balance_sheet_by_report_em",
        archive_type="symbol",
        symbol="SH600519"
    )
    print("\n13. symbol示例（贵州茅台）:")
    print(json.dumps(result13, indent=2, ensure_ascii=False))
    
    # 示例14: 错误情况 - symbol类型缺少symbol参数
    result14 = generator.get_path_info(
        source_name="akshare",
        data_type="stock_balance_sheet_by_report_em",
        archive_type="symbol"
        # 缺少symbol参数
    )
    print("\n14. 错误示例 - symbol类型缺少symbol参数:")
    print(json.dumps(result14, indent=2, ensure_ascii=False))
    
    # 示例15: 错误情况 - symbol类型股票代码格式错误
    result15 = generator.get_path_info(
        source_name="akshare",
        data_type="stock_balance_sheet_by_report_em",
        archive_type="symbol",
        symbol="600519"  # 缺少SH前缀
    )
    print("\n15. 错误示例 - symbol类型股票代码格式错误:")
    print(json.dumps(result15, indent=2, ensure_ascii=False))
    
    print("\n=== 示例运行完成 ===")


if __name__ == "__main__":
    main()