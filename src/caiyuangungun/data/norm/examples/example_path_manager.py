#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PathManager独立测试模块（使用真实数据）
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.norm.core.path_manager import PathManager


def test_path_manager_standalone():
    """测试PathManager独立功能（使用真实数据）"""
    print("=== PathManager独立测试（使用真实数据）===")
    
    # 使用真实base_path
    base_path = "/Users/daishun/个人文档/caiyuangungun/data/raw"
    supported_formats = ["parquet", "json", "csv"]
    default_format = "parquet"
    
    print(f"\n基础路径: {base_path}")
    print(f"支持格式: {supported_formats}")
    print(f"默认格式: {default_format}")
    
    # 创建PathManager实例
    path_manager = PathManager(
        base_path=base_path,
        supported_formats=supported_formats,
        default_format=default_format
    )
    
    # 定义测试对象（移除stock_zh_a_gdhs和stock_zh_a_hist_hfq）
    test_methods = [
        ("tushare", "trade_cal"),
        ("tushare", "daily"),
        ("tushare", "income_vip"),
        ("tushare", "fina_indicator_vip"),
        ("akshare", "stock_zcfz_bj_em"),
        ("akshare", "stock_balance_sheet_by_report_em")
    ]
    
    # 测试1: 获取方法文件路径
    print("\n【测试1】获取方法文件路径")
    for i, (source, method) in enumerate(test_methods, 1):
        print(f"\n输入: method_name='{method}' (来源: {source})")
        try:
            file_pairs = path_manager.get_method_file_paths(method)
            if file_pairs:
                print(f"输出: 共{len(file_pairs)}个文件对")
                # 只显示前10个
                for j, file_pair in enumerate(file_pairs[:10], 1):
                    print(f"  [{j}] parquet: {file_pair[0]}")
                    print(f"      json: {file_pair[1]}")
                if len(file_pairs) > 10:
                    print(f"  ... 还有{len(file_pairs) - 10}个文件对")
            else:
                print("输出: 未找到文件")
        except Exception as e:
            print(f"输出: 错误 - {e}")
    
    # 测试2: 按日期范围获取文件路径
    print("\n【测试2】按日期范围获取方法文件路径 (20240101~20240630)")
    for i, (source, method) in enumerate(test_methods, 1):
        print(f"\n输入: method_name='{method}', start_date='20240101', end_date='20240630' (来源: {source})")
        try:
            file_pairs = path_manager.get_method_file_paths_by_date_range(
                method, "20240101", "20240630"
            )
            if file_pairs:
                print(f"输出: 共{len(file_pairs)}个文件对")
                # 只显示前10个
                for j, file_pair in enumerate(file_pairs[:10], 1):
                    print(f"  [{j}] parquet: {file_pair[0]}")
                    print(f"      json: {file_pair[1]}")
                if len(file_pairs) > 10:
                    print(f"  ... 还有{len(file_pairs) - 10}个文件对")
            else:
                print("输出: 未找到文件")
        except Exception as e:
            print(f"输出: 错误 - {e}")
    
    # 测试3: 获取保存路径
    print("\n【测试3】获取保存路径")
    for i, (source, method) in enumerate(test_methods, 1):
        print(f"\n输入: method_name='{method}' (来源: {source})")
        try:
            save_path = path_manager.get_method_save_path(method)
            print(f"输出: {save_path}")
        except Exception as e:
            print(f"输出: 错误 - {e}")
    
    # 测试4: 获取可用日期
    print("\n【测试4】获取可用日期")
    for i, (source, method) in enumerate(test_methods, 1):
        print(f"\n输入: method_name='{method}' (来源: {source})")
        try:
            available_dates = path_manager.get_available_dates(method)
            if available_dates:
                print(f"输出: 共{len(available_dates)}个日期")
                # 只显示前10个
                for j, date in enumerate(available_dates[:10], 1):
                    print(f"  [{j}] {date}")
                if len(available_dates) > 10:
                    print(f"  ... 还有{len(available_dates) - 10}个日期")
            else:
                print("输出: 未找到日期")
        except Exception as e:
            print(f"输出: 错误 - {e}")
    
    print("\n=== PathManager独立测试完成 ===")


if __name__ == "__main__":
    test_path_manager_standalone()