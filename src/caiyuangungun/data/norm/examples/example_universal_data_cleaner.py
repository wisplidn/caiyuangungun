"""
测试通用数据清洗器的各个功能和基础清洗流水线配置
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os
from pathlib import Path
import sys
import json

# 添加项目路径到sys.path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.norm.processors.common.universal_data_cleaner import UniversalDataCleaner
from caiyuangungun.data.norm.core.config_manager import ConfigManager


class TestUniversalDataCleaner:
    """测试通用数据清洗器"""
    
    def setup_method(self):
        """测试前的设置"""
        # 创建测试用的股票基础信息
        self.stock_basic_df = pd.DataFrame({
            'symbol': ['000001', '000002', '600000', '600036'],
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH'],
            'name': ['平安银行', '万科A', '浦发银行', '招商银行']
        })
        
        # 创建BSE映射
        self.bse_mapping = {
            '000001': '000001',
            '000002': '000002', 
            '600000': '600000',
            '600036': '600036'
        }
        
        # 初始化清洗器
        self.cleaner = UniversalDataCleaner(
            stock_basic_df=self.stock_basic_df,
            bse_mapping=self.bse_mapping
        )
        
    def test_convert_date_format(self):
        """测试日期格式转换功能"""
        print("\n=== 测试日期格式转换功能 ===")
        
        # 创建测试数据
        test_data = pd.DataFrame({
            '公告日期': ['2025-08-26 00:00:00', '2024-12-31T00:00:00.000Z', '20231201', None],
            '报告期': ['2025-06-30T00:00:00.000Z', '2024-09-30 00:00:00', '20231130', ''],
            'NOTICE_DATE': ['2025-08-26 00:00:00', '2024-12-31T00:00:00.000Z', None, '20240101']
        })
        
        print("原始数据:")
        print(test_data)
        
        # 测试转换公告日期
        result1 = self.cleaner.convert_date_format(
            test_data, 
            field_name='ann_date', 
            source_field='公告日期',
            operation='add'
        )
        print("\n转换公告日期后:")
        print(result1[['公告日期', 'ann_date']])
        
        # 测试转换报告期
        result2 = self.cleaner.convert_date_format(
            result1,
            field_name='end_date',
            source_field='报告期', 
            operation='add'
        )
        print("\n转换报告期后:")
        print(result2[['报告期', 'end_date']])
        
        # 测试转换NOTICE_DATE
        result3 = self.cleaner.convert_date_format(
            result2,
            field_name='notice_date_formatted',
            source_field='NOTICE_DATE',
            operation='add'
        )
        print("\n转换NOTICE_DATE后:")
        print(result3[['NOTICE_DATE', 'notice_date_formatted']])
        
        # 验证结果
        assert 'ann_date' in result3.columns
        assert 'end_date' in result3.columns
        assert 'notice_date_formatted' in result3.columns
        
        # 验证具体转换结果
        assert result3.loc[0, 'ann_date'] == '20250826'
        assert result3.loc[1, 'end_date'] == '20240930'
        
    def test_apply_bse_mapping(self):
        """测试BSE代码映射功能"""
        print("\n=== 测试BSE代码映射功能 ===")
        
        # 创建测试数据
        test_data = pd.DataFrame({
            '股票代码': ['000001', '000002', '600000', '999999'],  # 最后一个不在映射中
            'SECURITY_CODE': ['000001', '000002', '600000', '888888']
        })
        
        print("原始数据:")
        print(test_data)
        
        # 测试映射股票代码
        result1 = self.cleaner.apply_bse_mapping(
            test_data,
            field_name='股票代码_mapped',
            source_field='股票代码',
            operation='add'
        )
        print("\n映射股票代码后:")
        print(result1[['股票代码', '股票代码_mapped']])
        
        # 测试映射SECURITY_CODE
        result2 = self.cleaner.apply_bse_mapping(
            result1,
            field_name='SECURITY_CODE_mapped', 
            source_field='SECURITY_CODE',
            operation='add'
        )
        print("\n映射SECURITY_CODE后:")
        print(result2[['SECURITY_CODE', 'SECURITY_CODE_mapped']])
        
        # 验证结果
        assert '股票代码_mapped' in result2.columns
        assert 'SECURITY_CODE_mapped' in result2.columns
        
        # 验证映射结果（不在映射中的保持原值）
        assert result2.loc[0, '股票代码_mapped'] == '000001'
        assert result2.loc[3, '股票代码_mapped'] == '999999'  # 不在映射中，保持原值
        
    def test_add_ts_code(self):
        """测试添加ts_code功能"""
        print("\n=== 测试添加ts_code功能 ===")
        
        # 创建测试数据
        test_data = pd.DataFrame({
            '股票代码_mapped': ['000001', '000002', '600000', '999999'],  # 最后一个不在股票基础信息中
            'SECURITY_CODE_mapped': ['000001', '000002', '600000', '888888']
        })
        
        print("原始数据:")
        print(test_data)
        
        # 测试添加ts_code（基于股票代码_mapped）
        result1 = self.cleaner.add_ts_code(
            test_data,
            field_name='ts_code',
            source_field='股票代码_mapped',
            operation='add'
        )
        print("\n添加ts_code后:")
        print(result1[['股票代码_mapped', 'ts_code']])
        
        # 测试添加ts_code（基于SECURITY_CODE_mapped）
        result2 = self.cleaner.add_ts_code(
            test_data,
            field_name='ts_code_2',
            source_field='SECURITY_CODE_mapped',
            operation='add'
        )
        print("\n基于SECURITY_CODE_mapped添加ts_code:")
        print(result2[['SECURITY_CODE_mapped', 'ts_code_2']])
        
        # 验证结果
        assert 'ts_code' in result1.columns
        assert 'ts_code_2' in result2.columns
        
        # 验证ts_code结果
        assert result1.loc[0, 'ts_code'] == '000001.SZ'
        assert result1.loc[2, 'ts_code'] == '600000.SH'
        
    def test_missing_source_field_warnings(self):
        """测试缺失源字段时的警告处理"""
        print("\n=== 测试缺失源字段时的警告处理 ===")
        
        # 创建测试数据（故意缺少某些字段）
        test_data = pd.DataFrame({
            'existing_field': ['value1', 'value2'],
            'another_field': ['data1', 'data2']
        })
        
        print("原始数据:")
        print(test_data)
        
        # 测试缺失源字段的情况
        print("\n测试convert_date_format缺失源字段:")
        result1 = self.cleaner.convert_date_format(
            test_data,
            field_name='ann_date',
            source_field='nonexistent_field',  # 不存在的字段
            operation='add'
        )
        print("结果列:", result1.columns.tolist())
        assert 'ann_date' not in result1.columns  # 应该没有添加新字段
        
        print("\n测试apply_bse_mapping缺失源字段:")
        result2 = self.cleaner.apply_bse_mapping(
            test_data,
            field_name='mapped_field',
            source_field='nonexistent_field',  # 不存在的字段
            operation='add'
        )
        print("结果列:", result2.columns.tolist())
        assert 'mapped_field' not in result2.columns  # 应该没有添加新字段
        
        print("\n测试add_ts_code缺失源字段:")
        result3 = self.cleaner.add_ts_code(
            test_data,
            field_name='ts_code',
            source_field='nonexistent_field',  # 不存在的字段
            operation='add'
        )
        print("结果列:", result3.columns.tolist())
        assert 'ts_code' not in result3.columns  # 应该没有添加新字段
        
    def test_empty_mapping_data(self):
        """测试映射数据为空时的处理"""
        print("\n=== 测试映射数据为空时的处理 ===")
        
        # 创建没有映射数据的清洗器
        empty_cleaner = UniversalDataCleaner(
            stock_basic_df=pd.DataFrame(),  # 空的股票基础信息
            bse_mapping={}  # 空的BSE映射
        )
        
        test_data = pd.DataFrame({
            '股票代码': ['000001', '600000'],
            'other_field': ['data1', 'data2']
        })
        
        print("原始数据:")
        print(test_data)
        
        # 测试空BSE映射
        print("\n测试空BSE映射:")
        result1 = empty_cleaner.apply_bse_mapping(
            test_data,
            field_name='股票代码_mapped',
            source_field='股票代码',
            operation='add'
        )
        print(result1[['股票代码', '股票代码_mapped']])
        assert '股票代码_mapped' in result1.columns
        assert result1.loc[0, '股票代码_mapped'] == '000001'  # 应该保持原值
        
        # 测试空股票基础信息
        print("\n测试空股票基础信息:")
        result2 = empty_cleaner.add_ts_code(
            test_data,
            field_name='ts_code',
            source_field='股票代码',
            operation='add'
        )
        print(result2[['股票代码', 'ts_code']])
        assert 'ts_code' in result2.columns
        # 应该根据代码前缀自动添加后缀
        assert result2.loc[0, 'ts_code'] == '000001.SZ'
        assert result2.loc[1, 'ts_code'] == '600000.SH'


def run_tests():
    """运行所有测试"""
    print("开始测试通用数据清洗器功能...")
    
    test_instance = TestUniversalDataCleaner()
    test_instance.setup_method()
    
    try:
        test_instance.test_convert_date_format()
        print("✓ 日期格式转换测试通过")
        
        test_instance.test_apply_bse_mapping()
        print("✓ BSE代码映射测试通过")
        
        test_instance.test_add_ts_code()
        print("✓ 添加ts_code测试通过")
        
        test_instance.test_missing_source_field_warnings()
        print("✓ 缺失源字段警告测试通过")
        
        test_instance.test_empty_mapping_data()
        print("✓ 空映射数据处理测试通过")
        
        print("\n🎉 所有测试通过！")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


def test_basic_cleaning_pipeline():
    """测试基础清洗流水线配置"""
    print("\n" + "="*60)
    print("开始测试基础清洗流水线配置...")
    print("="*60)
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager()
        
        # 获取基础清洗配置
        basic_config = config_manager.get_basic_cleaning_config()
        print(f"\n基础清洗配置加载状态: {'成功' if basic_config else '失败'}")
        
        if not basic_config:
            print("❌ 无法加载基础清洗配置文件")
            return
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH'],
            '公告日期': ['2025-08-26 00:00:00', '2024-12-31T00:00:00.000Z', '20231201', '20240101'],
            '报告期': ['2025-06-30T00:00:00.000Z', '2024-09-30 00:00:00', '20231130', '20240331'],
            '营业收入': [1000000, 2000000, 1500000, 1800000],
            '净利润': [100000, 200000, 150000, 180000]
        })
        
        # 创建股票基础信息（包含必要的字段）
        stock_basic_df = pd.DataFrame({
            'symbol': ['000001', '000002', '600000', '600036'],
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH'],
            'name': ['平安银行', '万科A', '浦发银行', '招商银行'],
            'list_status': ['L', 'L', 'L', 'L'],  # L表示上市
            'list_date': ['19910403', '19910129', '19990810', '20020408'],
            'delist_date': [None, None, None, None]
        })
        
        # 创建BSE映射
        bse_mapping = {
            '000001': '000001',
            '000002': '000002', 
            '600000': '600000',
            '600036': '600036'
        }
        
        # 初始化清洗器
        cleaner = UniversalDataCleaner(
            stock_basic_df=stock_basic_df,
            bse_mapping=bse_mapping
        )
        
        print(f"\n原始测试数据 ({len(test_data)} 行):")
        print(test_data)
        
        # 测试所有数据类型的清洗流水线
        data_types = ['income_statement', 'balancesheet', 'cashflow', 'fina_indicator', 'daily_data']
        
        for data_type in data_types:
            print(f"\n{'='*50}")
            print(f"测试数据类型: {data_type}")
            print(f"{'='*50}")
            
            # 获取启用的清洗流水线
            enabled_pipelines = config_manager.get_enabled_cleaning_pipelines(data_type)
            
            if not enabled_pipelines:
                print(f"⚠️  数据类型 {data_type} 没有启用的清洗流水线")
                continue
            
            print(f"启用的数据源数量: {len(enabled_pipelines)}")
            
            for source_name, pipeline_steps in enabled_pipelines.items():
                print(f"\n--- 处理数据源: {source_name} ---")
                print(f"清洗步骤数量: {len(pipeline_steps)}")
                
                # 复制测试数据用于清洗
                current_data = test_data.copy()
                
                # 执行清洗流水线
                for i, step in enumerate(pipeline_steps, 1):
                    function_name = step.get('function')
                    params = step.get('params', {})
                    
                    print(f"  步骤 {i}: {function_name}")
                    print(f"    参数: {params}")
                    
                    try:
                        # 执行清洗函数
                        if hasattr(cleaner, function_name):
                            func = getattr(cleaner, function_name)
                            current_data = func(current_data, **params)
                            print(f"    ✓ 执行成功，当前数据形状: {current_data.shape}")
                        else:
                            print(f"    ❌ 清洗函数 {function_name} 不存在")
                    
                    except Exception as e:
                        print(f"    ❌ 执行失败: {e}")
                
                print(f"\n{source_name} 清洗后的数据:")
                print(current_data)
                print(f"清洗后列名: {list(current_data.columns)}")
                
                # 检查是否添加了预期的列
                expected_columns = ['ts_code']  # 所有流水线都应该添加ts_code
                for col in expected_columns:
                    if col in current_data.columns:
                        print(f"  ✓ 成功添加列: {col}")
                    else:
                        print(f"  ⚠️  未找到预期列: {col}")
        
        print(f"\n{'='*60}")
        print("🎉 基础清洗流水线测试完成！")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n❌ 基础清洗流水线测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 运行基础功能测试
    run_tests()
    
    # 运行基础清洗流水线配置测试
    test_basic_cleaning_pipeline()