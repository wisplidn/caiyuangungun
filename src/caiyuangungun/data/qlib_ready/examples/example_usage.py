"""QLIB-READY层使用示例

本脚本展示如何使用QLIB-READY层处理数据，生成符合Qlib格式的数据文件。
"""

import os
import sys
import pandas as pd
from datetime import date, datetime
from pathlib import Path
import logging

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.caiyuangungun.data.qlib_ready.processors.quotes.manager import QlibReadyDataManager
from src.caiyuangungun.data.qlib_ready.processors.quotes.processor import QlibDataProcessor
from src.caiyuangungun.data.qlib_ready.core.validator import QlibFormatValidator
from src.caiyuangungun.contracts import InterfaceType

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def example_basic_usage():
    """基础使用示例"""
    print("=== 基础使用示例 ===")
    
    # 1. 初始化管理器
    data_root = "/Users/daishun/个人文档/caiyuangungun/test_data"
    manager = QlibReadyDataManager(data_root)
    
    # 2. 设置处理参数
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    symbols = ['000001.SZ', '000002.SZ', '600000.SH']
    
    print(f"处理参数:")
    print(f"  数据根目录: {data_root}")
    print(f"  日期范围: {start_date} 到 {end_date}")
    print(f"  股票代码: {symbols}")
    
    # 3. 生成Qlib数据（这里使用模拟数据，实际使用时会从NORM层读取）
    try:
        result = manager.generate_qlib_data(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols
        )
        
        print(f"\n处理结果:")
        print(f"  ✓ 处理行数: {result['processed_rows']}")
        print(f"  ✓ 生成特征: {len(result['feature_files'])}个")
        print(f"  ✓ 保存路径: {result['save_path']}")
        
        print(f"\n生成的特征文件:")
        for feature, file_path in result['feature_files'].items():
            print(f"  {feature}: {os.path.basename(file_path)}")
            
    except Exception as e:
        print(f"  ✗ 处理失败: {e}")
        

def example_step_by_step():
    """分步骤处理示例"""
    print("\n=== 分步骤处理示例 ===")
    
    # 1. 初始化组件
    data_root = "/Users/daishun/个人文档/caiyuangungun/test_data"
    manager = QlibReadyDataManager(data_root)
    processor = QlibDataProcessor()
    validator = QlibFormatValidator()
    
    print("步骤1: 初始化组件 ✓")
    
    # 2. 创建模拟数据（实际使用时从NORM层加载）
    symbols = ['000001.SZ', '000002.SZ']
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    
    # 模拟daily_quotes数据
    daily_quotes = []
    for symbol in symbols:
        for date_val in dates:
            daily_quotes.append({
                'symbol': symbol,
                'trade_date': date_val,
                'open': 10.0 + (hash(f"{symbol}{date_val}") % 100) / 10,
                'high': 11.0 + (hash(f"{symbol}{date_val}") % 100) / 10,
                'low': 9.0 + (hash(f"{symbol}{date_val}") % 100) / 10,
                'close': 10.5 + (hash(f"{symbol}{date_val}") % 100) / 10,
                'volume': 1000000 + (hash(f"{symbol}{date_val}") % 1000000),
                'amount': 10000000 + (hash(f"{symbol}{date_val}") % 10000000)
            })
    
    daily_quotes_df = pd.DataFrame(daily_quotes)
    print(f"步骤2: 创建模拟数据 ✓ (行数: {len(daily_quotes_df)})")
    
    # 3. 数据处理
    try:
        processed_data = processor.process_quotes_data(
            daily_quotes=daily_quotes_df,
            adj_factors=pd.DataFrame(),  # 空的复权因子数据
            basic_info=pd.DataFrame()    # 空的基础信息数据
        )
        print(f"步骤3: 数据处理 ✓ (处理后行数: {len(processed_data)})")
        
        # 4. 按特征分组
        feature_data = processor.split_by_features(processed_data)
        print(f"步骤4: 特征分组 ✓ (特征数: {len(feature_data)})")
        
        # 5. 格式验证
        validation_results = {}
        for feature_name, data in feature_data.items():
            validation_results[feature_name] = validator.validate_qlib_format(
                data, feature_name
            )
        
        valid_count = sum(1 for result in validation_results.values() if result['is_valid'])
        print(f"步骤5: 格式验证 ✓ (有效特征: {valid_count}/{len(validation_results)})")
        
        # 6. 保存数据
        saved_files = []
        for feature_name, data in feature_data.items():
            if validation_results[feature_name]['is_valid']:
                file_path = manager.save_data(
                    data, 
                    InterfaceType.QUOTES_DAILY,
                    feature_name=feature_name
                )
                saved_files.append(file_path)
        
        print(f"步骤6: 保存数据 ✓ (保存文件: {len(saved_files)}个)")
        
        # 7. 数据质量检查
        quality_report = processor.validate_data_quality(processed_data)
        print(f"步骤7: 质量检查 ✓")
        print(f"  - 总行数: {quality_report['total_rows']}")
        print(f"  - 股票数: {quality_report['total_symbols']}")
        print(f"  - 日期范围: {quality_report['date_range']['start'].date()} 到 {quality_report['date_range']['end'].date()}")
        
    except Exception as e:
        print(f"  ✗ 处理失败: {e}")


def example_data_validation():
    """数据验证示例"""
    print("\n=== 数据验证示例 ===")
    
    # 创建测试数据
    test_data = pd.DataFrame({
        'trade_date': pd.date_range('2024-01-01', periods=5),
        '000001.SZ': [10.1, 10.2, 10.3, 10.4, 10.5],
        '000002.SZ': [20.1, 20.2, 20.3, 20.4, 20.5]
    })
    
    validator = QlibFormatValidator()
    
    # 验证数据格式
    result = validator.validate_qlib_format(test_data, 'open')
    
    print(f"验证结果:")
    print(f"  有效性: {'✓' if result['is_valid'] else '✗'}")
    print(f"  错误数: {len(result['errors'])}")
    print(f"  警告数: {len(result['warnings'])}")
    print(f"  数据形状: {result['data_shape']}")
    
    if result['errors']:
        print(f"  错误信息:")
        for error in result['errors']:
            print(f"    - {error}")
    
    if result['warnings']:
        print(f"  警告信息:")
        for warning in result['warnings']:
            print(f"    - {warning}")


def example_load_data():
    """数据加载示例"""
    print("\n=== 数据加载示例 ===")
    
    data_root = "/Users/daishun/个人文档/caiyuangungun/test_data"
    manager = QlibReadyDataManager(data_root)
    
    try:
        # 加载特定特征数据
        open_data = manager.load_data(
            InterfaceType.QUOTES_DAILY,
            feature_name='open'
        )
        
        if open_data is not None:
            print(f"加载成功:")
            print(f"  数据形状: {open_data.shape}")
            print(f"  列名: {list(open_data.columns)}")
            print(f"  日期范围: {open_data['trade_date'].min()} 到 {open_data['trade_date'].max()}")
            
            # 显示前几行数据
            print(f"\n前3行数据:")
            print(open_data.head(3).to_string(index=False))
        else:
            print("  ✗ 数据文件不存在")
            
    except Exception as e:
        print(f"  ✗ 加载失败: {e}")


def main():
    """主函数"""
    print("QLIB-READY层使用示例")
    print("=" * 50)
    
    # 运行各种示例
    example_basic_usage()
    example_step_by_step()
    example_data_validation()
    example_load_data()
    
    print("\n=" * 50)
    print("示例运行完成！")
    print("\n更多信息请参考:")
    print("- README.md: 详细文档")
    print("- test_processor.py: 完整测试脚本")


if __name__ == "__main__":
    main()