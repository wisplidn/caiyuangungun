"""QLIB-READY层测试脚本

用于测试和验证QLIB-READY层的数据处理功能，处理一年的数据进行验证。
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
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


class QlibReadyTester:
    """QLIB-READY层测试器"""
    
    def __init__(self, data_root: str):
        """初始化测试器
        
        Args:
            data_root: 数据根目录
        """
        self.data_root = data_root
        self.manager = QlibReadyDataManager(data_root)
        self.processor = QlibDataProcessor()
        self.validator = QlibFormatValidator()
        
    def create_mock_data(self, 
                        start_date: date, 
                        end_date: date, 
                        symbols: list) -> tuple:
        """创建模拟数据用于测试
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            symbols: 股票代码列表
            
        Returns:
            (daily_quotes, adj_factors, basic_info) 元组
        """
        logger.info(f"创建模拟数据: {start_date} 到 {end_date}, {len(symbols)}只股票")
        
        # 生成交易日期（排除周末）
        date_range = pd.date_range(start_date, end_date, freq='D')
        trading_dates = [d for d in date_range if d.weekday() < 5]  # 排除周末
        
        # 创建日频行情数据
        daily_quotes_data = []
        np.random.seed(42)  # 固定随机种子以便复现
        
        for symbol in symbols:
            base_price = np.random.uniform(10, 100)  # 基础价格
            
            for trade_date in trading_dates:
                # 模拟价格波动
                change_pct = np.random.normal(0, 0.02)  # 2%标准差的正态分布
                
                open_price = base_price * (1 + change_pct)
                high_price = open_price * (1 + abs(np.random.normal(0, 0.01)))
                low_price = open_price * (1 - abs(np.random.normal(0, 0.01)))
                close_price = np.random.uniform(low_price, high_price)
                
                volume = np.random.randint(1000000, 10000000)
                amount = volume * np.random.uniform(low_price, high_price)
                
                daily_quotes_data.append({
                    'symbol': symbol,
                    'trade_date': trade_date,
                    'open': round(open_price, 2),
                    'high': round(high_price, 2),
                    'low': round(low_price, 2),
                    'close': round(close_price, 2),
                    'volume': volume,
                    'amount': round(amount, 2)
                })
                
                base_price = close_price  # 下一天的基础价格
                
        daily_quotes = pd.DataFrame(daily_quotes_data)
        
        # 创建复权因子数据
        adj_factors_data = []
        for symbol in symbols:
            for trade_date in trading_dates[::30]:  # 每30天一个复权因子
                adj_factors_data.append({
                    'symbol': symbol,
                    'trade_date': trade_date,
                    'adj_factor': np.random.uniform(0.8, 1.2)
                })
                
        adj_factors = pd.DataFrame(adj_factors_data)
        
        # 创建基础信息数据
        basic_info_data = []
        industries = ['银行', '地产', '医药', '科技', '制造']
        markets = ['主板', '中小板', '创业板']
        
        for symbol in symbols:
            basic_info_data.append({
                'symbol': symbol,
                'name': f'股票{symbol}',
                'industry': np.random.choice(industries),
                'market': np.random.choice(markets),
                'list_date': start_date - timedelta(days=np.random.randint(365, 3650)),
                'status': '正常'
            })
            
        basic_info = pd.DataFrame(basic_info_data)
        
        logger.info(f"模拟数据创建完成: 行情{len(daily_quotes)}条, 复权因子{len(adj_factors)}条, 基础信息{len(basic_info)}条")
        
        return daily_quotes, adj_factors, basic_info
        
    def test_data_processing(self, 
                           start_date: date = None, 
                           end_date: date = None,
                           symbols: list = None) -> dict:
        """测试数据处理流程
        
        Args:
            start_date: 开始日期，默认为一年前
            end_date: 结束日期，默认为今天
            symbols: 股票代码列表，默认为测试股票
            
        Returns:
            测试结果字典
        """
        # 设置默认参数
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365)
        if symbols is None:
            symbols = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
            
        logger.info(f"开始测试数据处理: {start_date} 到 {end_date}")
        
        test_results = {
            'start_date': start_date,
            'end_date': end_date,
            'symbols': symbols,
            'steps': {},
            'errors': [],
            'success': True
        }
        
        try:
            # 步骤1: 创建模拟数据
            logger.info("步骤1: 创建模拟数据")
            daily_quotes, adj_factors, basic_info = self.create_mock_data(
                start_date, end_date, symbols
            )
            test_results['steps']['mock_data'] = {
                'daily_quotes_rows': len(daily_quotes),
                'adj_factors_rows': len(adj_factors),
                'basic_info_rows': len(basic_info)
            }
            
            # 步骤2: 数据处理
            logger.info("步骤2: 数据处理")
            processed_data = self.processor.process_quotes_data(
                daily_quotes, adj_factors, basic_info
            )
            test_results['steps']['data_processing'] = {
                'processed_rows': len(processed_data),
                'columns': list(processed_data.columns)
            }
            
            # 步骤3: 按特征分组
            logger.info("步骤3: 按特征分组")
            feature_data = self.processor.split_by_features(processed_data)
            test_results['steps']['feature_split'] = {
                'feature_count': len(feature_data),
                'features': list(feature_data.keys())
            }
            
            # 步骤4: 格式验证
            logger.info("步骤4: 格式验证")
            validation_results = self.validator.validate_qlib_dataset(feature_data)
            
            valid_features = []
            invalid_features = []
            
            for feature_name, validation_result in validation_results.items():
                if validation_result['is_valid']:
                    valid_features.append(feature_name)
                else:
                    invalid_features.append(feature_name)
                    
            test_results['steps']['validation'] = {
                'valid_features': valid_features,
                'invalid_features': invalid_features,
                'validation_details': validation_results
            }
            
            # 步骤5: 保存数据
            logger.info("步骤5: 保存数据")
            saved_files = []
            
            for feature_name, data in feature_data.items():
                if validation_results[feature_name]['is_valid']:
                    file_path = self.manager.save_data(
                        data, 
                        InterfaceType.QUOTES_DAILY,
                        feature_name=feature_name
                    )
                    saved_files.append(file_path)
                    
            test_results['steps']['save_data'] = {
                'saved_files': saved_files,
                'file_count': len(saved_files)
            }
            
            # 步骤6: 数据质量检查
            logger.info("步骤6: 数据质量检查")
            quality_report = self.processor.validate_data_quality(processed_data)
            test_results['steps']['quality_check'] = quality_report
            
        except Exception as e:
            logger.error(f"测试过程中出现错误: {str(e)}")
            test_results['errors'].append(str(e))
            test_results['success'] = False
            
        return test_results
        
    def generate_test_report(self, test_results: dict) -> str:
        """生成测试报告
        
        Args:
            test_results: 测试结果
            
        Returns:
            格式化的测试报告
        """
        report_lines = []
        
        # 标题
        report_lines.append("=== QLIB-READY层数据处理测试报告 ===")
        report_lines.append("")
        
        # 基本信息
        report_lines.append(f"测试日期范围: {test_results['start_date']} 到 {test_results['end_date']}")
        report_lines.append(f"测试股票数量: {len(test_results['symbols'])}")
        report_lines.append(f"测试状态: {'✓ 成功' if test_results['success'] else '✗ 失败'}")
        report_lines.append("")
        
        # 各步骤结果
        steps = test_results.get('steps', {})
        
        for step_name, step_result in steps.items():
            step_title = step_name.replace('_', ' ').title()
            report_lines.append(f"【{step_title}】")
            
            if isinstance(step_result, dict):
                for key, value in step_result.items():
                    if isinstance(value, list) and len(value) > 5:
                        report_lines.append(f"  {key}: {len(value)}个项目")
                    else:
                        report_lines.append(f"  {key}: {value}")
            else:
                report_lines.append(f"  结果: {step_result}")
                
            report_lines.append("")
            
        # 错误信息
        errors = test_results.get('errors', [])
        if errors:
            report_lines.append("【错误信息】")
            for error in errors:
                report_lines.append(f"  ✗ {error}")
            report_lines.append("")
            
        return "\n".join(report_lines)
        
    def run_comprehensive_test(self) -> None:
        """运行综合测试"""
        logger.info("开始运行QLIB-READY层综合测试")
        
        # 测试一年数据
        test_results = self.test_data_processing()
        
        # 生成报告
        report = self.generate_test_report(test_results)
        
        # 输出报告
        print(report)
        
        # 保存报告到文件
        report_path = os.path.join(self.data_root, 'qlib_ready_test_report.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
            
        logger.info(f"测试报告已保存到: {report_path}")
        
        # 如果有验证详情，生成详细的验证报告
        validation_details = test_results.get('steps', {}).get('validation', {}).get('validation_details', {})
        if validation_details:
            detailed_report_lines = []
            detailed_report_lines.append("=== 详细验证报告 ===")
            detailed_report_lines.append("")
            
            for feature_name, validation_result in validation_details.items():
                feature_report = self.validator.generate_validation_report(validation_result)
                detailed_report_lines.append(feature_report)
                detailed_report_lines.append("\n" + "="*50 + "\n")
                
            detailed_report = "\n".join(detailed_report_lines)
            
            detailed_report_path = os.path.join(self.data_root, 'qlib_ready_validation_report.txt')
            with open(detailed_report_path, 'w', encoding='utf-8') as f:
                f.write(detailed_report)
                
            logger.info(f"详细验证报告已保存到: {detailed_report_path}")


def main():
    """主函数"""
    # 设置数据根目录
    data_root = os.path.join(os.getcwd(), 'test_data')
    
    # 创建测试器
    tester = QlibReadyTester(data_root)
    
    # 运行测试
    tester.run_comprehensive_test()


if __name__ == '__main__':
    main()