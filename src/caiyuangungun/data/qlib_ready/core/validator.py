"""QLIB-READY层格式验证器

负责验证数据是否符合Qlib格式要求，包括列名、数据类型、索引格式等。
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Set
import re
import logging

logger = logging.getLogger(__name__)


class QlibFormatValidator:
    """Qlib格式验证器
    
    负责验证数据是否符合Qlib格式要求：
    - 特征文件格式：行为日期，列为股票代码
    - 列名格式：符合Qlib命名规范
    - 数据类型：数值型数据
    - 索引格式：日期索引
    """
    
    # Qlib标准特征名称
    STANDARD_FEATURES = {
        'open', 'high', 'low', 'close', 'volume', 'amount', 
        'vwap', 'factor', 'change', 'pct_chg'
    }
    
    # 股票代码格式（支持多种格式）
    SYMBOL_PATTERNS = [
        r'^\d{6}\.(SH|SZ)$',  # 标准格式：000001.SZ
        r'^\d{6}$',           # 纯数字：000001
        r'^[A-Z]{2,4}\d{6}$', # Tushare格式：SZ000001
    ]
    
    def __init__(self):
        """初始化格式验证器"""
        self.symbol_regex = [re.compile(pattern) for pattern in self.SYMBOL_PATTERNS]
        
    def validate_symbol_data_format(self, 
                                   data: pd.DataFrame, 
                                   symbol: str) -> Dict[str, any]:
        """验证单个股票数据是否符合Qlib格式
        
        Args:
            data: 要验证的股票数据
            symbol: 股票代码
            
        Returns:
            验证结果字典
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'symbol': symbol,
            'data_shape': data.shape,
            'checks': {}
        }
        
        try:
            # 1. 基础格式检查
            self._check_symbol_basic_format(data, validation_result)
            
            # 2. 必需列检查
            self._check_required_columns(data, validation_result)
            
            # 3. 数据类型检查
            self._check_symbol_data_types(data, validation_result)
            
            # 4. 日期列检查
            self._check_date_column(data, validation_result)
            
            # 5. 股票代码列检查
            self._check_symbol_column(data, symbol, validation_result)
            
            # 6. 数据完整性检查
            self._check_data_completeness(data, validation_result)
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"验证过程出错: {str(e)}")
            
        # 如果有错误，标记为无效
        if validation_result['errors']:
            validation_result['is_valid'] = False
            
        return validation_result
    
    def validate_qlib_format(self, 
                           data: pd.DataFrame, 
                           feature_name: Optional[str] = None) -> Dict[str, any]:
        """验证数据是否符合Qlib格式（兼容旧版本）
        
        Args:
            data: 要验证的数据
            feature_name: 特征名称
            
        Returns:
            验证结果字典
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'feature_name': feature_name,
            'data_shape': data.shape,
            'checks': {}
        }
        
        try:
            # 1. 基础格式检查
            self._check_basic_format(data, validation_result)
            
            # 2. 索引格式检查
            self._check_index_format(data, validation_result)
            
            # 3. 列名格式检查
            self._check_column_format(data, validation_result)
            
            # 4. 数据类型检查
            self._check_data_types(data, validation_result)
            
            # 5. 特征名称检查
            if feature_name:
                self._check_feature_name(feature_name, validation_result)
                
            # 6. 数据完整性检查
            self._check_data_completeness(data, validation_result)
            
            # 7. 数据一致性检查
            self._check_data_consistency(data, validation_result)
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"验证过程出错: {str(e)}")
            
        # 如果有错误，标记为无效
        if validation_result['errors']:
            validation_result['is_valid'] = False
            
        return validation_result
        
    def _check_basic_format(self, data: pd.DataFrame, result: Dict) -> None:
        """检查基础格式"""
        checks = []
        
        # 检查是否为DataFrame
        if not isinstance(data, pd.DataFrame):
            result['errors'].append("数据必须是pandas DataFrame")
            return
            
        checks.append("数据类型: DataFrame ✓")
        
        # 检查是否为空
        if data.empty:
            result['errors'].append("数据不能为空")
            return
            
        checks.append(f"数据行数: {len(data)} ✓")
        
        # 检查最小列数
        if len(data.columns) < 2:
            result['errors'].append("数据至少需要2列（日期列和至少一个股票列）")
            return
            
        checks.append(f"数据列数: {len(data.columns)} ✓")
        
        result['checks']['basic_format'] = checks
    
    def _check_symbol_basic_format(self, data: pd.DataFrame, result: Dict) -> None:
        """检查股票数据基础格式"""
        checks = []
        
        # 检查是否为DataFrame
        if not isinstance(data, pd.DataFrame):
            result['errors'].append("数据必须是pandas DataFrame")
            return
            
        checks.append("数据类型: DataFrame ✓")
        
        # 检查是否为空
        if data.empty:
            result['errors'].append("数据不能为空")
            return
            
        checks.append(f"数据行数: {len(data)} ✓")
        
        # 检查最小列数（至少需要日期列、股票代码列和一些特征列）
        if len(data.columns) < 3:
            result['errors'].append("数据至少需要3列（日期列、股票代码列和特征列）")
            return
            
        checks.append(f"数据列数: {len(data.columns)} ✓")
        
        result['checks']['symbol_basic_format'] = checks
    
    def _check_required_columns(self, data: pd.DataFrame, result: Dict) -> None:
        """检查必需的列"""
        checks = []
        
        # 检查是否有日期列（通常是第一列）
        date_columns = ['date', 'trade_date', 'datetime']
        has_date_col = any(col.lower() in date_columns for col in data.columns)
        
        if not has_date_col:
            # 检查第一列是否可能是日期
            try:
                pd.to_datetime(data.iloc[:, 0])
                checks.append("日期列: 推断为第一列 ✓")
            except:
                result['errors'].append("未找到有效的日期列")
        else:
            checks.append("日期列: 明确标识 ✓")
        
        # 检查是否有股票代码列
        symbol_columns = ['symbol', 'code', 'stock_code', 'ts_code']
        has_symbol_col = any(col.lower() in symbol_columns for col in data.columns)
        
        if has_symbol_col:
            checks.append("股票代码列: 明确标识 ✓")
        else:
            result['warnings'].append("未找到明确的股票代码列，请确保数据包含股票标识")
        
        result['checks']['required_columns'] = checks
    
    def _check_symbol_data_types(self, data: pd.DataFrame, result: Dict) -> None:
        """检查股票数据的数据类型"""
        checks = []
        
        # 检查数值列
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        non_numeric_columns = data.select_dtypes(exclude=[np.number]).columns
        
        # 除了日期列和股票代码列，其他应该是数值型
        expected_numeric_count = len(data.columns) - 2  # 减去日期列和股票代码列
        actual_numeric_count = len(numeric_columns)
        
        if actual_numeric_count >= expected_numeric_count:
            checks.append(f"数值列数量: {actual_numeric_count} ✓")
        else:
            result['warnings'].append(f"数值列数量不足，期望至少{expected_numeric_count}列，实际{actual_numeric_count}列")
        
        # 检查是否有无效数据类型
        object_columns = data.select_dtypes(include=['object']).columns
        if len(object_columns) > 2:  # 允许日期列和股票代码列为object类型
            result['warnings'].append(f"存在过多非数值列: {list(object_columns)}")
        
        result['checks']['symbol_data_types'] = checks
    
    def _check_date_column(self, data: pd.DataFrame, result: Dict) -> None:
        """检查日期列格式"""
        checks = []
        
        # 尝试找到日期列
        date_col = None
        for col in data.columns:
            if col.lower() in ['date', 'trade_date', 'datetime']:
                date_col = col
                break
        
        if date_col is None:
            # 假设第一列是日期列
            date_col = data.columns[0]
        
        try:
            date_series = pd.to_datetime(data[date_col])
            checks.append(f"日期列解析: {date_col} ✓")
            
            # 检查日期是否有序
            if date_series.is_monotonic_increasing:
                checks.append("日期排序: 升序 ✓")
            else:
                result['warnings'].append("日期未按升序排列")
            
            # 检查日期范围
            date_range = f"{date_series.min().date()} 到 {date_series.max().date()}"
            checks.append(f"日期范围: {date_range} ✓")
            
        except Exception as e:
            result['errors'].append(f"日期列格式错误: {str(e)}")
        
        result['checks']['date_column'] = checks
    
    def _check_symbol_column(self, data: pd.DataFrame, symbol: str, result: Dict) -> None:
        """检查股票代码列"""
        checks = []
        
        # 检查股票代码格式
        if self._is_valid_symbol(symbol):
            checks.append(f"股票代码格式: {symbol} ✓")
        else:
            result['warnings'].append(f"股票代码格式可能不标准: {symbol}")
        
        # 检查数据中是否包含股票代码列
        symbol_columns = ['symbol', 'code', 'stock_code', 'ts_code']
        found_symbol_col = None
        
        for col in data.columns:
            if col.lower() in symbol_columns:
                found_symbol_col = col
                break
        
        if found_symbol_col:
            # 检查股票代码列的值是否一致
            unique_symbols = data[found_symbol_col].unique()
            if len(unique_symbols) == 1:
                checks.append(f"股票代码一致性: {found_symbol_col} ✓")
            else:
                result['warnings'].append(f"股票代码列包含多个值: {unique_symbols}")
        
        result['checks']['symbol_column'] = checks
        
    def _check_index_format(self, data: pd.DataFrame, result: Dict) -> None:
        """检查索引格式"""
        checks = []
        
        # 检查第一列是否为日期
        first_col = data.columns[0]
        
        # 尝试解析为日期
        try:
            if first_col.lower() in ['date', 'trade_date', 'datetime']:
                date_series = pd.to_datetime(data[first_col])
                checks.append(f"日期列识别: {first_col} ✓")
            else:
                # 检查第一列数据是否可以解析为日期
                date_series = pd.to_datetime(data.iloc[:, 0])
                checks.append(f"日期列推断: {first_col} ✓")
                
            # 检查日期是否有序
            if not date_series.is_monotonic_increasing:
                result['warnings'].append("日期序列不是递增的，建议排序")
            else:
                checks.append("日期序列: 递增 ✓")
                
            # 检查日期范围
            date_range = f"{date_series.min().date()} 到 {date_series.max().date()}"
            checks.append(f"日期范围: {date_range} ✓")
            
        except Exception as e:
            result['errors'].append(f"第一列无法解析为日期: {str(e)}")
            
        result['checks']['index_format'] = checks
        
    def _check_column_format(self, data: pd.DataFrame, result: Dict) -> None:
        """检查列名格式"""
        checks = []
        
        # 获取股票代码列（除第一列外的所有列）
        symbol_columns = data.columns[1:]
        
        valid_symbols = []
        invalid_symbols = []
        
        for symbol in symbol_columns:
            if self._is_valid_symbol(symbol):
                valid_symbols.append(symbol)
            else:
                invalid_symbols.append(symbol)
                
        checks.append(f"有效股票代码: {len(valid_symbols)}个")
        
        if invalid_symbols:
            if len(invalid_symbols) <= 5:
                result['warnings'].append(f"无效股票代码格式: {invalid_symbols}")
            else:
                result['warnings'].append(f"无效股票代码格式: {len(invalid_symbols)}个，如: {invalid_symbols[:5]}...")
                
        # 检查重复列名
        duplicate_cols = data.columns[data.columns.duplicated()].tolist()
        if duplicate_cols:
            result['errors'].append(f"存在重复列名: {duplicate_cols}")
        else:
            checks.append("列名唯一性: ✓")
            
        result['checks']['column_format'] = checks
        
    def _check_data_types(self, data: pd.DataFrame, result: Dict) -> None:
        """检查数据类型"""
        checks = []
        
        # 检查数值列的数据类型
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        non_numeric_columns = data.select_dtypes(exclude=[np.number]).columns
        
        # 除第一列（日期列）外，其他列应该是数值型
        expected_numeric = data.columns[1:]
        actual_numeric = [col for col in expected_numeric if col in numeric_columns]
        
        checks.append(f"数值列: {len(actual_numeric)}/{len(expected_numeric)}")
        
        # 检查非数值列（除日期列外）
        non_numeric_data_cols = [col for col in non_numeric_columns if col != data.columns[0]]
        if non_numeric_data_cols:
            result['warnings'].append(f"非数值数据列: {non_numeric_data_cols}")
            
        # 检查数据范围
        for col in actual_numeric:
            col_data = data[col]
            
            # 检查无穷值
            inf_count = np.isinf(col_data).sum()
            if inf_count > 0:
                result['warnings'].append(f"列 {col} 包含 {inf_count} 个无穷值")
                
            # 检查NaN值比例
            nan_count = col_data.isnull().sum()
            nan_pct = nan_count / len(col_data) * 100
            if nan_pct > 50:
                result['warnings'].append(f"列 {col} 缺失值比例过高: {nan_pct:.1f}%")
            elif nan_pct > 0:
                checks.append(f"列 {col} 缺失值: {nan_pct:.1f}%")
                
        result['checks']['data_types'] = checks
        
    def _check_feature_name(self, feature_name: str, result: Dict) -> None:
        """检查特征名称"""
        checks = []
        
        # 检查是否为标准特征
        if feature_name.lower() in self.STANDARD_FEATURES:
            checks.append(f"标准特征: {feature_name} ✓")
        else:
            result['warnings'].append(f"非标准特征名称: {feature_name}")
            
        # 检查命名规范
        if re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', feature_name):
            checks.append(f"命名规范: ✓")
        else:
            result['errors'].append(f"特征名称不符合命名规范: {feature_name}")
            
        result['checks']['feature_name'] = checks
        
    def _check_data_completeness(self, data: pd.DataFrame, result: Dict) -> None:
        """检查数据完整性"""
        checks = []
        
        # 检查总体缺失值比例
        total_cells = data.size
        missing_cells = data.isnull().sum().sum()
        missing_pct = missing_cells / total_cells * 100
        
        checks.append(f"总体缺失值比例: {missing_pct:.2f}%")
        
        if missing_pct > 30:
            result['warnings'].append(f"数据缺失比例过高: {missing_pct:.2f}%")
            
        # 检查每行的完整性
        row_completeness = (data.notna().sum(axis=1) / len(data.columns) * 100)
        incomplete_rows = (row_completeness < 50).sum()
        
        if incomplete_rows > 0:
            incomplete_pct = incomplete_rows / len(data) * 100
            result['warnings'].append(f"不完整行数: {incomplete_rows} ({incomplete_pct:.1f}%)")
        else:
            checks.append("行完整性: ✓")
            
        result['checks']['data_completeness'] = checks
        
    def _check_data_consistency(self, data: pd.DataFrame, result: Dict) -> None:
        """检查数据一致性"""
        checks = []
        
        # 检查日期连续性
        try:
            date_col = data.iloc[:, 0]
            dates = pd.to_datetime(date_col)
            
            # 检查是否有重复日期
            duplicate_dates = dates.duplicated().sum()
            if duplicate_dates > 0:
                result['warnings'].append(f"重复日期: {duplicate_dates}个")
            else:
                checks.append("日期唯一性: ✓")
                
            # 检查日期间隔
            if len(dates) > 1:
                date_diffs = dates.diff().dropna()
                most_common_diff = date_diffs.mode()
                if len(most_common_diff) > 0:
                    checks.append(f"主要日期间隔: {most_common_diff.iloc[0]}")
                    
        except Exception as e:
            result['warnings'].append(f"日期一致性检查失败: {str(e)}")
            
        # 检查数据分布一致性
        numeric_cols = data.select_dtypes(include=[np.number]).columns[1:]  # 排除日期列
        
        for col in numeric_cols[:5]:  # 只检查前5列以避免过多输出
            col_data = data[col].dropna()
            if len(col_data) > 0:
                # 检查异常值
                q1, q3 = col_data.quantile([0.25, 0.75])
                iqr = q3 - q1
                outliers = ((col_data < q1 - 1.5 * iqr) | (col_data > q3 + 1.5 * iqr)).sum()
                outlier_pct = outliers / len(col_data) * 100
                
                if outlier_pct > 5:
                    result['warnings'].append(f"列 {col} 异常值比例: {outlier_pct:.1f}%")
                    
        result['checks']['data_consistency'] = checks
        
    def _is_valid_symbol(self, symbol: str) -> bool:
        """检查股票代码格式是否有效"""
        if not isinstance(symbol, str):
            return False
            
        # 检查是否匹配任一格式
        for regex in self.symbol_regex:
            if regex.match(symbol):
                return True
                
        return False
        
    def generate_validation_report(self, validation_result: Dict) -> str:
        """生成验证报告
        
        Args:
            validation_result: 验证结果
            
        Returns:
            格式化的验证报告
        """
        report_lines = []
        
        # 标题
        feature_name = validation_result.get('feature_name', '未知特征')
        report_lines.append(f"=== Qlib格式验证报告: {feature_name} ===")
        report_lines.append("")
        
        # 基本信息
        shape = validation_result.get('data_shape', (0, 0))
        report_lines.append(f"数据维度: {shape[0]}行 × {shape[1]}列")
        
        # 验证结果
        is_valid = validation_result.get('is_valid', False)
        status = "✓ 通过" if is_valid else "✗ 失败"
        report_lines.append(f"验证状态: {status}")
        report_lines.append("")
        
        # 详细检查结果
        checks = validation_result.get('checks', {})
        for check_name, check_results in checks.items():
            report_lines.append(f"【{check_name.replace('_', ' ').title()}】")
            for result in check_results:
                report_lines.append(f"  {result}")
            report_lines.append("")
            
        # 错误信息
        errors = validation_result.get('errors', [])
        if errors:
            report_lines.append("【错误】")
            for error in errors:
                report_lines.append(f"  ✗ {error}")
            report_lines.append("")
            
        # 警告信息
        warnings = validation_result.get('warnings', [])
        if warnings:
            report_lines.append("【警告】")
            for warning in warnings:
                report_lines.append(f"  ⚠ {warning}")
            report_lines.append("")
            
        return "\n".join(report_lines)
        
    def validate_qlib_dataset(self, feature_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """验证整个Qlib数据集
        
        Args:
            feature_data: 按特征分组的数据字典
            
        Returns:
            每个特征的验证结果
        """
        dataset_validation = {}
        
        for feature_name, data in feature_data.items():
            logger.info(f"验证特征: {feature_name}")
            validation_result = self.validate_qlib_format(data, feature_name)
            dataset_validation[feature_name] = validation_result
            
        return dataset_validation