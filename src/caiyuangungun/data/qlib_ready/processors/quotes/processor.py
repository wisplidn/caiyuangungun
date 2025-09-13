"""QLIB-READY层数据处理器

负责具体的数据处理逻辑，包括数据提取、连接、清洗和格式转换。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

from ...core.base_processor import BaseQlibProcessor

logger = logging.getLogger(__name__)


class QlibDataProcessor(BaseQlibProcessor):
    """Qlib数据处理器
    
    负责处理从NORM层到QLIB-READY层的数据转换，包括：
    - 日频行情数据提取
    - 复权因子左连接
    - 基础信息匹配
    - 数据清洗和异常值处理
    - 按特征分组
    """
    
    # Qlib标准特征列名映射
    QLIB_FEATURE_MAPPING = {
        'open': '$open',
        'high': '$high', 
        'low': '$low',
        'close': '$close',
        'volume': '$volume',
        'amount': '$amount',
        'vwap': '$vwap',
        'adj_factor': '$factor',
        'change': '$change',
        'pct_chg': '$pct_chg'
    }
    
    # 需要剔除的异常标记
    EXCLUDE_REMARKS = {
        '异常', '停牌', '退市', '暂停上市', '计划剔除', 
        'ST', '*ST', 'PT', '风险警示'
    }
    
    def __init__(self):
        """初始化数据处理器"""
        pass
        
    def process_quotes_data(self, 
                           daily_quotes: pd.DataFrame,
                           adj_factors: pd.DataFrame,
                           basic_info: pd.DataFrame) -> pd.DataFrame:
        """处理日频行情数据
        
        Args:
            daily_quotes: 日频行情数据
            adj_factors: 复权因子数据
            basic_info: 基础信息数据
            
        Returns:
            处理后的完整数据
        """
        logger.info("开始处理日频行情数据")
        
        # 1. 数据预处理
        quotes_clean = self._preprocess_quotes(daily_quotes)
        adj_clean = self._preprocess_adj_factors(adj_factors)
        basic_clean = self._preprocess_basic_info(basic_info)
        
        # 2. 左连接复权因子
        quotes_with_adj = self._merge_adj_factors(quotes_clean, adj_clean)
        
        # 3. 左连接基础信息
        quotes_with_basic = self._merge_basic_info(quotes_with_adj, basic_clean)
        
        # 4. 数据清洗
        cleaned_data = self._clean_data(quotes_with_basic)
        
        # 5. 计算衍生特征
        enriched_data = self._calculate_derived_features(cleaned_data)
        
        # 6. 标准化列名
        standardized_data = self._standardize_columns(enriched_data)
        
        logger.info(f"数据处理完成，共{len(standardized_data)}行")
        return standardized_data
        
    def _preprocess_quotes(self, daily_quotes: pd.DataFrame) -> pd.DataFrame:
        """预处理日频行情数据"""
        df = daily_quotes.copy()
        
        # 确保必要列存在，支持多种股票代码列名
        symbol_cols = ['symbol', 'ts_code', 'code', 'stock_code']
        symbol_col = None
        for col in symbol_cols:
            if col in df.columns:
                symbol_col = col
                break
        
        if symbol_col is None:
            raise ValueError(f"日频行情数据缺少股票代码列，支持的列名: {symbol_cols}")
        
        # 统一股票代码列名为'symbol'
        if symbol_col != 'symbol':
            df = df.rename(columns={symbol_col: 'symbol'})
        
        required_cols = ['symbol', 'trade_date', 'open', 'high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"日频行情数据缺少必要列: {missing_cols}")
        
        # 检查成交量列，支持多种列名
        volume_cols = ['volume', 'vol', 'trade_vol']
        volume_col = None
        for col in volume_cols:
            if col in df.columns:
                volume_col = col
                break
        
        if volume_col is None:
            raise ValueError(f"日频行情数据缺少成交量列，支持的列名: {volume_cols}")
        
        # 统一成交量列名为'volume'
        if volume_col != 'volume':
            df = df.rename(columns={volume_col: 'volume'})
            
        # 转换数据类型
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 数值列转换
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # 排序
        df = df.sort_values(['symbol', 'trade_date']).reset_index(drop=True)
        
        return df
        
    def _preprocess_adj_factors(self, adj_factors: pd.DataFrame) -> pd.DataFrame:
        """预处理复权因子数据"""
        if adj_factors.empty:
            logger.warning("复权因子数据为空")
            return adj_factors
            
        df = adj_factors.copy()
        
        # 确保必要列存在，支持多种股票代码列名
        symbol_cols = ['symbol', 'ts_code', 'code', 'stock_code']
        symbol_col = None
        for col in symbol_cols:
            if col in df.columns:
                symbol_col = col
                break
        
        if symbol_col is None:
            raise ValueError(f"复权因子数据缺少股票代码列，支持的列名: {symbol_cols}")
        
        # 统一股票代码列名为'symbol'
        if symbol_col != 'symbol':
            df = df.rename(columns={symbol_col: 'symbol'})
        
        required_cols = ['symbol', 'trade_date', 'adj_factor']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"复权因子数据缺少必要列: {missing_cols}")
            
        # 转换数据类型
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['adj_factor'] = pd.to_numeric(df['adj_factor'], errors='coerce')
        
        # 去重，保留最新的复权因子
        df = df.drop_duplicates(['symbol', 'trade_date'], keep='last')
        
        return df
        
    def _preprocess_basic_info(self, basic_info: pd.DataFrame) -> pd.DataFrame:
        """预处理基础信息数据"""
        if basic_info.empty:
            logger.warning("基础信息数据为空")
            return basic_info
            
        df = basic_info.copy()
        
        # 确保必要列存在，支持多种股票代码列名
        symbol_cols = ['symbol', 'ts_code', 'code', 'stock_code']
        symbol_col = None
        for col in symbol_cols:
            if col in df.columns:
                symbol_col = col
                break
        
        if symbol_col is None:
            raise ValueError(f"基础信息数据缺少股票代码列，支持的列名: {symbol_cols}")
        
        # 统一股票代码列名为'symbol'
        if symbol_col != 'symbol':
            df = df.rename(columns={symbol_col: 'symbol'})
            
        # 去重
        df = df.drop_duplicates('symbol', keep='last')
        
        return df
        
    def _merge_adj_factors(self, 
                          quotes: pd.DataFrame, 
                          adj_factors: pd.DataFrame) -> pd.DataFrame:
        """左连接复权因子"""
        if adj_factors.empty:
            logger.warning("复权因子数据为空，跳过连接")
            quotes['adj_factor'] = 1.0  # 默认复权因子为1
            return quotes
            
        # 左连接
        merged = quotes.merge(
            adj_factors[['symbol', 'trade_date', 'adj_factor']], 
            on=['symbol', 'trade_date'], 
            how='left'
        )
        
        # 填充缺失的复权因子
        merged['adj_factor'] = merged['adj_factor'].fillna(1.0)
        
        logger.info(f"复权因子连接完成，匹配率: {(merged['adj_factor'] != 1.0).mean():.2%}")
        return merged
        
    def _merge_basic_info(self, 
                         quotes: pd.DataFrame, 
                         basic_info: pd.DataFrame) -> pd.DataFrame:
        """左连接基础信息"""
        if basic_info.empty:
            logger.warning("基础信息数据为空，跳过连接")
            return quotes
            
        # 选择需要的基础信息列
        basic_cols = ['symbol']
        optional_cols = ['name', 'industry', 'market', 'list_date', 'delist_date', 'status']
        
        for col in optional_cols:
            if col in basic_info.columns:
                basic_cols.append(col)
                
        # 左连接
        merged = quotes.merge(
            basic_info[basic_cols],
            on='symbol',
            how='left'
        )
        
        logger.info(f"基础信息连接完成")
        return merged
        
    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """数据清洗"""
        df = data.copy()
        initial_rows = len(df)
        
        # 1. 删除价格为0或负数的记录
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df = df[df[col] > 0]
                
        # 2. 删除成交量为负数的记录
        if 'volume' in df.columns:
            df = df[df['volume'] >= 0]
            
        # 3. 删除异常标记的记录
        if 'status' in df.columns:
            df = df[~df['status'].isin(self.EXCLUDE_REMARKS)]
            
        # 4. 删除备注包含异常标记的记录
        if 'remarks' in df.columns:
            mask = df['remarks'].fillna('').str.contains('|'.join(self.EXCLUDE_REMARKS), na=False)
            df = df[~mask]
            
        # 5. 删除价格异常的记录（如high < low）
        if all(col in df.columns for col in ['high', 'low']):
            df = df[df['high'] >= df['low']]
            
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            # 开盘价应该在最高最低价之间
            df = df[(df['open'] >= df['low']) & (df['open'] <= df['high'])]
            # 收盘价应该在最高最低价之间
            df = df[(df['close'] >= df['low']) & (df['close'] <= df['high'])]
            
        cleaned_rows = len(df)
        removed_rows = initial_rows - cleaned_rows
        
        logger.info(f"数据清洗完成，删除{removed_rows}行异常数据，剩余{cleaned_rows}行")
        return df.reset_index(drop=True)
        
    def _calculate_derived_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算衍生特征"""
        df = data.copy()
        
        # 计算VWAP（成交量加权平均价格）
        if all(col in df.columns for col in ['amount', 'volume']):
            df['vwap'] = np.where(
                df['volume'] > 0,
                df['amount'] / df['volume'],
                df['close']
            )
            
        # 计算涨跌幅
        if 'close' in df.columns:
            df = df.sort_values(['symbol', 'trade_date'])
            df['prev_close'] = df.groupby('symbol')['close'].shift(1)
            
            # 计算涨跌额
            df['change'] = df['close'] - df['prev_close']
            
            # 计算涨跌幅
            df['pct_chg'] = np.where(
                df['prev_close'] > 0,
                (df['close'] - df['prev_close']) / df['prev_close'] * 100,
                0
            )
            
        # 应用复权因子
        if 'adj_factor' in df.columns:
            price_cols = ['open', 'high', 'low', 'close', 'vwap']
            for col in price_cols:
                if col in df.columns:
                    df[f'{col}_adj'] = df[col] * df['adj_factor']
                    
        return df
        
    def _standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """标准化列名为Qlib格式"""
        df = data.copy()
        
        # 重命名列
        rename_map = {}
        for old_name, new_name in self.QLIB_FEATURE_MAPPING.items():
            if old_name in df.columns:
                rename_map[old_name] = new_name
                
        df = df.rename(columns=rename_map)
        
        # 确保必要的列存在
        required_cols = ['symbol', 'trade_date']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"缺少必要列: {col}")
                
        return df
        
    def group_by_symbols(self, data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """按股票代码分组数据
        
        将数据按股票代码分组，每个股票生成一个包含所有特征的DataFrame，
        符合qlib标准格式：每行一个交易日，包含该股票的所有特征。
        
        Args:
            data: 完整的数据
            
        Returns:
            按股票代码分组的数据字典
        """
        symbol_data = {}
        
        # 获取所有股票代码
        symbols = data['symbol'].unique()
        
        for symbol in symbols:
            # 筛选该股票的数据
            symbol_df = data[data['symbol'] == symbol].copy()
            
            # 按交易日期排序
            if 'trade_date' in symbol_df.columns:
                symbol_df = symbol_df.sort_values('trade_date')
            
            # 重置索引
            symbol_df = symbol_df.reset_index(drop=True)
            
            # 确保包含必要的列
            required_cols = ['trade_date', 'symbol']
            feature_cols = [col for col in symbol_df.columns if col.startswith('$')]
            
            # 重新排列列顺序：日期、股票代码、所有特征
            final_cols = required_cols + feature_cols
            available_cols = [col for col in final_cols if col in symbol_df.columns]
            
            symbol_df = symbol_df[available_cols]
            
            symbol_data[symbol] = symbol_df
                
        logger.info(f"按股票代码分组完成，共生成{len(symbol_data)}个股票文件")
        return symbol_data
        
    def validate_data_quality(self, data: pd.DataFrame) -> Dict[str, any]:
        """验证数据质量
        
        Args:
            data: 要验证的数据
            
        Returns:
            质量检查结果
        """
        quality_report = {
            'total_rows': len(data),
            'total_symbols': data['symbol'].nunique() if 'symbol' in data.columns else 0,
            'date_range': {
                'start': data['trade_date'].min() if 'trade_date' in data.columns else None,
                'end': data['trade_date'].max() if 'trade_date' in data.columns else None
            },
            'missing_values': {},
            'data_types': {},
            'anomalies': []
        }
        
        # 检查缺失值
        for col in data.columns:
            missing_count = data[col].isnull().sum()
            missing_pct = missing_count / len(data) * 100
            quality_report['missing_values'][col] = {
                'count': int(missing_count),
                'percentage': round(missing_pct, 2)
            }
            
        # 检查数据类型
        for col in data.columns:
            quality_report['data_types'][col] = str(data[col].dtype)
            
        # 检查异常值
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col.startswith('$'):  # 只检查特征列
                # 检查负值（价格和成交量不应为负）
                negative_count = (data[col] < 0).sum()
                if negative_count > 0:
                    quality_report['anomalies'].append({
                        'type': 'negative_values',
                        'column': col,
                        'count': int(negative_count)
                    })
                    
                # 检查极值
                q99 = data[col].quantile(0.99)
                q01 = data[col].quantile(0.01)
                outliers = ((data[col] > q99 * 10) | (data[col] < q01 / 10)).sum()
                if outliers > 0:
                    quality_report['anomalies'].append({
                        'type': 'extreme_values', 
                        'column': col,
                        'count': int(outliers)
                    })
                    
        return quality_report