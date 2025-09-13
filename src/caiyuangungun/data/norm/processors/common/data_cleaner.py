import pandas as pd
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    数据清洗器，用于处理合并后的数据
    功能：
    1. 根据BSE映射文件替换ts_code
    2. 将ts_code格式化为qlib标准格式（如000001SH）
    3. 调整字段命名和格式以符合qlib标准
    """
    
    def __init__(self, bse_mapping_path: str = None):
        """
        初始化数据清洗器
        
        Args:
            bse_mapping_path: BSE映射文件路径
        """
        self.bse_mapping_path = bse_mapping_path or '/Users/daishun/个人文档/caiyuangungun/data/raw/landing/manual/bse_mapping/bse_mapping.parquet'
        self.bse_mapping = None
        self._load_bse_mapping()
    
    def _load_bse_mapping(self):
        """
        加载BSE映射文件
        """
        try:
            if Path(self.bse_mapping_path).exists():
                self.bse_mapping = pd.read_parquet(self.bse_mapping_path)
                # 创建映射字典：旧代码_full -> 新代码_full
                self.ts_code_mapping = dict(zip(
                    self.bse_mapping['旧代码_full'], 
                    self.bse_mapping['新代码_full']
                ))
                logger.info(f"加载BSE映射文件成功，包含{len(self.ts_code_mapping)}条映射记录")
            else:
                logger.warning(f"BSE映射文件不存在: {self.bse_mapping_path}")
                self.ts_code_mapping = {}
        except Exception as e:
            logger.error(f"加载BSE映射文件失败: {e}")
            self.ts_code_mapping = {}
    
    def _apply_bse_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用BSE映射替换ts_code
        
        Args:
            df: 输入数据框
            
        Returns:
            替换后的数据框
        """
        if 'ts_code' not in df.columns or not self.ts_code_mapping:
            return df
        
        # 记录替换前的数量
        original_count = len(df)
        
        # 应用映射
        df = df.copy()
        df['ts_code'] = df['ts_code'].map(self.ts_code_mapping).fillna(df['ts_code'])
        
        # 统计替换数量
        replaced_count = sum(df['ts_code'].isin(self.ts_code_mapping.values()))
        logger.info(f"BSE映射替换完成，共替换{replaced_count}条记录")
        
        return df
    
    def _format_ts_code_to_qlib(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将ts_code格式化为qlib标准格式
        
        Args:
            df: 输入数据框
            
        Returns:
            格式化后的数据框
        """
        if 'ts_code' not in df.columns:
            return df
        
        df = df.copy()
        
        def convert_ts_code(ts_code):
            """
            转换ts_code格式
            从 '000001.SZ' 转换为 '000001SZ'
            从 '600000.SH' 转换为 '600000SH'
            """
            if pd.isna(ts_code) or not isinstance(ts_code, str):
                return ts_code
            
            # 移除点号
            return ts_code.replace('.', '')
        
        df['ts_code'] = df['ts_code'].apply(convert_ts_code)
        logger.info("ts_code格式转换完成")
        
        return df
    
    def _standardize_columns(self, df: pd.DataFrame, data_type: str = 'daily') -> pd.DataFrame:
        """
        标准化列名和格式以符合qlib标准
        
        Args:
            df: 输入数据框
            data_type: 数据类型 ('daily', 'stock_basic', 'income_y'等)
            
        Returns:
            标准化后的数据框
        """
        df = df.copy()
        
        # 通用列名映射
        common_column_mapping = {
            'ts_code': 'instrument',  # qlib标准字段名
        }
        
        # 根据数据类型定义特定的列名映射
        if data_type == 'daily':
            column_mapping = {
                **common_column_mapping,
                'trade_date': 'datetime',
                'pre_close': 'prev_close',
                'vol': 'volume',
                'amount': 'money',
                'pct_chg': 'pct_change'
                # 保持原有列名：open, high, low, close, change
            }
        elif data_type == 'stock_basic':
            column_mapping = {
                **common_column_mapping,
                'symbol': 'symbol',
                'name': 'name',
                'area': 'area',
                'industry': 'industry',
                'market': 'market',
                'list_date': 'list_date'
            }
        else:
            # 其他类型使用通用映射
            column_mapping = common_column_mapping
        
        # 重命名列
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_columns)
        
        # 格式化日期字段
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d', errors='coerce')
        
        if 'list_date' in df.columns:
            df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
        
        logger.info(f"列标准化完成，重命名了{len(existing_columns)}个字段")
        
        return df
    
    def clean_data(self, df: pd.DataFrame, data_type: str = 'daily') -> pd.DataFrame:
        """
        执行完整的数据清洗流程
        
        Args:
            df: 输入数据框
            data_type: 数据类型
            
        Returns:
            清洗后的数据框
        """
        logger.info(f"开始清洗{data_type}数据，输入数据形状: {df.shape}")
        
        # 1. 应用BSE映射
        df = self._apply_bse_mapping(df)
        
        # 2. 格式化ts_code
        df = self._format_ts_code_to_qlib(df)
        
        # 3. 标准化列名
        df = self._standardize_columns(df, data_type)
        
        logger.info(f"数据清洗完成，输出数据形状: {df.shape}")
        
        return df


def create_data_cleaner(bse_mapping_path: str = None) -> DataCleaner:
    """
    创建数据清洗器实例
    
    Args:
        bse_mapping_path: BSE映射文件路径
        
    Returns:
        DataCleaner实例
    """
    return DataCleaner(bse_mapping_path)