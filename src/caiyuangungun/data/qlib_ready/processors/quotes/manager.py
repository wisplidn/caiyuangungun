"""QLIB-READY层数据管理器

负责管理QLIB-READY层的数据存储和访问，生成符合Qlib格式的数据文件。
"""

import os
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime, date
import json

from ...base import BaseDataManager
from ...contracts import DataLayer, InterfaceType, DataContract
from ...core.base_manager import BaseQlibManager
from ...core.validator import QlibFormatValidator
from .processor import QlibDataProcessor


class QlibReadyDataManager(BaseDataManager, BaseQlibManager):
    """QLIB-READY层数据管理器
    
    负责从NORM层数据生成符合Qlib格式要求的数据文件，包括：
    - 提取daily_quotes数据
    - 与复权因子和基础数据进行左连接
    - 数据清洗和异常值处理
    - 按feature分组生成CSV文件
    """
    
    def __init__(self, data_root: str):
        """初始化QLIB-READY层数据管理器
        
        Args:
            data_root: 数据根目录路径
        """
        super().__init__(data_root)
        self.data_layer = "qlib-ready"  # 添加data_layer属性
        self.qlib_ready_path = Path(data_root) / "qlib-ready"
        self.processor = QlibDataProcessor()
        self.validator = QlibFormatValidator()
        
    def get_data_path(self, 
                      symbol: str,
                      interface_type: InterfaceType = None,
                      **kwargs) -> str:
        """
        获取QLIB-READY层数据路径（按股票代码分组）
        
        Args:
            symbol: 股票代码（如 000001.SZ）
            interface_type: 数据接口类型（可选，用于子目录分类）
            **kwargs: 额外参数
            
        Returns:
            数据文件路径
        """
        if interface_type:
            interface_value = interface_type.value if hasattr(interface_type, 'value') else str(interface_type)
            base_path = os.path.join(str(self.qlib_ready_path), interface_value)
        else:
            base_path = str(self.qlib_ready_path)
        
        # 返回按股票代码命名的CSV文件路径
        return os.path.join(base_path, f"{symbol}.csv")
        
    def save_data(self, 
                  data: pd.DataFrame,
                  symbol: str,
                  interface_type: InterfaceType = None,
                  **kwargs) -> str:
        """
        保存QLIB-READY层数据（按股票代码分组）
        
        Args:
            data: 要保存的数据（包含该股票的所有特征）
            symbol: 股票代码（如 000001.SZ）
            interface_type: 数据接口类型（可选，用于子目录分类）
            **kwargs: 额外参数
            
        Returns:
            保存的文件路径
        """
        
        # 获取保存路径
        file_path = self.get_data_path(symbol, interface_type)
            
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 确保数据格式符合qlib要求：包含日期列和股票代码列
        if 'symbol' not in data.columns:
            data = data.copy()
            data['symbol'] = symbol
            
        # 确保日期列存在且格式正确
        date_cols = ['date', 'trade_date', 'datetime']
        date_col = None
        for col in date_cols:
            if col in data.columns:
                date_col = col
                break
                
        if date_col:
            # 确保日期列在第一列
            cols = [date_col] + [col for col in data.columns if col != date_col]
            data = data[cols]
            # 按日期排序
            data = data.sort_values(date_col)
        
        # 保存为CSV格式
        data.to_csv(file_path, index=False)
        
        # 元数据信息已集成到CSV文件中，不再单独保存JSON文件
        
        return file_path
        

        
    def load_data(self, 
                  symbol: str,
                  interface_type: InterfaceType = None,
                  **kwargs) -> pd.DataFrame:
        """
        加载QLIB-READY层数据（按股票代码加载）
        
        Args:
            symbol: 股票代码（如 000001.SZ）
            interface_type: 数据接口类型（可选）
            **kwargs: 额外参数
            
        Returns:
            加载的数据
        """
        file_path = self.get_data_path(symbol, interface_type)
            
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")
            
        return pd.read_csv(file_path)
        
    def process_daily_quotes(self, 
                           start_date: Union[str, date], 
                           end_date: Union[str, date],
                           symbols: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """处理日频行情数据，生成符合Qlib格式的特征文件
        
        Args:
            start_date: 开始日期
            end_date: 结束日期 
            symbols: 股票代码列表，为None时处理所有股票
            
        Returns:
            按特征名称分组的数据字典
        """
        # 从NORM层加载数据
        daily_quotes = self._load_norm_data('daily_quotes', start_date, end_date, symbols)
        adj_factors = self._load_norm_data('adj_factors', start_date, end_date, symbols)
        basic_info = self._load_norm_data('basic_info', symbols=symbols)
        
        # 数据处理和连接
        processed_data = self.processor.process_quotes_data(
            daily_quotes, adj_factors, basic_info
        )
        
        # 按特征分组
        feature_data = self.processor.split_by_features(processed_data)
        
        return feature_data
        
    def generate_qlib_data(self, 
                          start_date: Union[str, date],
                          end_date: Union[str, date], 
                          symbols: Optional[List[str]] = None,
                          save_files: bool = True) -> Dict[str, pd.DataFrame]:
        """生成完整的Qlib格式数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            symbols: 股票代码列表
            save_files: 是否保存文件
            
        Returns:
            按股票代码分组的数据字典
        """
        # 加载NORM层数据
        daily_quotes = self._load_norm_data('daily_quotes', start_date, end_date, symbols)
        adj_factors = self._load_norm_data('adj_factors', start_date, end_date, symbols)
        basic_info = self._load_norm_data('basic_info', start_date, end_date, symbols)
        
        # 处理数据
        processed_data = self.processor.process_quotes_data(
            daily_quotes, adj_factors, basic_info
        )
        
        # 按股票代码分组
        symbol_data = self.processor.group_by_symbols(processed_data)
        
        if save_files:
            # 保存各个股票的数据文件
            for symbol, data in symbol_data.items():
                self.save_data(
                    data, 
                    symbol,
                    InterfaceType.QUOTES_DAILY
                )
                
        return symbol_data
        
    def _load_norm_data(self, 
                       data_type: str, 
                       start_date: Optional[Union[str, date]] = None,
                       end_date: Optional[Union[str, date]] = None,
                       symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """从NORM层加载数据
        
        Args:
            data_type: 数据类型 (daily_quotes, adj_factors, basic_info)
            start_date: 开始日期
            end_date: 结束日期
            symbols: 股票代码列表
            
        Returns:
            加载的数据
        """
        # TODO: 实现从NORM层加载数据的逻辑
        # 这里需要根据实际的NORM层数据存储格式来实现
        norm_data_path = os.path.join(
            self.data_root, 
            DataLayer.NORM.value, 
            f"{data_type}.parquet"
        )
        
        if not os.path.exists(norm_data_path):
            raise FileNotFoundError(f"NORM层数据文件不存在: {norm_data_path}")
            
        data = pd.read_parquet(norm_data_path)
        
        # 应用日期和股票代码过滤
        if start_date and 'trade_date' in data.columns:
            data = data[data['trade_date'] >= pd.to_datetime(start_date)]
        if end_date and 'trade_date' in data.columns:
            data = data[data['trade_date'] <= pd.to_datetime(end_date)]
        if symbols and 'symbol' in data.columns:
            data = data[data['symbol'].isin(symbols)]
            
        return data
    
    def list_available_symbols(self, interface_type: InterfaceType) -> List[str]:
        """列出可用的股票代码
        
        Args:
            interface_type: 接口类型
            
        Returns:
            可用的股票代码列表
        """
        qlib_ready_dir = os.path.join(self.data_root, DataLayer.QLIB_READY.value)
        interface_dir = os.path.join(qlib_ready_dir, interface_type.value)
        
        if not os.path.exists(interface_dir):
            return []
        
        symbols = []
        for filename in os.listdir(interface_dir):
            if filename.endswith('.csv'):
                symbol = filename.replace('.csv', '')
                symbols.append(symbol)
        
        return sorted(symbols)