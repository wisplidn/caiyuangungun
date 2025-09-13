"""QLIB-READY层基础处理器抽象类

定义所有数据域处理器的通用接口和基础功能。
"""

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseQlibProcessor(ABC):
    """Qlib数据处理器基类
    
    定义所有数据域处理器的通用接口，包括：
    - 数据提取和连接
    - 数据清洗和异常值处理
    - 格式转换和验证
    - 特征映射和分组
    """
    
    # 需要剔除的异常标记（所有处理器通用）
    EXCLUDE_REMARKS = {
        '异常', '停牌', '退市', '暂停上市', '计划剔除', 
        'ST', '*ST', 'PT', '风险警示'
    }
    
    def __init__(self):
        """初始化基础处理器"""
        pass
    
    @property
    @abstractmethod
    def feature_mapping(self) -> Dict[str, str]:
        """获取特征列名映射
        
        Returns:
            Dict[str, str]: 原始列名到Qlib标准列名的映射
        """
        pass
    
    @property
    @abstractmethod
    def required_columns(self) -> List[str]:
        """获取必需的数据列
        
        Returns:
            List[str]: 必需的数据列名列表
        """
        pass
    
    @abstractmethod
    def process_data(self, 
                    data: pd.DataFrame,
                    **kwargs) -> pd.DataFrame:
        """处理数据的核心方法
        
        Args:
            data: 输入数据
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        pass
    
    def validate_input_data(self, data: pd.DataFrame) -> bool:
        """验证输入数据格式
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 验证是否通过
        """
        if data.empty:
            logger.warning("输入数据为空")
            return False
            
        missing_cols = set(self.required_columns) - set(data.columns)
        if missing_cols:
            logger.error(f"缺少必需列: {missing_cols}")
            return False
            
        return True
    
    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """通用数据清洗逻辑
        
        Args:
            data: 输入数据
            
        Returns:
            pd.DataFrame: 清洗后的数据
        """
        logger.info(f"开始数据清洗，原始数据行数: {len(data)}")
        
        # 删除包含异常标记的记录
        if 'remark' in data.columns:
            before_count = len(data)
            data = data[~data['remark'].isin(self.EXCLUDE_REMARKS)]
            after_count = len(data)
            logger.info(f"删除异常标记记录: {before_count - after_count}条")
        
        # 删除重复记录
        before_count = len(data)
        data = data.drop_duplicates()
        after_count = len(data)
        if before_count != after_count:
            logger.info(f"删除重复记录: {before_count - after_count}条")
        
        # 删除全为空值的行
        before_count = len(data)
        data = data.dropna(how='all')
        after_count = len(data)
        if before_count != after_count:
            logger.info(f"删除全空行: {before_count - after_count}条")
        
        logger.info(f"数据清洗完成，最终数据行数: {len(data)}")
        return data
    
    def apply_feature_mapping(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用特征列名映射
        
        Args:
            data: 输入数据
            
        Returns:
            pd.DataFrame: 映射后的数据
        """
        # 只重命名存在的列
        existing_mapping = {
            old_name: new_name 
            for old_name, new_name in self.feature_mapping.items() 
            if old_name in data.columns
        }
        
        if existing_mapping:
            data = data.rename(columns=existing_mapping)
            logger.info(f"应用特征映射: {existing_mapping}")
        
        return data
    
    def remove_outliers(self, 
                       data: pd.DataFrame, 
                       columns: List[str] = None,
                       method: str = 'iqr',
                       threshold: float = 1.5) -> pd.DataFrame:
        """移除异常值
        
        Args:
            data: 输入数据
            columns: 需要处理的列名列表，None表示处理所有数值列
            method: 异常值检测方法 ('iqr', 'zscore')
            threshold: 异常值阈值
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if columns is None:
            columns = data.select_dtypes(include=[np.number]).columns.tolist()
        
        original_count = len(data)
        
        for col in columns:
            if col not in data.columns:
                continue
                
            if method == 'iqr':
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                data = data[(data[col] >= lower_bound) & (data[col] <= upper_bound)]
            
            elif method == 'zscore':
                z_scores = np.abs((data[col] - data[col].mean()) / data[col].std())
                data = data[z_scores <= threshold]
        
        removed_count = original_count - len(data)
        if removed_count > 0:
            logger.info(f"移除异常值: {removed_count}条记录")
        
        return data
    
    def group_by_feature(self, data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """按特征分组数据
        
        Args:
            data: 输入数据
            
        Returns:
            Dict[str, pd.DataFrame]: 按特征分组的数据字典
        """
        feature_groups = {}
        
        # 获取所有特征列（以$开头的列）
        feature_columns = [col for col in data.columns if col.startswith('$')]
        
        # 基础列（symbol, datetime等）
        base_columns = [col for col in data.columns if not col.startswith('$')]
        
        for feature_col in feature_columns:
            feature_name = feature_col[1:]  # 去掉$前缀
            feature_data = data[base_columns + [feature_col]].copy()
            feature_data = feature_data.rename(columns={feature_col: feature_name})
            feature_groups[feature_name] = feature_data
        
        logger.info(f"按特征分组完成，共{len(feature_groups)}个特征组")
        return feature_groups