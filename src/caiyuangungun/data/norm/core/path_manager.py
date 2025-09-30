"""
路径管理器模块
用于管理数据文件路径，提供文件路径生成和查找功能
基于实际文件系统结构动态识别路径，而不依赖配置文件构造
"""

import os
import glob
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path


class PathManager:
    """路径管理器类"""
    
    def __init__(self, base_path: str, supported_formats: List[str] = None, default_format: str = "parquet"):
        """
        初始化路径管理器
        
        Args:
            base_path: 数据文件的基础路径
            supported_formats: 支持的文件格式列表，默认为["parquet", "json"]
            default_format: 默认文件格式，默认为"parquet"
        """
        self.base_path = base_path
        self.supported_formats = supported_formats or ["parquet", "json"]
        self.default_format = default_format
        
        # 日期模式识别正则表达式（调整优先级，避免股票代码被误识别为日期）
        self.date_patterns = {
            'daily': re.compile(r'(\d{6})/(\d{2})', re.IGNORECASE),    # YYYYMM/DD
            'monthly': re.compile(r'(\d{6})(?:/|$)', re.IGNORECASE),   # YYYYMM
            'quarterly': re.compile(r'(\d{4}Q[1-4])(?:/|$)', re.IGNORECASE), # YYYYQ1-Q4
            'yearly': re.compile(r'(\d{4})(?:/|$)', re.IGNORECASE),    # YYYY
        }
    
    def get_method_file_paths(self, method_name: str) -> List[Tuple[str, str]]:
        """
        基于实际文件系统结构动态获取method的文件路径对
        不再依赖配置文件构造，而是扫描实际存在的目录结构
        
        Args:
            method_name: method名称
            
        Returns:
            文件路径对列表，每个元素为(parquet_path, json_path)的元组
        """
        # 构建基础搜索路径：base_path/landing/*/method_name
        # 扫描所有可能的source目录
        landing_path = os.path.join(self.base_path, "landing")
        if not os.path.exists(landing_path):
            return []
        
        file_pairs = []
        
        # 遍历landing下的所有source目录
        for source_name in os.listdir(landing_path):
            source_path = os.path.join(landing_path, source_name)
            if not os.path.isdir(source_path):
                continue
                
            method_path = os.path.join(source_path, method_name)
            if not os.path.exists(method_path):
                continue
            
            # 递归扫描method目录下的所有子目录，寻找数据文件
            for root, dirs, files in os.walk(method_path):
                # 检查是否同时存在parquet和json文件
                parquet_files = [f for f in files if f.endswith('.parquet')]
                json_files = [f for f in files if f.endswith('.json')]
                
                # 匹配同名文件
                for parquet_file in parquet_files:
                    base_name = os.path.splitext(parquet_file)[0]
                    json_file = base_name + '.json'
                    
                    if json_file in json_files:
                        parquet_path = os.path.join(root, parquet_file)
                        json_path = os.path.join(root, json_file)
                        file_pairs.append((parquet_path, json_path))
        
        return sorted(file_pairs)
    
    def get_method_file_paths_by_date_range(self, method_name: str, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """
        基于实际文件系统结构和日期范围动态获取文件路径对
        自动识别日期类型并过滤符合条件的文件
        
        Args:
            method_name: method名称
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            文件路径对列表，每个元素为(parquet_path, json_path)的元组
        """
        # 先获取所有文件路径
        all_file_pairs = self.get_method_file_paths(method_name)
        
        if not all_file_pairs:
            return []
        
        # 解析日期范围
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            return []
        
        filtered_pairs = []
        
        for parquet_path, json_path in all_file_pairs:
            # 从路径中提取日期信息
            date_info = self._extract_date_from_path(parquet_path)
            
            if date_info:
                date_type, date_value = date_info
                
                # 根据日期类型判断是否在范围内
                if self._is_date_in_range(date_type, date_value, start_dt, end_dt):
                    filtered_pairs.append((parquet_path, json_path))
        
        return sorted(filtered_pairs)
    
    def _extract_date_from_path(self, file_path: str) -> Optional[Tuple[str, str]]:
        """
        从文件路径中提取日期信息
        
        Args:
            file_path: 文件路径
            
        Returns:
            (日期类型, 日期值) 或 None
        """
        # 获取相对于base_path的路径部分
        try:
            rel_path = os.path.relpath(file_path, self.base_path)
        except ValueError:
            return None
        
        # 尝试匹配各种日期模式
        for date_type, pattern in self.date_patterns.items():
            match = pattern.search(rel_path)
            if match:
                if date_type == 'daily':
                    year_month, day = match.groups()
                    return (date_type, year_month + day)
                elif date_type in ['monthly', 'yearly', 'quarterly']:
                    return (date_type, match.group(1))
        
        return None
    
    def _is_date_in_range(self, date_type: str, date_value: str, start_dt: datetime, end_dt: datetime) -> bool:
        """
        判断日期是否在指定范围内
        
        Args:
            date_type: 日期类型
            date_value: 日期值
            start_dt: 开始日期
            end_dt: 结束日期
            
        Returns:
            是否在范围内
        """
        try:
            if date_type == 'daily':
                # YYYYMMDD格式
                file_dt = datetime.strptime(date_value, "%Y%m%d")
                return start_dt <= file_dt <= end_dt
            
            elif date_type == 'monthly':
                # YYYYMM格式，检查月份是否在范围内
                file_dt = datetime.strptime(date_value + "01", "%Y%m%d")
                # 月末
                if file_dt.month == 12:
                    month_end = file_dt.replace(year=file_dt.year + 1, month=1, day=1) - timedelta(days=1)
                else:
                    month_end = file_dt.replace(month=file_dt.month + 1, day=1) - timedelta(days=1)
                
                return not (month_end < start_dt or file_dt > end_dt)
            
            elif date_type == 'quarterly':
                # YYYYQ1-Q4格式
                year = int(date_value[:4])
                quarter = int(date_value[5])
                
                # 季度开始和结束月份
                quarter_start_month = (quarter - 1) * 3 + 1
                quarter_end_month = quarter * 3
                
                quarter_start = datetime(year, quarter_start_month, 1)
                if quarter_end_month == 12:
                    quarter_end = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    quarter_end = datetime(year, quarter_end_month + 1, 1) - timedelta(days=1)
                
                return not (quarter_end < start_dt or quarter_start > end_dt)
            
            elif date_type == 'yearly':
                # YYYY格式
                year = int(date_value)
                year_start = datetime(year, 1, 1)
                year_end = datetime(year, 12, 31)
                
                return not (year_end < start_dt or year_start > end_dt)
            
            else:
                # 其他类型（如symbol）不进行日期过滤
                return True
                
        except (ValueError, TypeError):
            return False
    
    def get_method_save_path(self, method_name: str) -> str:
        """
        基于base_path构造保存文件路径
        路径格式：base_path的父目录/norm/method_name（移除raw部分）
        
        Args:
            method_name: method名称
            
        Returns:
            保存文件路径
        """
        # 获取base_path的父目录，然后构造norm路径
        parent_path = os.path.dirname(self.base_path)  # 移除raw部分
        norm_data_path = os.path.join(parent_path, "norm", method_name)
        
        return norm_data_path
    
    def get_method_save_file_path(self, method_name: str, file_format: str = None) -> str:
        """
        获取method的完整保存文件路径
        
        Args:
            method_name: method名称
            file_format: 文件格式，如果为None则使用默认格式
            
        Returns:
            完整的保存文件路径
        """
        if file_format is None:
            file_format = self.default_format
        
        save_dir = self.get_method_save_path(method_name)
        filename = f"data.{file_format}"
        
        return os.path.join(save_dir, filename)
    
    def validate_method_paths(self, method_name: str) -> bool:
        """
        验证method的路径配置是否有效
        
        Args:
            method_name: method名称
            
        Returns:
            路径配置是否有效
        """
        method_info = self.config_manager.get_method_info(method_name)
        if not method_info:
            return False
        
        storage_type = method_info.get("storage_type")
        if not storage_type:
            return False
        
        archive_info = self.config_manager.get_archive_type_info(storage_type)
        if not archive_info:
            return False
        
        # 检查基础路径是否存在
        if not os.path.exists(self.base_path):
            return False
        
        return True
    
    def get_available_dates(self, method_name: str) -> List[str]:
        """
        基于实际文件系统结构获取可用日期列表
        自动识别日期类型并提取日期信息
        
        Args:
            method_name: method名称
            
        Returns:
            可用日期列表，格式为YYYYMMDD或其他识别的日期格式
        """
        # 获取所有文件路径
        all_file_pairs = self.get_method_file_paths(method_name)
        
        if not all_file_pairs:
            return []
        
        dates = set()
        
        for parquet_path, _ in all_file_pairs:
            # 从路径中提取日期信息
            date_info = self._extract_date_from_path(parquet_path)
            
            if date_info:
                date_type, date_value = date_info
                
                # 根据日期类型格式化输出
                if date_type == 'daily':
                    dates.add(date_value)  # YYYYMMDD
                elif date_type == 'monthly':
                    dates.add(date_value)  # YYYYMM
                elif date_type == 'quarterly':
                    dates.add(date_value)  # YYYYQ1-Q4
                elif date_type == 'yearly':
                    dates.add(date_value)  # YYYY
        
        # 检查是否是股票代码列表（如果包含000001则认为是股票列表）
        date_list = sorted(list(dates))
        if date_list and '000001' in date_list:
            return []  # 如果是股票列表，返回空列表
        
        return date_list