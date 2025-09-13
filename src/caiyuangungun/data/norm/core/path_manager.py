#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
路径管理器 - 统一管理Raw层和Norm层的路径构建

职责：
1. 基于配置文件动态构建Raw层数据读取路径
2. 构建Norm层各阶段的数据读取和写入路径
3. 提供路径验证和目录创建功能
4. 支持路径模板参数化和动态替换
"""

import os
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
from pathlib import Path
import glob
from calendar import monthrange

from .config_manager import get_config_manager
from ....contracts import DataSource


class PathManager:
    """路径管理器
    
    统一管理Raw层和Norm层的路径构建，支持动态配置和模板化路径生成。
    """
    
    def __init__(self):
        """初始化路径管理器"""
        self.config_manager = get_config_manager()
        self._path_config = None
        self._load_path_config()
    
    def _load_path_config(self):
        """加载路径配置"""
        self._path_config = self.config_manager.get_path_config()
        if not self._path_config:
            raise ValueError("Failed to load path configuration")
    
    def get_raw_path(self, source: DataSource, data_type: str, 
                    date_info: Optional[Dict[str, str]] = None,
                    create_dirs: bool = False) -> str:
        """获取Raw层数据读取路径
        
        Args:
            source: 数据源
            data_type: 数据类型/接口名称
            date_info: 日期信息字典，包含year_month, day等
            create_dirs: 是否创建目录
            
        Returns:
            完整的文件路径
            
        Raises:
            ValueError: 当存储类型不支持或配置缺失时
        """
        # 获取存储类型
        storage_type = self.config_manager.get_storage_type(source, data_type)
        if not storage_type:
            raise ValueError(f"No storage type found for {source.value}.{data_type}")
        
        # 获取存储类型配置
        storage_types = self.config_manager.get_storage_types()
        archive_config = storage_types.get(storage_type)
        if not archive_config or not archive_config.get("enabled", True):
            raise ValueError(f"Storage type {storage_type} is not enabled or configured")
        
        # 构建路径模板参数
        path_params = {
            'source_name': source.value,
            'data_type': data_type
        }
        
        # 添加日期信息
        if date_info:
            path_params.update(date_info)
        
        # 格式化路径
        try:
            relative_path = archive_config['path_pattern'].format(**path_params)
        except KeyError as e:
            raise ValueError(f"Missing required path parameter for {storage_type}: {e}")
        
        # 构建完整路径
        raw_config = self._path_config.get('raw_layer', {})
        paths_config = raw_config.get('paths', {})
        archive_subpath = paths_config.get('archive_subpath', 'archive')
        
        # 获取文件配置
        file_config = raw_config.get('file_config', {})
        filename_template = file_config.get('filename_template', 'data.{file_type}')
        default_format = file_config.get('default_format', 'parquet')
        filename = filename_template.format(file_type=default_format)
        
        base_path = raw_config.get('base_path', '')
        full_path = os.path.join(
            base_path,
            archive_subpath,
            relative_path,
            filename
        )
        
        # 创建目录（如果需要）
        if create_dirs:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        return full_path
    
    def get_norm_path(self, source: DataSource, data_interface: str, stage: str,
                     create_dirs: bool = False, year: Optional[int] = None) -> str:
        """获取Norm层数据路径
        
        Args:
            source: 数据源
            data_interface: 数据接口名称
            stage: 处理阶段 (stage_1_merge, stage_2_clean, stage_3_reconcile)
            create_dirs: 是否创建目录
            year: 年份，用于DAILY类型的路径生成
            
        Returns:
            完整的文件路径
            
        Raises:
            ValueError: 当阶段不支持或配置缺失时
        """
        # 获取存储类型
        storage_type = self.config_manager.get_storage_type(source, data_interface)
        if not storage_type:
            raise ValueError(f"No storage type found for {source.value}.{data_interface}")
        
        norm_config = self._path_config.get('norm_layer', {})
        
        # 获取阶段配置
        processing_stages = norm_config.get('processing_stages', {})
        stage_config = processing_stages.get(stage)
        if not stage_config or not stage_config.get("enabled", True):
            raise ValueError(f"Stage {stage} is not enabled or configured")
        
        # 构建路径模板参数
        path_params = {
            'source_name': source.value,
            'data_interface': data_interface
        }
        
        # 获取路径模式
        path_pattern = ""
        if "path_patterns" in stage_config:
            path_pattern = stage_config["path_patterns"].get(storage_type, "")
        elif "path_pattern" in stage_config:
            # 向后兼容旧的path_pattern
            path_pattern = stage_config.get("path_pattern", "")
        
        if not path_pattern:
            raise ValueError(f"No path pattern found for {stage} with storage type {storage_type}")
        
        # 检查DAILY类型是否需要年份参数
        if storage_type == "DAILY" and "{year}" in path_pattern:
            if year is None:
                raise ValueError(f"Year parameter is required for DAILY storage type in {stage}")
            path_params['year'] = str(year)
        elif storage_type == "DAILY" and year is not None:
            # DAILY类型但路径模式不需要年份参数
            path_params['year'] = str(year)
        
        # 格式化路径
        try:
            relative_path = path_pattern.format(**path_params)
        except KeyError as e:
            raise ValueError(f"Missing required path parameter for {stage}: {e}")
        
        # 获取文件配置
        file_config = stage_config.get('file_config', {})
        filename_template = file_config.get('filename_template', 'data.parquet')
        filename = filename_template
        
        # 构建完整路径
        base_path = norm_config.get('base_path', '')
        full_path = os.path.join(
            base_path,
            relative_path,
            filename
        )
        
        # 创建目录（如果需要）
        if create_dirs:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        return full_path
    
    def get_all_norm_stage_paths(self, source: DataSource, data_interface: str,
                                create_dirs: bool = False, year: Optional[int] = None) -> Dict[str, str]:
        """获取所有Norm层阶段的路径
        
        Args:
            source: 数据源
            data_interface: 数据接口名称
            create_dirs: 是否创建目录
            year: 年份，用于DAILY类型的路径生成
            
        Returns:
            阶段名称到路径的映射字典
        """
        norm_config = self._path_config.get('norm_layer', {})
        processing_stages = norm_config.get('processing_stages', {})
        stage_paths = {}
        
        for stage_name in processing_stages.keys():
            try:
                path = self.get_norm_path(source, data_interface, stage_name, create_dirs, year)
                stage_paths[stage_name] = path
            except ValueError:
                # 跳过未启用或配置错误的阶段
                continue
        
        return stage_paths
    
    def validate_path(self, path: str, check_exists: bool = False) -> bool:
        """验证路径是否有效
        
        Args:
            path: 要验证的路径
            check_exists: 是否检查路径是否存在
            
        Returns:
            路径是否有效
        """
        try:
            # 检查路径格式
            Path(path)
            
            # 检查是否存在（如果需要）
            if check_exists:
                return os.path.exists(path)
            
            return True
        except (OSError, ValueError):
            return False
    
    def get_available_storage_types(self) -> List[str]:
        """获取可用的存储类型列表
        
        Returns:
            存储类型名称列表
        """
        storage_types = self.config_manager.get_storage_types()
        return [st for st, config in storage_types.items() 
                if config.get("enabled", True)]
    
    def get_available_norm_stages(self) -> List[str]:
        """获取可用的Norm层处理阶段列表
        
        Returns:
            阶段名称列表
        """
        norm_config = self._path_config.get('norm_layer', {})
        processing_stages = norm_config.get('processing_stages', {})
        return [stage for stage, config in processing_stages.items()
                if config.get("enabled", True)]
    
    def get_storage_type_info(self, storage_type: str) -> Optional[Dict[str, Any]]:
        """获取存储类型的详细信息
        
        Args:
            storage_type: 存储类型名称
            
        Returns:
            存储类型配置信息，如果不存在则返回None
        """
        storage_types = self.config_manager.get_storage_types()
        return storage_types.get(storage_type)
    
    def get_stage_info(self, stage: str) -> Optional[Dict[str, Any]]:
        """获取Norm层阶段的详细信息
        
        Args:
            stage: 阶段名称
            
        Returns:
            阶段配置信息，如果不存在则返回None
        """
        norm_config = self._path_config.get('norm_layer', {})
        processing_stages = norm_config.get('processing_stages', {})
        return processing_stages.get(stage)
    
    def get_raw_file_paths(self, source: DataSource, data_type: str, 
                          time_range: Optional[Union[str, Dict[str, Any]]] = None) -> List[str]:
        """获取Raw层数据文件路径列表
        
        Args:
            source: 数据源
            data_type: 数据类型/接口名称
            time_range: 时间范围参数
                - 对于DAILY类型: 可以是"202410"(年月)或{"year": 2024, "month": 10}
                - 对于MONTHLY类型: 可以是"2024"(年份)或{"year": 2024}
                - 对于SNAPSHOT类型: 忽略此参数
                
        Returns:
            实际存在的文件路径列表
            
        Raises:
            ValueError: 当存储类型不支持或配置缺失时
        """
        # 获取存储类型
        storage_type = self.config_manager.get_storage_type(source, data_type)
        if not storage_type:
            raise ValueError(f"No storage type found for {source.value}.{data_type}")
        
        file_paths = []
        
        if storage_type == "SNAPSHOT":
            # SNAPSHOT类型直接返回单个文件路径，只检查landing目录
            raw_config = self._path_config.get('raw_layer', {})
            base_path = raw_config.get('base_path', '')
            paths_config = raw_config.get('paths', {})
            landing_subpath = paths_config.get('landing_subpath', 'landing')
            
            # 获取存储类型配置
            storage_types = self.config_manager.get_storage_types()
            archive_config = storage_types.get(storage_type)
            
            if archive_config:
                # 构建路径模板参数
                path_params = {
                    'source_name': source.value,
                    'data_type': data_type
                }
                
                try:
                    relative_path = archive_config['path_pattern'].format(**path_params)
                    
                    # 获取文件配置
                    file_config = raw_config.get('file_config', {})
                    filename_template = file_config.get('filename_template', 'data.{file_type}')
                    default_format = file_config.get('default_format', 'parquet')
                    filename = filename_template.format(file_type=default_format)
                    
                    # 只检查landing目录
                    full_path = os.path.join(
                        base_path,
                        landing_subpath,
                        relative_path,
                        filename
                    )
                    if os.path.exists(full_path):
                        file_paths.append(full_path)
                            
                except (ValueError, KeyError, OSError):
                    pass
                
        elif storage_type == "DAILY":
            # DAILY类型根据时间范围生成路径列表
            file_paths = self._get_daily_file_paths(source, data_type, time_range)
            
        elif storage_type == "MONTHLY":
            # MONTHLY类型根据时间范围生成路径列表
            file_paths = self._get_monthly_file_paths(source, data_type, time_range)
            
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        return file_paths
    
    def _get_daily_file_paths(self, source: DataSource, data_type: str, 
                             time_range: Optional[Union[str, Dict[str, Any]]]) -> List[str]:
        """获取DAILY类型的文件路径列表"""
        file_paths = []
        
        # 解析时间范围
        if isinstance(time_range, str):
            # 格式: "202410"
            if len(time_range) == 6:
                year = int(time_range[:4])
                month = int(time_range[4:6])
                months_to_process = [(year, month)]
            elif len(time_range) == 4:
                # 格式: "2024" - 处理整年
                year = int(time_range)
                months_to_process = [(year, m) for m in range(1, 13)]
            else:
                raise ValueError(f"Invalid time_range format for DAILY: {time_range}")
        elif isinstance(time_range, dict):
            year = time_range.get('year')
            month = time_range.get('month')
            if year and month:
                months_to_process = [(year, month)]
            elif year:
                months_to_process = [(year, m) for m in range(1, 13)]
            else:
                raise ValueError("Invalid time_range dict for DAILY: missing year")
        else:
            # 默认处理当前年份
            current_year = datetime.now().year
            months_to_process = [(current_year, m) for m in range(1, 13)]
        
        # 获取Raw层配置
        raw_config = self._path_config.get('raw_layer', {})
        base_path = raw_config.get('base_path', '')
        paths_config = raw_config.get('paths', {})
        landing_subpath = paths_config.get('landing_subpath', 'landing')
        
        # 获取存储类型配置
        storage_types = self.config_manager.get_storage_types()
        storage_type = self.config_manager.get_storage_type(source, data_type)
        archive_config = storage_types.get(storage_type)
        
        if not archive_config:
            return file_paths
        
        # 获取文件配置
        file_config = raw_config.get('file_config', {})
        filename_template = file_config.get('filename_template', 'data.{file_type}')
        default_format = file_config.get('default_format', 'parquet')
        filename = filename_template.format(file_type=default_format)
        
        # 为每个月的每一天生成路径，只检查landing目录
        for year, month in months_to_process:
            days_in_month = monthrange(year, month)[1]
            for day in range(1, days_in_month + 1):
                year_month = f"{year}{month:02d}"
                date_info = {
                    "year_month": year_month,
                    "day": f"{day:02d}"
                }
                
                # 构建路径模板参数
                path_params = {
                    'source_name': source.value,
                    'data_type': data_type
                }
                path_params.update(date_info)
                
                try:
                    relative_path = archive_config['path_pattern'].format(**path_params)
                    
                    # 只检查landing目录
                    full_path = os.path.join(
                        base_path,
                        landing_subpath,
                        relative_path,
                        filename
                    )
                    if os.path.exists(full_path):
                        file_paths.append(full_path)
                            
                except (ValueError, KeyError, OSError):
                    continue
        
        return file_paths
    
    def _get_monthly_file_paths(self, source: DataSource, data_type: str, 
                               time_range: Optional[Union[str, Dict[str, Any]]]) -> List[str]:
        """获取MONTHLY类型的文件路径列表"""
        file_paths = []
        
        # 获取Raw层配置
        raw_config = self._path_config.get('raw_layer', {})
        base_path = raw_config.get('base_path', '')
        paths_config = raw_config.get('paths', {})
        landing_subpath = paths_config.get('landing_subpath', 'landing')
        
        # 只搜索landing目录
        search_paths = [
            os.path.join(base_path, landing_subpath, source.value, data_type)
        ]
        
        # 解析时间范围过滤
        year_filter = None
        if isinstance(time_range, str) and len(time_range) == 4:
            year_filter = int(time_range)
        elif isinstance(time_range, dict) and 'year' in time_range:
            year_filter = time_range['year']
        
        for search_path in search_paths:
            if os.path.exists(search_path):
                # 使用glob递归搜索所有parquet文件
                pattern = os.path.join(search_path, '**', '*.parquet')
                found_files = glob.glob(pattern, recursive=True)
                
                # 如果有年份过滤，则过滤文件
                if year_filter:
                    filtered_files = []
                    for file_path in found_files:
                        # 从路径中提取年份信息进行过滤
                        if str(year_filter) in file_path:
                            filtered_files.append(file_path)
                    file_paths.extend(filtered_files)
                else:
                    file_paths.extend(found_files)
        
        return file_paths


# 全局路径管理器实例
_path_manager_instance = None


def get_path_manager() -> PathManager:
    """获取全局路径管理器实例
    
    Returns:
        PathManager实例
    """
    global _path_manager_instance
    if _path_manager_instance is None:
        _path_manager_instance = PathManager()
    return _path_manager_instance