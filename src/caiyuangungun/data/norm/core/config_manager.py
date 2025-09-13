"""配置管理器模块

职责：
- 读取/校验/发布配置文件（JSON）
- 管理配置版本，提供统一的配置访问接口
- 支持数据源配置和路径管理配置的统一管理
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

from ....contracts import DataSource


# StorageType 现在从配置文件动态读取，不再使用硬编码枚举


class ConfigType(Enum):
    """配置类型枚举"""
    DATA_SOURCES = "data_sources"
    PATH_MANAGEMENT = "path_management"


@dataclass
class DataSourceConfig:
    """数据源配置数据类"""
    name: str
    enabled: bool
    data_definitions: Dict[str, Dict[str, Any]]


@dataclass
class PathConfig:
    """路径配置数据类"""
    base_path: str
    path_pattern: str
    file_config: Dict[str, Any]
    enabled: bool = True


class ConfigManager:
    """配置管理器
    
    提供统一的配置文件读取、校验和访问接口。
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径，默认为当前模块的configs目录
        """
        if config_dir is None:
            current_dir = Path(__file__).parent
            config_dir = current_dir.parent / "configs"
        
        self.config_dir = Path(config_dir)
        self._configs: Dict[ConfigType, Dict[str, Any]] = {}
        self._load_all_configs()
    
    def _load_all_configs(self) -> None:
        """加载所有配置文件"""
        config_files = {
            ConfigType.DATA_SOURCES: "data_sources_config.json",
            ConfigType.PATH_MANAGEMENT: "path_management_config.json"
        }
        
        for config_type, filename in config_files.items():
            config_path = self.config_dir / filename
            if config_path.exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        self._configs[config_type] = json.load(f)
                except (json.JSONDecodeError, IOError) as e:
                    raise ValueError(f"Failed to load config {filename}: {e}")
            else:
                raise FileNotFoundError(f"Config file not found: {config_path}")
    
    def get_data_source_config(self, source: DataSource) -> Optional[DataSourceConfig]:
        """获取数据源配置
        
        Args:
            source: 数据源枚举
            
        Returns:
            数据源配置对象，如果不存在则返回None
        """
        config = self._configs.get(ConfigType.DATA_SOURCES, {})
        data_sources = config.get("data_sources", {})
        source_config = data_sources.get(source.value)
        
        if source_config:
            return DataSourceConfig(
                name=source_config["name"],
                enabled=source_config["enabled"],
                data_definitions=source_config["data_definitions"]
            )
        return None
    
    def get_enabled_data_interfaces(self, source: DataSource) -> List[str]:
        """获取指定数据源的启用接口列表
        
        Args:
            source: 数据源枚举
            
        Returns:
            启用的数据接口名称列表
        """
        source_config = self.get_data_source_config(source)
        if not source_config or not source_config.enabled:
            return []
        
        enabled_interfaces = []
        for interface_name, interface_config in source_config.data_definitions.items():
            if interface_config.get("enabled", False):
                enabled_interfaces.append(interface_name)
        
        return enabled_interfaces
    
    def get_storage_type(self, source: DataSource, interface_name: str) -> Optional[str]:
        """获取指定数据接口的存储类型
        
        Args:
            source: 数据源枚举
            interface_name: 数据接口名称
            
        Returns:
            存储类型字符串，如果不存在则返回None
        """
        source_config = self.get_data_source_config(source)
        if not source_config:
            return None
        
        interface_config = source_config.data_definitions.get(interface_name)
        if interface_config:
            return interface_config.get("storage_type")
        return None
    
    def get_raw_path_config(self, storage_type: str) -> Optional[PathConfig]:
        """获取Raw层路径配置
        
        Args:
            storage_type: 存储类型字符串
            
        Returns:
            路径配置对象，如果不存在则返回None
        """
        config = self._configs.get(ConfigType.PATH_MANAGEMENT, {})
        path_mgmt = config.get("path_management", {})
        raw_layer = path_mgmt.get("raw_layer", {})
        archive_types = raw_layer.get("archive_types", {})
        
        archive_config = archive_types.get(storage_type)
        if archive_config and archive_config.get("enabled", True):
            return PathConfig(
                base_path=raw_layer.get("base_path", ""),
                path_pattern=archive_config.get("path_pattern", ""),
                file_config=raw_layer.get("file_config", {}),
                enabled=archive_config.get("enabled", True)
            )
        return None
    
    def get_norm_path_config(self, stage: str, storage_type: Optional[str] = None) -> Optional[PathConfig]:
        """获取Norm层路径配置
        
        Args:
            stage: 处理阶段（stage_1_merge, stage_2_clean, stage_3_reconcile）
            storage_type: 存储类型，用于选择对应的路径模式
            
        Returns:
            路径配置对象，如果不存在则返回None
        """
        config = self._configs.get(ConfigType.PATH_MANAGEMENT, {})
        path_mgmt = config.get("path_management", {})
        norm_layer = path_mgmt.get("norm_layer", {})
        processing_stages = norm_layer.get("processing_stages", {})
        
        stage_config = processing_stages.get(stage)
        if stage_config and stage_config.get("enabled", True):
            # 支持新的path_patterns结构
            path_pattern = ""
            if "path_patterns" in stage_config and storage_type:
                path_pattern = stage_config["path_patterns"].get(storage_type, "")
            elif "path_pattern" in stage_config:
                # 向后兼容旧的path_pattern
                path_pattern = stage_config.get("path_pattern", "")
            
            return PathConfig(
                base_path=norm_layer.get("base_path", ""),
                path_pattern=path_pattern,
                file_config=stage_config.get("file_config", {}),
                enabled=stage_config.get("enabled", True)
            )
        return None
    
    def get_path_config(self) -> Optional[Dict[str, Any]]:
        """获取路径管理配置
        
        Returns:
            路径配置字典，如果不存在则返回None
        """
        config = self._configs.get(ConfigType.PATH_MANAGEMENT, {})
        path_mgmt = config.get("path_management", {})
        
        if not path_mgmt:
            return None
            
        return path_mgmt
    
    def get_storage_types(self) -> Dict[str, Dict[str, Any]]:
        """获取所有存储类型配置
        
        Returns:
            存储类型配置字典
        """
        config = self._configs.get(ConfigType.PATH_MANAGEMENT, {})
        path_mgmt = config.get("path_management", {})
        raw_layer = path_mgmt.get("raw_layer", {})
        return raw_layer.get("archive_types", {})
    
    def validate_config(self) -> bool:
        """校验配置文件的完整性和正确性
        
        Returns:
            校验是否通过
        """
        try:
            # 检查必要的配置是否存在
            if ConfigType.DATA_SOURCES not in self._configs:
                return False
            if ConfigType.PATH_MANAGEMENT not in self._configs:
                return False
            
            # 检查数据源配置结构
            data_sources_config = self._configs[ConfigType.DATA_SOURCES]
            if "data_sources" not in data_sources_config:
                return False
            
            # 检查路径管理配置结构
            path_mgmt_config = self._configs[ConfigType.PATH_MANAGEMENT]
            if "path_management" not in path_mgmt_config:
                return False
            
            return True
        except Exception:
            return False
    
    def reload_configs(self) -> None:
        """重新加载所有配置文件"""
        self._configs.clear()
        self._load_all_configs()


# 全局配置管理器实例
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """获取全局配置管理器实例
    
    Returns:
        配置管理器实例
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager