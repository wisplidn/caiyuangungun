#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源管理器模块

功能说明：
- 轻量级数据源容器/注册中心
- 支持懒加载实例化
- 提供健康检查和运维接口
- 统一数据源生命周期管理

设计原则：
- 最小职责：只管理数据源的注册、实例化、健康检查
- 不涉及业务逻辑：端点能力、业务编排由Service层负责
- 可扩展：为未来多数据源做准备
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Any, Type
from dataclasses import dataclass
from pathlib import Path
import importlib
import importlib.util

# 直接导入BaseDataSource
try:
    from .base_data_source import BaseDataSource
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    from base_data_source import BaseDataSource


@dataclass
class DataSourceInfo:
    """数据源信息"""
    name: str
    enabled: bool
    class_path: str
    config: Dict[str, Any]
    instance: Optional[BaseDataSource] = None


class DataSourceManager:
    """数据源管理器 - 轻量级容器/注册中心"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化数据源管理器
        
        Args:
            config: 配置字典，由service层提供
        """
        self.logger = logging.getLogger(__name__)
        self.config = config or {}
        
        # 数据源注册表
        self._sources: Dict[str, DataSourceInfo] = {}
        self._instances: Dict[str, BaseDataSource] = {}
        self._lock = threading.RLock()
        
        # 管理器设置 - 从外部config覆盖默认值
        default_settings = {
            "lazy_loading": True,
            "health_check_interval": 300,
            "log_level": "INFO",
            "enable_monitoring": True
        }
        self._settings = {**default_settings, **self.config.get('manager_settings', {})}
        
        # 加载配置
        self._load_config()
        
        # 设置日志级别
        if self._settings.get("log_level"):
            self.logger.setLevel(
                getattr(logging, self._settings["log_level"])
            )
        
        # 如果不是懒加载模式，预创建所有启用的数据源实例
        if not self._settings.get("lazy_loading", True):
            self._preload_instances()
    
    def _load_config(self) -> None:
        """加载配置"""
        try:
            # 从传入的config中提取数据源配置
            data_sources = self.config.get('data_sources', {})
            
            if not data_sources:
                self.logger.warning("未找到data_sources配置，将使用空配置")
                return
            
            # 注册数据源
            for name, source_config in data_sources.items():
                # 简化配置格式
                manager_config = {
                    "enabled": source_config.get("enabled", True),
                    "class_path": source_config.get("class_path", ""),
                    "config": source_config  # 直接传递完整的source_config
                }
                
                self._register_source(name, manager_config)
            
            self.logger.info(f"注册数据源数量: {len(self._sources)}")
            
            # 记录配置加载时间
            self._last_config_load = time.time()
            
        except Exception as e:
            self.logger.error(f"加载配置失败: {e}")
            raise
    
    def _preload_instances(self) -> None:
        """预加载所有启用的数据源实例（非懒加载模式）"""
        for name, source_info in self._sources.items():
            if source_info.enabled:
                try:
                    instance = self._create_instance(name)
                    self._instances[name] = instance
                    source_info.instance = instance
                    self.logger.info(f"预加载数据源实例: {name}")
                except Exception as e:
                    self.logger.error(f"预加载数据源实例 {name} 失败: {e}")
    
    def _register_source(self, name: str, config: Dict[str, Any]) -> None:
        """注册数据源
        
        Args:
            name: 数据源名称
            config: 数据源配置
        """
        try:
            source_info = DataSourceInfo(
                name=name,
                enabled=config.get("enabled", True),
                class_path=config["class_path"],
                config=config.get("config", {})
            )
            
            self._sources[name] = source_info
            self.logger.debug(f"已注册数据源: {name}")
            
        except KeyError as e:
            self.logger.error(f"注册数据源 {name} 失败，缺少必要配置: {e}")
            raise
    
    def _create_instance(self, name: str) -> BaseDataSource:
        """创建数据源实例
        
        Args:
            name: 数据源名称
            
        Returns:
            数据源实例
        """
        source_info = self._sources[name]
        
        try:
            # 动态导入类
            module_path, class_name = source_info.class_path.rsplit('.', 1)
            
            # 简化导入逻辑，直接使用importlib导入
            try:
                module = importlib.import_module(module_path)
            except ImportError as e:
                # 如果直接导入失败，尝试添加当前目录到路径
                if module_path.startswith('sources.'):
                    try:
                        # 尝试从当前目录的sources模块导入
                        import sys
                        from pathlib import Path
                        current_dir = Path(__file__).parent.parent
                        if str(current_dir) not in sys.path:
                            sys.path.insert(0, str(current_dir))
                        module = importlib.import_module(module_path)
                    except ImportError:
                        raise ImportError(f"无法导入模块 {module_path}: {e}")
                else:
                    raise ImportError(f"无法导入模块 {module_path}: {e}")
                
            source_class: Type[BaseDataSource] = getattr(module, class_name)
            
            # 将字典配置转换为DataSourceConfig对象
            try:
                from .base_data_source import DataSourceConfig
            except ImportError:
                from base_data_source import DataSourceConfig
            
            # 创建DataSourceConfig对象
            if isinstance(source_info.config, dict):
                data_source_config = DataSourceConfig(
                    name=source_info.config.get('name', name),
                    source_type=source_info.config.get('source_type', name),
                    connection_params=source_info.config.get('connection_params', {}),
                    rate_limit=source_info.config.get('rate_limit'),
                    timeout=source_info.config.get('timeout', 30),
                    retry_count=source_info.config.get('retry_count', 3)
                )
            else:
                # 如果已经是DataSourceConfig对象，直接使用
                data_source_config = source_info.config
            
            # 使用DataSourceConfig对象创建实例
            instance = source_class(data_source_config)
            
            # 创建实例后自动调用connect方法建立连接
            if hasattr(instance, 'connect'):
                try:
                    connect_result = instance.connect()
                    if connect_result:
                        self.logger.info(f"数据源 {name} 连接成功")
                    else:
                        self.logger.warning(f"数据源 {name} 连接失败")
                except Exception as e:
                    self.logger.error(f"数据源 {name} 连接时发生错误: {e}")
            
            self.logger.info(f"已创建数据源实例: {name}")
            return instance
            
        except Exception as e:
            error_msg = f"创建数据源实例 {name} 失败: {e}"
            self.logger.error(error_msg)
            source_info.error_message = error_msg
            raise
    
    def list_sources(self) -> List[Dict[str, Any]]:
        """列出所有注册的数据源
        
        Returns:
            数据源信息列表
        """
        sources = []
        for name, info in self._sources.items():
            sources.append({
                "name": name,
                "enabled": info.enabled,
                "class_path": info.class_path,
                "config": info.config,
                "instance_created": name in self._instances
            })
        return sources
    
    def get_instance(self, name: str, strict: bool = True) -> Optional[BaseDataSource]:
        """获取数据源实例
        
        Args:
            name: 数据源名称
            strict: 严格模式，失败时抛异常而不是返回None
            
        Returns:
            数据源实例，如果不存在或未启用则返回None（非严格模式）
            
        Raises:
            ValueError: 严格模式下，数据源未注册、未启用或创建失败时抛出
        """
        if name not in self._sources:
            error_msg = f"数据源 {name} 未注册"
            self.logger.warning(error_msg)
            if strict:
                raise ValueError(error_msg)
            return None
        
        source_info = self._sources[name]
        if not source_info.enabled:
            error_msg = f"数据源 {name} 未启用"
            self.logger.warning(error_msg)
            if strict:
                raise ValueError(error_msg)
            return None
        
        with self._lock:
            # 如果已有实例，直接返回
            if name in self._instances:
                return self._instances[name]
            
            # 懒加载模式：需要时才创建实例
            if self._settings.get("lazy_loading", True):
                try:
                    instance = self._create_instance(name)
                    self._instances[name] = instance
                    source_info.instance = instance
                    return instance
                except Exception as e:
                    error_msg = f"获取数据源实例 {name} 失败: {e}"
                    self.logger.error(error_msg)
                    if strict:
                        raise ValueError(error_msg) from e
                    return None
            else:
                # 非懒加载模式：实例应该已经在初始化时创建
                error_msg = f"数据源 {name} 实例未找到，可能预加载失败"
                self.logger.warning(error_msg)
                if strict:
                    raise ValueError(error_msg)
                return None
    

    def get_metrics(self) -> Dict[str, Any]:
        """获取运行指标
        
        Returns:
            运行指标数据
        """
        if not self._settings.get("enable_monitoring", True):
            return {"monitoring": "disabled"}
        
        metrics = {
            "total_sources": len(self._sources),
            "enabled_sources": sum(1 for s in self._sources.values() if s.enabled),
            "active_instances": len(self._instances),
            "last_config_load": getattr(self, '_last_config_load', None)
        }
        
        return metrics
    
    def shutdown(self) -> None:
        """关闭管理器，清理资源"""
        self.logger.info("正在关闭数据源管理器...")
        
        # 关闭所有数据源连接
        for name, instance in self._instances.items():
            try:
                if hasattr(instance, 'disconnect'):
                    instance.disconnect()
                    self.logger.debug(f"已关闭数据源 {name} 的连接")
            except Exception as e:
                self.logger.warning(f"关闭数据源 {name} 连接时出错: {e}")
        
        self._instances.clear()
        self.logger.info("数据源管理器已关闭")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()