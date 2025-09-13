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

# 使用绝对导入避免相对导入问题
import sys
from pathlib import Path

# 添加core目录到Python路径
core_dir = Path(__file__).parent
if str(core_dir) not in sys.path:
    sys.path.insert(0, str(core_dir))

# 使用绝对路径导入避免相对导入问题
def import_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# 导入ConfigManager
config_manager_module = import_from_path("config_manager", core_dir / "config_manager.py")
ConfigManager = config_manager_module.ConfigManager

# 导入BaseDataSource
base_data_source_module = import_from_path("base_data_source", core_dir / "base_data_source.py")
BaseDataSource = base_data_source_module.BaseDataSource


@dataclass
class DataSourceInfo:
    """数据源信息"""
    name: str
    enabled: bool
    class_path: str
    config_file: str
    health_check: Dict[str, Any]
    instance: Optional[BaseDataSource] = None
    last_health_check: Optional[float] = None
    health_status: bool = True
    error_message: Optional[str] = None


class DataSourceManager:
    """数据源管理器 - 轻量级容器/注册中心"""
    
    def __init__(self, config_file: str = "data_source_manager_config.json"):
        """初始化数据源管理器
        
        Args:
            config_file: 配置文件名
        """
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager()
        self.config_file = config_file
        
        # 数据源注册表
        self._sources: Dict[str, DataSourceInfo] = {}
        self._instances: Dict[str, BaseDataSource] = {}
        self._lock = threading.RLock()
        
        # 管理器设置
        self._settings = {
            "lazy_loading": True,
            "health_check_interval": 300,
            "log_level": "INFO",
            "enable_monitoring": True
        }
        
        # 加载配置
        self._load_config()
        
        # 设置日志级别
        if self._settings.get("log_level"):
            logging.getLogger(__name__).setLevel(
                getattr(logging, self._settings["log_level"])
            )
    
    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            # 获取所有配置
            all_config = self.config_manager.get_all_config()
            
            # 从统一配置中提取数据源配置
            unified_data_config = all_config.get('unified_data_config', {})
            data_sources = unified_data_config.get('data_sources', {})
            
            if not data_sources:
                self.logger.error("未找到统一数据配置或data_sources配置")
                return
            
            # 注册数据源
            for name, source_config in data_sources.items():
                # 转换配置格式以适配DataSourceManager
                manager_config = {
                    "enabled": source_config.get("enabled", True),
                    "class_path": source_config.get("class_path", ""),
                    "config_file": "unified_data_config.json",  # 统一使用unified配置
                    "health_check": source_config.get("health_check", {})
                }
                
                # 设置实例化方法为config_manager方式
                if "health_check" not in manager_config:
                    manager_config["health_check"] = {}
                if "instantiation" not in manager_config["health_check"]:
                    manager_config["health_check"]["instantiation"] = {
                        "method": "config_manager"
                    }
                
                self._register_source(name, manager_config)
            
            self.logger.info(f"已加载配置文件: {self.config_file}")
            self.logger.info(f"注册数据源数量: {len(self._sources)}")
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            raise
    
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
                config_file=config["config_file"],
                health_check=config.get("health_check", {})
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
            
            # 处理相对导入路径 - 始终使用绝对路径导入避免相对导入问题
            if module_path.startswith('caiyuangungun.data.raw.sources'):
                # 使用绝对路径导入
                sources_dir = Path(__file__).parent.parent / 'sources'
                module_file = sources_dir / f"{class_name.lower().replace('datasource', '_source')}.py"
                
                if module_file.exists():
                    spec = importlib.util.spec_from_file_location(module_path, str(module_file))
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                else:
                    # 尝试其他可能的文件名格式
                    alternative_names = [
                        f"{name}_source.py",
                        f"{name}.py",
                        f"{class_name.lower()}.py"
                    ]
                    
                    module = None
                    for alt_name in alternative_names:
                        alt_file = sources_dir / alt_name
                        if alt_file.exists():
                            spec = importlib.util.spec_from_file_location(module_path, str(alt_file))
                            module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(module)
                            break
                    
                    if module is None:
                        raise ImportError(f"无法找到数据源模块文件: {module_file} 或其替代文件")
            else:
                # 对于其他模块路径，也尝试使用绝对路径导入
                try:
                    # 尝试直接导入（可能会失败）
                    module = importlib.import_module(module_path)
                except ImportError:
                    # 如果直接导入失败，尝试从当前项目路径导入
                    project_root = Path(__file__).parent.parent.parent.parent.parent
                    module_parts = module_path.split('.')
                    module_file = project_root
                    for part in module_parts:
                        module_file = module_file / part
                    module_file = module_file.with_suffix('.py')
                    
                    if module_file.exists():
                        spec = importlib.util.spec_from_file_location(module_path, str(module_file))
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                    else:
                        raise ImportError(f"无法导入模块: {module_path}")
                
            source_class: Type[BaseDataSource] = getattr(module, class_name)
            
            # 创建实例 - 根据配置决定实例化方式
            instantiation_config = source_info.health_check.get('instantiation', {})
            instantiation_method = instantiation_config.get('method', 'standard')
            
            if instantiation_method == 'config_manager':
                # 使用ConfigManager方式实例化（适用于需要从ConfigManager加载配置的数据源）
                # 不传递任何参数，让数据源自己从ConfigManager加载配置
                instance = source_class()
            elif instantiation_method == 'custom':
                # 自定义实例化参数
                custom_params = instantiation_config.get('params', {})
                instance = source_class(**custom_params)
            else:
                # 标准实例化方式 - 获取数据源配置
                source_config = self.config_manager.get_data_source_config(name)
                if not source_config:
                    self.logger.warning(f"未找到数据源 {name} 的配置")
                    source_config = {}
                instance = source_class(source_config)
            
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
                "config_file": info.config_file,
                "health_status": info.health_status,
                "last_health_check": info.last_health_check,
                "error_message": info.error_message,
                "instance_created": name in self._instances
            })
        return sources
    
    def get_instance(self, name: str) -> Optional[BaseDataSource]:
        """获取数据源实例
        
        Args:
            name: 数据源名称
            
        Returns:
            数据源实例，如果不存在或未启用则返回None
        """
        if name not in self._sources:
            self.logger.warning(f"数据源 {name} 未注册")
            return None
        
        source_info = self._sources[name]
        if not source_info.enabled:
            self.logger.warning(f"数据源 {name} 未启用")
            return None
        
        with self._lock:
            # 如果已有实例，直接返回
            if name in self._instances:
                return self._instances[name]
            
            # 懒加载：需要时才创建实例
            if self._settings.get("lazy_loading", True):
                try:
                    instance = self._create_instance(name)
                    self._instances[name] = instance
                    source_info.instance = instance
                    return instance
                except Exception as e:
                    self.logger.error(f"获取数据源实例 {name} 失败: {e}")
                    return None
            
            return None
    
    def health_check(self, source_name: Optional[str] = None) -> Dict[str, Any]:
        """执行健康检查
        
        Args:
            source_name: 指定数据源名称，None表示检查所有数据源
            
        Returns:
            健康检查结果
        """
        results = {}
        current_time = time.time()
        
        # 确定要检查的数据源
        sources_to_check = [source_name] if source_name else list(self._sources.keys())
        
        for name in sources_to_check:
            if name not in self._sources:
                results[name] = {"status": "error", "message": "数据源未注册"}
                continue
            
            source_info = self._sources[name]
            if not source_info.enabled:
                results[name] = {"status": "disabled", "message": "数据源未启用"}
                continue
            
            # 检查是否需要执行健康检查
            health_config = source_info.health_check
            if not health_config.get("enabled", False):
                results[name] = {"status": "skipped", "message": "健康检查未启用"}
                continue
            
            try:
                # 获取实例
                instance = self.get_instance(name)
                if not instance:
                    results[name] = {"status": "error", "message": "无法获取数据源实例"}
                    source_info.health_status = False
                    continue
                
                # 执行健康检查
                endpoint = health_config.get("endpoint", "stock_basic")
                timeout = health_config.get("timeout", 10)
                
                # 简单的连接检查
                if hasattr(instance, 'is_connected') and not instance.is_connected():
                    instance.connect()
                
                # 尝试获取少量数据验证连接
                test_params = {"limit": 1}
                if endpoint == "stock_basic":
                    test_params = {}
                elif endpoint in ["daily", "adj_factor"]:
                    test_params = {"trade_date": "20240101", "limit": 1}
                elif endpoint == "stock_lrb_em":
                    test_params = {"date": "20240331"}
                
                start_time = time.time()
                # 使用正确的方法名和参数格式
                if hasattr(instance, 'fetch_data'):
                    data = instance.fetch_data(endpoint, **test_params)
                else:
                    data = instance.get_data(endpoint, test_params)
                elapsed_time = time.time() - start_time
                
                if data is not None and len(data) >= 0:
                    results[name] = {
                        "status": "healthy",
                        "message": "健康检查通过",
                        "response_time": elapsed_time,
                        "data_count": len(data) if hasattr(data, '__len__') else 0
                    }
                    source_info.health_status = True
                    source_info.error_message = None
                else:
                    results[name] = {"status": "warning", "message": "返回数据为空"}
                    source_info.health_status = False
                
            except Exception as e:
                error_msg = f"健康检查失败: {str(e)}"
                results[name] = {"status": "error", "message": error_msg}
                source_info.health_status = False
                source_info.error_message = error_msg
                self.logger.error(f"数据源 {name} 健康检查失败: {e}")
            
            finally:
                source_info.last_health_check = current_time
        
        return results
    
    def reload_config(self) -> bool:
        """重新加载配置
        
        Returns:
            是否成功重新加载
        """
        try:
            # 清理现有实例（如果需要）
            old_instances = self._instances.copy()
            
            # 重新加载配置
            self._sources.clear()
            self._instances.clear()
            self._load_config()
            
            # 关闭旧实例的连接
            for instance in old_instances.values():
                try:
                    if hasattr(instance, 'disconnect'):
                        instance.disconnect()
                except Exception as e:
                    self.logger.warning(f"关闭旧实例连接时出错: {e}")
            
            self.logger.info("配置重新加载成功")
            return True
            
        except Exception as e:
            self.logger.error(f"重新加载配置失败: {e}")
            return False
    
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
            "healthy_sources": sum(1 for s in self._sources.values() if s.health_status),
            "sources_with_errors": sum(1 for s in self._sources.values() if s.error_message),
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