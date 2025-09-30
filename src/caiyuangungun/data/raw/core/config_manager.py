"""简化的配置管理器模块

提供基础的配置管理功能，支持JSON配置文件加载和环境变量覆盖。
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
import logging
from copy import deepcopy


class ConfigManager:
    """简化的配置管理器
    
    功能特性：
    1. JSON配置文件加载
    2. 环境变量覆盖
    3. 嵌套配置访问
    4. 配置缓存
    """
    
    def __init__(self, config_dir: Optional[Union[str, Path]] = None, env_prefix: str = "CAIYUAN_"):
        """初始化配置管理器
        
        Args:
            config_dir: 配置文件目录
            env_prefix: 环境变量前缀
        """
        self.env_prefix = env_prefix
        self.config_dir = Path(config_dir) if config_dir else self._get_default_config_dir()
        self._config: Dict[str, Any] = {}
        self._logger = logging.getLogger(__name__)
        
        # 加载配置
        self._load_configs()
        
    def _get_default_config_dir(self) -> Path:
        """获取默认配置目录"""
        # 尝试从环境变量获取
        if config_dir := os.getenv(f"{self.env_prefix}CONFIG_DIR"):
            return Path(config_dir)
            
        # 使用项目根目录下的data/config目录
        current_dir = Path(__file__).parent
        # 从 src/caiyuangungun/data/raw/core 向上找到项目根目录
        project_root = current_dir.parent.parent.parent.parent.parent
        return project_root / "data" / "config"
        
    def _load_configs(self) -> None:
        """加载所有JSON配置文件"""
        if not self.config_dir.exists():
            self._logger.warning(f"配置目录不存在: {self.config_dir}")
            return
            
        # 加载所有JSON文件
        for config_file in self.config_dir.glob("*.json"):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    # 使用文件名（不含扩展名）作为配置键
                    config_name = config_file.stem
                    self._config[config_name] = config_data
                    self._logger.info(f"已加载配置文件: {config_name}")
            except Exception as e:
                self._logger.error(f"加载配置文件失败 {config_file}: {e}")
                
        # 应用环境变量覆盖
        self._apply_env_overrides()
        
    def _apply_env_overrides(self) -> None:
        """应用环境变量覆盖"""
        for env_key, env_value in os.environ.items():
            if env_key.startswith(self.env_prefix):
                # 转换环境变量名为配置路径
                # 例如: CAIYUAN_TUSHARE_TOKEN -> tushare.token
                config_path = env_key[len(self.env_prefix):].lower().replace('_', '.')
                self._set_nested_value(self._config, config_path, env_value)
                
    def _set_nested_value(self, config: Dict[str, Any], path: str, value: Any) -> None:
        """设置嵌套配置值"""
        keys = path.split('.')
        current = config
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
            
        # 尝试类型转换
        final_key = keys[-1]
        if isinstance(value, str):
            # 尝试转换为合适的类型
            if value.lower() in ('true', 'false'):
                value = value.lower() == 'true'
            elif value.isdigit():
                value = int(value)
            elif value.replace('.', '').replace('-', '').isdigit():
                value = float(value)
                
        current[final_key] = value
        
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值
        
        Args:
            key: 配置键，支持点分隔的嵌套路径
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        current = self._config
        
        # 首先尝试直接获取
        try:
            for k in keys:
                current = current[k]
            return current
        except (KeyError, TypeError):
            pass
            
        # 如果直接获取失败，尝试从unified_data_config中获取
        if 'unified_data_config' in self._config:
            try:
                current = self._config['unified_data_config']
                for k in keys:
                    current = current[k]
                return current
            except (KeyError, TypeError):
                pass
                
        return default
            
    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        self._set_nested_value(self._config, key, value)
        
    def get_section(self, section: str) -> Dict[str, Any]:
        """获取配置段"""
        return self.get(section, {})
        
    def has(self, key: str) -> bool:
        """检查配置键是否存在"""
        return self.get(key) is not None
        
    def reload(self) -> None:
        """重新加载配置"""
        self._logger.info("重新加载配置")
        self._config.clear()
        self._load_configs()
        
    def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置"""
        return deepcopy(self._config)
        
    def save_config(self, config_name: str, config_data: Dict[str, Any]) -> None:
        """保存配置到JSON文件
        
        Args:
            config_name: 配置名称（不包含扩展名）
            config_data: 要保存的配置数据
        """
        file_path = self.config_dir / f"{config_name}.json"
        
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存配置文件
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2, ensure_ascii=False, default=str)
                
        self._logger.info(f"已保存配置: {config_name}")
        
        # 更新内存中的配置
        self._config[config_name] = config_data
        
    def __getitem__(self, key: str) -> Any:
        """支持字典式访问"""
        return self.get(key)
        
    def __setitem__(self, key: str, value: Any) -> None:
        """支持字典式设置"""
        self.set(key, value)
        
    def __contains__(self, key: str) -> bool:
        """支持 in 操作符"""
        return self.has(key)


# 全局配置管理器实例
_global_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """获取全局配置管理器实例"""
    global _global_config_manager
    if _global_config_manager is None:
        _global_config_manager = ConfigManager()
    return _global_config_manager


def init_config_manager(config_dir: Optional[Union[str, Path]] = None, **kwargs) -> ConfigManager:
    """初始化全局配置管理器"""
    global _global_config_manager
    _global_config_manager = ConfigManager(config_dir=config_dir, **kwargs)
    return _global_config_manager


# 便捷函数
def get_config(key: str, default: Any = None) -> Any:
    """获取配置值的便捷函数"""
    return get_config_manager().get(key, default)


def get_section(section: str) -> Dict[str, Any]:
    """获取配置段的便捷函数"""
    return get_config_manager().get_section(section)