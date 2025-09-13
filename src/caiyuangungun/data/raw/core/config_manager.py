"""配置管理器模块

提供统一的配置管理功能，支持多种配置源、环境变量覆盖、
配置验证和动态加载等功能。

重构说明：
- 适配新的配置文件结构（unified_data_config.json、tushare_limitmax_config.json、dto_validation_config.json）
- 增强数据源配置管理
- 添加DTO验证支持
- 优化配置加载和访问性能
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime
import re
from dataclasses import dataclass, field
import logging
from copy import deepcopy
import re


@dataclass
class ConfigSource:
    """配置源定义"""
    name: str
    path: Path
    format: str  # json, yaml, py
    priority: int = 0  # 优先级，数字越大优先级越高
    required: bool = False
    last_modified: Optional[datetime] = None


@dataclass
class ConfigValidationRule:
    """配置验证规则"""
    field_path: str  # 字段路径，如 "tushare.connection_params.token"
    required: bool = False
    data_type: Optional[type] = None
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    pattern: Optional[str] = None
    custom_validator: Optional[callable] = None


@dataclass
class DataSourceConfig:
    """数据源配置"""
    name: str
    source_type: str
    enabled: bool
    class_path: str
    connection_params: Dict[str, Any]
    description: str
    health_check: Dict[str, Any]
    data_definitions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TushareConfig:
    """Tushare配置"""
    token: str
    timeout: int
    max_requests_per_minute: int
    retry_count: int
    retry_delay: float = 1.0


@dataclass
class DataDefinition:
    """数据定义配置"""
    data_source: str
    method: str
    description: str
    storage_type: str
    required_params: Union[List[str], Dict[str, Any]]
    start_date: str
    lookback_periods: int


@dataclass
class LimitMaxConfig:
    """Limitmax配置"""
    endpoint: str
    limit_value: int
    reason: str
    last_updated: str


class ConfigManager:
    """配置管理器
    
    功能特性：
    1. 多配置源支持（JSON、YAML、Python模块）
    2. 环境变量覆盖
    3. 配置验证
    4. 动态重载
    5. 配置缓存
    6. 配置继承和合并
    """
    
    def __init__(self, 
                 config_dir: Optional[Union[str, Path]] = None,
                 auto_reload: bool = False,
                 env_prefix: str = "CAIYUAN_"):
        """初始化配置管理器
        
        Args:
            config_dir: 配置文件目录
            auto_reload: 是否自动重载配置
            env_prefix: 环境变量前缀
        """
        self.auto_reload = auto_reload
        self.env_prefix = env_prefix
        self.config_dir = Path(config_dir) if config_dir else self._get_default_config_dir()
        
        # 内部状态
        self._config_sources: List[ConfigSource] = []
        self._merged_config: Dict[str, Any] = {}
        self._validation_rules: List[ConfigValidationRule] = []
        self._logger = logging.getLogger(__name__)
        
        # 专用配置缓存
        self._unified_config: Optional[Dict[str, Any]] = None
        self._limitmax_config: Optional[Dict[str, Any]] = None
        self._dto_validation_config: Optional[Dict[str, Any]] = None
        
        # 初始化
        self._discover_config_sources()
        self._load_all_configs()
        self._load_specialized_configs()
        
    def _get_default_config_dir(self) -> Path:
        """获取默认配置目录"""
        # 尝试从环境变量获取
        if config_dir := os.getenv(f"{self.env_prefix}CONFIG_DIR"):
            return Path(config_dir)
            
        # 使用当前文件所在目录的config子目录
        current_dir = Path(__file__).parent
        return current_dir.parent / "config"
        
    def _discover_config_sources(self) -> None:
        """发现配置源"""
        if not self.config_dir.exists():
            self._logger.warning(f"配置目录不存在: {self.config_dir}")
            return
            
        # 支持的配置文件格式
        supported_formats = {
            '.json': 'json',
            '.yaml': 'yaml', 
            '.yml': 'yaml',
            '.py': 'python'
        }
        
        for config_file in self.config_dir.glob("*"):
            if config_file.is_file():
                suffix = config_file.suffix.lower()
                if suffix in supported_formats:
                    source = ConfigSource(
                        name=config_file.stem,
                        path=config_file,
                        format=supported_formats[suffix],
                        last_modified=datetime.fromtimestamp(config_file.stat().st_mtime)
                    )
                    self._config_sources.append(source)
                    
        # 按优先级排序（文件名包含priority的优先级更高）
        self._config_sources.sort(key=lambda x: (
            1 if 'priority' in x.name.lower() else 0,
            x.name
        ))
        
        self._logger.info(f"发现 {len(self._config_sources)} 个配置源")
        
    def _load_specialized_configs(self) -> None:
        """加载专用配置文件"""
        # 加载统一数据配置
        unified_config_path = self.config_dir / "unified_data_config.json"
        if unified_config_path.exists():
            try:
                with open(unified_config_path, 'r', encoding='utf-8') as f:
                    self._unified_config = json.load(f)
                self._logger.info("已加载统一数据配置文件")
            except Exception as e:
                self._logger.error(f"加载统一数据配置失败: {e}")
                
        # 加载limitmax配置
        limitmax_config_path = self.config_dir / "tushare_limitmax_config.json"
        if limitmax_config_path.exists():
            try:
                with open(limitmax_config_path, 'r', encoding='utf-8') as f:
                    self._limitmax_config = json.load(f)
                self._logger.info("已加载Tushare limitmax配置文件")
            except Exception as e:
                self._logger.error(f"加载limitmax配置失败: {e}")
                
        # 加载DTO验证配置
        dto_config_path = self.config_dir / "dto_validation_config.json"
        if dto_config_path.exists():
            try:
                with open(dto_config_path, 'r', encoding='utf-8') as f:
                    self._dto_validation_config = json.load(f)
                self._logger.info("已加载DTO验证配置文件")
            except Exception as e:
                self._logger.error(f"加载DTO验证配置失败: {e}")
        
    def _load_config_file(self, source) -> Dict[str, Any]:
        """加载单个配置文件"""
        try:
            # 如果传入的是路径对象，直接处理
            if hasattr(source, 'suffix'):
                file_path = source
                if source.suffix == '.json':
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                elif source.suffix in ['.yaml', '.yml']:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return yaml.safe_load(f) or {}
                elif source.suffix == '.py':
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(source.stem, file_path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    config = {}
                    for attr_name in dir(module):
                        if not attr_name.startswith('_') and not callable(getattr(module, attr_name)):
                            config[attr_name] = getattr(module, attr_name)
                    return config
            
            # 如果传入的是ConfigSource对象
            elif hasattr(source, 'format'):
                if source.format == 'json':
                    with open(source.path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                        
                elif source.format == 'yaml':
                    with open(source.path, 'r', encoding='utf-8') as f:
                        return yaml.safe_load(f) or {}
                        
                elif source.format == 'python':
                    # 动态导入Python配置模块
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(source.name, source.path)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # 提取配置变量（排除私有属性和函数）
                    config = {}
                    for attr_name in dir(module):
                        if not attr_name.startswith('_') and not callable(getattr(module, attr_name)):
                            config[attr_name] = getattr(module, attr_name)
                    return config
                
        except Exception as e:
            file_path = getattr(source, 'path', source)
            self._logger.error(f"加载配置文件失败 {file_path}: {e}")
            return {}
            
    def _load_all_configs(self) -> None:
        """加载所有配置"""
        self._merged_config = {}
        
        # 按优先级加载配置文件
        for source in self._config_sources:
            config = self._load_config_file(source)
            # 将配置放在以文件名为key的命名空间下
            self._merged_config[source.name] = config
            # 同时也合并到根级别（保持向后兼容）
            self._merge_config(self._merged_config, config)
            
        # 应用环境变量覆盖
        self._apply_env_overrides()
        
        # 验证配置
        self._validate_config()
        
    def _merge_config(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """深度合并配置"""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._merge_config(target[key], value)
            else:
                target[key] = deepcopy(value)
                
    def _apply_env_overrides(self) -> None:
        """应用环境变量覆盖"""
        for env_key, env_value in os.environ.items():
            if env_key.startswith(self.env_prefix):
                # 转换环境变量名为配置路径
                # 例如: CAIYUAN_TUSHARE_TOKEN -> tushare.token
                config_path = env_key[len(self.env_prefix):].lower().replace('_', '.')
                self._set_nested_value(self._merged_config, config_path, env_value)
                
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
            elif value.replace('.', '').isdigit():
                value = float(value)
                
        current[final_key] = value
        
    def _validate_config(self) -> None:
        """验证配置"""
        errors = []
        
        for rule in self._validation_rules:
            try:
                value = self.get_nested_value(rule.field_path)
                
                # 必填检查
                if rule.required and value is None:
                    errors.append(f"必填字段缺失: {rule.field_path}")
                    continue
                    
                if value is None:
                    continue
                    
                # 类型检查
                if rule.data_type and not isinstance(value, rule.data_type):
                    errors.append(f"字段类型错误 {rule.field_path}: 期望 {rule.data_type.__name__}, 实际 {type(value).__name__}")
                    
                # 值范围检查
                if rule.allowed_values and value not in rule.allowed_values:
                    errors.append(f"字段值不在允许范围内 {rule.field_path}: {value}")
                    
                # 数值范围检查
                if rule.min_value is not None and isinstance(value, (int, float)) and value < rule.min_value:
                    errors.append(f"字段值小于最小值 {rule.field_path}: {value} < {rule.min_value}")
                    
                if rule.max_value is not None and isinstance(value, (int, float)) and value > rule.max_value:
                    errors.append(f"字段值大于最大值 {rule.field_path}: {value} > {rule.max_value}")
                    
                # 正则表达式检查
                if rule.pattern and isinstance(value, str):
                    import re
                    if not re.match(rule.pattern, value):
                        errors.append(f"字段值不匹配模式 {rule.field_path}: {value}")
                        
                # 自定义验证器
                if rule.custom_validator:
                    if not rule.custom_validator(value):
                        errors.append(f"字段值未通过自定义验证 {rule.field_path}: {value}")
                        
            except Exception as e:
                errors.append(f"验证字段时出错 {rule.field_path}: {e}")
                
        if errors:
            error_msg = "配置验证失败:\n" + "\n".join(errors)
            self._logger.error(error_msg)
            raise ValueError(error_msg)
            
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值
        
        Args:
            key: 配置键，支持点分隔的嵌套路径
            default: 默认值
            
        Returns:
            配置值
        """
        return self.get_nested_value(key, default)
        
    def get_nested_value(self, path: str, default: Any = None) -> Any:
        """获取嵌套配置值"""
        keys = path.split('.')
        current = self._merged_config
        
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default
            
    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        self._set_nested_value(self._merged_config, key, value)
        
    def get_section(self, section: str) -> Dict[str, Any]:
        """获取配置段"""
        return self.get(section, {})
        
    def has(self, key: str) -> bool:
        """检查配置键是否存在"""
        return self.get_nested_value(key) is not None
        
    def add_validation_rule(self, rule: ConfigValidationRule) -> None:
        """添加验证规则"""
        self._validation_rules.append(rule)
        
    def reload(self) -> None:
        """重新加载配置"""
        self._logger.info("重新加载配置")
        self._discover_config_sources()
        self._load_all_configs()
        self._load_specialized_configs()
        
    def get_data_source_config(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取数据源配置
        
        Args:
            source_name: 数据源名称（如 'tushare'）
            
        Returns:
            数据源配置字典，如果不存在则返回None
        """
        if self._unified_config and 'data_sources' in self._unified_config:
            return self._unified_config['data_sources'].get(source_name)
        return self.get_section(source_name)
        
    def get_data_definition(self, source_name: str, data_type: str) -> Optional[Dict[str, Any]]:
        """获取数据定义配置
        
        Args:
            source_name: 数据源名称（如 'tushare'）
            data_type: 数据类型（如 'stock_basic'）
            
        Returns:
            数据定义配置字典，如果不存在则返回None
        """
        if self._unified_config and 'data_sources' in self._unified_config:
            source_config = self._unified_config['data_sources'].get(source_name)
            if source_config and 'data_definitions' in source_config:
                return source_config['data_definitions'].get(data_type)
        return None
        
    def get_all_data_definitions(self, source_name: str) -> Dict[str, Any]:
        """获取指定数据源的所有数据定义
        
        Args:
            source_name: 数据源名称（如 'tushare'）
            
        Returns:
            所有数据定义的字典
        """
        if self._unified_config and 'data_sources' in self._unified_config:
            source_config = self._unified_config['data_sources'].get(source_name)
            if source_config and 'data_definitions' in source_config:
                return source_config['data_definitions']
        return {}
        
    def get_archiver_config(self, archiver_name: str) -> Optional[Dict[str, Any]]:
        """获取归档器配置"""
        return self.get_section(archiver_name)
        
    def get_tushare_config(self, config_name: str = "tushare") -> 'TushareConfig':
        """获取Tushare配置
        
        Args:
            config_name: 配置名称
            
        Returns:
            TushareConfig: Tushare配置对象
        """
        config_data = self.get_section(config_name)
        if not config_data:
            raise ValueError(f"未找到Tushare配置: {config_name}")
            
        # 从connection_params中提取配置
        conn_params = config_data.get('connection_params', {})
        return TushareConfig(
            token=conn_params.get('token', ''),
            timeout=conn_params.get('timeout', 30),
            max_requests_per_minute=conn_params.get('max_requests_per_minute', 200),
            retry_count=conn_params.get('retry_count', 3),
            retry_delay=conn_params.get('retry_delay', 1.0)
        )
        
    def get_tushare_raw_config(self, config_name: str = "tushare") -> Dict[str, Any]:
        """获取Tushare原始配置数据
        
        Args:
            config_name: 配置名称
            
        Returns:
            Dict[str, Any]: 原始配置数据
        """
        # 优先从统一配置中获取
        if self._unified_config and 'data_sources' in self._unified_config:
            tushare_config = self._unified_config['data_sources'].get('tushare')
            if tushare_config:
                return tushare_config
                
        config_data = self.get_section(config_name)
        if not config_data:
            raise ValueError(f"未找到Tushare配置: {config_name}")
        return config_data
        
    def get_limitmax_config(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """获取指定端点的limitmax配置
        
        Args:
            endpoint: API端点名称
            
        Returns:
            limitmax配置字典，如果不存在则返回None
        """
        if self._limitmax_config and 'endpoint_limits' in self._limitmax_config:
            return self._limitmax_config['endpoint_limits'].get(endpoint)
        return None
        
    def get_all_limitmax_configs(self) -> Dict[str, Any]:
        """获取所有limitmax配置
        
        Returns:
            所有limitmax配置的字典
        """
        if self._limitmax_config and 'endpoint_limits' in self._limitmax_config:
            return self._limitmax_config['endpoint_limits']
        return {}
        
    def get_dto_validation_rules(self) -> Dict[str, Any]:
        """获取DTO验证规则
        
        Returns:
            DTO验证规则字典
        """
        if self._dto_validation_config and 'field_validations' in self._dto_validation_config:
            return self._dto_validation_config['field_validations']
        return {}
        
    def get_archive_types(self) -> List[str]:
        """获取允许的归档类型
        
        Returns:
            允许的归档类型列表
        """
        if self._dto_validation_config and 'field_validations' in self._dto_validation_config:
            archive_rule = self._dto_validation_config['field_validations'].get('archive_types')
            if archive_rule and 'enum' in archive_rule:
                return archive_rule['enum']
        return ['SNAPSHOT', 'DAILY', 'MONTHLY', 'QUARTERLY']  # 默认值
        
    def save_limitmax_config(self, endpoint: str, limitmax_value: int) -> None:
        """保存或更新limitmax配置
        
        Args:
            endpoint: API端点名称
            limitmax_value: limitmax值
        """
        if not self._limitmax_config:
            self._limitmax_config = {'endpoint_limits': {}}
            
        self._limitmax_config['endpoint_limits'][endpoint] = {
            'limitmax': limitmax_value,
            'updated_at': datetime.now().isoformat()
        }
        
        # 保存到文件
        self.save_config('tushare_limitmax_config', self._limitmax_config)
        
    def update_limitmax_config(self, endpoint: str, limitmax_value: int) -> None:
        """更新limitmax配置
        
        Args:
            endpoint: API端点名称
            limitmax_value: 新的limitmax值
        """
        field_path = f"endpoint_limits.{endpoint}.limitmax"
        self.update_config_field('tushare_limitmax_config', field_path, limitmax_value)
        
        # 同时更新时间戳
        timestamp_path = f"endpoint_limits.{endpoint}.updated_at"
        self.update_config_field('tushare_limitmax_config', timestamp_path, datetime.now().isoformat())
        
        # 更新内存缓存
        if self._limitmax_config and 'endpoint_limits' in self._limitmax_config:
            if endpoint in self._limitmax_config['endpoint_limits']:
                self._limitmax_config['endpoint_limits'][endpoint]['limitmax'] = limitmax_value
                self._limitmax_config['endpoint_limits'][endpoint]['updated_at'] = datetime.now().isoformat()
        
    def get_path_config(self) -> Dict[str, Path]:
        """获取路径配置"""
        base_path = Path(self.get('base_data_path', './data'))
        
        return {
            'base_data_path': base_path,
            'raw_landing_path': base_path / "raw" / "landing",
            'raw_norm_path': base_path / "raw" / "norm", 
            'interim_path': base_path / "interim",
            'log_path': base_path / "logs"
        }
    
    def process_request_params(self, required_params: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """处理请求参数，替换占位符
        
        Args:
            required_params: 必需参数配置字典
            **kwargs: 额外参数，如日期等
            
        Returns:
            处理后的参数字典
        """
        processed_params = {}
        
        # 处理required_params中的占位符
        for param_name, param_value in required_params.items():
            if param_value == "<TRADE_DATE>":
                # 特殊标记，使用日期生成器
                date_param = kwargs.get('trade_date') or kwargs.get('date')
                if date_param:
                    processed_params[param_name] = date_param
            elif param_value == "<MONTHLY_DATE>":
                # 特殊标记，使用月度日期生成器
                date_param = kwargs.get('monthly_date') or kwargs.get('date')
                if date_param:
                    processed_params[param_name] = date_param
            elif param_value == "<QUARTERLY_DATE>":
                # 特殊标记，使用季度日期生成器
                date_param = kwargs.get('quarterly_date') or kwargs.get('date')
                if date_param:
                    processed_params[param_name] = date_param
            else:
                # 有默认值，直接使用
                processed_params[param_name] = param_value
                
        # 添加其他非占位符参数
        for key, value in kwargs.items():
            if key not in processed_params:
                processed_params[key] = value
                
        return processed_params
        
    def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置"""
        return deepcopy(self._merged_config)
        
    def save_config(self, config_name: str, config_data: Dict[str, Any], format: str = 'json') -> None:
        """保存配置到文件
        
        Args:
            config_name: 配置名称（不包含扩展名）
            config_data: 要保存的配置数据
            format: 保存格式，支持 'json' 或 'yaml'
        """
        # 确定文件扩展名
        if format.lower() == 'json':
            file_path = self.config_dir / f"{config_name}.json"
        elif format.lower() in ('yaml', 'yml'):
            file_path = self.config_dir / f"{config_name}.yaml"
        else:
            raise ValueError(f"不支持的保存格式: {format}")
            
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存配置文件
        if format.lower() == 'json':
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False, default=str)
        elif format.lower() in ('yaml', 'yml'):
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
                
        self._logger.info(f"[Config] 已保存: {config_name}")
        
        # 更新内存中的配置
        self._merged_config[config_name] = config_data
        
        # 如果是专用配置文件，同时更新专用缓存
        if config_name == 'unified_data_config':
            self._unified_config = config_data
        elif config_name == 'tushare_limitmax_config':
            self._limitmax_config = config_data
        elif config_name == 'dto_validation_config':
            self._dto_validation_config = config_data
    
    def update_config_field(self, config_name: str, field_path: str, new_value: Any, format: str = 'json') -> None:
        """部分更新配置文件中的特定字段
        
        Args:
            config_name: 配置名称（不包含扩展名）
            field_path: 字段路径，如 'api_endpoints.stock_basic.limitmax'
            new_value: 新值
            format: 文件格式，支持 'json' 或 'yaml'
        """
        # 确定文件路径
        if format.lower() == 'json':
            file_path = self.config_dir / f"{config_name}.json"
        elif format.lower() in ('yaml', 'yml'):
            file_path = self.config_dir / f"{config_name}.yaml"
        else:
            raise ValueError(f"不支持的格式: {format}")
            
        # 检查文件是否存在
        if not file_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {file_path}")
            
        try:
            # 读取现有配置
            if format.lower() == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            elif format.lower() in ('yaml', 'yml'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f) or {}
                    
            # 更新指定字段
            self._set_nested_value(config_data, field_path, new_value)
            
            # 写回文件
            if format.lower() == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False, default=str)
            elif format.lower() in ('yaml', 'yml'):
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
                    
            # 更新内存中的配置
            if config_name in self._merged_config:
                self._set_nested_value(self._merged_config[config_name], field_path, new_value)
                
            self._logger.info(f"[Config] {field_path} = {new_value}")
            
        except Exception as e:
            self._logger.error(f"更新配置字段失败 [{config_name}.{field_path}]: {e}")
            raise
        
    def export_config(self, output_path: Union[str, Path], format: str = 'json') -> None:
        """导出配置到文件"""
        output_path = Path(output_path)
        
        if format.lower() == 'json':
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self._merged_config, f, indent=2, ensure_ascii=False, default=str)
        elif format.lower() in ('yaml', 'yml'):
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._merged_config, f, default_flow_style=False, allow_unicode=True)
        else:
            raise ValueError(f"不支持的导出格式: {format}")
            
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


def get_data_source_config(source_name: str) -> Optional[Dict[str, Any]]:
    """获取数据源配置的便捷函数"""
    return get_config_manager().get_data_source_config(source_name)


def get_data_definition(source_name: str, data_type: str) -> Optional[Dict[str, Any]]:
    """获取数据定义配置的便捷函数"""
    return get_config_manager().get_data_definition(source_name, data_type)


def get_all_data_definitions(source_name: str) -> Dict[str, Any]:
    """获取所有数据定义的便捷函数"""
    return get_config_manager().get_all_data_definitions(source_name)


def get_limitmax_config(endpoint: str) -> Optional[Dict[str, Any]]:
    """获取limitmax配置的便捷函数"""
    return get_config_manager().get_limitmax_config(endpoint)


def get_all_limitmax_configs() -> Dict[str, Any]:
    """获取所有limitmax配置的便捷函数"""
    return get_config_manager().get_all_limitmax_configs()


def get_dto_validation_rules() -> Dict[str, Any]:
    """获取DTO验证规则的便捷函数"""
    return get_config_manager().get_dto_validation_rules()


def get_archive_types() -> List[str]:
    """获取允许的归档类型的便捷函数"""
    return get_config_manager().get_archive_types()


def save_limitmax_config(endpoint: str, limitmax_value: int) -> None:
    """保存limitmax配置的便捷函数"""
    return get_config_manager().save_limitmax_config(endpoint, limitmax_value)


def update_limitmax_config(endpoint: str, limitmax_value: int) -> None:
    """更新limitmax配置的便捷函数"""
    return get_config_manager().update_limitmax_config(endpoint, limitmax_value)


def get_archiver_config(archiver_name: str) -> Optional[Dict[str, Any]]:
    """获取归档器配置的便捷函数"""
    return get_config_manager().get_archiver_config(archiver_name)


def get_path_config() -> Dict[str, Path]:
    """获取路径配置的便捷函数"""
    return get_config_manager().get_path_config()