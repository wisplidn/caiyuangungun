"""数据源抽象基类模块

定义了所有数据源必须实现的基础接口和配置结构。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import logging


class DataSourceValidationError(Exception):
    """数据源参数验证错误"""
    pass


@dataclass
class DataSourceConfig:
    """数据源配置类
    
    包含数据源连接和操作所需的基础配置信息。
    """
    name: str  # 数据源名称
    source_type: str  # 数据源类型 (tushare, wind, eastmoney等)
    connection_params: Dict[str, Any]  # 连接参数
    rate_limit: Optional[int] = None  # 请求频率限制
    timeout: Optional[int] = 30  # 超时时间
    retry_count: Optional[int] = 3  # 重试次数


@dataclass
class ConfigDTO:
    """通用数据源配置DTO基类"""
    timeout: int = 30
    max_requests_per_minute: int = 60
    retry_count: int = 3
    retry_delay: float = 1.0


@dataclass
class TushareConfigDTO(ConfigDTO):
    """Tushare数据源配置DTO"""
    token: str = ""
    max_requests_per_minute: int = 200
    

class BaseDataSource(ABC):
    """数据源抽象基类
    
    所有具体数据源实现都必须继承此类并实现其抽象方法。
    提供了数据源的基础生命周期管理和数据获取接口。
    """
    
    def __init__(self, config: DataSourceConfig):
        """初始化数据源
        
        Args:
            config: 数据源配置对象
            
        Raises:
            DataSourceValidationError: 当配置验证失败时
        """
        self.config = config
        self._connected = False
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 验证基础配置
        self._validate_base_config(config)
        
        # 转换为具体数据源配置
        try:
            self._source_config = self._convert_config(config)
            # 验证转换后的配置
            self._validate_source_config(self._source_config)
        except Exception as e:
            error_msg = f"配置转换失败: {str(e)}"
            self.logger.error(error_msg)
            raise DataSourceValidationError(error_msg)
        
    @abstractmethod
    def connect(self) -> bool:
        """建立数据源连接
        
        Returns:
            bool: 连接是否成功
        """
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """断开数据源连接"""
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """检查连接状态
        
        Returns:
            bool: 是否已连接
        """
        pass
        
    def _validate_fetch_params(self, endpoint: str, **params) -> None:
        """验证fetch_data参数
        
        Args:
            endpoint: API端点名称
            **params: 其他参数
            
        Raises:
            DataSourceValidationError: 当参数验证失败时
        """
        if not endpoint or not endpoint.strip():
            error_msg = "endpoint参数不能为空"
            self.logger.error(error_msg)
            raise DataSourceValidationError(error_msg)
    
    @abstractmethod
    def fetch_data(self, endpoint: str, **params) -> pd.DataFrame:
        """获取数据
        
        Args:
            endpoint: API端点名称
            **params: 其他参数
            
        Returns:
            pd.DataFrame: 数据DataFrame
            
        Raises:
            DataSourceValidationError: 当参数验证失败时
        """
        # 验证基础参数
        self._validate_fetch_params(endpoint, **params)
        # 子类实现具体逻辑
        
    def _validate_base_config(self, config: DataSourceConfig) -> None:
        """验证基础配置
        
        Args:
            config: 数据源配置对象
            
        Raises:
            DataSourceValidationError: 当配置验证失败时
        """
        if not config:
            raise DataSourceValidationError("配置对象不能为空")
            
        if not config.name or not config.name.strip():
            raise DataSourceValidationError("数据源名称不能为空")
            
        if not config.source_type or not config.source_type.strip():
            raise DataSourceValidationError("数据源类型不能为空")
            
        if config.connection_params is None:
            raise DataSourceValidationError("连接参数不能为None")
    
    def _validate_source_config(self, source_config: Any) -> None:
        """验证具体数据源配置
        
        子类应重写此方法以实现具体的配置验证逻辑。
        
        Args:
            source_config: 具体数据源的配置对象
            
        Raises:
            DataSourceValidationError: 当配置验证失败时
        """
        pass
        
    def _convert_config(self, config: DataSourceConfig) -> Any:
        """将通用配置转换为具体数据源配置
        
        子类应重写此方法以实现具体的配置转换逻辑。
        
        Args:
            config: 通用数据源配置
            
        Returns:
            Any: 具体数据源的配置对象
        """
        return None
        
    def get_source_info(self) -> Dict[str, Any]:
        """获取数据源信息
        
        Returns:
            Dict[str, Any]: 数据源基础信息
        """
        return {
            'name': self.config.name,
            'type': self.config.source_type,
            'connected': self.is_connected(),
            'rate_limit': self.config.rate_limit,
            'timeout': self.config.timeout
        }