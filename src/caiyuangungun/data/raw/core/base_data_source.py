"""数据源抽象基类模块

定义了所有数据源必须实现的基础接口和配置结构。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


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
    

class BaseDataSource(ABC):
    """数据源抽象基类
    
    所有具体数据源实现都必须继承此类并实现其抽象方法。
    提供了数据源的基础生命周期管理和数据获取接口。
    """
    
    def __init__(self, config: DataSourceConfig):
        """初始化数据源
        
        Args:
            config: 数据源配置对象
        """
        self.config = config
        self._connected = False
        
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
        
    @abstractmethod
    def get_available_assets(self) -> List[str]:
        """获取可用资产列表
        
        Returns:
            List[str]: 资产代码列表
        """
        pass
        
    @abstractmethod
    def fetch_data(self, 
                   asset: str,
                   start_date: datetime,
                   end_date: datetime,
                   **kwargs) -> pd.DataFrame:
        """获取指定资产的数据
        
        Args:
            asset: 资产代码
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 数据DataFrame
        """
        pass
        
    @abstractmethod
    def validate_asset(self, asset: str) -> bool:
        """验证资产代码是否有效
        
        Args:
            asset: 资产代码
            
        Returns:
            bool: 是否有效
        """
        pass
        
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