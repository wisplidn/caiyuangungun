"""AkShare数据源实现

提供AkShare库的数据获取功能，支持东方财富等多个数据源的股票数据。
实现了BaseDataSource接口，提供统一的数据访问方式。
"""

import pandas as pd
from typing import Dict, Any
import logging
import time
from dataclasses import dataclass

try:
    import akshare as ak
except ImportError:
    raise ImportError("请安装akshare包以使用akshare数据源")

try:
    from core.base_data_source import BaseDataSource, DataSourceConfig, ConfigDTO, DataSourceValidationError
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / 'core'))
    from base_data_source import BaseDataSource, DataSourceConfig, ConfigDTO, DataSourceValidationError


class AkshareDataSource(BaseDataSource):
    """AkShare数据源实现
    
    提供AkShare库的数据获取功能，支持多种股票数据的获取。
    """
    
    def __init__(self, config: DataSourceConfig):
        """初始化AkShare数据源
        
        Args:
            config: 数据源配置对象
        """
        super().__init__(config)
        self.logger = logging.getLogger(__name__)
        
        # 使用转换后的配置
        akshare_config = self._source_config
        self.timeout = akshare_config.timeout
        self.max_requests_per_minute = akshare_config.max_requests_per_minute
        self.retry_count = akshare_config.retry_count
        self.retry_delay = akshare_config.retry_delay
        self._last_request_time = 0
        self._request_count = 0
        self._request_window_start = 0
        
        self._connected = False
        self.logger.info("AkShare数据源初始化完成")
    
    def _validate_source_config(self, source_config: ConfigDTO) -> None:
        """验证AkShare配置
        
        Args:
            source_config: AkShare配置对象
            
        Raises:
            DataSourceValidationError: 当配置验证失败时
        """
        if not isinstance(source_config, ConfigDTO):
            raise DataSourceValidationError("AkShare配置必须是ConfigDTO类型")
            
        if source_config.retry_count < 0:
            raise DataSourceValidationError("重试次数不能为负数")
            
        if source_config.retry_delay < 0:
            raise DataSourceValidationError("重试延迟不能为负数")
    
    def _convert_config(self, config: DataSourceConfig) -> ConfigDTO:
        """将通用配置转换为AkShare配置
        
        Args:
            config: 通用数据源配置
            
        Returns:
            ConfigDTO: AkShare配置对象
        """
        connection_params = config.connection_params or {}
        return ConfigDTO(
            timeout=connection_params.get('timeout', 30),
            max_requests_per_minute=connection_params.get('max_requests_per_minute', 60),
            retry_count=connection_params.get('retry_count', 3),
            retry_delay=connection_params.get('retry_delay', 1.0)
        )
    
    def connect(self) -> bool:
        """建立AkShare连接
        
        AkShare不需要显式连接，测试接口可用性
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 测试akshare是否可用
            test_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20240102")
            if isinstance(test_df, pd.DataFrame):
                self._connected = True
                self.logger.info("AkShare连接成功")
                return True
            else:
                self.logger.error("AkShare连接测试失败")
                return False
        except Exception as e:
            self.logger.error(f"AkShare连接失败: {e}")
            self._connected = False
            return False
    
    def disconnect(self) -> None:
        """断开AkShare连接"""
        self._connected = False
        self.logger.info("AkShare连接已断开")
    
    def is_connected(self) -> bool:
        """检查连接状态
        
        Returns:
            bool: 是否已连接
        """
        return self._connected
    
    def fetch_data(self, endpoint: str, **params) -> pd.DataFrame:
        """获取指定类型的数据
        
        Args:
            endpoint: 数据类型，如 'stock_zh_a_spot_em', 'stock_zh_a_hist'
            **params: 传递给akshare函数的参数
            
        Returns:
            pd.DataFrame: 获取的数据
            
        Raises:
            RuntimeError: 当获取数据失败时
            DataSourceValidationError: 当参数验证失败时
        """
        time.sleep(0.5)

        # 调用父类验证
        super()._validate_fetch_params(endpoint, **params)
        
        # AkShare特定验证
        self._validate_akshare_params(endpoint, **params)
        
        if not self.is_connected():
            raise RuntimeError("数据源未连接")
        
        # 频率限制检查
        self._check_rate_limit()
        
        # 重试机制（指数退避）
        for attempt in range(self.retry_count):
            try:
                # 调用akshare方法
                if hasattr(ak, endpoint):
                    method = getattr(ak, endpoint)
                    data = method(**params)
                    
                    # 更新请求计数
                    self._update_request_count()
                    
                    self.logger.info(f"成功获取 {endpoint} 数据，形状: {data.shape}")
                    return data
                else:
                    raise ValueError(f"AkShare中不存在方法: {endpoint}")
                    
            except Exception as e:
                self.logger.warning(f"第 {attempt + 1} 次尝试获取 {endpoint} 数据失败: {e}")
                if attempt < self.retry_count - 1:
                    # 指数退避策略
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                else:
                    self.logger.error(f"获取 {endpoint} 数据失败，已重试 {self.retry_count} 次")
                    raise RuntimeError(f"获取 {endpoint} 数据失败: {e}")
    
    def _validate_akshare_params(self, endpoint: str, **params) -> None:
        """验证AkShare特定参数
        
        Args:
            endpoint: AkShare函数名
            **params: 函数参数
            
        Raises:
            DataSourceValidationError: 当参数验证失败时
        """
        # 检查endpoint是否为有效的AkShare函数
        if not hasattr(ak, endpoint):
            error_msg = f"AkShare中不存在函数: {endpoint}"
            self.logger.error(error_msg)
            raise DataSourceValidationError(error_msg)
            
        # 可以根据需要添加更多特定验证逻辑
        # 例如：某些函数的必需参数检查等
    
    def _check_rate_limit(self):
        """检查并执行频率限制"""
        current_time = time.time()
        
        # 重置请求窗口
        if current_time - self._request_window_start >= 60:
            self._request_count = 0
            self._request_window_start = current_time
        
        # 检查是否超过频率限制
        if self._request_count >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self._request_window_start)
            if sleep_time > 0:
                self.logger.info(f"达到频率限制，等待 {sleep_time:.1f} 秒")
                time.sleep(sleep_time)
                self._request_count = 0
                self._request_window_start = time.time()
    
    def _update_request_count(self):
        """更新请求计数"""
        self._request_count += 1
        self._last_request_time = time.time()
    
