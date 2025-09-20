"""Tushare数据源实现

提供Tushare Pro API的数据获取功能，支持股票基础信息、行情数据、财务数据等。
实现了BaseDataSource接口，提供统一的数据访问方式。
基于JSON配置文件自动生成接口调用，支持字段验证和monthly_date自动转换。
"""

import pandas as pd
from typing import Dict, Any
import logging
import time
from dataclasses import dataclass


try:
    import chinadata.ca_data as ts  # tushare的特殊客户通道，修改会导致token失效
except ImportError:
    raise ImportError("请安装chinadata包以使用tushare数据源")

try:
    from caiyuangungun.data.raw.core.base_data_source import BaseDataSource, DataSourceConfig, TushareConfigDTO, DataSourceValidationError
except ImportError:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / 'core'))
    from base_data_source import BaseDataSource, DataSourceConfig, TushareConfigDTO, DataSourceValidationError


class TushareDataSource(BaseDataSource):
    """Tushare数据源实现
    
    提供Tushare Pro API的数据获取功能。
    基于JSON配置文件自动生成接口调用。
    """
    
    def __init__(self, config: DataSourceConfig):
        """初始化Tushare数据源
        
        Args:
            config: 数据源配置对象
        """
        super().__init__(config)
        
        # 使用转换后的配置
        tushare_config = self._source_config
        self.max_requests_per_minute = tushare_config.max_requests_per_minute
        self.retry_count = tushare_config.retry_count
        self.retry_delay = tushare_config.retry_delay
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pro = None
        self._last_request_time = 0
        self._request_count = 0
        self._minute_start = 0
    
    def _validate_source_config(self, source_config: TushareConfigDTO) -> None:
        """验证Tushare配置
        
        Args:
            source_config: Tushare配置对象
            
        Raises:
            DataSourceValidationError: 当配置验证失败时
        """
        if not isinstance(source_config, TushareConfigDTO):
            raise DataSourceValidationError("Tushare配置必须是TushareConfigDTO类型")
            
        if not source_config.token or not source_config.token.strip():
            raise DataSourceValidationError("Tushare token不能为空，请在connection_params中提供有效的token")
            
        if source_config.retry_count < 0:
            raise DataSourceValidationError("重试次数不能为负数")
            
        if source_config.retry_delay < 0:
            raise DataSourceValidationError("重试延迟不能为负数")
            
        if source_config.max_requests_per_minute <= 0:
            raise DataSourceValidationError("每分钟最大请求数必须大于0")
    
    def _convert_config(self, config: DataSourceConfig) -> TushareConfigDTO:
        """将通用配置转换为Tushare配置
        
        Args:
            config: 通用数据源配置
            
        Returns:
            TushareConfigDTO: Tushare配置对象
        """
        connection_params = config.connection_params or {}
        return TushareConfigDTO(
            token=connection_params.get('token', ''),
            max_requests_per_minute=connection_params.get('max_requests_per_minute', 200),
            retry_count=connection_params.get('retry_count', 3),
            retry_delay=connection_params.get('retry_delay', 1.0)
        )
        
    def connect(self) -> bool:
        """连接到Tushare API
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 使用传入配置中的token
            if not self._source_config.token:
                self.logger.error("Tushare token未配置")
                return False
            
            # 设置token并创建pro接口
            ts.set_token(self._source_config.token)
            self.pro = ts.pro_api()
            
            # 测试连接
            test_result = self.pro.trade_cal(exchange='', start_date='20240101', end_date='20240102')
            if test_result is not None:
                self._connected = True
                self.logger.info("Tushare API连接成功")
                return True
            else:
                self.logger.error("Tushare API连接测试失败")
                self._connected = False
                return False
                
        except Exception as e:
            self.logger.error(f"连接Tushare API失败: {e}")
            self._connected = False
            return False
    
    def disconnect(self) -> None:
        """断开Tushare连接"""
        self.pro = None
        self._connected = False
        self.logger.info("Tushare连接已断开")
    
    def is_connected(self) -> bool:
        """检查连接状态
        
        Returns:
            bool: 是否已连接
        """
        return self._connected and self.pro is not None
    
    def _rate_limit_check(self) -> None:
        """检查并执行速率限制"""
        current_time = time.time()
        current_minute = int(current_time // 60)
        
        # 如果是新的分钟，重置计数器
        if current_minute != self._minute_start:
            self._minute_start = current_minute
            self._request_count = 0
        
        # 检查是否超过每分钟限制
        if self._request_count >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time % 60)
            self.logger.warning(f"达到速率限制，等待 {sleep_time:.1f} 秒")
            time.sleep(sleep_time)
            self._minute_start = int(time.time() // 60)
            self._request_count = 0
        
        # 增加请求计数
        self._request_count += 1
        self._last_request_time = current_time
    
    def fetch_data(self, endpoint: str, **params) -> pd.DataFrame:
        """获取数据的统一接口
        
        Args:
            endpoint: Tushare API方法名
            **params: 接口参数，支持limitmax参数控制单次请求最大记录数
            
        Returns:
            pd.DataFrame: 数据DataFrame
            
        Raises:
            DataSourceValidationError: 当参数验证失败时
        """
        # 调用父类验证
        super()._validate_fetch_params(endpoint, **params)
        
        # Tushare特定验证
        if not self.is_connected():
            raise RuntimeError("Tushare未连接")
        
        # 提取limitmax参数，默认为3000，提取后在表格删除，因为不需要作为参数传递给tushare
        limitmax = params.pop('limitmax', 3000)
        self.limitmax = limitmax
        
        try:
            # 执行速率限制检查
            self._rate_limit_check()
            
            # 1. 发起请求
            api_method = getattr(self.pro, endpoint)
            self.logger.info(f"[{endpoint}] 1.发起请求 - 调用API方法: {endpoint}")
            
            # 实现翻页逻辑
            all_data = []
            offset = 0
            page_count = 0
            
            while True:
                page_count += 1
                # 添加offset参数（如果需要）
                current_params = params.copy()
                if offset > 0:
                    current_params['offset'] = offset
                # 获取当前页数据（带重试机制）
                current_data = self._fetch_with_retry(api_method, current_params, endpoint)
                
                # 2. 请求结果状态，有几行
                if current_data is None or current_data.empty:
                    self.logger.info(f"[{endpoint}] 2.请求结果状态 - 第{page_count}页: 空数据，停止翻页")
                    break
                
                current_count = len(current_data)
                self.logger.info(f"[{endpoint}] 2.请求结果状态 - 第{page_count}页: 获取到{current_count}行数据")
                all_data.append(current_data)
                
                # 3. 检测limit结果，执行翻页，有几行
                if current_count > self.limitmax:
                    # 更新limitmax值并返回给调用方
                    old_limitmax = self.limitmax
                    self.limitmax = current_count
                    self.logger.info(f"[{endpoint}] 3.检测limit结果 - 当前页{current_count}行 > limitmax({old_limitmax})，已更新limitmax为{self.limitmax}并继续翻页")
                    
                    offset += current_count
                    
                    # 执行速率限制检查（翻页时也需要）
                    self._rate_limit_check()
                elif current_count == self.limitmax:
                    self.logger.info(f"[{endpoint}] 3.检测limit结果 - 当前页{current_count}行 = limitmax({self.limitmax})，继续翻页")
                    
                    offset += current_count
                    
                    # 执行速率限制检查（翻页时也需要）
                    self._rate_limit_check()
                else:
                    # 返回行数 < limitmax，停止翻页
                    self.logger.info(f"[{endpoint}] 3.检测limit结果 - 当前页{current_count}行 < limitmax({self.limitmax})，停止翻页")
                    break
            
            # 4. 合并数据行数
            if all_data:
                merged_data = pd.concat(all_data, ignore_index=True)
                merged_count = len(merged_data)
                self.logger.info(f"[{endpoint}] 4.合并数据行数 - 共{page_count}页数据合并为{merged_count}行")
                
                # 5. 执行去重，去重后行数
                # 所有数据类型统一使用全字段去重
                deduplicated_data = merged_data.drop_duplicates(keep='first')
                
                final_count = len(deduplicated_data)
                removed_count = merged_count - final_count
                if removed_count > 0:
                    self.logger.info(f"[{endpoint}] 5.执行去重 - 去重前{merged_count}行，去重后{final_count}行，去除{removed_count}条重复数据")
                else:
                    self.logger.info(f"[{endpoint}] 5.执行去重 - 无重复数据，保持{final_count}行")
                
                return deduplicated_data
            else:
                self.logger.info(f"[{endpoint}] 4.合并数据行数 - 无数据可合并")
                return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"获取数据失败 [{endpoint}]: {e}")
            raise
    

    

      
    # ==================== 辅助方法 ====================
    

    
    def _fetch_with_retry(self, api_method, params: Dict[str, Any], endpoint_name: str, max_retries: int = None):
        """带重试机制的数据获取方法
        
        Tushare有时会返回假空值（应该有数据但返回空），通过重试可以避免这个问题
        使用指数退避策略进行重试
        
        Args:
            api_method: Tushare API方法
            params: 请求参数
            endpoint_name: 端点名称（用于日志）
            max_retries: 最大重试次数
            
        Returns:
            pd.DataFrame: 获取到的数据
        """
        import time
        
        # 如果未指定重试次数，使用配置中的值
        if max_retries is None:
            max_retries = self.retry_count
        
        for attempt in range(max_retries + 1):  # +1 因为第一次不算重试
            try:
                # 执行速率限制检查
                if attempt > 0:
                    self._rate_limit_check()
                
                # 调用API
                data = api_method(**params)
                
                # 如果获取到数据，直接返回
                if data is not None and not data.empty:
                    if attempt > 0:
                        self.logger.info(f"重试成功 [{endpoint_name}] 第{attempt}次重试获取到{len(data)}条数据")
                    return data
                
                # 如果是空数据且还有重试机会
                if attempt < max_retries:
                    self.logger.warning(f"获取到空数据 [{endpoint_name}] 第{attempt + 1}次尝试，将进行重试")
                    # 指数退避：使用配置的retry_delay作为基础延迟
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                else:
                    # 最后一次尝试仍然是空数据
                    self.logger.info(f"重试{max_retries}次后仍为空数据 [{endpoint_name}]，确认为真空值")
                    return data
                    
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"API调用异常 [{endpoint_name}] 第{attempt + 1}次尝试: {e}，将进行重试")
                    # 指数退避：异常时使用更长的延迟
                    delay = self.retry_delay * (2 ** attempt) * 2
                    time.sleep(delay)
                else:
                    self.logger.error(f"重试{max_retries}次后仍然失败 [{endpoint_name}]: {e}")
                    raise
        
        return pd.DataFrame()
    

    

    

    

