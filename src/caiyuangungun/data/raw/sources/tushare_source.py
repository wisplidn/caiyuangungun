"""Tushare数据源实现

提供Tushare Pro API的数据获取功能，支持股票基础信息、行情数据、财务数据等。
实现了BaseDataSource接口，提供统一的数据访问方式。
基于JSON配置文件自动生成接口调用，支持字段验证和monthly_date自动转换。
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, date
import logging
import time
import re
from dataclasses import dataclass

try:
    import chinadata.ca_data as ts  # tushare的特殊客户通道，修改会导致token失效
except ImportError:
    raise ImportError("请安装chinadata包以使用tushare数据源")

# 使用绝对导入避免相对导入问题
import sys
from pathlib import Path
import importlib.util

# 动态导入BaseDataSource
base_data_source_path = Path(__file__).parent.parent / 'core' / 'base_data_source.py'
if base_data_source_path.exists():
    spec = importlib.util.spec_from_file_location("base_data_source", str(base_data_source_path))
    base_data_source_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base_data_source_module)
    BaseDataSource = base_data_source_module.BaseDataSource
else:
    # 如果找不到BaseDataSource，创建一个简单的基类
    class BaseDataSource:
        def __init__(self):
            pass
        def connect(self):
            return True
        def disconnect(self):
            pass
        def is_connected(self):
            return True
        def fetch_data(self, **kwargs):
            raise NotImplementedError

# 动态导入ConfigManager
config_manager_path = Path(__file__).parent.parent / 'core' / 'config_manager.py'
if config_manager_path.exists():
    spec = importlib.util.spec_from_file_location("config_manager", str(config_manager_path))
    config_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_manager_module)
    ConfigManager = config_manager_module.ConfigManager
    TushareConfig = getattr(config_manager_module, 'TushareConfig', None)
else:
    raise ImportError("找不到config_manager.py文件")


class TushareDataSource(BaseDataSource):
    """Tushare数据源实现
    
    提供Tushare Pro API的数据获取功能。
    基于JSON配置文件自动生成接口调用。
    """
    
    def __init__(self, config: Optional[TushareConfig] = None, config_name: str = "tushare"):
        """初始化Tushare数据源
        
        Args:
            config: Tushare配置，如果为None则从配置管理器加载
            config_name: 配置名称，用于从配置管理器加载配置
        """
        # 保存ConfigManager实例
        self.config_manager = ConfigManager()
        
        if config is None:
            # 从ConfigManager加载tushare数据源配置
            tushare_config = self.config_manager.get_data_source_config('tushare')
            if not tushare_config:
                raise ValueError(f"未找到tushare数据源配置")
            
            # 从connection_params中提取TushareConfig
            conn_params = tushare_config.get('connection_params', {})
            config = TushareConfig(
                token=conn_params.get('token', ''),
                timeout=conn_params.get('timeout', 30),
                max_requests_per_minute=conn_params.get('max_requests_per_minute', 200),
                retry_count=conn_params.get('retry_count', 3),
                retry_delay=conn_params.get('retry_delay', 1.0)
            )
            
            # 保存完整配置数据用于API端点配置
            self._config_data = tushare_config
        else:
            # 如果提供了config，需要单独加载API配置
            tushare_config = self.config_manager.get_data_source_config('tushare')
            if not tushare_config:
                raise ValueError(f"未找到tushare数据源配置")
            self._config_data = tushare_config
        
        super().__init__(config)
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pro = None
        self._last_request_time = 0
        self._request_count = 0
        self._minute_start = 0
        
    def connect(self) -> bool:
        """建立Tushare连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 设置token并初始化pro接口
            ts.set_token(self.config.token)
            self.pro = ts.pro_api()
            
            # 测试连接
            test_df = self.pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240102')
            # 检查返回结果是否为DataFrame且不为空
            if hasattr(test_df, 'empty') and not test_df.empty:
                self._connected = True
                self.logger.info("Tushare连接成功")
                return True
            else:
                # 如果返回的是字符串错误信息，记录具体错误
                if isinstance(test_df, str):
                    self.logger.error(f"Tushare连接测试失败: {test_df}")
                else:
                    self.logger.error("Tushare连接测试失败: 返回数据为空或格式错误")
                return False
                
        except Exception as e:
            self.logger.error(f"Tushare连接失败: {e}")
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
        if self._request_count >= self.config.max_requests_per_minute:
            sleep_time = 60 - (current_time % 60)
            self.logger.warning(f"达到速率限制，等待 {sleep_time:.1f} 秒")
            time.sleep(sleep_time)
            self._minute_start = int(time.time() // 60)
            self._request_count = 0
        
        # 增加请求计数
        self._request_count += 1
        self._last_request_time = current_time
    
    def fetch_data(self, 
                   endpoint_name: str = None,
                   **kwargs) -> pd.DataFrame:
        """获取数据的统一接口
        
        Args:
            endpoint_name: API接口名称，如果不提供则从kwargs中获取
            **kwargs: 接口参数
            
        Returns:
            pd.DataFrame: 数据DataFrame
        """
        if not self.is_connected():
            raise RuntimeError("Tushare未连接")
        
        # 如果没有指定endpoint_name，尝试从kwargs中获取
        if endpoint_name is None:
            endpoint_name = kwargs.pop('endpoint_name', None)
            if endpoint_name is None:
                raise ValueError("必须指定endpoint_name参数")
        
        try:
            # 获取API配置 - 从data_definitions中获取
            api_config = self._config_data.get('data_definitions', {}).get(endpoint_name)
            if not api_config:
                raise ValueError(f"未找到接口配置: {endpoint_name}")
            
            # 将data_definitions格式转换为api_endpoints格式
            if 'method' in api_config:
                api_config['api_method'] = api_config['method']
            if 'required_params' in api_config:
                api_config['required_fields'] = api_config['required_params']
            
            # 参数预处理
            processed_params = self._process_params(kwargs, api_config)
            
            # 保存最后处理的参数，供raw_data_service获取真实API参数
            self._last_processed_params = processed_params.copy()
            
            # 验证必需参数
            self._validate_required_params(processed_params, api_config)
            
            # 字段验证
            self._validate_fields(processed_params)
            
            # 执行速率限制检查
            self._rate_limit_check()
            
            # 1. 发起请求
            method_name = api_config['method']
            api_method = getattr(self.pro, method_name)
            self.logger.info(f"[{endpoint_name}] 1.发起请求 - 调用API方法: {method_name}")
            
            # 实现翻页逻辑
            all_data = []
            offset = 0
            
            # 获取limitmax值：优先从ConfigManager的limitmax配置获取，其次从api_config，最后使用默认值5000
            limitmax_config = self.config_manager.get_limitmax_config(endpoint_name)
            if limitmax_config and 'limitmax' in limitmax_config:
                limitmax = limitmax_config['limitmax']
            else:
                limitmax = api_config.get('limitmax', 3000)  # 使用更合理的默认值3000
            
            page_count = 0
            
            while True:
                page_count += 1
                # 添加offset参数（如果需要）
                current_params = processed_params.copy()
                if offset > 0:
                    current_params['offset'] = offset
                
                # 获取当前页数据（带重试机制）
                current_data = self._fetch_with_retry(api_method, current_params, endpoint_name)
                
                # 2. 请求结果状态，有几行
                if current_data is None or current_data.empty:
                    self.logger.info(f"[{endpoint_name}] 2.请求结果状态 - 第{page_count}页: 空数据，停止翻页")
                    break
                
                current_count = len(current_data)
                self.logger.info(f"[{endpoint_name}] 2.请求结果状态 - 第{page_count}页: 获取到{current_count}行数据")
                all_data.append(current_data)
                
                # 3. 检测limit结果，执行翻页，有几行
                if current_count > limitmax:
                    # 更新配置中的limitmax
                    self._update_limitmax(endpoint_name, current_count)
                    old_limitmax = limitmax
                    limitmax = current_count
                    self.logger.info(f"[{endpoint_name}] 3.检测limit结果 - 当前页{current_count}行 > limitmax({old_limitmax})，已更新limitmax为{limitmax}并继续翻页")
                    
                    offset += current_count
                    
                    # 执行速率限制检查（翻页时也需要）
                    self._rate_limit_check()
                elif current_count == limitmax:
                    self.logger.info(f"[{endpoint_name}] 3.检测limit结果 - 当前页{current_count}行 = limitmax({limitmax})，继续翻页")
                    
                    offset += current_count
                    
                    # 执行速率限制检查（翻页时也需要）
                    self._rate_limit_check()
                else:
                    # 返回行数 < limitmax，停止翻页
                    self.logger.info(f"[{endpoint_name}] 3.检测limit结果 - 当前页{current_count}行 < limitmax({limitmax})，停止翻页")
                    break
            
            # 4. 合并数据行数
            if all_data:
                merged_data = pd.concat(all_data, ignore_index=True)
                merged_count = len(merged_data)
                self.logger.info(f"[{endpoint_name}] 4.合并数据行数 - 共{page_count}页数据合并为{merged_count}行")
                
                # 5. 执行去重，去重后行数
                # 所有数据类型统一使用全字段去重
                deduplicated_data = merged_data.drop_duplicates(keep='first')
                
                final_count = len(deduplicated_data)
                removed_count = merged_count - final_count
                if removed_count > 0:
                    self.logger.info(f"[{endpoint_name}] 5.执行去重 - 去重前{merged_count}行，去重后{final_count}行，去除{removed_count}条重复数据")
                else:
                    self.logger.info(f"[{endpoint_name}] 5.执行去重 - 无重复数据，保持{final_count}行")
                
                return deduplicated_data
            else:
                self.logger.info(f"[{endpoint_name}] 4.合并数据行数 - 无数据可合并")
                return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"获取数据失败 [{endpoint_name}]: {e}")
            raise
    
    def _process_params(self, params: Dict[str, Any], api_config: Dict[str, Any]) -> Dict[str, Any]:
        """参数预处理，包括monthly_date转换
        
        Args:
            params: 输入参数
            api_config: API配置
            
        Returns:
            处理后的参数字典
        """
        processed_params = params.copy()
        
        # 处理monthly_date转换
        if 'monthly_date' in processed_params:
            monthly_date = processed_params.pop('monthly_date')
            if isinstance(monthly_date, str) and len(monthly_date) == 6:
                year = monthly_date[:4]
                month = monthly_date[4:6]
                
                # 转换为start_date和end_date
                start_date = f"{year}{month}01"
                
                # 计算月末日期
                if month == '12':
                    next_year = str(int(year) + 1)
                    next_month = '01'
                else:
                    next_year = year
                    next_month = f"{int(month) + 1:02d}"
                
                from datetime import datetime, timedelta
                next_month_first = datetime.strptime(f"{next_year}{next_month}01", "%Y%m%d")
                end_date = (next_month_first - timedelta(days=1)).strftime("%Y%m%d")
                
                processed_params['start_date'] = start_date
                processed_params['end_date'] = end_date
                
                self.logger.debug(f"monthly_date {monthly_date} 转换为 start_date={start_date}, end_date={end_date}")
        
        return processed_params
    
    def _validate_required_params(self, params: Dict[str, Any], api_config: Dict[str, Any]) -> None:
        """验证必需参数
        
        Args:
            params: 输入参数
            api_config: API配置
            
        Raises:
            ValueError: 缺少必需参数或有多余参数
        """
        if isinstance(api_config, list):
            # 兼容直接传入required_params列表的情况
            required_params = api_config
        else:
            required_params = api_config.get('required_params', [])
        
        # 如果required_params是字典（包含默认值），提取键
        if isinstance(required_params, dict):
            required_param_names = list(required_params.keys())
        else:
            required_param_names = required_params
        
        # 对于monthly_date，检查是否已转换为start_date和end_date
        if 'monthly_date' in required_param_names:
            if 'monthly_date' not in params and ('start_date' in params and 'end_date' in params):
                # monthly_date已被转换，更新required_params
                required_param_names = [p for p in required_param_names if p != 'monthly_date']
                required_param_names.extend(['start_date', 'end_date'])
        
        # 检查缺少的参数
        missing_params = set(required_param_names) - set(params.keys())
        if missing_params:
            raise ValueError(f"缺少必需参数: {', '.join(missing_params)}")
        
        # 对于有默认值的配置，不检查多余参数
        if not isinstance(required_params, dict):
            # 检查多余的参数（有且仅有）
            extra_params = set(params.keys()) - set(required_param_names)
            if extra_params:
                raise ValueError(f"包含多余参数: {', '.join(extra_params)}，required_params意味着有且仅有这些参数: {', '.join(required_param_names)}")
    
    def _validate_fields(self, params: Dict[str, Any], validation_rules: Dict[str, Any] = None) -> None:
        """字段验证
        
        Args:
            params: 参数字典
            validation_rules: 验证规则，如果不提供则从配置中获取
            
        Raises:
            ValueError: 当字段格式不正确时
        """
        if validation_rules is None:
            field_validation = self._config_data.get('field_validation', {})
        else:
            field_validation = validation_rules
        
        for param_name, param_value in params.items():
            if param_name in field_validation:
                validation_rule = field_validation[param_name]
                
                # 类型检查
                expected_type = validation_rule.get('type')
                if expected_type == 'string' and not isinstance(param_value, str):
                    raise ValueError(f"参数 {param_name} 必须是字符串类型")
                
                # 正则表达式验证
                pattern = validation_rule.get('pattern')
                if pattern and isinstance(param_value, str):
                    if not re.match(pattern, param_value):
                        description = validation_rule.get('description', '格式不正确')
                        raise ValueError(f"参数 {param_name} {description}，当前值: {param_value}")
  
    # ==================== 抽象方法实现 ====================
    
    def get_available_assets(self) -> List[str]:
        """获取可用资产列表
        
        Returns:
            可用资产代码列表
        """
        try:
            # 获取股票基础信息
            df = self.fetch_data('stock_basic')
            return df['ts_code'].tolist() if not df.empty else []
        except Exception as e:
            self.logger.error(f"获取可用资产列表失败: {e}")
            return []
    
    def validate_asset(self, asset: str) -> bool:
        """验证资产代码是否有效
        
        Args:
            asset: 资产代码
            
        Returns:
            是否有效
        """
        if not asset or not isinstance(asset, str):
            return False
        
        # 简单的格式验证：6位数字.交易所代码
        import re
        pattern = r'^\d{6}\.(SH|SZ)$'
        return bool(re.match(pattern, asset))
      
    # ==================== 辅助方法 ====================
    
    def save_data(self, data: pd.DataFrame, source_name: str, data_type: str, params: Dict[str, Any] = None) -> bool:
        """保存数据到文件系统
        
        Args:
            data: 要保存的数据
            source_name: 数据源名称
            data_type: 数据类型
            params: 获取数据时使用的参数
            
        Returns:
            bool: 是否保存了新数据（如果数据未变更则返回False）
        """
        if data is None or data.empty:
            self.logger.warning(f"[{source_name}:{data_type}] 数据为空，跳过保存")
            return False
        
        # 生成文件路径
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        data_file_path, config_file_path = self._generate_file_paths(source_name, data_type, timestamp)
        
        # 6. 准备写入，执行MD5检查
        data_md5 = self._calculate_md5(data)
        self.logger.info(f"[{source_name}:{data_type}] 6.准备写入 - 计算数据MD5: {data_md5[:8]}...")
        
        # 准备配置数据
        config_data = {
            'source_name': source_name,
            'data_type': data_type,
            'timestamp': timestamp,
            'md5': data_md5,
            'row_count': len(data),
            'params': params or {}
        }
        
        # 检查文件是否已存在
        if os.path.exists(data_file_path) and os.path.exists(config_file_path):
            # 加载现有配置
            existing_config = self._load_config(config_file_path)
            existing_md5 = existing_config.get('md5', '')
            
            # 比较MD5值
            if existing_md5 == data_md5:
                # 数据相同，跳过保存
                self.logger.info(f"[{source_name}:{data_type}] 6.MD5检查 - 数据未变更，跳过保存")
                return False
            else:
                # 7. 检查到发生变更，转移文件
                self.logger.info(f"[{source_name}:{data_type}] 7.检查到发生变更 - MD5不同({existing_md5[:8]}... -> {data_md5[:8]}...)，转移现有文件")
                self._archive_existing_files(data_file_path, config_file_path, existing_config)
        else:
            self.logger.info(f"[{source_name}:{data_type}] 6.MD5检查 - 新文件，准备保存")
        
        # 8. 保存成功
        data.to_json(data_file_path, orient='records', date_format='iso', indent=2)
        self._save_config(config_file_path, config_data)
        self.logger.info(f"[{source_name}:{data_type}] 8.保存成功 - 数据文件和配置文件已保存")
        
        return True
    
    def _fetch_with_retry(self, api_method, params: Dict[str, Any], endpoint_name: str, max_retries: int = None):
        """带重试机制的数据获取方法
        
        Tushare有时会返回假空值（应该有数据但返回空），通过重试可以避免这个问题
        
        Args:
            api_method: Tushare API方法
            params: 请求参数
            endpoint_name: 端点名称（用于日志）
            max_retries: 最大重试次数
            
        Returns:
            pd.DataFrame: 获取到的数据
        """
        import time
        
        # 从配置中获取重试次数，如果未指定则使用配置文件中的默认值
        if max_retries is None:
            max_retries = self.config_manager.get('retry_count', 3)
        
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
                    # 短暂等待后重试
                    time.sleep(0.5)
                else:
                    # 最后一次尝试仍然是空数据
                    self.logger.info(f"重试{max_retries}次后仍为空数据 [{endpoint_name}]，确认为真空值")
                    return data
                    
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"API调用异常 [{endpoint_name}] 第{attempt + 1}次尝试: {e}，将进行重试")
                    time.sleep(1)  # 异常时等待更长时间
                else:
                    self.logger.error(f"重试{max_retries}次后仍然失败 [{endpoint_name}]: {e}")
                    raise
        
        return pd.DataFrame()  # 理论上不会到达这里
    
    def _update_limitmax(self, endpoint_name: str, new_limitmax: int):
        """更新配置文件中指定端点的limitmax值
        
        Args:
            endpoint_name: API端点名称
            new_limitmax: 新的limitmax值
        """
        try:
            # 更新内存中的配置
            if 'data_definitions' in self._config_data and endpoint_name in self._config_data['data_definitions']:
                self._config_data['data_definitions'][endpoint_name]['limitmax'] = new_limitmax
                
                # 使用新的ConfigManager API更新limitmax配置
                self.config_manager.update_limitmax_config(endpoint_name, new_limitmax)
                
                # 9. 更新limitmax信息
                self.logger.info(f"[{endpoint_name}] 9.更新limitmax信息 - 已更新limitmax为{new_limitmax}")
            else:
                self.logger.warning(f"未找到端点配置: {endpoint_name}")
                
        except Exception as e:
            self.logger.error(f"更新limitmax失败 [{endpoint_name}]: {e}")
    

    

