"""AkShare数据源实现

提供AkShare库的数据获取功能，支持东方财富等多个数据源的股票数据。
实现了BaseDataSource接口，提供统一的数据访问方式。
基于JSON配置文件自动生成接口调用，支持字段验证和quarterly_date自动转换。
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, date
import logging
import time
import re
from dataclasses import dataclass

try:
    import akshare as ak
except ImportError:
    raise ImportError("请安装akshare包以使用akshare数据源")

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
else:
    raise ImportError("找不到config_manager.py文件")

# 动态导入UniversalArchiver
universal_archiver_path = Path(__file__).parent.parent / 'core' / 'universal_archiver.py'
if universal_archiver_path.exists():
    spec = importlib.util.spec_from_file_location("universal_archiver", str(universal_archiver_path))
    universal_archiver_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(universal_archiver_module)
    UniversalArchiver = universal_archiver_module.UniversalArchiver
else:
    UniversalArchiver = None


class AkshareDataSource(BaseDataSource):
    """AkShare数据源实现
    
    提供AkShare库的数据获取功能。
    基于JSON配置文件自动生成接口调用。
    """
    
    def __init__(self, config_name: str = "akshare"):
        """初始化AkShare数据源
        
        Args:
            config_name: 配置名称，用于从配置管理器加载配置
        """
        # 保存ConfigManager实例
        self.config_manager = ConfigManager()
        
        # 从ConfigManager加载akshare数据源配置
        akshare_config = self.config_manager.get_data_source_config('akshare')
        if not akshare_config:
            raise ValueError(f"未找到akshare数据源配置")
        
        # 保存完整配置数据用于API端点配置
        self._config_data = akshare_config
        
        # 初始化通用归档器
        if UniversalArchiver:
            self.archiver = UniversalArchiver(source_name='akshare')
        else:
            self.archiver = None
        
        # 设置日志
        self.logger = logging.getLogger(f"{__name__}.AkshareDataSource")
        
        # 连接状态
        self._connected = False
    
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
    
    def fetch_data(self, data_type: str, **kwargs) -> pd.DataFrame:
        """基于配置获取数据的通用方法
        
        Args:
            data_type: 数据类型，对应配置中的data_definitions键
            **kwargs: 请求参数
            
        Returns:
            pd.DataFrame: 获取的数据
            
        Raises:
            ValueError: 当数据类型不存在或参数无效时
            RuntimeError: 当数据源未连接或获取数据失败时
        """
        if not self.is_connected():
            raise RuntimeError("数据源未连接，请先调用connect()方法")
        
        # 获取数据定义配置
        data_definitions = self._config_data.get('data_definitions', {})
        if data_type not in data_definitions:
            raise ValueError(f"未找到数据类型 '{data_type}' 的配置")
        
        data_config = data_definitions[data_type]
        method_name = data_config.get('method')
        if not method_name:
            raise ValueError(f"数据类型 '{data_type}' 缺少method配置")
        
        # 处理请求参数
        processed_params = self.config_manager.process_request_params(
            data_config.get('required_params', {}), 
            **kwargs
        )
        
        # 验证参数
        self._validate_params(processed_params, data_config.get('field_validation', {}))
        
        try:
            # 调用akshare方法
            akshare_method = getattr(ak, method_name)
            self.logger.info(f"调用 ak.{method_name} 获取 {data_type} 数据，参数: {processed_params}")
            
            data = akshare_method(**processed_params)
            
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.warning(f"获取到空数据: {data_type}")
                return pd.DataFrame()
            
            self.logger.info(f"成功获取 {data_type} 数据，行数: {len(data)}")
            
            # 使用通用归档器保存数据
            if self.archiver:
                # 从配置中获取存储类型
                data_config = self._config_data.get('data_definitions', {}).get(data_type, {})
                archive_type = data_config.get('storage_type', 'SNAPSHOT')
                
                # 为归档准备参数，确保参数名与路径生成器期望的一致
                archive_kwargs = kwargs.copy()
                # 特殊处理：对于东财财报数据，将date转换为quarterly_date
                financial_report_types = ['stock_lrb_em', 'stock_zcfz_em', 'stock_zcfz_bj_em', 'stock_xjll_em']
                if data_type in financial_report_types and 'date' in archive_kwargs:
                    archive_kwargs['quarterly_date'] = archive_kwargs.pop('date')
                
                self.archiver.archive_data(
                    data=data,
                    source_name='akshare',
                    data_type=data_type,
                    archive_type=archive_type,
                    **archive_kwargs
                )
            
            return data
            
        except Exception as e:
            self.logger.error(f"获取 {data_type} 数据失败: {e}")
            raise RuntimeError(f"获取 {data_type} 数据失败: {e}")
    
    def get_api_config(self, data_type: str) -> Dict[str, Any]:
        """获取指定数据类型的API配置
        
        Args:
            data_type: 数据类型名称
            
        Returns:
            Dict[str, Any]: API配置信息
            
        Raises:
            ValueError: 当数据类型不存在时
        """
        data_definitions = self._config_data.get('data_definitions', {})
        if data_type not in data_definitions:
            raise ValueError(f"未找到数据类型 '{data_type}' 的配置")
        
        api_config = data_definitions[data_type].copy()
        
        # 转换为与tushare兼容的格式
        if 'method' in api_config:
            api_config['api_method'] = api_config['method']
        if 'required_params' in api_config:
            api_config['required_fields'] = api_config['required_params']
            
        return api_config
    
    def _process_params(self, params: Dict[str, Any], api_config: Dict[str, Any]) -> Dict[str, Any]:
        """处理请求参数
        
        Args:
            params: 原始参数
            api_config: API配置
            
        Returns:
            Dict[str, Any]: 处理后的参数
        """
        # 使用ConfigManager处理参数
        required_params = api_config.get('required_params', {})
        return self.config_manager.process_request_params(required_params, **params)
    
    def _validate_params(self, params: Dict[str, Any], validation_rules: Dict[str, Any] = None) -> None:
        """验证请求参数
        
        Args:
            params: 请求参数
            validation_rules: 验证规则，如果为None则使用配置中的规则
            
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
    
    def get_available_assets(self) -> List[str]:
        """获取可用资产列表
        
        Returns:
            List[str]: 资产代码列表
        """
        try:
            # 获取A股股票列表
            stock_list = ak.stock_zh_a_spot_em()
            return stock_list['代码'].tolist() if '代码' in stock_list.columns else []
        except Exception as e:
            self.logger.error(f"获取资产列表失败: {e}")
            return []
    
    def validate_asset(self, asset: str) -> bool:
        """验证资产代码是否有效
        
        Args:
            asset: 资产代码
            
        Returns:
            bool: 资产代码是否有效
        """
        # 简单的股票代码格式验证
        if len(asset) == 6 and asset.isdigit():
            return True
        return False
    
    # ==================== 具体数据获取方法 ====================
    
    def get_stock_lrb_em(self, quarterly_date: str) -> pd.DataFrame:
        """获取东方财富利润表数据
        
        Args:
            quarterly_date: 季报日期，格式为"YYYYMMDD"，如"20240331"
                 可选值: {"XXXX0331", "XXXX0630", "XXXX0930", "XXXX1231"}
                 从20081231开始
        
        Returns:
            pd.DataFrame: 利润表数据
            
        Raises:
            ValueError: 日期格式不正确
            RuntimeError: 数据源未连接或获取数据失败
        """
        try:
            data = self.fetch_data('stock_lrb_em', date=quarterly_date)
            return {
                'success': True,
                'data': data.to_dict('records') if not data.empty else [],
                'errors': []
            }
        except Exception as e:
            return {
                'success': False,
                'data': [],
                'errors': [str(e)]
            }