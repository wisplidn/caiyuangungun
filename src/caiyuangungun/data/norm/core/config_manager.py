"""
配置管理器模块
用于读取和管理配置文件，提供统一的配置访问接口
"""

import json
import os
import pandas as pd
from typing import Dict, List, Optional, Any
from pathlib import Path


class ConfigManager:
    """配置管理器类"""
    
    def __init__(self, config_dir: str = None):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径，默认为项目根目录下的data/config
        """
        if config_dir is None:
            # 默认配置目录 - 从当前文件位置计算到项目根目录
            # 当前文件: src/caiyuangungun/data/norm/core/config_manager.py
            # 项目根目录: /Users/daishun/个人文档/caiyuangungun
            project_root = Path(__file__).parent.parent.parent.parent.parent.parent
            config_dir = project_root / "data" / "config"
        
        self.config_dir = Path(config_dir)
        self.project_root = Path(__file__).parent.parent.parent.parent.parent.parent
        self._unified_config = None
        self._path_generator_config = None
        self._tushare_config = None
        self._bse_code_mapping = None  # 北交所代码映射
        self._stock_basic_df = None  # 股票基础信息DataFrame
        self._basic_cleaning_config = None  # 基础清洗配置
        
        # 加载配置文件
        self._load_configs()
    
    def _load_configs(self):
        """加载所有配置文件"""
        try:
            # 加载统一数据配置
            unified_config_path = self.config_dir / "unified_data_config.json"
            if unified_config_path.exists():
                with open(unified_config_path, 'r', encoding='utf-8') as f:
                    self._unified_config = json.load(f)
            
            # 加载路径生成器配置
            path_config_path = self.config_dir / "path_generator_config.json"
            if path_config_path.exists():
                with open(path_config_path, 'r', encoding='utf-8') as f:
                    self._path_generator_config = json.load(f)
            
            # 加载tushare限制配置
            tushare_config_path = self.config_dir / "tushare_limitmax_config.json"
            if tushare_config_path.exists():
                with open(tushare_config_path, 'r', encoding='utf-8') as f:
                    self._tushare_config = json.load(f)
            
            # 加载北交所代码切换配置
            bse_code_path = self.config_dir / "bse_mapping.json"
            if bse_code_path.exists():
                with open(bse_code_path, 'r', encoding='utf-8') as f:
                    self._bse_code_mapping = json.load(f)
            
            # 加载基础清洗配置
            basic_cleaning_path = self.config_dir / "basic_cleaning_pipeline_config.json"
            if basic_cleaning_path.exists():
                with open(basic_cleaning_path, 'r', encoding='utf-8') as f:
                    self._basic_cleaning_config = json.load(f)
                    
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {e}")
    
    def get_stock_basic_info(self) -> pd.DataFrame:
        """
        获取股票基础信息DataFrame
        
        Returns:
            包含ts_code和symbol映射的DataFrame
        """
        if self._stock_basic_df is None:
            self._load_stock_basic_data()
        
        return self._stock_basic_df.copy()
    
    def _load_stock_basic_data(self):
        """加载股票基础数据"""
        try:
            # 读取stock_basic数据
            stock_basic_path = self.project_root / "data" / "raw" / "landing" / "tushare" / "stock_basic" / "data.parquet"
            
            if not stock_basic_path.exists():
                raise FileNotFoundError(f"股票基础数据文件不存在: {stock_basic_path}")
            
            # 读取parquet文件
            stock_basic_df = pd.read_parquet(stock_basic_path)
            
            # 对ts_code和symbol进行bse_mapping
            bse_mapping = self.get_bse_code_mapping()
            
            if bse_mapping:
                # 应用BSE映射到symbol
                stock_basic_df['symbol'] = stock_basic_df['symbol'].map(bse_mapping).fillna(stock_basic_df['symbol'])
                
                # 应用BSE映射到ts_code - 需要处理完整的ts_code格式
                def map_ts_code(ts_code):
                    if pd.isna(ts_code):
                        return ts_code
                    # 提取代码部分（去掉.BJ等后缀）
                    if '.' in str(ts_code):
                        code_part, suffix = str(ts_code).split('.', 1)
                        # 如果代码部分在映射中，则替换
                        if code_part in bse_mapping:
                            return f"{bse_mapping[code_part]}.{suffix}"
                    return ts_code
                
                stock_basic_df['ts_code'] = stock_basic_df['ts_code'].apply(map_ts_code)
            
            # 保留ts_code、symbol、list_status、list_date和delist_date列作为映射表
            self._stock_basic_df = stock_basic_df[['ts_code', 'symbol', 'list_status', 'list_date', 'delist_date']].copy()
            
        except Exception as e:
            raise RuntimeError(f"加载股票基础数据失败: {e}")
    
    def get_bse_mapping_dict(self) -> Dict[str, str]:
        """
        获取BSE映射字典
        
        Returns:
            BSE代码映射字典
        """
        return self.get_bse_code_mapping()
    
    def get_methods_list(self) -> List[str]:
        """
        获取所有可用的methods列表
        
        Returns:
            methods名称列表
        """
        methods = []
        
        if self._unified_config and "data_sources" in self._unified_config:
            for source_name, source_config in self._unified_config["data_sources"].items():
                if "methods" in source_config:
                    methods.extend(source_config["methods"].keys())
        
        return sorted(list(set(methods)))
    
    def get_archive_types_list(self) -> List[str]:
        """
        获取所有可用的archive_types列表
        
        Returns:
            archive_types名称列表
        """
        archive_types = []
        
        if self._path_generator_config and "path_generator" in self._path_generator_config:
            path_gen_config = self._path_generator_config["path_generator"]
            if "archive_types" in path_gen_config:
                archive_types = list(path_gen_config["archive_types"].keys())
        
        return sorted(archive_types)
    
    def get_method_info(self, method_name: str) -> Optional[Dict[str, Any]]:
        """
        根据指定method获取method相关信息
        
        Args:
            method_name: method名称
            
        Returns:
            method配置信息字典，如果不存在返回None
        """
        if not self._unified_config or "data_sources" not in self._unified_config:
            return None
        
        for source_name, source_config in self._unified_config["data_sources"].items():
            if "methods" in source_config and method_name in source_config["methods"]:
                method_info = source_config["methods"][method_name].copy()
                method_info["source_name"] = source_name
                method_info["source_config"] = {
                    "name": source_config.get("name"),
                    "source_type": source_config.get("source_type"),
                    "enabled": source_config.get("enabled"),
                    "class_path": source_config.get("class_path"),
                    "connection_params": source_config.get("connection_params")
                }
                return method_info
        
        return None
    
    def get_archive_type_info(self, archive_type: str) -> Optional[Dict[str, Any]]:
        """
        根据指定archive_type获取路径配置信息
        
        Args:
            archive_type: 归档类型名称
            
        Returns:
            archive_type配置信息字典，如果不存在返回None
        """
        if not self._path_generator_config or "path_generator" not in self._path_generator_config:
            return None
        
        path_gen_config = self._path_generator_config["path_generator"]
        
        if "archive_types" in path_gen_config and archive_type in path_gen_config["archive_types"]:
            archive_info = path_gen_config["archive_types"][archive_type].copy()
            
            # 添加基础路径信息
            archive_info["base_path"] = path_gen_config.get("base_path")
            archive_info["paths"] = path_gen_config.get("paths")
            archive_info["file_config"] = path_gen_config.get("file_config")
            
            return archive_info
        
        return None
    
    def get_base_path(self) -> str:
        """
        获取基础数据路径
        
        Returns:
            基础数据路径
        """
        if self._path_generator_config and "path_generator" in self._path_generator_config:
            return self._path_generator_config["path_generator"].get("base_path", "")
        return ""
    
    def get_norm_base_path(self) -> str:
        """
        获取norm数据基础路径
        从path_generator_config.json读取raw路径并转换为norm路径
        
        Returns:
            norm数据基础路径
        """
        raw_base_path = self.get_base_path()
        if raw_base_path:
            # 将raw路径转换为norm路径
            # 例如: /Users/daishun/个人文档/caiyuangungun/data/raw -> /Users/daishun/个人文档/caiyuangungun/data/norm
            from pathlib import Path
            raw_path = Path(raw_base_path)
            norm_path = raw_path.parent / "norm"
            return str(norm_path)
        return ""
    
    def get_supported_formats(self) -> List[str]:
        """
        获取支持的文件格式列表
        
        Returns:
            支持的文件格式列表
        """
        if (self._path_generator_config and 
            "path_generator" in self._path_generator_config and
            "file_config" in self._path_generator_config["path_generator"]):
            
            file_config = self._path_generator_config["path_generator"]["file_config"]
            return file_config.get("supported_formats", ["parquet", "json"])
        
        return ["parquet", "json"]
    
    def get_default_format(self) -> str:
        """
        获取默认文件格式
        
        Returns:
            默认文件格式
        """
        if (self._path_generator_config and 
            "path_generator" in self._path_generator_config and
            "file_config" in self._path_generator_config["path_generator"]):
            
            file_config = self._path_generator_config["path_generator"]["file_config"]
            return file_config.get("default_format", "parquet")
        
        return "parquet"
    
    def is_method_enabled(self, method_name: str) -> bool:
        """
        检查指定method是否启用
        
        Args:
            method_name: method名称
            
        Returns:
            是否启用
        """
        method_info = self.get_method_info(method_name)
        if method_info:
            return method_info.get("enable", False)
        return False
    
    def is_archive_type_enabled(self, archive_type: str) -> bool:
        """
        检查指定archive_type是否启用
        
        Args:
            archive_type: 归档类型名称
            
        Returns:
            是否启用
        """
        archive_info = self.get_archive_type_info(archive_type)
        if archive_info:
            return archive_info.get("enabled", False)
        return False
    
    def get_bse_code_mapping(self) -> Dict[str, str]:
        """
        获取北交所新老代码映射表
        
        Returns:
            代码映射字典，键为旧代码，值为新代码
        """
        if self._bse_code_mapping and "code_mapping" in self._bse_code_mapping:
            return self._bse_code_mapping["code_mapping"]
        return {}
    
    def load_config(self, config_filename: str) -> Dict:
        """
        加载指定的配置文件
        
        Args:
            config_filename: 配置文件名
            
        Returns:
            配置文件内容字典
        """
        config_path = self.config_dir / config_filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_processor_config(self, processor_name: str) -> Dict:
        """
        获取指定处理器的配置信息
        
        Args:
            processor_name: 处理器名称，如 'fin_is_processor'
            
        Returns:
            处理器配置字典
        """
        # 尝试从specialized_cleaning_pipeline_config.json加载
        try:
            config = self.load_config("specialized_cleaning_pipeline_config.json")
            if processor_name in config:
                return config[processor_name]
        except FileNotFoundError:
            pass
        
        # 如果没有找到，抛出异常
        raise ValueError(f"未找到处理器配置: {processor_name}")
    
    def get_all_processor_configs(self) -> Dict:
        """
        获取所有处理器的配置信息
        
        Returns:
            所有处理器配置字典
        """
        try:
            return self.load_config("specialized_cleaning_pipeline_config.json")
        except FileNotFoundError:
            return {}
    
    def get_processor_pipeline(self, processor_name: str) -> List[Dict]:
        """
        获取指定处理器的清洗流水线配置
        
        Args:
            processor_name: 处理器名称
            
        Returns:
            清洗流水线步骤列表
        """
        processor_config = self.get_processor_config(processor_name)
        return processor_config.get("cleaning_pipeline", [])
    
    def get_processor_paths(self, processor_name: str) -> Dict[str, str]:
        """
        获取指定处理器的输入输出路径配置
        
        Args:
            processor_name: 处理器名称
            
        Returns:
            包含input_path和output_path的字典
        """
        processor_config = self.get_processor_config(processor_name)
        return {
            "input_path": processor_config.get("input_path", ""),
            "output_path": processor_config.get("output_path", "")
        }
    
    def get_basic_cleaning_config(self) -> Dict:
        """
        获取基础清洗配置
        
        Returns:
            基础清洗配置字典
        """
        return self._basic_cleaning_config or {}
    
    def get_enabled_cleaning_pipelines(self, data_type: str) -> Dict[str, List[Dict]]:
        """
        获取指定数据类型的启用清洗流水线
        
        Args:
            data_type: 数据类型，如 'income_statement', 'balancesheet' 等
            
        Returns:
            启用的清洗流水线字典，键为数据源名称，值为清洗步骤列表
        """
        if not self._basic_cleaning_config or data_type not in self._basic_cleaning_config:
            return {}
        
        data_config = self._basic_cleaning_config[data_type]
        cleaning_pipelines = data_config.get("cleaning_pipelines", {})
        
        enabled_pipelines = {}
        for source_name, pipeline_config in cleaning_pipelines.items():
            # 检查新格式（带enabled字段）
            if isinstance(pipeline_config, dict):
                if pipeline_config.get("enabled", True):  # 默认启用
                    enabled_pipelines[source_name] = pipeline_config.get("pipeline", [])
            # 兼容旧格式（直接是列表）
            elif isinstance(pipeline_config, list):
                enabled_pipelines[source_name] = pipeline_config
        
        return enabled_pipelines
