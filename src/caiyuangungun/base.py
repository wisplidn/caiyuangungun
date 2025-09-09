"""
数据分层架构的基础类
"""
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime, date
from abc import ABC, abstractmethod
import logging

from .contracts import DataContract, DataLayer, InterfaceType, DEFAULT_CONTRACT, DataSource


class BaseDataManager(ABC):
    """数据管理器基类"""
    
    def __init__(self, 
                 data_root: Union[str, Path],
                 contract: DataContract = None):
        """
        初始化数据管理器
        
        Args:
            data_root: 数据根目录
            contract: 数据契约配置
        """
        self.data_root = Path(data_root)
        self.contract = contract or DEFAULT_CONTRACT
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 确保目录存在
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保必要目录存在"""
        for layer in DataLayer:
            layer_path = self.data_root / layer.value
            layer_path.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def get_data_path(self, interface_type: InterfaceType, **kwargs) -> Path:
        """获取数据文件路径"""
        pass
    
    @abstractmethod
    def save_data(self, data: pd.DataFrame, interface_type: InterfaceType, **kwargs) -> Path:
        """保存数据"""
        pass
    
    @abstractmethod
    def load_data(self, interface_type: InterfaceType, **kwargs) -> pd.DataFrame:
        """加载数据"""
        pass


class RawDataManager(BaseDataManager):
    """Raw层数据管理器 - 唯一真实来源"""
    
    def __init__(self, data_root: Union[str, Path], contract: DataContract = None):
        super().__init__(data_root, contract)
        self.raw_path = self.data_root / "raw"
    
    def get_data_path(self, 
                      interface_type: InterfaceType,
                      source: str = "tushare",
                      stage: str = "landing",
                      **kwargs) -> Path:
        """
        获取Raw层数据路径
        
        Args:
            interface_type: 接口类型
            source: 数据源 (tushare, manual)
            stage: 阶段 (landing, norm, archive)
        """
        return self.raw_path / stage / source / f"{interface_type.value}.parquet"
    
    def save_data(self, 
                  data: pd.DataFrame,
                  interface_type: InterfaceType,
                  source: str = "tushare", 
                  stage: str = "landing",
                  **kwargs) -> Path:
        """保存Raw层数据为Parquet格式"""
        file_path = self.get_data_path(interface_type, source, stage, **kwargs)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 添加元数据
        metadata = {
            "source": source,
            "interface_type": interface_type.value,
            "stage": stage,
            "created_at": datetime.now().isoformat(),
            "primary_keys": json.dumps(self.contract.primary_keys.get(interface_type, [])),
            "row_count": len(data)
        }
        
        table = pa.Table.from_pandas(data)
        table = table.replace_schema_metadata(metadata)
        
        pq.write_table(
            table, 
            file_path,
            compression=self.contract.storage_config.parquet_compression,
            row_group_size=self.contract.storage_config.parquet_row_group_size
        )
        
        self.logger.info(f"Saved {len(data)} rows to {file_path}")
        return file_path
    
    def load_data(self, 
                  interface_type: InterfaceType,
                  source: str = "tushare",
                  stage: str = "landing", 
                  **kwargs) -> pd.DataFrame:
        """加载Raw层数据"""
        file_path = self.get_data_path(interface_type, source, stage, **kwargs)
        
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        
        return pd.read_parquet(file_path)


class NormDataManager(BaseDataManager):
    """Norm层数据管理器 - 接口级规范化"""
    
    # 统一将传入的来源标识（DataSource 或 str）转成用于文件名前缀的字符串
    @staticmethod
    def _source_prefix(source: Optional[Union[str, DataSource]]) -> str:
        if source is None:
            return ""
        if isinstance(source, DataSource):
            return f"{source.name.lower()}_"
        return f"{str(source).lower()}_"
    
    def __init__(self, data_root: Union[str, Path], contract: DataContract = None):
        super().__init__(data_root, contract)
        self.norm_path = self.data_root / "norm"
    
    def get_data_path(self, 
                      interface_type: InterfaceType,
                      partition: Optional[str] = None,
                      source: Optional[Union[str, DataSource]] = None,
                      **kwargs) -> Path:
        """获取Norm层数据路径
        命名支持：
        - 默认：by_interface/<interface>.csv
        - 分区：by_interface/<interface>/<partition>.csv
        - 带来源：在文件名追加前缀“<source>_”，即 <source>_<interface>.csv 或 <source>_<partition>.csv
        """
        base_path = self.norm_path / "by_interface" / interface_type.value
        prefix = self._source_prefix(source)
        
        if partition:
            return base_path / f"{prefix}{partition}.csv"
        else:
            return base_path / f"{prefix}{interface_type.value}.csv"
    
    def get_schema_path(self, interface_type: InterfaceType, source: Optional[Union[str, DataSource]] = None) -> Path:
        """获取Schema文件路径，按需带来源前缀"""
        prefix = self._source_prefix(source)
        return self.norm_path / "schemas" / f"{prefix}{interface_type.value}_schema.json"
    
    def get_decisions_path(self, interface_type: InterfaceType, source: Optional[Union[str, DataSource]] = None) -> Path:
        """获取决策记录文件路径，按需带来源前缀"""
        prefix = self._source_prefix(source)
        return self.norm_path / "decisions" / f"{prefix}{interface_type.value}_decisions.jsonl"
    
    def save_schema(self, interface_type: InterfaceType, schema: Dict[str, Any], source: Optional[Union[str, DataSource]] = None) -> Path:
        """保存数据Schema"""
        schema_path = self.get_schema_path(interface_type, source)
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(schema_path, 'w', encoding='utf-8') as f:
            json.dump(schema, f, ensure_ascii=False, indent=2)
        
        return schema_path
    
    def save_decisions(self, 
                       interface_type: InterfaceType, 
                       decisions: List[Dict[str, Any]],
                       source: Optional[Union[str, DataSource]] = None) -> Path:
        """保存去重/合并决策记录"""
        decisions_path = self.get_decisions_path(interface_type, source)
        decisions_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(decisions_path, 'a', encoding='utf-8') as f:
            for decision in decisions:
                f.write(json.dumps(decision, ensure_ascii=False) + '\n')
        
        return decisions_path
    
    def save_data(self, 
                  data: pd.DataFrame,
                  interface_type: InterfaceType,
                  partition: Optional[str] = None,
                  decisions: Optional[List[Dict[str, Any]]] = None,
                  source: Optional[Union[str, DataSource]] = None,
                  **kwargs) -> Path:
        """保存Norm层数据为CSV格式；当传入source时采用“来源_原成分”命名。"""
        file_path = self.get_data_path(interface_type, partition, source=source, **kwargs)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存CSV
        data.to_csv(file_path, index=False, encoding='utf-8')
        
        # 保存Schema
        schema = {
            "interface_type": interface_type.value,
            "columns": {col: str(dtype) for col, dtype in data.dtypes.items()},
            "primary_keys": self.contract.primary_keys.get(interface_type, []),
            "row_count": len(data),
            "created_at": datetime.now().isoformat(),
            "partition": partition,
            "source": source.name.lower() if isinstance(source, DataSource) else (str(source).lower() if source else None),
        }
        self.save_schema(interface_type, schema, source=source)
        
        # 保存决策记录
        if decisions:
            self.save_decisions(interface_type, decisions, source=source)
        
        self.logger.info(f"Saved {len(data)} rows to {file_path}")
        return file_path
    
    def load_data(self, 
                  interface_type: InterfaceType,
                  partition: Optional[str] = None,
                  source: Optional[Union[str, DataSource]] = None,
                  **kwargs) -> pd.DataFrame:
        """加载Norm层数据，支持按来源前缀命名加载"""
        file_path = self.get_data_path(interface_type, partition, source=source, **kwargs)
        
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        
        return pd.read_csv(file_path, encoding='utf-8')





class QlibReadyManager(BaseDataManager):
    """Qlib就绪层数据管理器 - 按标的拆分CSV，包含所有特征"""
    
    def __init__(self, data_root: Union[str, Path], contract: DataContract = None):
        super().__init__(data_root, contract)
        self.qlib_ready_path = self.data_root / "qlib-ready"
    
    def get_data_path(self, 
                      symbol: str,
                      data_type: str = "features",
                      **kwargs) -> Path:
        """
        获取Qlib就绪数据路径
        
        Args:
            symbol: 股票代码 (Qlib格式 SH600519)
            data_type: 数据类型 (features包含行情+财务+分析师等所有特征)
        """
        return self.qlib_ready_path / f"{symbol}.csv"
    
    def save_data(self, 
                  data: pd.DataFrame,
                  symbol: str,
                  **kwargs) -> Path:
        """保存Qlib就绪数据 - 包含所有特征的宽表"""
        file_path = self.get_data_path(symbol, **kwargs)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 确保数据按日期升序排列
        date_col = None
        for col in ['date', 'trade_date', 'datetime']:
            if col in data.columns:
                date_col = col
                break
        
        if date_col:
            data = data.sort_values(date_col)
        
        # 保存为CSV，列名使用小写下划线格式（Qlib友好）
        data.to_csv(file_path, index=False)
        
        self.logger.info(f"Saved {len(data)} rows for {symbol} to {file_path}")
        return file_path
    
    def load_data(self, 
                  symbol: str,
                  **kwargs) -> pd.DataFrame:
        """加载Qlib就绪数据"""
        file_path = self.get_data_path(symbol, **kwargs)
        
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
        
        return pd.read_csv(file_path)


class DataLayerFactory:
    """数据层管理器工厂类"""
    
    @staticmethod
    def create_manager(layer: DataLayer, 
                       data_root: Union[str, Path],
                       contract: DataContract = None) -> BaseDataManager:
        """创建对应层的数据管理器"""
        managers = {
            DataLayer.RAW: RawDataManager,
            DataLayer.NORM: NormDataManager,
            DataLayer.QLIB_READY: QlibReadyManager
        }
        
        manager_class = managers.get(layer)
        if not manager_class:
            raise ValueError(f"Unsupported data layer: {layer}")
        
        return manager_class(data_root, contract)
