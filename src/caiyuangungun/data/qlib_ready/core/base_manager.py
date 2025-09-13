"""QLIB-READY层基础管理器抽象类

定义所有数据域管理器的通用接口和基础功能。
"""

import os
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, date
import json
import logging

from ....base import BaseDataManager
from ....contracts import DataLayer, InterfaceType, DataContract

logger = logging.getLogger(__name__)


class BaseQlibManager(BaseDataManager, ABC):
    """QLIB-READY层基础管理器
    
    定义所有数据域管理器的通用接口，包括：
    - 数据路径管理
    - 数据存储和读取
    - 元数据管理
    - 质量检查
    """
    
    def __init__(self, data_root: str, domain_name: str):
        """初始化基础管理器
        
        Args:
            data_root: 数据根目录路径
            domain_name: 数据域名称（如quotes、financials等）
        """
        super().__init__(data_root)
        self.data_layer = "qlib-ready"
        self.domain_name = domain_name
        self.qlib_ready_path = Path(data_root) / "qlib-ready"
        self.domain_path = self.qlib_ready_path / domain_name
        
        # 确保目录存在
        self.domain_path.mkdir(parents=True, exist_ok=True)
    
    @property
    @abstractmethod
    def supported_interface_types(self) -> List[InterfaceType]:
        """获取支持的接口类型
        
        Returns:
            List[InterfaceType]: 支持的接口类型列表
        """
        pass
    
    @abstractmethod
    def get_processor(self):
        """获取对应的数据处理器
        
        Returns:
            BaseQlibProcessor: 数据处理器实例
        """
        pass
    
    def get_data_path(self, 
                      symbol: str,
                      interface_type: InterfaceType = None,
                      feature_name: str = None,
                      **kwargs) -> str:
        """获取数据文件路径
        
        Args:
            symbol: 股票代码（如 000001.SZ）
            interface_type: 数据接口类型（可选）
            feature_name: 特征名称（可选）
            **kwargs: 其他参数
            
        Returns:
            str: 数据文件路径
        """
        if feature_name:
            # 按特征分组的路径
            feature_dir = self.domain_path / "features" / feature_name
            feature_dir.mkdir(parents=True, exist_ok=True)
            return str(feature_dir / f"{symbol}.csv")
        elif interface_type:
            # 按接口类型分组的路径
            interface_dir = self.domain_path / interface_type.value
            interface_dir.mkdir(parents=True, exist_ok=True)
            return str(interface_dir / f"{symbol}.csv")
        else:
            # 默认路径
            return str(self.domain_path / f"{symbol}.csv")
    
    def save_data(self, 
                  data: pd.DataFrame,
                  symbol: str = None,
                  interface_type: InterfaceType = None,
                  feature_name: str = None,
                  **kwargs) -> str:
        """保存数据到文件
        
        Args:
            data: 要保存的数据
            symbol: 股票代码
            interface_type: 数据接口类型
            feature_name: 特征名称
            **kwargs: 其他参数
            
        Returns:
            str: 保存的文件路径
        """
        if symbol:
            file_path = self.get_data_path(
                symbol=symbol,
                interface_type=interface_type,
                feature_name=feature_name,
                **kwargs
            )
        else:
            # 批量保存的情况
            if feature_name:
                file_path = str(self.domain_path / "features" / f"{feature_name}.csv")
            else:
                file_path = str(self.domain_path / "data.csv")
        
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # 保存数据
        data.to_csv(file_path, index=False)
        logger.info(f"数据已保存到: {file_path}，行数: {len(data)}")
        
        return file_path
    
    def load_data(self, 
                  symbol: str = None,
                  interface_type: InterfaceType = None,
                  feature_name: str = None,
                  **kwargs) -> Optional[pd.DataFrame]:
        """从文件加载数据
        
        Args:
            symbol: 股票代码
            interface_type: 数据接口类型
            feature_name: 特征名称
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 加载的数据，如果文件不存在则返回None
        """
        if symbol:
            file_path = self.get_data_path(
                symbol=symbol,
                interface_type=interface_type,
                feature_name=feature_name,
                **kwargs
            )
        else:
            if feature_name:
                file_path = str(self.domain_path / "features" / f"{feature_name}.csv")
            else:
                file_path = str(self.domain_path / "data.csv")
        
        if not os.path.exists(file_path):
            logger.warning(f"数据文件不存在: {file_path}")
            return None
        
        try:
            data = pd.read_csv(file_path)
            logger.info(f"数据已加载: {file_path}，行数: {len(data)}")
            return data
        except Exception as e:
            logger.error(f"加载数据失败: {file_path}，错误: {e}")
            return None
    
    def list_symbols(self, 
                     interface_type: InterfaceType = None,
                     feature_name: str = None) -> List[str]:
        """列出所有可用的股票代码
        
        Args:
            interface_type: 数据接口类型
            feature_name: 特征名称
            
        Returns:
            List[str]: 股票代码列表
        """
        if feature_name:
            search_dir = self.domain_path / "features" / feature_name
        elif interface_type:
            search_dir = self.domain_path / interface_type.value
        else:
            search_dir = self.domain_path
        
        if not search_dir.exists():
            return []
        
        symbols = []
        for file_path in search_dir.glob("*.csv"):
            symbol = file_path.stem
            if self._is_valid_symbol(symbol):
                symbols.append(symbol)
        
        return sorted(symbols)
    
    def _is_valid_symbol(self, symbol: str) -> bool:
        """验证股票代码格式
        
        Args:
            symbol: 股票代码
            
        Returns:
            bool: 是否为有效的股票代码
        """
        # 简单的股票代码格式验证
        if not symbol:
            return False
        
        # 检查是否包含交易所后缀
        if symbol.endswith(('.SZ', '.SH', '.BJ')):
            code_part = symbol[:-3]
            return code_part.isdigit() and len(code_part) == 6
        
        return False
    
    def get_data_summary(self) -> Dict[str, Any]:
        """获取数据摘要信息
        
        Returns:
            Dict[str, Any]: 数据摘要信息
        """
        summary = {
            'domain_name': self.domain_name,
            'domain_path': str(self.domain_path),
            'total_symbols': 0,
            'interface_types': {},
            'features': {},
            'last_updated': None
        }
        
        # 统计各接口类型的数据
        for interface_type in self.supported_interface_types:
            symbols = self.list_symbols(interface_type=interface_type)
            summary['interface_types'][interface_type.value] = len(symbols)
            summary['total_symbols'] = max(summary['total_symbols'], len(symbols))
        
        # 统计特征数据
        features_dir = self.domain_path / "features"
        if features_dir.exists():
            for feature_dir in features_dir.iterdir():
                if feature_dir.is_dir():
                    feature_name = feature_dir.name
                    symbols = self.list_symbols(feature_name=feature_name)
                    summary['features'][feature_name] = len(symbols)
        
        # 获取最后更新时间
        if self.domain_path.exists():
            summary['last_updated'] = datetime.fromtimestamp(
                self.domain_path.stat().st_mtime
            ).isoformat()
        
        return summary
    
    def clean_data(self, 
                   interface_type: InterfaceType = None,
                   feature_name: str = None,
                   confirm: bool = False) -> int:
        """清理数据文件
        
        Args:
            interface_type: 要清理的接口类型，None表示清理所有
            feature_name: 要清理的特征名称，None表示清理所有
            confirm: 是否确认删除
            
        Returns:
            int: 删除的文件数量
        """
        if not confirm:
            logger.warning("清理数据需要确认，请设置confirm=True")
            return 0
        
        deleted_count = 0
        
        if feature_name:
            # 清理特定特征
            feature_dir = self.domain_path / "features" / feature_name
            if feature_dir.exists():
                for file_path in feature_dir.glob("*.csv"):
                    file_path.unlink()
                    deleted_count += 1
                feature_dir.rmdir()
        elif interface_type:
            # 清理特定接口类型
            interface_dir = self.domain_path / interface_type.value
            if interface_dir.exists():
                for file_path in interface_dir.glob("*.csv"):
                    file_path.unlink()
                    deleted_count += 1
                interface_dir.rmdir()
        else:
            # 清理整个域
            if self.domain_path.exists():
                for file_path in self.domain_path.rglob("*.csv"):
                    file_path.unlink()
                    deleted_count += 1
        
        logger.info(f"清理完成，删除了{deleted_count}个文件")
        return deleted_count