"""
数据分层架构的关键契约与约定
"""
from typing import Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum


class DataLayer(Enum):
    """数据层次枚举"""
    RAW = "raw"           # 唯一真实来源，忠于Tushare
    NORM = "norm"         # 接口级规范化，每个接口一份CSV
    QLIB_READY = "qlib-ready"  # Qlib就绪层，按标的拆分CSV，包含所有特征


class InterfaceType(Enum):
    """数据接口类型"""
    QUOTES_DAILY = "quotes_daily"     # 日频行情
    FINANCIALS = "financials"         # 财务数据(合并IS/BS/CF)
    ANALYST = "analyst"               # 分析师预期
    CORP_ACTIONS = "corp_actions"     # 公司行为
    UNIVERSE = "universe"             # 股票池
    CALENDAR = "calendar"             # 交易日历
    REF = "ref"                       # 参考数据


class DataSource(Enum):
    """数据源类型"""
    TUSHARE = "tushare"
    AKSHARE = "akshare"
    OTHER = "other"


class DedupeStrategy(Enum):
    """去重策略"""
    KEEP_LATEST_UPDATE = "keep_latest_update"     # 保留最新更新
    KEEP_EARLIEST_ANN = "keep_earliest_ann"       # 保留最早公告
    KEEP_OFFICIAL_REPORT = "keep_official_report" # 保留正式报告


@dataclass
class CodeMapping:
    """股票代码映射配置"""
    standard_format: str = "{code}.{exchange}"  # 标准格式: 000001.SZ
    ts_code_format: str = "{code}.{exchange}"   # Tushare格式
    qlib_format: str = "{exchange}{code}"       # Qlib格式: SH600519
    
    exchange_mapping: Dict[str, str] = None
    
    def __post_init__(self):
        if self.exchange_mapping is None:
            self.exchange_mapping = {
                "SZ": "SZ",
                "SH": "SH", 
                "BJ": "BJ"
            }
    
    def to_qlib_format(self, ts_code: str) -> str:
        """转换为Qlib格式: SH600519"""
        if "." not in ts_code:
            raise ValueError(f"Invalid ts_code format: {ts_code}")
        
        code, exchange = ts_code.split(".")
        return f"{exchange.upper()}{code}"
    
    def from_qlib_format(self, qlib_code: str) -> str:
        """从Qlib格式转换为标准格式"""
        if qlib_code.startswith(("SH", "SZ", "BJ")):
            exchange = qlib_code[:2]
            code = qlib_code[2:]
            return f"{code}.{exchange}"
        else:
            raise ValueError(f"Invalid qlib code format: {qlib_code}")


@dataclass
class FinancialFieldConfig:
    """财务字段配置"""
    sc_suffix: str = "_sc"      # 季累后缀
    sq_suffix: str = "_sq"      # 单季后缀  
    ttm_suffix: str = "_ttm"    # TTM后缀
    yoy_suffix: str = "_yoy"    # 同比后缀
    qoq_suffix: str = "_qoq"    # 环比后缀


@dataclass
class PITConfig:
    """PIT(Point In Time)配置"""
    available_date_rule: str = "ann_date_next_trading_day"  # 可用日规则
    lookback_quarters: int = 8                              # 回刷季度数
    lookback_days: int = 120                               # 行情回刷天数
    
    def get_available_date_offset(self, is_after_close: bool = True) -> int:
        """获取可用日偏移量（交易日）"""
        return 1 if is_after_close else 0


@dataclass  
class DataQualityRule:
    """数据质量规则"""
    max_null_ratio: float = 0.95       # 最大空值比例
    max_duplicate_ratio: float = 0.01  # 最大重复比例
    outlier_std_threshold: float = 5.0 # 异常值标准差阈值
    
    # 财务数据特定规则
    max_negative_sq_ratio: float = 0.1  # 单季差分为负的最大比例
    min_ttm_coverage: float = 0.8       # TTM最小覆盖率


@dataclass
class StorageConfig:
    """存储配置"""
    # CSV保留策略
    norm_csv_retention_days: int = -1     # norm层CSV长期保留
    qlib_feed_retention_days: int = 30    # qlib-feed短期保留
    
    # Parquet配置
    parquet_compression: str = "snappy"
    parquet_row_group_size: int = 50000
    
    # 分区配置
    norm_partition_cols: List[str] = None
    domain_partition_cols: List[str] = None
    
    def __post_init__(self):
        if self.norm_partition_cols is None:
            self.norm_partition_cols = ["period_end", "update_date"]
        if self.domain_partition_cols is None:
            self.domain_partition_cols = ["year", "quarter"]


class DataContract:
    """数据契约总配置"""
    
    def __init__(self):
        self.code_mapping = CodeMapping()
        self.financial_field = FinancialFieldConfig()  
        self.pit_config = PITConfig()
        self.quality_rule = DataQualityRule()
        self.storage_config = StorageConfig()
        
        # 主键定义
        self.primary_keys = {
            InterfaceType.QUOTES_DAILY: ["symbol", "trade_date"],
            InterfaceType.FINANCIALS: ["symbol", "period_end", "report_type", "statement_type", "update_flag"],
            InterfaceType.ANALYST: ["symbol", "ann_date", "report_date", "analyst_id"],
            InterfaceType.CORP_ACTIONS: ["symbol", "ex_date", "action_type"],
            InterfaceType.UNIVERSE: ["symbol"],
            InterfaceType.CALENDAR: ["exchange", "calendar_date"],
            InterfaceType.REF: ["symbol"]
        }
        
        # 去重策略映射
        self.dedupe_strategies = {
            InterfaceType.QUOTES_DAILY: DedupeStrategy.KEEP_LATEST_UPDATE,
            InterfaceType.FINANCIALS: DedupeStrategy.KEEP_OFFICIAL_REPORT,
            InterfaceType.ANALYST: DedupeStrategy.KEEP_LATEST_UPDATE,
            InterfaceType.CORP_ACTIONS: DedupeStrategy.KEEP_LATEST_UPDATE,
        }
    
    def get_standard_code(self, ts_code: str) -> str:
        """转换为标准代码格式"""
        if "." not in ts_code:
            raise ValueError(f"Invalid ts_code format: {ts_code}")
        
        code, exchange = ts_code.split(".")
        return f"{code}.{exchange}"
    
    def get_field_names(self, base_name: str) -> Dict[str, str]:
        """获取字段的各种后缀名称"""
        return {
            "sc": f"{base_name}{self.financial_field.sc_suffix}",
            "sq": f"{base_name}{self.financial_field.sq_suffix}", 
            "ttm": f"{base_name}{self.financial_field.ttm_suffix}",
            "yoy": f"{base_name}{self.financial_field.yoy_suffix}",
            "qoq": f"{base_name}{self.financial_field.qoq_suffix}"
        }


# 全局默认配置实例
DEFAULT_CONTRACT = DataContract()
