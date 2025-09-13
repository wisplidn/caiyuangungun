"""
Base Processor (Norm)

职责：
- 面向“接口级批量处理”的抽象（如 fin_is 年度数据）
- 提供：基础清洗、字段标准化、类型转换、主键构建、批量质量检查入口

TODO:
- 约定process_batch(dataframe, context)->(dataframe, decisions)
- 约定context包含：interface_type, year, partitions, contract, audit_engine
- 记录清洗/转换决策到 decisions（后续写入 decisions/ JSONL）
"""

# --- Minimal runnable base implementation ---
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from caiyuangungun.contracts import InterfaceType, DataContract, DEFAULT_CONTRACT, DataSource


@dataclass
class BatchContext:
    """处理上下文：由流水线构造并传入各处理器。
    - interface_type: 本次处理的数据接口类型
    - year/partitions: 可选分区信息（如年度、季度等）
    - contract: 全局数据契约（主键、去重策略等）
    - audit_engine: 审计/日志引擎（可选）
    - source: 数据源（tushare/akshare/other），用于字段口径与去重差异化处理
    """
    interface_type: InterfaceType
    year: Optional[int] = None
    partitions: Optional[Dict[str, Any]] = None
    contract: DataContract = DEFAULT_CONTRACT
    audit_engine: Any = None
    source: Optional[DataSource] = None


@dataclass
class BatchResult:
    """批处理输出结果统一结构。"""
    clean_df: pd.DataFrame
    decisions: Dict[str, Any]
    quality_report: Dict[str, Any]


class BaseProcessor:
    """处理器基类：所有具体处理器需实现 process_batch 方法。"""

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        raise NotImplementedError("process_batch must be implemented by subclasses")