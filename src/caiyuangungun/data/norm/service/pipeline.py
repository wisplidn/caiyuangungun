"""
Norm Pipeline（编排）

职责：
- 编排：读取 -> 批量清洗 -> 质量校验 -> 决策去重 -> （可选）按股票清洗 -> 输出
- 支持：全量回填 / 增量更新 / 断点续传 / 并行处理

TODO:
- 入口 run(interface_type, years, mode, options)
- 日志与审计：对接 audit_bridge，输出到 decisions/ 与 processing_logs/
- 与 io、core、processors 的协作契约
"""

# --- Minimal runnable implementation ---
from typing import Optional

import pandas as pd

from caiyuangungun.contracts import InterfaceType, DataSource
from caiyuangungun.data.norm.core.contracts import BatchContext
from caiyuangungun.data.norm.core.base_processor import BatchResult
from caiyuangungun.data.norm.core.processor_manager import get_processor


def run(interface_type: InterfaceType, df: pd.DataFrame, context: Optional[BatchContext] = None, source: Optional[DataSource] = None) -> BatchResult:
    """最小流水线入口：将传入的 DataFrame 交由对应处理器处理。

    参数：
    - interface_type: 数据接口类型
    - df: 原始或已对齐基础字段的 DataFrame
    - context: 可选上下文；若不提供，将创建默认上下文
    - source: 数据源（tushare/akshare/other），写入 BatchContext，供处理器差异化处理
    """
    if context is None:
        context = BatchContext(interface_type=interface_type)

    # 写入数据源
    if source is not None:
        context.source = source

    processor = get_processor(interface_type, source=source)
    return processor.process_batch(df, context)