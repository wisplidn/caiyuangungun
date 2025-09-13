"""
Calendar Processor（交易日历规范化处理器）

TODO:
- 字段与类型标准化
- 连续性与有效性检查
- 输出：clean_df, decisions, quality_report
"""
# --- Minimal implementation placeholder ---
from typing import Any, Dict

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult


class CalendarProcessor(BaseProcessor):
    """占位实现，暂时仅返回输入数据。"""

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        # TODO: 连续性校验、节假日与临停日合规检查
        return BatchResult(
            clean_df=df.copy(),
            decisions={"interface": InterfaceType.CALENDAR.value, "status": "placeholder"},
            quality_report={"row_count": len(df)}
        )