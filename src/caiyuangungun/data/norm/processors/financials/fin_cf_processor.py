"""
Fin CF Processor（现金流量表规范化处理器）

TODO:
- 参照利润表处理器，定义专属勾稽与完整性规则
- 输出与日志记录保持一致
"""
# --- Minimal implementation placeholder ---
from typing import Any, Dict

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult


class FinCFProcessor(BaseProcessor):
    """占位实现，暂时仅返回输入数据。"""

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        # TODO: 字段标准化、类型转换、财务勾稽与主键去重
        return BatchResult(
            clean_df=df.copy(),
            decisions={"interface": InterfaceType.FINANCIALS.value, "statement": "cf", "status": "placeholder"},
            quality_report={"row_count": len(df)}
        )