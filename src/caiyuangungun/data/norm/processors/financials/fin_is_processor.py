"""
Fin IS Processor（利润表规范化处理器）

TODO:
- 输入：年度合并后的 parquet（接口级批量）
- 步骤：字段标准化、类型转换、主键构建（symbol, period_end, report_type, update_flag）
- 检查：收入-成本-费用勾稽、完整性、异常值
- 输出：clean_df, decisions, quality_report
- 备注：断点续传与分批（按股票范围或按partition）
"""
# --- Minimal implementation placeholder ---
from typing import Any, Dict

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult


class FinISProcessor(BaseProcessor):
    """占位实现，暂时仅返回输入数据。"""

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        # TODO: 字段标准化、类型转换、财务勾稽与主键去重
        return BatchResult(
            clean_df=df.copy(),
            decisions={"interface": InterfaceType.FINANCIALS.value, "statement": "is", "status": "placeholder"},
            quality_report={"row_count": len(df)}
        )