"""
Ref Processor（参考数据规范化处理器）

TODO:
- 生成/更新代码映射表（如北交所新老代码）
- 提供查询接口供其他处理器使用（后续实现）
"""

# --- Minimal implementation placeholder ---
from typing import Any, Dict

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult


class RefProcessor(BaseProcessor):
    """占位实现，暂时仅返回输入数据。"""

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        # TODO: 生成/更新代码映射、行业分类等表
        return BatchResult(
            clean_df=df.copy(),
            decisions={"interface": InterfaceType.REF.value, "status": "placeholder"},
            quality_report={"row_count": len(df)}
        )