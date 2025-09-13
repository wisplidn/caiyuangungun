"""
Fin BS Processor（资产负债表规范化处理器）

TODO:
- 参照利润表处理器，定义专属勾稽与完整性规则
- 输出与日志记录保持一致
"""

# --- Minimal runnable implementation ---
from typing import Any, Dict, List

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import (
    BaseProcessor,
    BatchContext,
    BatchResult,
)


class FinBSProcessor(BaseProcessor):
    """最小实现：
    - 标准化 symbol/period_end 等基础字段
    - 将 period_end 转为日期；数值字段尽量转为 numeric
    - 使用财务类主键进行去重（保留最后一条）
    - 输出基础质量统计与决策记录
    注：完整的勾稽（资产=负债+所有者权益等）后续补充
    """

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        df2 = df.copy()

        # 1) 字段重命名/标准化（若存在）
        rename_map: Dict[str, str] = {
            "ts_code": "symbol",
            "end_date": "period_end",
            "f_ann_date": "ann_date",
            "announcement_date": "ann_date",
        }
        df2.rename(columns={k: v for k, v in rename_map.items() if k in df2.columns}, inplace=True)

        # 2) 类型转换
        if "period_end" in df2.columns:
            df2["period_end"] = pd.to_datetime(df2["period_end"], errors="coerce")
        if "ann_date" in df2.columns:
            df2["ann_date"] = pd.to_datetime(df2["ann_date"], errors="coerce")

        # 尝试将常见数值列转为 numeric（存在即转）
        numeric_candidates: List[str] = [
            "total_assets", "total_liab", "total_equity", "assets", "liabilities", "equity",
        ]
        for col in numeric_candidates:
            if col in df2.columns:
                df2[col] = pd.to_numeric(df2[col], errors="coerce")

        # 3) 去重（财务主键）
        pk = context.contract.primary_keys.get(
            InterfaceType.FINANCIALS,
            ["symbol", "period_end", "report_type", "statement_type", "update_flag"],
        )
        before_cnt = len(df2)
        if all(c in df2.columns for c in pk):
            df2.sort_values(by=pk, inplace=True)
            df2 = df2.drop_duplicates(subset=pk, keep="last")
        after_cnt = len(df2)

        decisions: Dict[str, Any] = {
            "dedupe": {
                "interface": InterfaceType.FINANCIALS.value,
                "primary_keys": pk,
                "dropped": int(before_cnt - after_cnt),
                "strategy": str(context.contract.dedupe_strategies.get(InterfaceType.FINANCIALS, "keep_official_report")),
            }
        }
        quality_report: Dict[str, Any] = {
            "row_count": int(after_cnt),
            "null_ratio": df2.isna().mean().to_dict(),
        }

        return BatchResult(clean_df=df2, decisions=decisions, quality_report=quality_report)