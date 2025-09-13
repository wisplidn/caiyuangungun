"""
Quotes Daily Processor（日频行情规范化处理器）

TODO:
- 字段标准化：open, high, low, close, volume, amount, factor 等
- 类型转换与缺失处理
- 去重与质量检查
- 输出：clean_df, decisions, quality_report
"""

# --- Minimal runnable implementation ---
from typing import Any, Dict

import pandas as pd

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import (
    BaseProcessor,
    BatchContext,
    BatchResult,
)


class QuotesDailyProcessor(BaseProcessor):
    """最小实现：
    - 对常见字段重命名/标准化（若存在）
    - 基础类型转换
    - 按主键去重（保留最后一条）
    - 产出基础质量统计与决策记录
    """

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        df2 = df.copy()

        # 1) 字段标准化（存在则处理）
        rename_map: Dict[str, str] = {
            "ts_code": "symbol",
            "trade_dt": "trade_date",
        }
        df2.rename(columns={k: v for k, v in rename_map.items() if k in df2.columns}, inplace=True)

        # 确保主键存在（尽量不报错，留给上层质量检查）
        pk = context.contract.primary_keys.get(InterfaceType.QUOTES_DAILY, ["symbol", "trade_date"])

        # 2) 基础类型转换
        if "trade_date" in df2.columns:
            # 支持 20240630 或 2024-06-30
            df2["trade_date"] = pd.to_datetime(df2["trade_date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume", "amount", "factor"]:
            if col in df2.columns:
                df2[col] = pd.to_numeric(df2[col], errors="coerce")

        before_cnt = len(df2)
        # 3) 去重：保留最后一条
        if all(c in df2.columns for c in pk):
            df2.sort_values(by=pk, inplace=True)
            df2 = df2.drop_duplicates(subset=pk, keep="last")
        after_cnt = len(df2)

        # 4) 输出决策与质量报告
        decisions: Dict[str, Any] = {
            "dedupe": {
                "interface": InterfaceType.QUOTES_DAILY.value,
                "primary_keys": pk,
                "dropped": int(before_cnt - after_cnt),
                "strategy": str(context.contract.dedupe_strategies.get(InterfaceType.QUOTES_DAILY, "keep_latest_update")),
            }
        }
        quality_report: Dict[str, Any] = {
            "row_count": int(after_cnt),
            "null_ratio": df2.isna().mean().to_dict(),
        }

        return BatchResult(clean_df=df2, decisions=decisions, quality_report=quality_report)