"""
Dividend Processor（分红数据处理器）

专门处理分红数据的清洗和标准化，包括：
- 剔除空值记录
- 重复记录验证
- 字段重命名
- 输出标准化parquet文件
"""

import pandas as pd
from typing import Any, Dict, Optional, List
import json
from pathlib import Path
from datetime import datetime
import logging

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult

logger = logging.getLogger('dividend_processor')


class DividendProcessor(BaseProcessor):
    """分红数据专用处理器"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加分红数据特有的功能
        self.cleaning_functions.update(self._register_dividend_cleaning_functions())
        

    def _register_dividend_cleaning_functions(self):
        """注册分红数据专用清洗函数"""
        return {
            'filter_implementation_only': self.filter_implementation_only,
            'handle_duplicate_records': self.handle_duplicate_records,
            'rename_columns': self.rename_columns,
        }

    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "dividend" / "debug"

    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'dividend'

    def _get_audit_config(self) -> Optional[dict]:
        """获取稽核配置 - 分红数据不使用稽核配置"""
        return None

    def _get_output_fields(self, df: pd.DataFrame) -> List[str]:
        """获取输出字段列表 - 分红数据保留所有字段"""
        return list(df.columns)


    def filter_implementation_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """只保留div_proc为'实施'的记录，去除预案和股东大会通过等状态"""
        initial_count = len(df)
        
        # 检查div_proc列是否存在
        if 'div_proc' not in df.columns:
            logger.warning("div_proc列不存在，跳过实施状态过滤")
            return df
        
        # 只保留div_proc为'实施'的记录
        df_filtered = df[df['div_proc'] == '实施'].copy()
        removed_count = initial_count - len(df_filtered)
        
        logger.info(f"过滤div_proc状态：移除了{removed_count}行非'实施'状态的记录，剩余{len(df_filtered)}行")
        
        return df_filtered

    def handle_duplicate_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理重复记录：按ts_code+end_date+ann_date分组，应用去重逻辑
        
        处理规则：
        1. 取imp_ann_date最新的一条
        2. 如果imp_ann_date相同，则取第一条
        3. 如果两行的cash_div_tax差异超过10%，则全部保留等待报错预警
        
        Args:
            df: 输入DataFrame
            
        Returns:
            处理后的DataFrame
        """
        logger.info("开始处理重复记录")
        initial_count = len(df)
        
        # 检查必需列是否存在
        key_columns = ['ts_code', 'end_date', 'ann_date']
        required_columns = key_columns + ['imp_ann_date', 'cash_div_tax']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"缺少必需的列: {missing_columns}，跳过重复记录处理")
            return df
        
        # 找出重复记录
        duplicates_mask = df.duplicated(subset=key_columns, keep=False)
        if not duplicates_mask.any():
            logger.info("没有发现重复记录，跳过处理")
            return df
        
        duplicate_groups = df[duplicates_mask].groupby(key_columns)
        logger.info(f"发现{len(duplicate_groups)}组重复记录，共{duplicates_mask.sum()}条记录")
        
        # 存储需要保留的记录索引
        keep_indices = []
        # 存储非重复记录的索引
        non_duplicate_indices = df[~duplicates_mask].index.tolist()
        
        for group_key, group_df in duplicate_groups:
            ts_code, end_date, ann_date = group_key
            logger.debug(f"处理重复组: ts_code={ts_code}, end_date={end_date}, ann_date={ann_date}")
            
            # 检查cash_div_tax差异
            cash_div_tax_values = group_df['cash_div_tax'].dropna()
            if len(cash_div_tax_values) >= 2:
                max_val = cash_div_tax_values.max()
                min_val = cash_div_tax_values.min()
                if max_val > 0:  # 避免除零错误
                    diff_ratio = abs(max_val - min_val) / max_val
                    if diff_ratio > 0.1:  # 差异超过10%
                        logger.warning(f"组 {group_key} 的cash_div_tax差异超过10% ({diff_ratio:.2%})，保留所有记录等待报错预警")
                        keep_indices.extend(group_df.index.tolist())
                        continue
            
            # 按imp_ann_date排序（最新的在前）
            group_df_sorted = group_df.sort_values('imp_ann_date', ascending=False, na_position='last')
            
            # 取第一条记录（imp_ann_date最新的，如果相同则取原始顺序的第一条）
            selected_record = group_df_sorted.iloc[0]
            keep_indices.append(selected_record.name)
            
            logger.debug(f"选择记录: index={selected_record.name}, imp_ann_date={selected_record['imp_ann_date']}")
        
        # 合并非重复记录和选择的重复记录
        final_indices = non_duplicate_indices + keep_indices
        result_df = df.loc[final_indices].copy()
        
        removed_count = initial_count - len(result_df)
        logger.info(f"重复记录处理完成：移除了{removed_count}条记录，剩余{len(result_df)}行")
        
        return result_df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据统一配置文件重命名列"""
        logger.info("开始根据统一配置文件重命名列")
        
        # 获取统一配置
        unified_config = self._get_unified_config()
        
        if not unified_config:
            logger.warning("没有找到统一配置，跳过字段重命名")
            return df
        
        # 创建重命名映射
        rename_mapping = {}
        for tushare_name, config in unified_config.items():
            renamed_field = config.get('renamed_field', tushare_name)
            if tushare_name in df.columns and tushare_name != renamed_field:
                rename_mapping[tushare_name] = renamed_field
        
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"重命名了{len(rename_mapping)}个列: {rename_mapping}")
        else:
            logger.info("没有需要重命名的列")
        
        return df

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        """处理分红数据批次"""
        try:
            original_shape = df.shape
            
            # 执行清洗流程
            pipeline_steps = self.config.get('cleaning_pipeline', [])
            
            for step in pipeline_steps:
                function_name = step.get('function')
                if function_name in self.cleaning_functions:
                    func = self.cleaning_functions[function_name]
                    df = func(df)
                    logger.info(f"执行 {function_name}: 数据形状 {df.shape}")
                else:
                    logger.warning(f"未找到处理函数 {function_name}")
            
            # 生成处理报告
            decisions = {
                "interface": InterfaceType.CORP_ACTIONS.value,
                "processor": "dividend_processor",
                "original_shape": original_shape,
                "final_shape": df.shape,
                "steps_executed": [step.get('function') for step in pipeline_steps]
            }
            
            quality_report = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "rows_removed": original_shape[0] - df.shape[0],
                "columns_renamed": len(self.rename_mapping) if self.rename_mapping else 0
            }
            
            return BatchResult(
                clean_df=df,
                decisions=decisions,
                quality_report=quality_report
            )
            
        except Exception as e:
            logger.error(f"处理分红数据时发生错误: {e}")
            raise