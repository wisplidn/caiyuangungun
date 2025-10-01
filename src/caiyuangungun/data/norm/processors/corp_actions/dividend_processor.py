"""
Dividend Processor（分红数据处理器）

专门处理分红数据的清洗和标准化，包括：
- 剔除空值记录
- 重复记录验证
- 字段重命名
- 输出标准化parquet文件
"""

import pandas as pd
from typing import Any, Dict, Optional
import json
from pathlib import Path
from datetime import datetime
import logging

from caiyuangungun.contracts import InterfaceType
from caiyuangungun.data.norm.core.base_processor import BaseProcessor, BatchContext, BatchResult
from caiyuangungun.data.norm.processors.common.universal_data_cleaner import UniversalDataCleaner

logger = logging.getLogger('dividend_processor')


class DividendProcessor(BaseProcessor):
    """分红数据专用处理器"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__()
        self.config = config
        self.cleaner = UniversalDataCleaner()
        self.rename_mapping = self._load_rename_mapping()
        # 获取项目根目录，用于创建debug文件夹
        self.project_root = Path(self.config.get('project_root', Path(__file__).parent.parent.parent.parent.parent.parent))

    def _load_rename_mapping(self) -> Dict[str, str]:
        """加载字段重命名映射配置"""
        try:
            config_path = Path(self.config.get('config_file', 'data/config/column_rename_mapping.json'))
            if not config_path.is_absolute():
                # 如果是相对路径，从项目根目录开始
                project_root = self.config.get('project_root', Path(__file__).parent.parent.parent.parent.parent.parent)
                config_path = Path(project_root) / config_path
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                return config_data.get('dividend', {}).get('mapping', {})
        except Exception as e:
            print(f"警告：无法加载重命名配置文件: {e}")
            return {}

    def remove_empty_ts_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """剔除ts_code为空的行"""
        initial_count = len(df)
        df_cleaned = df.dropna(subset=['ts_code'])
        df_cleaned = df_cleaned[df_cleaned['ts_code'] != '']
        removed_count = initial_count - len(df_cleaned)
        if removed_count > 0:
            print(f"移除了{removed_count}行ts_code为空的数据，剩余{len(df_cleaned)}行")
        return df_cleaned

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
        print(f"过滤div_proc状态：移除了{removed_count}行非'实施'状态的记录，剩余{len(df_filtered)}行")
        
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
        print(f"重复记录处理完成：移除了{removed_count}条记录，剩余{len(result_df)}行")
        
        return result_df

    def validate_unique_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重复记录质检：检查ts_code+end_date+ann_date组合是否存在重复
        
        Args:
            df: 输入DataFrame
            
        Returns:
            原始DataFrame（如果通过质检）
            
        Raises:
            ValueError: 如果存在重复记录
        """
        logger.info("开始重复记录质检")
        
        # 检查关键字段组合的重复情况
        key_columns = ['ts_code', 'end_date', 'ann_date']
        
        # 检查必需列是否存在
        missing_columns = [col for col in key_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"缺少必需的列: {missing_columns}")
        
        # 检查是否存在重复
        duplicates = df.duplicated(subset=key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            # 获取重复记录的详细信息
            duplicate_records = df[duplicates]
            logger.error(f"发现{duplicate_count}条重复记录")
            
            # 创建debug文件夹 - 使用项目根目录下的data/norm/dividend/debug
            debug_dir = self.project_root / "data" / "norm" / "dividend" / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # 输出重复记录到CSV文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"duplicate_records_dividend_{timestamp}.csv"
            csv_path = debug_dir / csv_filename
            
            duplicate_records.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.error(f"重复记录详情已输出到: {csv_path}")
            
            # 在日志中显示前10条重复记录的关键信息
            unique_duplicates = duplicate_records[key_columns].drop_duplicates()
            logger.error("重复记录详情:")
            for _, record in unique_duplicates.head(10).iterrows():  # 只显示前10条
                logger.error(f"  ts_code={record['ts_code']}, end_date={record['end_date']}, ann_date={record['ann_date']}")
            
            raise ValueError(f"发现重复记录，ts_code+end_date+ann_date组合不唯一:\n{unique_duplicates}")
        
        return df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据配置文件重命名列"""
        if not self.rename_mapping:
            print("警告：没有找到重命名映射配置，跳过字段重命名")
            return df
        
        # 只重命名存在的列
        columns_to_rename = {old_name: new_name for old_name, new_name in self.rename_mapping.items() 
                           if old_name in df.columns}
        
        if columns_to_rename:
            df = df.rename(columns=columns_to_rename)
            print(f"已重命名字段: {columns_to_rename}")
        
        return df

    def output_parquet_file(self, df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """输出parquet文件，保留所有字段"""
        print(f"准备输出parquet文件，数据形状: {df.shape}")
        if output_path:
            # 确保输出目录存在
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            # 保存文件
            df.to_parquet(output_path, index=False)
            print(f"数据已保存到: {output_path}")
        return df

    def process_batch(self, df: pd.DataFrame, context: BatchContext) -> BatchResult:
        """处理分红数据批次"""
        try:
            original_shape = df.shape
            
            # 执行清洗流程
            pipeline_steps = self.config.get('cleaning_pipeline', [])
            
            for step in pipeline_steps:
                function_name = step.get('function')
                if hasattr(self, function_name):
                    func = getattr(self, function_name)
                    df = func(df)
                    print(f"执行 {function_name}: 数据形状 {df.shape}")
                else:
                    print(f"警告：未找到处理函数 {function_name}")
            
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
            print(f"处理分红数据时发生错误: {e}")
            raise
