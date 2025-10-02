#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DailyBasic Processor（每日基本指标数据处理器）

专门处理每日基本指标数据的清洗和标准化，包括：
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

from caiyuangungun.data.norm.core.base_processor import BaseProcessor

logger = logging.getLogger('daily_basic_processor')


class DailyBasicProcessor(BaseProcessor):
    """每日基本指标数据专用处理器"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加每日基本指标数据特有的功能
        self.cleaning_functions.update(self._register_daily_basic_cleaning_functions())
        

    def _register_daily_basic_cleaning_functions(self):
        """注册每日基本指标数据专用清洗函数"""
        return {
            'filter_listing_dates': self.filter_listing_dates,
            'rename_columns': self.rename_columns,
        }

    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "daily_data" / "debug"

    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'daily_basic'

    def _get_audit_config(self) -> Optional[dict]:
        """获取稽核配置 - 每日基本指标数据不使用稽核配置"""
        return None

    def _get_output_fields(self, df: pd.DataFrame) -> List[str]:
        """获取输出字段列表 - 每日基本指标数据只保留核心字段"""
        # 定义需要保留的核心字段（按QLib标准）
        core_fields = [
            'ts_code', 'trade_date', 'close',
            'turnover_rate', 'turnover_rate_f', 'volume_ratio',
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
            'dv_ratio', 'dv_ttm',
            'total_shares', 'float_shares', 'free_shares',
            'total_market_cap', 'float_market_cap'
        ]
        # 只返回存在的字段
        return [f for f in core_fields if f in df.columns]

    def _get_grouping_fields(self, df: pd.DataFrame) -> List[str]:
        """获取分组字段 - 每日基本指标数据使用 ts_code + trade_date"""
        return ['ts_code', 'trade_date']

    def filter_listing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """过滤上市前和退市后的数据 - 高性能版本"""
        logger.info("开始过滤上市前和退市后的数据")
        
        initial_count = len(df)
        
        # 检查必需列是否存在
        required_columns = ['trade_date', 'list_date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"缺少必需的列: {missing_columns}，跳过上市日期过滤")
            return df
        
        # 北交所特殊处理：2021年11月15日正式开市
        # 对于上市时间早于此日期的北交所股票，调整为开市日
        BJE_OPENING_DATE = 20211115
        is_bje = df['ts_code'].str.endswith('.BJ')
        
        # 复制list_date以便修改
        list_date_adjusted = df['list_date'].copy()
        
        # 高性能优化：日期已经是YYYYMMDD字符串格式，直接转int比较
        if df['trade_date'].dtype == 'object':
            # 字符串格式直接转int（如'20080103' -> 20080103）
            trade_date_int = df['trade_date'].astype(int)
            list_date_int = list_date_adjusted.astype(int)
            
            # 北交所调整：如果是北交所股票且上市日期早于2021-11-15，调整为2021-11-15
            bje_early_mask = is_bje & (list_date_int < BJE_OPENING_DATE)
            if bje_early_mask.any():
                bje_count = bje_early_mask.sum()
                logger.info(f"发现{bje_count}条北交所记录的上市日期早于2021-11-15，调整为北交所开市日")
                list_date_int = list_date_int.where(~bje_early_mask, BJE_OPENING_DATE)
                
        elif df['trade_date'].dtype == 'datetime64[ns]':
            # 如果是datetime格式，转换为YYYYMMDD整数
            trade_date_int = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d').astype(int)
            list_date_int = pd.to_datetime(list_date_adjusted).dt.strftime('%Y%m%d').astype(int)
            
            # 北交所调整
            bje_early_mask = is_bje & (list_date_int < BJE_OPENING_DATE)
            if bje_early_mask.any():
                bje_count = bje_early_mask.sum()
                logger.info(f"发现{bje_count}条北交所记录的上市日期早于2021-11-15，调整为北交所开市日")
                list_date_int = list_date_int.where(~bje_early_mask, BJE_OPENING_DATE)
        else:
            # 已经是整数，直接使用
            trade_date_int = df['trade_date']
            list_date_int = list_date_adjusted
            
            # 北交所调整
            bje_early_mask = is_bje & (list_date_int < BJE_OPENING_DATE)
            if bje_early_mask.any():
                bje_count = bje_early_mask.sum()
                logger.info(f"发现{bje_count}条北交所记录的上市日期早于2021-11-15，调整为北交所开市日")
                list_date_int = list_date_int.where(~bje_early_mask, BJE_OPENING_DATE)
        
        # 向量化过滤：trade_date >= list_date（上市当天及之后）
        mask = trade_date_int >= list_date_int
        
        # 如果有退市日期，添加退市过滤条件
        if 'delist_date' in df.columns:
            # 处理delist_date为None的情况
            delist_notna = df['delist_date'].notna()
            if delist_notna.any():
                # 字符串直接转int，NaN会保持为NaN
                delist_date_int = pd.to_numeric(df['delist_date'], errors='coerce')
                # 过滤：delist_date为空 OR trade_date <= delist_date
                mask = mask & (delist_date_int.isna() | (trade_date_int <= delist_date_int))
        
        df_filtered = df[mask]
        
        removed_count = initial_count - len(df_filtered)
        logger.info(f"过滤了{removed_count}行上市前或退市后的数据，剩余{len(df_filtered)}行")
        
        return df_filtered


    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据统一配置文件重命名列"""
        logger.info("开始根据统一配置文件重命名列")
        
        # 获取统一配置
        unified_config = self._get_unified_config()
        
        if not unified_config:
            logger.warning("没有找到统一配置，跳过字段重命名")
            return df
        
        # 获取配置节名称并提取对应部分
        config_section = self._get_config_section()
        section_config = unified_config.get(config_section, {})
        
        if not section_config:
            logger.warning(f"没有找到配置节 '{config_section}'，跳过字段重命名")
            return df
        
        # 创建重命名映射
        rename_mapping = {}
        for tushare_name, config in section_config.items():
            renamed_field = config.get('renamed_field', tushare_name)
            if tushare_name in df.columns and tushare_name != renamed_field:
                rename_mapping[tushare_name] = renamed_field
        
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"重命名了{len(rename_mapping)}个列: {rename_mapping}")
        else:
            logger.info("没有需要重命名的列")
        
        return df


def main():
    """主函数 - 仅用于测试，实际使用应通过ProcessorService"""
    # 创建一个基本的测试配置
    test_config = {
        'project_root': Path(__file__).parent.parent.parent.parent.parent,
        'input_path': 'data/norm/daily_data/merged/daily_basic.parquet',
        'output_path': 'data/norm/daily_data/cleaned/daily_basic.parquet',
        'cleaning_pipeline': [
            {'function': 'remove_empty_ts_code'},
            {'function': 'validate_unique_records'},
            {'function': 'rename_columns'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = DailyBasicProcessor(test_config)
    result_df = processor.process_pipeline()
    print(f"处理完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()

