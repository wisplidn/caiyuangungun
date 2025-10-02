#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging

from caiyuangungun.data.norm.core.base_processor import FinancialProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_bs_processor')


class FinBSProcessor(FinancialProcessor):
    """资产负债表数据处理器"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加资产负债表特有的功能
        self.cleaning_functions.update(self._register_bs_cleaning_functions())
        
        logger.info("FinBSProcessor初始化完成")
    
    def _register_bs_cleaning_functions(self):
        """注册资产负债表专用清洗函数"""
        return {
            'handle_11_records': self.handle_11_records,
        }
    
    def _validate_audit_config(self):
        """验证稽核配置是否存在"""
        # 从统一字段配置中获取稽核配置
        unified_config = self._get_unified_config()
        if not unified_config:
            raise ValueError("未找到统一字段配置")
        
        config_section = self._get_config_section()
        if config_section not in unified_config:
            raise ValueError(f"未找到{config_section}配置节")
        
        audit_config = unified_config[config_section]
        if not audit_config:
            raise ValueError(f"{config_section}稽核配置为空")
        
        logger.info(f"稽核配置验证通过，包含{len(audit_config)}个字段配置")
    
    def _get_audit_config_key(self) -> str:
        """获取稽核配置的key"""
        return 'balance_sheet_audit'
    
    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'balance_sheet'
    
    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "balancesheet" / "debug"
    
    
    def handle_11_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理1-1组合的重复记录 - 保留ann_date更新的一行，如果ann_date一致则全部保留"""
        logger.info("开始处理1-1组合重复记录")
        
        start_time = time.time()
        
        # 1. 早期过滤：使用groupby().size()快速识别可能的11组合
        group_sizes = df.groupby(['ts_code', 'end_date', 'f_ann_date']).size()
        potential_11_groups = group_sizes[group_sizes == 2].index
        
        if len(potential_11_groups) == 0:
            logger.info("没有符合条件的1-1组合，直接返回原数据")
            return df
        
        # 2. 11组合计算：仅对潜在组进行详细分析
        calc_start = time.time()
        
        # 转换update_flag为数值类型以提高性能
        df = df.copy()
        df['update_flag_num'] = pd.to_numeric(df['update_flag'], errors='coerce')
        
        # 创建过滤条件
        df['_group_key'] = list(zip(df['ts_code'], df['end_date'], df['f_ann_date']))
        mask = df['_group_key'].isin(potential_11_groups)
        filtered_df = df[mask]
        
        # 对过滤后的数据进行分组统计
        group_stats = filtered_df.groupby(['ts_code', 'end_date', 'f_ann_date'])['update_flag_num'].agg(['min', 'max', 'sum']).reset_index()
        
        # 使用数学性质识别11组合：min=1, max=1, sum=2
        target_groups = group_stats[
            (group_stats['min'] == 1) & 
            (group_stats['max'] == 1) & 
            (group_stats['sum'] == 2)
        ]
        
        calc_time = time.time() - calc_start
        logger.info(f"11组合计算耗时: {calc_time:.4f}秒")
        logger.info(f"发现符合条件的1-1组合: {len(target_groups)} 个")
        
        if len(target_groups) == 0:
            df = df.drop(columns=['update_flag_num', '_group_key'])
            logger.info("没有符合条件的1-1组合，直接返回原数据")
            return df
        
        # 3. 11组合数据处理：保留ann_date更新的一行，如果ann_date一致则全部保留
        process_start = time.time()
        
        # 创建目标组标识
        target_keys = set(zip(target_groups['ts_code'], target_groups['end_date'], target_groups['f_ann_date']))
        df['_is_target_group'] = df['_group_key'].isin(target_keys)
        
        # 分离目标组和非目标组
        non_target_df = df[~df['_is_target_group']].copy()
        target_df = df[df['_is_target_group']].copy()
        
        # 对目标组进行处理：保留ann_date最新的记录
        if len(target_df) > 0:
            # 创建临时的datetime列用于比较，但保持原始ann_date不变
            target_df['_temp_ann_date'] = pd.to_datetime(target_df['ann_date'])
            
            # 按组找到最大的ann_date
            max_ann_dates = target_df.groupby(['ts_code', 'end_date', 'f_ann_date'])['_temp_ann_date'].max().reset_index()
            max_ann_dates.rename(columns={'_temp_ann_date': 'max_ann_date'}, inplace=True)
            
            # 合并回原数据
            target_df = target_df.merge(max_ann_dates, on=['ts_code', 'end_date', 'f_ann_date'], how='left')
            
            # 保留ann_date等于最大值的记录
            processed_target_df = target_df[target_df['_temp_ann_date'] == target_df['max_ann_date']].copy()
            processed_target_df = processed_target_df.drop(columns=['_temp_ann_date', 'max_ann_date'])
        else:
            processed_target_df = target_df
        
        # 合并结果
        result_df = pd.concat([non_target_df, processed_target_df], ignore_index=True)
        
        # 清理临时列
        result_df = result_df.drop(columns=['update_flag_num', '_group_key', '_is_target_group'])
        
        process_time = time.time() - process_start
        total_time = time.time() - start_time
        
        logger.info(f"11组合数据处理耗时: {process_time:.4f}秒")
        logger.info(f"处理1-1组合完成，总耗时: {total_time:.4f}秒，最终行数: {len(result_df)} (原始: {len(df)})")
        
        return result_df


def main():
    """主函数 - 仅用于测试，实际使用应通过ProcessorService"""
    # 创建一个基本的测试配置
    test_config = {
        'project_root': Path(__file__).parent.parent.parent.parent.parent,
        'input_path': 'data/raw/balance_sheet/balance_sheet_y.parquet',
        'output_path': 'data/norm/balance_sheet/cleaned/balance_sheet_y.parquet',
        'cleaning_pipeline': [
            {'function': 'remove_empty_ts_code'},
            {'function': 'handle_01_records'},
            {'function': 'handle_11_records'},
            {'function': 'validate_unique_records'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinBSProcessor(test_config)
    result_df = processor.process_pipeline()
    print(f"处理完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()