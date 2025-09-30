#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
from datetime import datetime
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_bs_processor')


class FinBSProcessor:
    """资产负债表数据处理器"""
    
    def __init__(self, config: dict):
        """
        初始化财务报表处理器
        
        Args:
            config: 完整的processor配置字典
        """
        self.config = config
        self.project_root = Path(config.get('project_root', Path(__file__).parent.parent.parent.parent.parent))
        self.cleaning_functions = self._register_cleaning_functions()
        
        logger.info("FinBSProcessor初始化完成")
    
    
    def _register_cleaning_functions(self):
        """注册清洗函数"""
        return {
            'remove_empty_ts_code': self.remove_empty_ts_code,
            'handle_01_records': self.handle_01_records,
            'handle_11_records': self.handle_11_records,
            'validate_unique_records': self.validate_unique_records,
            'output_parquet_file': self.output_parquet_file
        }
    
    def remove_empty_ts_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除ts_code为空的行"""
        logger.info("开始移除ts_code为空的行")
        
        initial_count = len(df)
        # 移除ts_code为空或NaN的行
        df_cleaned = df.dropna(subset=['ts_code'])
        df_cleaned = df_cleaned[df_cleaned['ts_code'].str.strip() != '']
        
        removed_count = initial_count - len(df_cleaned)
        logger.info(f"移除了{removed_count}行ts_code为空的数据，剩余{len(df_cleaned)}行")
        
        return df_cleaned
    
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

    def handle_01_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理0-1组合的重复记录 - 超高性能优化版本"""
        logger.info("开始处理0-1组合重复记录")
        
        start_time = time.time()
        
        # 1. 早期过滤：使用groupby().size()快速识别可能的01组合
        group_sizes = df.groupby(['ts_code', 'end_date', 'f_ann_date']).size()
        potential_01_groups = group_sizes[group_sizes == 2].index
        
        if len(potential_01_groups) == 0:
            logger.info("没有符合条件的0-1组合，直接返回原数据")
            return df
        
        # 2. 01组合计算：仅对潜在组进行详细分析
        calc_start = time.time()
        
        # 转换update_flag为数值类型以提高性能
        df = df.copy()
        df['update_flag_num'] = pd.to_numeric(df['update_flag'], errors='coerce')
        
        # 创建过滤条件
        df['_group_key'] = list(zip(df['ts_code'], df['end_date'], df['f_ann_date']))
        mask = df['_group_key'].isin(potential_01_groups)
        filtered_df = df[mask]
        
        # 对过滤后的数据进行分组统计
        group_stats = filtered_df.groupby(['ts_code', 'end_date', 'f_ann_date'])['update_flag_num'].agg(['min', 'max', 'sum']).reset_index()
        
        # 使用数学性质识别01组合：min=0, max=1, sum=1
        target_groups = group_stats[
            (group_stats['min'] == 0) & 
            (group_stats['max'] == 1) & 
            (group_stats['sum'] == 1)
        ]
        
        calc_time = time.time() - calc_start
        logger.info(f"01组合计算耗时: {calc_time:.4f}秒")
        logger.info(f"发现符合条件的0-1组合: {len(target_groups)} 个")
        
        if len(target_groups) == 0:
            df = df.drop(columns=['update_flag_num', '_group_key'])
            logger.info("没有符合条件的0-1组合，直接返回原数据")
            return df
        
        # 3. 01组合数据处理：完全向量化处理
        process_start = time.time()
        
        # 创建目标组标识
        target_keys = set(zip(target_groups['ts_code'], target_groups['end_date'], target_groups['f_ann_date']))
        df['_is_target_group'] = df['_group_key'].isin(target_keys)
        
        # 向量化处理：保留非目标组的所有记录 + 目标组中update_flag=1的记录
        result_df = df[(~df['_is_target_group']) | (df['update_flag_num'] == 1)].copy()
        
        # 清理临时列
        result_df = result_df.drop(columns=['update_flag_num', '_group_key', '_is_target_group'])
        
        process_time = time.time() - process_start
        total_time = time.time() - start_time
        
        logger.info(f"01组合数据处理耗时: {process_time:.4f}秒")
        logger.info(f"处理0-1组合完成，总耗时: {total_time:.4f}秒，最终行数: {len(result_df)} (原始: {len(df)})")
        
        return result_df
    
    def validate_unique_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重复记录质检：检查ts_code+end_date+f_ann_date组合是否存在重复
        
        Args:
            df: 输入DataFrame
            
        Returns:
            原始DataFrame（如果通过质检）
            
        Raises:
            ValueError: 如果存在重复记录
        """
        logger.info("开始重复记录质检")
        
        # 检查关键字段组合的重复情况
        key_columns = ['ts_code', 'end_date', 'f_ann_date']
        
        # 检查是否存在重复
        duplicates = df.duplicated(subset=key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            # 获取重复记录的详细信息
            duplicate_records = df[duplicates]
            logger.error(f"发现{duplicate_count}条重复记录")
            
            # 创建debug文件夹 - 使用项目根目录下的data/norm/fina_indicator/debug
            debug_dir = self.project_root / "data" / "norm" / "fina_indicator" / "debug"
            debug_dir.mkdir(exist_ok=True)
            
            # 输出重复记录到CSV文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"duplicate_records_fin_bs_{timestamp}.csv"
            csv_path = debug_dir / csv_filename
            
            duplicate_records.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.error(f"重复记录详情已输出到: {csv_path}")
            
            # 在日志中显示前10条重复记录的关键信息
            unique_duplicates = duplicate_records[key_columns].drop_duplicates()
            logger.error("重复记录详情:")
            for _, record in unique_duplicates.head(10).iterrows():  # 只显示前10条
                logger.error(f"  ts_code={record['ts_code']}, end_date={record['end_date']}, f_ann_date={record['f_ann_date']}")
            
            raise ValueError(f"数据质检失败：发现{duplicate_count}条重复记录（ts_code+end_date+f_ann_date组合重复）")
        
        logger.info("重复记录质检通过")
        return df
    
    def output_parquet_file(self, df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """输出parquet文件，保留全部字段"""
        logger.info("开始输出parquet文件")
        
        # 确定输出路径
        if output_path is None:
            output_path = self.config.get('output_path', 'data/norm/balance_sheet/cleaned/balance_sheet_y.parquet')
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 输出parquet文件，保留所有字段
        df.to_parquet(output_path, index=False)
        
        logger.info(f"parquet文件已输出到: {output_path}")
        logger.info(f"输出数据形状: {df.shape}")
        logger.info(f"输出字段数: {len(df.columns)}")
        
        # 返回原始的完整DataFrame
        return df
    
    def process_pipeline(self, input_path: str = None, output_path: str = None) -> pd.DataFrame:
        """执行完整的数据清洗流程"""
        logger.info("开始执行资产负债表数据清洗流程")
        
        # 读取数据
        if input_path is None:
            input_path = self.config.get('input_path', 'data/raw/balance_sheet/balance_sheet_y.parquet')
        
        logger.info(f"读取数据文件: {input_path}")
        df = pd.read_parquet(input_path)
        logger.info(f"原始数据形状: {df.shape}")
        
        # 获取清洗流程配置
        pipeline_steps = self.config.get('cleaning_pipeline', [])
        
        # 执行每个清洗步骤
        for step in pipeline_steps:
            function_name = step.get('function')
            if function_name in self.cleaning_functions:
                logger.info(f"执行清洗步骤: {function_name}")
                if function_name == 'output_parquet_file':
                    df = self.cleaning_functions[function_name](df, output_path)
                else:
                    df = self.cleaning_functions[function_name](df)
                logger.info(f"步骤完成，当前数据形状: {df.shape}")
            else:
                logger.warning(f"未找到清洗函数: {function_name}")
        
        logger.info("资产负债表数据清洗流程完成")
        return df


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