#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
import time 
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_indicator_processor')


class FinIndicatorProcessor:
    """财务指标数据处理器"""
    
    def __init__(self, config: dict):
        """
        初始化处理器
        
        Args:
            config: 完整的processor配置字典
        """
        self.config = config
        self.project_root = Path(config.get('project_root', Path(__file__).parent.parent.parent.parent.parent))
        self.cleaning_functions = self._register_cleaning_functions()
        
        logger.info("FinIndicatorProcessor初始化完成")
    
    def _register_cleaning_functions(self):
        """注册清洗函数"""
        return {
            'remove_empty_ts_code': self.remove_empty_ts_code,
            'remove_empty_ann_date': self.remove_empty_ann_date,
            'drop_columns_and_duplicates': self.drop_columns_and_duplicates,
            'handle_01_records': self.handle_01_records,
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
    
    def remove_empty_ann_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除ann_date为空的行"""
        logger.info("开始移除ann_date为空的行")
        
        initial_count = len(df)
        # 移除ann_date为空或NaN的行
        df_cleaned = df.dropna(subset=['ann_date'])
        
        removed_count = initial_count - len(df_cleaned)
        logger.info(f"移除了{removed_count}行ann_date为空的数据，剩余{len(df_cleaned)}行")
        
        return df_cleaned
    
    def drop_columns_and_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """删除指定列并去重"""
        logger.info("开始删除指定列并去重")
        
        # 获取要删除的列
        columns_to_drop = ['fcff', 'fcfe', 'fcff_ps', 'fcfe_ps']
        
        # 删除指定列（如果存在）
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            logger.info(f"删除了列: {existing_columns_to_drop}")
        else:
            logger.info("没有找到需要删除的列")
        
        # 去重（基于所有列）
        initial_count = len(df)
        df_deduplicated = df.drop_duplicates()
        removed_count = initial_count - len(df_deduplicated)
        
        logger.info(f"去重完成，删除了{removed_count}行重复数据，剩余{len(df_deduplicated)}行")
        
        return df_deduplicated
    
    def handle_01_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理0-1组合的重复记录 - 超高性能优化版本"""
        logger.info("开始处理0-1组合重复记录")
        
        df = df.copy()
        
        # 1. 早期筛选 - 只对可能有重复的记录进行groupby
        start_time = time.time()
        
        # 先找出所有可能的重复组合键
        key_counts = df.groupby(['ts_code', 'end_date', 'ann_date']).size()
        potential_duplicates = key_counts[key_counts == 2].index
        
        if len(potential_duplicates) == 0:
            logger.info("没有重复记录，直接返回原数据")
            return df
        
        # 只对可能重复的记录进行详细分析
        potential_mask = df.set_index(['ts_code', 'end_date', 'ann_date']).index.isin(potential_duplicates)
        potential_df = df[potential_mask]
        
        # 2. 高效的01组合识别 - 使用数值转换加速比较
        # 临时转换为数值进行快速计算
        potential_df_numeric = potential_df.copy()
        potential_df_numeric['update_flag_num'] = potential_df_numeric['update_flag'].astype(int)
        
        # 只计算必要的统计信息
        group_stats = potential_df_numeric.groupby(['ts_code', 'end_date', 'ann_date']).agg({
            'update_flag_num': ['min', 'max', 'sum']
        }).reset_index()
        
        # 扁平化列名
        group_stats.columns = ['ts_code', 'end_date', 'ann_date', 'min_flag', 'max_flag', 'sum_flag']
        
        # 01组合的特征：min=0, max=1, sum=1 (0+1=1)
        target_groups = group_stats[
            (group_stats['min_flag'] == 0) & 
            (group_stats['max_flag'] == 1) & 
            (group_stats['sum_flag'] == 1)
        ]
        
        calc_time = time.time() - start_time
        logger.info(f"01组合计算完成，耗时: {calc_time:.4f}秒，发现符合条件的0-1组合: {len(target_groups)} 个")
        
        if len(target_groups) == 0:
            logger.info("没有符合条件的0-1组合，直接返回原数据")
            return df
        
        # 3. 01组合数据处理 - 完全向量化，无遍历
        process_start_time = time.time()
        
        # 创建多重索引用于高效匹配
        target_groups_set = set(zip(target_groups['ts_code'], target_groups['end_date'], target_groups['ann_date']))
        
        # 向量化创建组合键
        df_keys = pd.Series(list(zip(df['ts_code'], df['end_date'], df['ann_date'])), index=df.index)
        
        # 向量化判断是否为目标组
        is_target_mask = df_keys.isin(target_groups_set)
        
        # 向量化筛选：对于目标组，只保留update_flag='1'的记录；对于非目标组，全部保留
        keep_mask = (~is_target_mask) | ((is_target_mask) & (df['update_flag'] == '1'))
        
        # 直接筛选结果
        result_df = df[keep_mask].copy()
        
        process_time = time.time() - process_start_time
        logger.info(f"01组合数据处理完成，耗时: {process_time:.4f}秒")
        
        total_time = calc_time + process_time
        logger.info(f"处理0-1组合完成，总耗时: {total_time:.4f}秒，最终行数: {len(result_df)} (原始: {len(df)})")
        
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
        
        # 检查是否存在重复
        duplicates = df.duplicated(subset=key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            # 获取重复记录的详细信息
            duplicate_records = df[duplicates]
            logger.error(f"发现{duplicate_count}条重复记录")
            
            # 创建debug文件夹
            debug_dir = Path("debug")
            debug_dir.mkdir(exist_ok=True)
            
            # 输出重复记录到CSV文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"duplicate_records_fin_indicator_{timestamp}.csv"
            csv_path = debug_dir / csv_filename
            
            duplicate_records.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.error(f"重复记录详情已输出到: {csv_path}")
            
            # 在日志中显示前10条重复记录的关键信息
            unique_duplicates = duplicate_records[key_columns].drop_duplicates()
            logger.error("重复记录详情:")
            for _, record in unique_duplicates.head(10).iterrows():  # 只显示前10条
                logger.error(f"  ts_code={record['ts_code']}, end_date={record['end_date']}, ann_date={record['ann_date']}")
            
            raise ValueError(f"数据质检失败：发现{duplicate_count}条重复记录（ts_code+end_date+ann_date组合重复）")
        
        logger.info("重复记录质检通过")
        return df
    
    def output_parquet_file(self, df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """输出parquet文件，保留全部字段"""
        logger.info("开始输出parquet文件")
        
        # 确定输出路径
        if output_path is None:
            output_path = self.config.get('output_path', 'data/norm/fina_indicator/cleaned/fina_indicator_vip.parquet')
        
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
        logger.info("开始执行财务指标数据清洗流程")
        
        # 读取数据
        if input_path is None:
            input_path = self.config.get('input_path', 'data/raw/fina_indicator/fina_indicator_vip.parquet')
        
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
        
        logger.info("财务指标数据清洗流程完成")
        return df


def main():
    """主函数 - 仅用于测试，实际使用应通过ProcessorService"""
    # 创建一个基本的测试配置
    test_config = {
        'project_root': Path(__file__).parent.parent.parent.parent.parent,
        'input_path': 'data/raw/fina_indicator/fina_indicator_vip.parquet',
        'output_path': 'data/norm/fina_indicator/cleaned/fina_indicator_vip.parquet',
        'cleaning_pipeline': [
            {'function': 'remove_empty_ts_code'},
            {'function': 'remove_empty_ann_date'},
            {'function': 'drop_columns_and_duplicates'},
            {'function': 'handle_01_records'},
            {'function': 'validate_unique_records'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinIndicatorProcessor(test_config)
    result_df = processor.process_pipeline()
    print(f"处理完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()