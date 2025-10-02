#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import struct
from typing import List, Dict, Optional, Any
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import logging

from caiyuangungun.data.norm.core.base_processor import FinancialProcessor

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_is_processor')


class FinISProcessor(FinancialProcessor):
    """利润表数据专用清洗器类"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加利润表特有的功能
        self.cleaning_functions.update(self._register_is_cleaning_functions())
        
        logger.info("FinISProcessor初始化完成")
    
    def _register_is_cleaning_functions(self):
        """注册利润表专用清洗函数"""
        return {
            'drop_columns_and_duplicates': self.drop_columns_and_duplicates,
            'create_assets_impair_loss_fixed': self.create_assets_impair_loss_fixed,
            'create_credit_impa_loss_fixed': self.create_credit_impa_loss_fixed,
            'create_total_opcost_fixed': self.create_total_opcost_fixed,
            'rename_fixed_columns': self.rename_fixed_columns,
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
        return 'income_statement_audit'
    
    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'income_statement'
    
    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "income_statement" / "debug"
    
    def drop_columns_and_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """剔除ebit列和ebitda列，并对所有列执行删除重复行"""
        logger.info("开始剔除ebit和ebitda列，并删除重复行")
        
        # 剔除ebit和ebitda列
        columns_to_drop = ['ebit', 'ebitda']
        existing_columns = [col for col in columns_to_drop if col in df.columns]
        
        if existing_columns:
            df = df.drop(columns=existing_columns)
            logger.info(f"已剔除列: {existing_columns}")
        else:
            logger.info("未找到ebit或ebitda列")
        
        # 删除重复行
        before_count = len(df)
        df_cleaned = df.drop_duplicates()
        after_count = len(df_cleaned)
        removed_count = before_count - after_count
        
        logger.info(f"删除重复行完成，原始行数: {before_count}, 删除行数: {removed_count}, 剩余行数: {after_count}")
        
        return df_cleaned
    
    def create_assets_impair_loss_fixed(self, df: pd.DataFrame) -> pd.DataFrame:
        """创建资产减值损失修正列"""
        logger.info("开始创建assets_impair_loss_fixed列")
        
        # 确保end_date是数值类型
        df['end_date'] = pd.to_numeric(df['end_date'], errors='coerce')
        
        # 创建修正列
        df['assets_impair_loss_fixed'] = np.where(
            df['end_date'] >= 20190630,
            df['assets_impair_loss'],
            -1 * df['assets_impair_loss']
        )
        
        logger.info("assets_impair_loss_fixed列创建完成")
        
        return df
    
    def create_credit_impa_loss_fixed(self, df: pd.DataFrame) -> pd.DataFrame:
        """创建信用减值损失修正列"""
        logger.info("开始创建credit_impa_loss_fixed列")
        
        # 确保end_date是数值类型
        df['end_date'] = pd.to_numeric(df['end_date'], errors='coerce')
        
        # 创建修正列
        df['credit_impa_loss_fixed'] = np.where(
            df['end_date'] >= 20190630,
            df['credit_impa_loss'],
            -1 * df['credit_impa_loss']
        )
        
        logger.info("credit_impa_loss_fixed列创建完成")
        
        return df
    
    def create_total_opcost_fixed(self, df: pd.DataFrame) -> pd.DataFrame:
        """创建营业总成本修正列"""
        logger.info("开始创建total_opcost_fixed列")
        
        # 判断total_opcost是否为空，如果不为空则使用原值，否则计算
        df['total_opcost_fixed'] = np.where(
            df['total_opcost'].notna(),
            df['total_opcost'],
            df['total_cogs'] + df['credit_impa_loss_fixed'] + df['assets_impair_loss_fixed']
        )
        
        logger.info("total_opcost_fixed列创建完成")
        
        return df
    
    def rename_fixed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """重命名修正列，将_fixed列覆盖原始列"""
        logger.info("开始重命名修正列")
        
        # 定义重命名映射
        rename_mapping = {
            'total_opcost_fixed': 'total_opcost',
            'credit_impa_loss_fixed': 'credit_impa_loss',
            'assets_impair_loss_fixed': 'assets_impair_loss'
        }
        
        df = df.copy()
        
        # 处理每个修正列
        for fixed_col, original_col in rename_mapping.items():
            if fixed_col in df.columns:
                if original_col in df.columns:
                    # 删除原始列
                    df = df.drop(columns=[original_col])
                    logger.info(f"删除原始列: {original_col}")
                
                # 重命名修正列
                df = df.rename(columns={fixed_col: original_col})
                logger.info(f"重命名列: {fixed_col} -> {original_col}")
        
        return df


def main():
    """主函数 - 仅用于测试，实际使用应通过ProcessorService"""
    # 创建一个基本的测试配置
    test_config = {
        'project_root': Path(__file__).parent.parent.parent.parent.parent,
        'input_path': 'data/norm/income_statement/merged/income_y.parquet',
        'output_path': 'data/norm/income_statement/cleaned/income_y.parquet',
        'cleaning_pipeline': [
            {'function': 'remove_empty_ts_code'},
            {'function': 'drop_columns_and_duplicates'},
            {'function': 'handle_01_records'},
            {'function': 'create_assets_impair_loss_fixed'},
            {'function': 'create_credit_impa_loss_fixed'},
            {'function': 'create_total_opcost_fixed'},
            {'function': 'validate_relationship_groups'},
            {'function': 'validate_unique_records'},
            {'function': 'rename_fixed_columns'},
            {'function': 'rename_columns_by_audit_config'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinISProcessor(test_config)
    
    # 执行清洗流程
    result_df = processor.process_pipeline()
    
    print(f"清洗完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()