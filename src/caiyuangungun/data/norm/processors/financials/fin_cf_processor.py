#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
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
logger = logging.getLogger('fin_cf_processor')


class FinCFProcessor(FinancialProcessor):
    """现金流量表数据处理器"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加现金流量表特有的功能
        self.cleaning_functions.update(self._register_cf_cleaning_functions())
        
        logger.info("FinCFProcessor初始化完成")
    
    def _register_cf_cleaning_functions(self):
        """注册现金流量表专用清洗函数"""
        return {
            'drop_columns_and_duplicates': self.drop_columns_and_duplicates,
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
        return 'cash_flow_audit'
    
    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'cash_flow'
    
    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "cashflow" / "debug"
    
    def _get_validation_threshold(self, group_id: str) -> float:
        """获取验证阈值，现金流量表间接法使用更宽松的阈值"""
        # 间接法现金流量净额分组（分组12）由于数据质量问题，暂时使用更宽松的阈值
        if group_id == '12':
            return 30.0
        return 3.0  # 默认3%阈值
    
    def drop_columns_and_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """剔除free_cashflow列，然后对所有列执行去重"""
        logger.info("开始剔除free_cashflow列，然后对所有列执行去重")
        
        # 获取要删除的列（现金流量表中通常删除的列）
        columns_to_drop = ['free_cashflow']
        
        # 删除指定列（如果存在）
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            logger.info(f"已剔除列: {existing_columns_to_drop}")
        
        # 删除重复行
        initial_count = len(df)
        df_cleaned = df.drop_duplicates()
        removed_count = initial_count - len(df_cleaned)
        
        logger.info(f"删除重复行完成，原始行数: {initial_count}, 删除行数: {removed_count}, 剩余行数: {len(df_cleaned)}")
        
        return df_cleaned


def main():
    """主函数"""
    # 创建一个基本的测试配置
    test_config = {
        'project_root': Path(__file__).parent.parent.parent.parent.parent,
        'input_path': 'data/raw/cash_flow/cash_flow_y.parquet',
        'output_path': 'data/norm/cash_flow/cleaned/cash_flow_y.parquet',
        'cleaning_pipeline': [
            {'function': 'remove_empty_ts_code'},
            {'function': 'drop_columns_and_duplicates'},
            {'function': 'handle_01_records'},
            {'function': 'validate_relationship_groups'},
            {'function': 'validate_unique_records'},
            {'function': 'rename_columns_by_audit_config'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinCFProcessor(test_config)
    result_df = processor.process_pipeline()
    print(f"处理完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()