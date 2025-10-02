#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging

from caiyuangungun.data.norm.core.base_processor import BaseProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_indicator_processor')


class FinIndicatorProcessor(BaseProcessor):
    """财务指标数据处理器 - 继承基础处理器，不需要勾稽关系验证"""
    
    def __init__(self, config: dict):
        super().__init__(config)
        
        # 扩展清洗函数 - 添加财务指标特有的功能
        self.cleaning_functions.update(self._register_indicator_cleaning_functions())
        
        # 财务指标处理器需要稽核配置来进行字段重命名
        self._validate_basic_config()
        
        logger.info("FinIndicatorProcessor初始化完成")
    
    def _register_indicator_cleaning_functions(self):
        """注册财务指标专用清洗函数"""
        return {
            'remove_empty_ann_date': self.remove_empty_ann_date,
            'drop_columns_and_duplicates': self.drop_columns_and_duplicates,
            'rename_financial_indicator_columns': self.rename_financial_indicator_columns,
        }
    
    def _validate_basic_config(self):
        """验证基本配置是否存在"""
        if not hasattr(self, 'config') or not self.config:
            raise ValueError("未找到配置：config为空")
        
        # 财务指标处理器需要稽核配置来进行字段重命名
        if 'audit_configs' in self.config and 'financial_statements_audit' in self.config['audit_configs']:
            audit_configs = self.config['audit_configs']['financial_statements_audit']
            if 'financial_indicators' in audit_configs:
                logger.info(f"稽核配置验证通过，包含{len(audit_configs['financial_indicators'])}个财务指标字段配置")
            else:
                logger.warning("稽核配置中缺少financial_indicators部分")
        else:
            logger.warning("未找到稽核配置，字段重命名功能将跳过")
        
        # 检查基本的路径配置
        required_keys = ['input_path', 'output_path', 'cleaning_pipeline']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            logger.warning(f"缺少配置项: {missing_keys}，将使用默认值")
        
        logger.info("基本配置验证通过")
    
    def _get_config_section(self) -> str:
        """获取配置节名称"""
        return 'financial_indicators'
    
    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        return self.project_root / "data" / "norm" / "fina_indicator" / "debug"
    
    def _get_audit_config(self) -> Optional[dict]:
        """获取稽核配置"""
        if hasattr(self, 'config') and 'audit_configs' in self.config:
            audit_configs = self.config['audit_configs']
            if 'financial_statements_audit' in audit_configs:
                full_audit_config = audit_configs['financial_statements_audit']
                # 提取financial_indicators部分
                if 'financial_indicators' in full_audit_config:
                    return full_audit_config['financial_indicators']
        return None
    
    def _get_output_fields(self, df: pd.DataFrame) -> List[str]:
        """获取财务指标输出字段列表"""
        # 获取稽核配置中的字段
        audit_columns = set()
        audit_config = self._get_audit_config()
        
        if audit_config:
            # 获取重命名后的字段名，并保持原始顺序
            audit_field_order = []
            for tushare_name, config in audit_config.items():
                renamed_field = config.get('renamed_field', tushare_name)
                audit_columns.add(renamed_field)
                audit_field_order.append(renamed_field)
            
            logger.info(f"从稽核配置中获取到{len(audit_columns)}个字段")
        else:
            audit_field_order = []
        
        # 必须保留的基础字段（按指定顺序）- 财务指标使用ann_date而不是f_ann_date
        required_fields = ['ts_code', 'ann_date', 'end_date', 'file_path', 'file_md5']
        
        # 添加用于处理的字段
        processing_fields = ['update_flag']
        
        # 按顺序构建字段列表：1.指定字段在前，2.处理字段，3.配置字段在后
        ordered_fields = []
        
        # 先添加必须字段（存在的）
        for field in required_fields:
            if field in df.columns:
                ordered_fields.append(field)
        
        # 添加处理用字段（存在的）
        for field in processing_fields:
            if field in df.columns and field not in ordered_fields:
                ordered_fields.append(field)
        
        # 再添加稽核配置字段（按配置顺序，排除已添加的）
        for field in audit_field_order:
            if field in df.columns and field not in ordered_fields:
                ordered_fields.append(field)
        
        return ordered_fields
    
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
    
    def rename_financial_indicator_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据稽核配置文件重命名财务指标字段：将Tushare英文字段名重命名为标准化字段名"""
        logger.info("开始根据稽核配置文件重命名财务指标字段")
        
        # 获取稽核配置
        audit_config = self._get_audit_config()
        
        if not audit_config:
            logger.warning("未找到财务指标稽核配置，跳过字段重命名")
            return df
        
        # 构建重命名映射
        rename_mapping = {}
        for tushare_name, config in audit_config.items():
            renamed_field = config.get('renamed_field')
            if renamed_field and tushare_name in df.columns:
                rename_mapping[tushare_name] = renamed_field
        
        if not rename_mapping:
            logger.info("没有需要重命名的字段")
            return df
        
        # 执行重命名
        df_renamed = df.rename(columns=rename_mapping)
        
        logger.info(f"重命名了{len(rename_mapping)}个字段")
        return df_renamed


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
            {'function': 'rename_financial_indicator_columns'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinIndicatorProcessor(test_config)
    result_df = processor.process_pipeline()
    print(f"处理完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()