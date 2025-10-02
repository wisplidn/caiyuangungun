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
from abc import ABC, abstractmethod
from dataclasses import dataclass

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('base_processor')


@dataclass
class BatchContext:
    """批处理上下文"""
    contract: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass  
class BatchResult:
    """批处理结果"""
    clean_df: pd.DataFrame
    decisions: Dict[str, Any]
    quality_report: Dict[str, Any]


class BaseProcessor(ABC):
    """
    基础数据处理器 - 包含100%通用的功能
    
    所有数据处理器的基类，提供通用的数据清洗功能：
    - 移除空值记录
    - 处理0-1组合重复记录  
    - 重复记录质检
    - 输出parquet文件
    - 根据配置重命名列
    """
    
    def __init__(self, config: dict):
        """
        初始化基础处理器
        
        Args:
            config: 完整的processor配置字典
        """
        self.config = config
        self.project_root = Path(config.get('project_root', Path(__file__).parent.parent.parent.parent.parent))
        
        # 注册通用清洗函数
        self.cleaning_functions = self._register_base_cleaning_functions()
        
        logger.info(f"{self.__class__.__name__}初始化完成")
    
    def _register_base_cleaning_functions(self):
        """注册基础清洗函数"""
        return {
            'remove_empty_ts_code': self.remove_empty_ts_code,
            'handle_01_records': self.handle_01_records,
            'validate_unique_records': self.validate_unique_records,
            'rename_columns_by_audit_config': self.rename_columns_by_audit_config,
            'output_parquet_file': self.output_parquet_file
        }
    
    def remove_empty_ts_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """移除ts_code为空的行 - 高性能版本"""
        logger.info("开始移除ts_code为空的行")
        
        initial_count = len(df)
        # 向量化操作：直接筛选非空且非空字符串的行
        # 避免使用str.strip()，因为对千万级数据很慢
        mask = df['ts_code'].notna() & (df['ts_code'] != '') & (df['ts_code'] != ' ')
        df_cleaned = df[mask]
        
        removed_count = initial_count - len(df_cleaned)
        logger.info(f"移除了{removed_count}行ts_code为空的数据，剩余{len(df_cleaned)}行")
        
        return df_cleaned
    
    def handle_01_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理0-1组合的重复记录 - 超高性能优化版本"""
        logger.info("开始处理0-1组合重复记录")
        
        start_time = time.time()
        
        # 获取分组字段 - 支持不同的分组方式
        group_fields = self._get_grouping_fields(df)
        
        # 1. 早期过滤：使用groupby().size()快速识别可能的01组合
        group_sizes = df.groupby(group_fields).size()
        potential_01_groups = group_sizes[group_sizes == 2].index
        
        if len(potential_01_groups) == 0:
            logger.info("没有符合条件的0-1组合，直接返回原数据")
            return df
        
        # 2. 01组合计算：仅对潜在组进行详细分析
        calc_start = time.time()
        
        # 只对可能重复的记录进行详细分析，避免全表copy
        potential_mask = df.set_index(group_fields).index.isin(potential_01_groups)
        potential_df = df[potential_mask].copy()  # 只复制需要分析的部分
        
        # 临时转换为数值进行快速计算
        potential_df['update_flag_num'] = pd.to_numeric(potential_df['update_flag'], errors='coerce')
        
        # 只计算必要的统计信息
        group_stats = potential_df.groupby(group_fields).agg({
            'update_flag_num': ['min', 'max', 'sum']
        }).reset_index()
        
        # 扁平化列名
        columns = group_fields + ['min_flag', 'max_flag', 'sum_flag']
        group_stats.columns = columns
        
        # 01组合的特征：min=0, max=1, sum=1 (0+1=1)
        target_groups = group_stats[
            (group_stats['min_flag'] == 0) & 
            (group_stats['max_flag'] == 1) & 
            (group_stats['sum_flag'] == 1)
        ]
        
        calc_time = time.time() - calc_start
        logger.info(f"01组合计算完成，耗时: {calc_time:.4f}秒，发现符合条件的0-1组合: {len(target_groups)} 个")
        
        if len(target_groups) == 0:
            logger.info("没有符合条件的0-1组合，直接返回原数据")
            return df
        
        # 3. 01组合数据处理：分离处理策略（最高效）
        process_start = time.time()
        
        # 创建目标组的索引集合
        target_tuples = set()
        for _, row in target_groups.iterrows():
            target_tuples.add(tuple(row[field] for field in group_fields))
        
        # 分离目标组和非目标组的数据
        df_copy = df.copy()  # 避免修改原始数据
        df_copy['_temp_key'] = list(zip(*[df_copy[field] for field in group_fields]))
        
        # 非目标组：直接保留所有记录
        non_target_mask = ~df_copy['_temp_key'].isin(target_tuples)
        non_target_df = df_copy[non_target_mask].drop(columns=['_temp_key'])
        
        # 目标组：只保留update_flag=1的记录
        target_mask = df_copy['_temp_key'].isin(target_tuples)
        target_df = df_copy[target_mask].copy()
        target_df['update_flag_num'] = pd.to_numeric(target_df['update_flag'], errors='coerce')
        target_filtered_df = target_df[target_df['update_flag_num'] == 1].drop(columns=['_temp_key', 'update_flag_num'])
        
        # 合并结果
        result_df = pd.concat([non_target_df, target_filtered_df], ignore_index=True)
        
        process_time = time.time() - process_start
        total_time = time.time() - start_time
        
        logger.info(f"01组合数据处理耗时: {process_time:.4f}秒")
        logger.info(f"处理0-1组合完成，总耗时: {total_time:.4f}秒，最终行数: {len(result_df)} (原始: {len(df)})")
        
        return result_df
    
    def _get_grouping_fields(self, df: pd.DataFrame) -> List[str]:
        """
        获取分组字段，支持不同数据类型的分组方式
        
        Args:
            df: 输入DataFrame
            
        Returns:
            分组字段列表
        """
        # 财务数据通常使用 ['ts_code', 'end_date', 'f_ann_date']
        if 'f_ann_date' in df.columns:
            return ['ts_code', 'end_date', 'f_ann_date']
        # 财务指标数据使用 ['ts_code', 'end_date', 'ann_date']  
        elif 'ann_date' in df.columns:
            return ['ts_code', 'end_date', 'ann_date']
        # 默认分组
        else:
            return ['ts_code', 'end_date']
    
    def validate_unique_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        重复记录质检：检查关键字段组合是否存在重复
        
        Args:
            df: 输入DataFrame
            
        Returns:
            原始DataFrame（如果通过质检）
            
        Raises:
            ValueError: 如果存在重复记录
        """
        logger.info("开始重复记录质检")
        
        # 获取关键字段组合
        key_columns = self._get_grouping_fields(df)
        
        # 检查是否存在重复
        duplicates = df.duplicated(subset=key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            # 获取重复记录的详细信息
            duplicate_records = df[duplicates]
            logger.error(f"发现{duplicate_count}条重复记录")
            
            # 创建debug文件夹
            debug_dir = self._get_debug_dir()
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # 输出重复记录到CSV文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"duplicate_records_{self.__class__.__name__.lower()}_{timestamp}.csv"
            csv_path = debug_dir / csv_filename
            
            duplicate_records.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.error(f"重复记录详情已输出到: {csv_path}")
            
            # 在日志中显示前10条重复记录的关键信息
            unique_duplicates = duplicate_records[key_columns].drop_duplicates()
            logger.error("重复记录详情:")
            for _, record in unique_duplicates.head(10).iterrows():  # 只显示前10条
                key_info = ", ".join([f"{col}={record[col]}" for col in key_columns])
                logger.error(f"  {key_info}")
            
            key_fields_str = "+".join(key_columns)
            raise ValueError(f"数据质检失败：发现{duplicate_count}条重复记录（{key_fields_str}组合重复）")
        
        logger.info("重复记录质检通过")
        return df
    
    def rename_columns_by_audit_config(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据统一配置文件重命名列"""
        logger.info("开始根据统一配置文件重命名列")
        
        # 获取配置
        unified_config = self._get_unified_config()
        
        if not unified_config:
            logger.warning("未找到统一配置，跳过列重命名")
            return df
        
        # 获取配置节名称（如 balance_sheet, income_statement 等）
        config_section = self._get_config_section()
        
        # 从统一配置中提取对应的配置节
        section_config = unified_config.get(config_section, {})
        
        if not section_config:
            logger.warning(f"统一配置中未找到 {config_section} 配置节，跳过列重命名")
            return df
        
        # 创建重命名映射
        rename_mapping = {}
        for tushare_name, config in section_config.items():
            renamed_field = config.get('renamed_field', tushare_name)
            if tushare_name in df.columns and tushare_name != renamed_field:
                rename_mapping[tushare_name] = renamed_field
        
        if rename_mapping:
            df = df.rename(columns=rename_mapping)
            logger.info(f"从 {config_section} 节重命名了{len(rename_mapping)}个列")
        else:
            logger.info(f"{config_section} 节中没有需要重命名的列")
        
        return df
    
    def output_parquet_file(self, df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """输出parquet文件，保留指定字段 - 高性能版本"""
        logger.info("开始输出parquet文件")
        
        # 确定输出路径
        if output_path is None:
            output_path = self.config.get('output_path')
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 获取需要保留的字段
        ordered_fields = self._get_output_fields(df)
        
        if not ordered_fields:
            logger.warning("没有找到任何需要保留的字段，保留所有字段")
            output_df = df
        else:
            output_df = df[ordered_fields]
            logger.info(f"保留了{len(ordered_fields)}个字段（按指定顺序）")
        
        # 使用PyArrow引擎输出，性能更好，压缩率更高
        output_df.to_parquet(
            output_path, 
            index=False, 
            engine='pyarrow',
            compression='snappy'  # snappy压缩速度快
        )
        
        logger.info(f"parquet文件已输出到: {output_path}")
        logger.info(f"输出数据形状: {output_df.shape}")
        logger.info(f"输出字段数: {len(output_df.columns)}")
        
        # 返回输出的DataFrame
        return output_df
    
    def process_pipeline(self, input_path: str = None, output_path: str = None) -> pd.DataFrame:
        """执行完整的数据清洗流程"""
        logger.info(f"开始执行{self.__class__.__name__}数据清洗流程")
        
        # 读取数据
        if input_path is None:
            input_path = self.config.get('input_path')
        
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
        
        logger.info(f"{self.__class__.__name__}数据清洗流程完成")
        return df
    
    # 抽象方法 - 子类必须实现
    @abstractmethod
    def _get_debug_dir(self) -> Path:
        """获取debug目录路径"""
        pass
    
    @abstractmethod
    def _get_audit_config(self) -> Optional[dict]:
        """获取稽核配置"""
        pass
    
    def _get_unified_config(self) -> Optional[dict]:
        """获取统一配置 - 通用实现，从config中获取unified_field_config"""
        if hasattr(self, 'config') and 'unified_field_config' in self.config:
            # 返回完整的统一配置
            return self.config['unified_field_config']
        
        # 如果config中没有，尝试从配置文件加载（兜底方案）
        logger.warning("配置中未找到unified_field_config，尝试从文件加载")
        import json
        try:
            config_path = self.project_root / "data" / "config" / "rename_config.json"
            with open(config_path, 'r', encoding='utf-8') as f:
                unified_config = json.load(f)
            
            return unified_config
        except Exception as e:
            logger.warning(f"无法加载统一配置文件: {e}")
            return None
    
    @abstractmethod
    def _get_config_section(self) -> str:
        """获取配置节名称 - 子类必须实现"""
        pass
    
    @abstractmethod
    def _get_output_fields(self, df: pd.DataFrame) -> List[str]:
        """获取输出字段列表"""
        pass


class FinancialProcessor(BaseProcessor):
    """
    财务数据处理器基类 - 包含财务数据通用功能
    
    为财务报表数据（资产负债表、利润表、现金流量表）提供通用功能：
    - 勾稽关系验证
    - 稽核配置管理
    - 财务数据特有的处理逻辑
    """
    
    def __init__(self, config: dict):
        super().__init__(config)
        
        # 早期验证：检查必要的稽核配置
        self._validate_audit_config()
        
        # 扩展清洗函数
        self.cleaning_functions.update(self._register_financial_cleaning_functions())
    
    def _register_financial_cleaning_functions(self):
        """注册财务数据专用清洗函数"""
        return {
            'validate_relationship_groups': self.validate_relationship_groups,
        }
    
    @abstractmethod
    def _validate_audit_config(self):
        """验证稽核配置是否存在 - 子类必须实现"""
        pass
    
    @abstractmethod
    def _get_audit_config_key(self) -> str:
        """获取稽核配置的key - 子类必须实现"""
        pass
    
    def _get_audit_config(self) -> Optional[dict]:
        """获取稽核配置"""
        if not hasattr(self, 'config') or 'audit_configs' not in self.config:
            return None
        
        audit_configs = self.config['audit_configs']
        audit_config_key = self._get_audit_config_key()
        
        if audit_config_key not in audit_configs:
            return None
        
        full_audit_config = audit_configs[audit_config_key]
        
        # 如果是新的分组结构，提取对应部分
        config_section = self._get_config_section()
        if config_section in full_audit_config:
            return full_audit_config[config_section]
        else:
            return full_audit_config
    
    @abstractmethod
    def _get_config_section(self) -> str:
        """获取配置节名称 - 子类必须实现"""
        pass
    
    def validate_relationship_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """验证勾稽关系：检查各个分组的层级1求和与汇总项之间的差异"""
        logger.info("开始验证勾稽关系")
        
        # 获取稽核配置
        audit_config = self._get_audit_config()
        
        if not audit_config:
            raise ValueError("勾稽关系验证失败：未找到稽核配置")
        
        # 解析relationship分组
        groups = self._parse_relationship_groups(audit_config)
        
        if not groups:
            logger.info("未找到可验证的分组，跳过勾稽关系验证")
            return df
        
        logger.info(f"发现 {len(groups)} 个分组需要验证")
        
        # 筛选comp_type=1的记录（支持字符串和数字类型）
        if 'comp_type' in df.columns:
            # 兼容字符串和数字类型的comp_type
            test_df = df[(df['comp_type'] == 1) | (df['comp_type'] == '1')].copy()
            logger.info(f"筛选comp_type=1的记录: {len(test_df)} 条")
        else:
            logger.warning("未找到comp_type字段，使用全部数据")
            test_df = df.copy()
        
        if len(test_df) == 0:
            logger.warning("没有可用于验证的数据")
            return df
        
        # 验证各个分组 - 添加总体性能监控
        validation_start_time = time.time()
        validation_results = []
        
        for group_id, group_info in groups.items():
            result = self._validate_single_group(test_df, group_id, group_info)
            if result:
                validation_results.append(result)
        
        total_validation_time = time.time() - validation_start_time
        logger.info(f"\n所有分组验证总耗时: {total_validation_time:.2f}秒")
        
        # 输出验证结果汇总
        self._print_validation_summary(validation_results)
        
        return df
    
    def _parse_relationship_groups(self, audit_config: dict) -> dict:
        """解析relationship字段，按分组组织数据。处理复杂关系如5_;6_1，并处理负号前缀如-5_1"""
        groups = {}
        
        for tushare_name, config in audit_config.items():
            relationship = config.get('relationship', '')
            report_item = config.get('report_item', '')
            
            if not relationship:
                continue
            
            # 处理汇总项目（不包含下划线的relationship）
            if '_' not in relationship:
                group_num = relationship
                if group_num not in groups:
                    groups[group_num] = {
                        "group_name": "",
                        "level_1_items": [],
                        "summary_item": "",
                        "summary_item_name": ""
                    }
                
                if not groups[group_num]["summary_item"]:
                    groups[group_num]["summary_item"] = tushare_name
                    groups[group_num]["summary_item_name"] = report_item
                    groups[group_num]["group_name"] = report_item
                continue
            
            # 处理复杂关系（如"5_;6_1"）
            relationship_parts = relationship.split(';')
            
            for rel_part in relationship_parts:
                rel_part = rel_part.strip()
                if not rel_part or '_' not in rel_part:
                    continue
                
                # 解析单个relationship，处理负号前缀
                is_negative = rel_part.startswith('-')
                if is_negative:
                    rel_part = rel_part[1:]  # 移除负号前缀
                
                parts = rel_part.split('_')
                group_num = parts[0]
                level = parts[1] if len(parts) > 1 else ''
                
                if group_num not in groups:
                    groups[group_num] = {
                        "group_name": "",
                        "level_1_items": [],
                        "summary_item": "",
                        "summary_item_name": ""
                    }
                
                if level == '1':
                    # 层级1项目
                    item_info = {
                        "tushare_name": tushare_name,
                        "report_item": report_item,
                        "is_negative": is_negative  # 添加负数标识
                    }
                    # 避免重复添加
                    if item_info not in groups[group_num]["level_1_items"]:
                        groups[group_num]["level_1_items"].append(item_info)
                elif level == '':
                    # 汇总项目（如果已经有汇总项，优先保留第一个）
                    if not groups[group_num]["summary_item"]:
                        groups[group_num]["summary_item"] = tushare_name
                        groups[group_num]["summary_item_name"] = report_item
                        groups[group_num]["group_name"] = report_item
        
        # 过滤掉没有汇总项或没有层级1项目的分组
        filtered_groups = {k: v for k, v in groups.items() 
                          if v["summary_item"] and v["level_1_items"]}
        
        return filtered_groups
    
    def _validate_single_group(self, df: pd.DataFrame, group_id: str, group_info: dict) -> dict:
        """验证单个分组的勾稽关系 - 高性能优化版本"""
        start_time = time.time()
        logger.info(f"\n=== 验证分组 {group_id}: {group_info['group_name']} ===")
        
        # 获取层级1项目的字段名
        level_1_fields = [item["tushare_name"] for item in group_info["level_1_items"]]
        summary_field = group_info["summary_item"]
        
        # 检查字段是否存在
        available_level_1_fields = [field for field in level_1_fields if field in df.columns]
        
        if summary_field not in df.columns:
            logger.warning(f"汇总字段 {summary_field} 不存在，跳过分组 {group_id}")
            return None
        
        if not available_level_1_fields:
            logger.warning(f"分组 {group_id} 没有可用的层级1字段，跳过")
            return None
        
        logger.info(f"汇总项: {summary_field}")
        logger.info(f"层级1项目 ({len(available_level_1_fields)}/{len(level_1_fields)}): {available_level_1_fields}")
        
        # 高性能向量化计算层级1求和，支持负数项目处理
        level_1_data = df[available_level_1_fields]
        
        # 处理负数项目（如"减:库存股"）- 通用化方案
        adjusted_level_1_data = level_1_data.copy()
        
        # 识别并处理负数项目（基于relationship字段的前置符号）
        for item in group_info["level_1_items"]:
            field_name = item["tushare_name"]
            report_item = item.get("report_item", "")
            is_negative = item.get("is_negative", False)
            
            if field_name in adjusted_level_1_data.columns:
                # 检查是否为负数项目（基于relationship字段的前置符号）
                if is_negative:
                    # 将负数项目的值取负数
                    adjusted_level_1_data[field_name] = -adjusted_level_1_data[field_name]
                    logger.debug(f"处理负数项目: {field_name} ({report_item}) - 基于relationship前置符号")
        
        # 使用向量化操作替代apply，大幅提升性能
        # 对于null值处理：先填充0，然后求和，最后处理全null的情况
        level_1_filled = adjusted_level_1_data.fillna(0)
        calculated_sum = level_1_filled.sum(axis=1)
        
        # 处理全都是null的情况：如果原始数据全都是null，结果应该是null
        all_null_mask = level_1_data.isna().all(axis=1)
        calculated_sum.loc[all_null_mask] = np.nan
        
        # 获取汇总项数据
        summary_data = df[summary_field]
        
        # 向量化计算差异
        difference = calculated_sum - summary_data
        abs_difference = difference.abs()
        
        # 快速筛选非空的记录
        valid_mask = ~(calculated_sum.isna() | summary_data.isna())
        valid_count = valid_mask.sum()
        
        if valid_count == 0:
            logger.warning(f"分组 {group_id} 没有有效的对比数据")
            return None
        
        # 向量化计算差异大于5万的比例
        large_diff_mask = (abs_difference > 50000) & valid_mask
        large_diff_count = large_diff_mask.sum()
        large_diff_percentage = (large_diff_count / valid_count * 100) if valid_count > 0 else 0
        
        calc_time = time.time() - start_time
        
        # 输出结果
        logger.info(f"有效记录数: {valid_count:,}")
        logger.info(f"差异>5万记录数: {large_diff_count:,}")
        logger.info(f"差异>5万百分比: {large_diff_percentage:.2f}%")
        logger.info(f"计算耗时: {calc_time:.2f}秒")
        
        # 获取阈值 - 允许子类自定义
        threshold = self._get_validation_threshold(group_id)
        
        # 阈值检查：如果差异>5万记录数占总有效记录数的比例超过阈值则报错
        if large_diff_percentage > threshold:
            error_msg = f"勾稽关系验证失败：分组 {group_id} ({group_info['group_name']}) 差异>5万记录数比例 {large_diff_percentage:.2f}% 超过{threshold}%阈值"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 如果差异比例较高，显示一些具体例子
        if large_diff_percentage > 1:
            logger.info("差异较大的前3个案例:")
            # 优化样本获取，只获取前3个
            large_diff_indices = df.index[large_diff_mask]
            sample_indices = large_diff_indices[:3] if len(large_diff_indices) >= 3 else large_diff_indices
            
            for idx in sample_indices:
                calc_val = calculated_sum.iloc[idx] if not np.isnan(calculated_sum.iloc[idx]) else 0
                summ_val = summary_data.iloc[idx] if not np.isnan(summary_data.iloc[idx]) else 0
                diff_val = difference.iloc[idx] if not np.isnan(difference.iloc[idx]) else 0
                
                logger.info(f"  {df.iloc[idx]['ts_code']} {df.iloc[idx]['end_date']}: "
                           f"计算值={calc_val:,.0f}, 汇总值={summ_val:,.0f}, 差异={diff_val:,.0f}")
        
        return {
            "group_id": group_id,
            "group_name": group_info['group_name'],
            "valid_records": valid_count,
            "large_diff_count": large_diff_count,
            "large_diff_percentage": large_diff_percentage,
            "level_1_fields_count": len(available_level_1_fields),
            "total_level_1_fields": len(level_1_fields),
            "calc_time": calc_time
        }
    
    def _get_validation_threshold(self, group_id: str) -> float:
        """获取验证阈值，子类可以重写以自定义阈值"""
        return 3.0  # 默认3%阈值
    
    def _print_validation_summary(self, results: list):
        """打印验证结果汇总"""
        if not results:
            logger.info("没有验证结果")
            return
        
        logger.info("\n=== 勾稽关系验证汇总报告 ===")
        logger.info(f"{'ID':<4} {'分组名称':<25} {'字段数':<10} {'有效记录':<12} {'差异>5万%':<12} {'计算耗时':<10}")
        logger.info("-" * 85)
        
        for result in results:
            group_name = result['group_name'][:23] if len(result['group_name']) > 23 else result['group_name']
            calc_time = result.get('calc_time', 0)
            logger.info(f"{result['group_id']:<4} {group_name:<25} "
                       f"{result['level_1_fields_count']}/{result['total_level_1_fields']:<10} "
                       f"{result['valid_records']:,<12} "
                       f"{result['large_diff_percentage']:.1f}%<11 "
                       f"{calc_time:.2f}s")
        
        # 统计总体情况
        total_groups = len(results)
        high_diff_groups = sum(1 for r in results if r['large_diff_percentage'] > 3)
        
        logger.info(f"\n总计: {total_groups} 个分组, 其中 {high_diff_groups} 个分组差异比例>3%")
        logger.info("=== 勾稽关系验证完成 ===\n")
    
    
    def _get_output_fields(self, df: pd.DataFrame) -> List[str]:
        """获取财务数据输出字段列表"""
        # 获取统一配置中的字段
        audit_columns = set()
        unified_config = self._get_unified_config()
        
        if unified_config:
            # 获取配置节名称（如 balance_sheet, income_statement 等）
            config_section = self._get_config_section()
            
            # 从统一配置中提取对应的配置节
            section_config = unified_config.get(config_section, {})
            
            if not section_config:
                logger.warning(f"统一配置中未找到 {config_section} 配置节")
                audit_field_order = []
            else:
                # 获取重命名后的字段名，并保持原始顺序
                audit_field_order = []
                for tushare_name, config in section_config.items():
                    renamed_field = config.get('renamed_field', tushare_name)
                    audit_columns.add(renamed_field)
                    audit_field_order.append(renamed_field)
                
                logger.info(f"从统一配置的 {config_section} 节中获取到{len(audit_columns)}个字段")
        else:
            audit_field_order = []
        
        # 必须保留的基础字段（按指定顺序）
        required_fields = ['ts_code', 'f_ann_date', 'end_date', 'file_path', 'file_md5']
        
        # 添加用于勾稽关系验证的字段
        validation_fields = ['comp_type', 'ann_date', 'update_flag']
        
        # 按顺序构建字段列表：1.指定字段在前，2.验证字段，3.配置字段在后
        ordered_fields = []
        
        # 先添加必须字段（存在的）
        for field in required_fields:
            if field in df.columns:
                ordered_fields.append(field)
        
        # 添加验证用字段（存在的）
        for field in validation_fields:
            if field in df.columns and field not in ordered_fields:
                ordered_fields.append(field)
        
        # 再添加配置字段（按配置顺序，排除已添加的）
        for field in audit_field_order:
            if field in df.columns and field not in ordered_fields:
                ordered_fields.append(field)
        
        return ordered_fields
