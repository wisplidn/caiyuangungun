"""
利润表数据专用清洗器
基于specialized_cleaning_pipeline_config.json配置实现专用清洗流程
"""

import os
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Any
from pathlib import Path
import logging
from datetime import datetime
import time

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('fin_is_processor')


class FinISProcessor:
    """利润表数据专用清洗器类"""
    
    def __init__(self, config: dict):
        """
        初始化利润表数据专用清洗器
        
        Args:
            config: 完整的processor配置字典
        """
        self.config = config
        self.project_root = Path(config.get('project_root', Path(__file__).parent.parent.parent.parent.parent))
        
        # 注册所有清洗函数
        self._register_cleaning_functions()
        
        logger.info("FinISProcessor初始化完成")
        
    
    def _register_cleaning_functions(self):
        """注册所有可用的清洗函数"""
        self.cleaning_functions = {
            'remove_empty_ts_code': self.remove_empty_ts_code,
            'drop_columns_and_duplicates': self.drop_columns_and_duplicates,
            'handle_01_records': self.handle_01_records,
            'create_assets_impair_loss_fixed': self.create_assets_impair_loss_fixed,
            'create_credit_impa_loss_fixed': self.create_credit_impa_loss_fixed,
            'create_total_opcost_fixed': self.create_total_opcost_fixed,
            'validate_total_revenue': self.validate_total_revenue,
            'validate_total_opcost': self.validate_total_opcost,
            'validate_operate_profit': self.validate_operate_profit,
            'validate_net_income': self.validate_net_income,
            'validate_unique_records': self.validate_unique_records,
            'rename_fixed_columns': self.rename_fixed_columns,
            'output_parquet_file': self.output_parquet_file
        }
    
    def remove_empty_ts_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """剔除ts_code为空的行"""
        logger.info("开始剔除ts_code为空的行")
        before_count = len(df)
        
        # 剔除ts_code为空的行
        df_cleaned = df.dropna(subset=['ts_code'])
        df_cleaned = df_cleaned[df_cleaned['ts_code'] != '']
        
        after_count = len(df_cleaned)
        removed_count = before_count - after_count
        
        logger.info(f"剔除ts_code为空的行完成，原始行数: {before_count}, 剔除行数: {removed_count}, 剩余行数: {after_count}")
        
        return df_cleaned
    
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
    
    def _prepare_numeric_data(self, df: pd.DataFrame, target_col: str, sum_cols: List[str]) -> pd.DataFrame:
        """
        统一的数值数据准备方法
        - target_col: 目标字段（保持原始空值）
        - sum_cols: 求和字段（空值转为0）
        """
        result = df.copy()
        
        # 目标字段：只转换数据类型，保持空值
        if target_col in result.columns:
            result[target_col] = pd.to_numeric(result[target_col], errors='coerce')
        
        # 求和字段：转换数据类型并填充0
        for col in sum_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0)
        
        return result
    
    def _validate_calculation(self, df: pd.DataFrame, target_col: str, sum_cols: List[str], 
                            calc_total_col: str, calc_sum_col: str, diff_col: str,
                            threshold: float = 50000, max_error_rate: float = 0.01,
                            validation_name: str = "计算") -> pd.DataFrame:
        """
        通用的计算验证方法
        """
        # 数据准备
        prepared_df = self._prepare_numeric_data(df, target_col, sum_cols)
        
        # 计算
        prepared_df[calc_total_col] = prepared_df[target_col]
        prepared_df[calc_sum_col] = prepared_df[sum_cols].sum(axis=1)
        prepared_df[diff_col] = prepared_df[calc_total_col] - prepared_df[calc_sum_col]
        
        # 验证（只对非空值记录）
        valid_records = prepared_df[~pd.isna(prepared_df[calc_total_col])]
        error_records = valid_records[abs(valid_records[diff_col]) > threshold]
        error_count = len(error_records)
        total_count = len(valid_records)
        error_rate = error_count / total_count if total_count > 0 else 0
        
        logger.info(f"{validation_name}核对结果: 有效记录数={total_count}, 误差>{threshold/10000}万记录数={error_count}, 误差率={error_rate:.4f}")
        
        if error_rate > max_error_rate:
            error_msg = f"{validation_name}核对失败，误差率{error_rate:.4f}超过{max_error_rate*100}%阈值"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"{validation_name}核对通过")
        return prepared_df

    def validate_total_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        """营业收入核对"""
        logger.info("开始营业收入核对")
        
        # 筛选comp_type=1的记录
        comp_type_1 = df[df['comp_type'] == "1"].copy()
        
        if len(comp_type_1) == 0:
            logger.warning("未找到comp_type=1的记录，跳过营业收入核对")
            return df
        
        # 使用通用验证方法
        validated_df = self._validate_calculation(
            comp_type_1,
            target_col='total_revenue',
            sum_cols=['revenue', 'int_income', 'prem_earned', 'comm_income'],
            calc_total_col='revenue_calc_total',
            calc_sum_col='revenue_calc_sum',
            diff_col='revenue_diff',
            validation_name='营业收入'
        )
        
        # 将计算列合并回原DataFrame
        calc_cols = ['revenue_calc_total', 'revenue_calc_sum', 'revenue_diff']
        df.loc[df['comp_type'] == "1", calc_cols] = validated_df[calc_cols]
        
        return df

    def validate_total_opcost(self, df: pd.DataFrame) -> pd.DataFrame:
        """营业成本核对"""
        logger.info("开始营业成本核对")
        
        # 筛选comp_type=1的记录
        comp_type_1 = df[df['comp_type'] == "1"].copy()
        
        if len(comp_type_1) == 0:
            logger.warning("未找到comp_type=1的记录，跳过营业成本核对")
            return df
        
        # 使用通用验证方法
        validated_df = self._validate_calculation(
            comp_type_1,
            target_col='total_opcost_fixed',
            sum_cols=['oper_cost', 'int_exp', 'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp', 'fin_exp', 'rd_exp'],
            calc_total_col='opcost_calc_total',
            calc_sum_col='opcost_calc_sum',
            diff_col='opcost_diff',
            validation_name='营业成本'
        )
        
        # 将计算列合并回原DataFrame
        calc_cols = ['opcost_calc_total', 'opcost_calc_sum', 'opcost_diff']
        df.loc[df['comp_type'] == "1", calc_cols] = validated_df[calc_cols]
        
        return df

    def validate_operate_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """营业利润核对"""
        logger.info("开始营业利润核对")
        
        # 筛选comp_type=1的记录
        comp_type_1 = df[df['comp_type'] == "1"].copy()
        
        if len(comp_type_1) == 0:
            logger.warning("未找到comp_type=1的记录，跳过营业利润核对")
            return df
        
        # 准备数据 - 包含完整的营业利润计算字段
        prepared_df = self._prepare_numeric_data(
            comp_type_1,
            target_col='operate_profit',
            sum_cols=['total_revenue', 'total_opcost_fixed', 'fv_value_chg_gain', 'invest_income', 
                     'assets_impair_loss_fixed', 'credit_impa_loss_fixed', 'asset_disp_income', 
                     'oth_income', 'forex_gain', 'net_expo_hedging_benefits']
        )
        
        # 营业利润验证逻辑：计算 operate_profit - (total_revenue - total_opcost_fixed + 其他收益项)
        # 这是验证营业利润与其组成部分的差异
        prepared_df['profit_calc_base'] = prepared_df['total_revenue'] - prepared_df['total_opcost_fixed']
        prepared_df['profit_calc_adjustments'] = (
            prepared_df['fv_value_chg_gain'] + prepared_df['invest_income'] + 
            prepared_df['assets_impair_loss_fixed'] + prepared_df['credit_impa_loss_fixed'] + 
            prepared_df['asset_disp_income'] + prepared_df['oth_income'] + 
            prepared_df['forex_gain'] + prepared_df['net_expo_hedging_benefits']
        )
        prepared_df['profit_calc_expected'] = prepared_df['profit_calc_base'] + prepared_df['profit_calc_adjustments']
        prepared_df['profit_actual'] = prepared_df['operate_profit']
        # 计算差异：实际营业利润 - 计算出的营业利润
        prepared_df['profit_diff'] = prepared_df['profit_actual'] - prepared_df['profit_calc_expected']
        
        # 验证（只对非空值记录）
        valid_records = prepared_df[~pd.isna(prepared_df['profit_actual']) & ~pd.isna(prepared_df['profit_calc_expected'])]
        error_records = valid_records[abs(valid_records['profit_diff']) > 50000]
        error_count = len(error_records)
        total_count = len(valid_records)
        error_rate = error_count / total_count if total_count > 0 else 0
        
        logger.info(f"营业利润核对结果: 有效记录数={total_count}, 误差>5万记录数={error_count}, 误差率={error_rate:.4f}")
        
        if error_rate > 0.015:  # 1.5%阈值
            error_msg = f"营业利润核对失败，误差率{error_rate:.4f}超过1.5%阈值"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("营业利润核对通过")
        
        # 将计算列合并回原DataFrame
        calc_cols = ['profit_calc_base', 'profit_calc_adjustments', 'profit_calc_expected', 'profit_actual', 'profit_diff']
        df.loc[df['comp_type'] == "1", calc_cols] = prepared_df[calc_cols]
        
        return df

    def validate_net_income(self, df: pd.DataFrame) -> pd.DataFrame:
        """净利润核对"""
        logger.info("开始净利润核对")
        
        # 筛选comp_type=1的记录
        comp_type_1 = df[df['comp_type'] == "1"].copy()
        
        if len(comp_type_1) == 0:
            logger.warning("未找到comp_type=1的记录，跳过净利润核对")
            return df
        
        # 准备数据
        prepared_df = self._prepare_numeric_data(
            comp_type_1,
            target_col='n_income',
            sum_cols=['operate_profit', 'non_oper_income', 'non_oper_exp', 'income_tax']
        )
        
        # 净利润的特殊计算逻辑
        prepared_df['net_income_calc_base'] = prepared_df['operate_profit']
        prepared_df['net_income_calc_adjustments'] = (
            prepared_df['non_oper_income'] - 
            prepared_df['non_oper_exp'] - 
            prepared_df['income_tax']
        )
        prepared_df['net_income_calc_expected'] = prepared_df['net_income_calc_base'] + prepared_df['net_income_calc_adjustments']
        prepared_df['net_income_actual'] = prepared_df['n_income']
        prepared_df['net_income_diff'] = prepared_df['net_income_calc_expected'] - prepared_df['net_income_actual']
        
        # 验证（只对非空值记录）
        valid_records = prepared_df[~pd.isna(prepared_df['net_income_actual']) & ~pd.isna(prepared_df['net_income_calc_expected'])]
        error_records = valid_records[abs(valid_records['net_income_diff']) > 50000]
        error_count = len(error_records)
        total_count = len(valid_records)
        error_rate = error_count / total_count if total_count > 0 else 0
        
        logger.info(f"净利润核对结果: 有效记录数={total_count}, 误差>5万记录数={error_count}, 误差率={error_rate:.4f}")
        
        # # 导出CSV文件供检查
        # csv_output_path = "/Users/daishun/个人文档/caiyuangungun/data/norm/income_statement/debug/net_income_validation.csv"
        # os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
        
        # # 导出所有comp_type=1的记录，按误差大小排序
        # prepared_df_sorted = prepared_df.sort_values('net_income_diff', key=abs, ascending=False)
        # prepared_df_sorted.to_csv(csv_output_path, index=False)
        
        # logger.info(f"净利润验证数据已导出到: {csv_output_path}")
        # logger.info(f"导出记录数: {len(prepared_df_sorted)}")
        
        if error_rate > 0.01:  # 超过1%
            error_msg = f"净利润核对失败，误差率{error_rate:.4f}超过1%阈值"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("净利润核对通过")
        
        # 将计算列合并回原DataFrame
        calc_cols = ['net_income_calc_base', 'net_income_calc_adjustments', 'net_income_calc_expected', 'net_income_actual', 'net_income_diff']
        df.loc[df['comp_type'] == "1", calc_cols] = prepared_df[calc_cols]
        
        return df
    
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
            
            # 创建debug文件夹
            debug_dir = Path("debug")
            debug_dir.mkdir(exist_ok=True)
            
            # 输出重复记录到CSV文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"duplicate_records_fin_is_{timestamp}.csv"
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
    
    def output_parquet_file(self, df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """按指定字段顺序输出parquet文件"""
        logger.info("开始输出parquet文件")
        
        # 指定的字段顺序
        output_columns = [
            'ts_code', 'f_ann_date', 'end_date', 'total_revenue', 'revenue', 'int_income',
            'prem_earned', 'comm_income', 'total_opcost', 'oper_cost', 'int_exp', 'comm_exp',
            'biz_tax_surchg', 'sell_exp', 'admin_exp', 'fin_exp', 'fin_exp_int_exp', 'fin_exp_int_inc',
            'rd_exp', 'credit_impa_loss', 'assets_impair_loss', 'oth_income', 'asset_disp_income',
            'fv_value_chg_gain', 'invest_income', 'forex_gain', 'net_expo_hedging_benefits',
            'operate_profit', 'non_oper_income', 'non_oper_exp', 'total_profit', 'income_tax',
            'n_income', 'n_income_attr_p', 'oth_compr_income', 't_compr_income', 'compr_inc_attr_p',
            'basic_eps', 'diluted_eps', 'file_path', 'file_md5'
        ]
        
        # 只选择存在的列
        existing_columns = [col for col in output_columns if col in df.columns]
        missing_columns = [col for col in output_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"以下字段在数据中不存在，将被跳过: {missing_columns}")
        
        # 选择并重新排序列 - 只输出指定的标准字段
        output_df = df[existing_columns].copy()
        
        # 确定输出路径
        if output_path is None:
            output_path = self.config.get('output_path', 'data/norm/income_statement/cleaned/income_y.parquet')
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 输出parquet文件
        output_df.to_parquet(output_path, index=False)
        
        logger.info(f"parquet文件已输出到: {output_path}")
        logger.info(f"输出数据形状: {output_df.shape}")
        logger.info(f"输出字段数: {len(existing_columns)}")
        
        # 重要：返回原始的完整DataFrame，而不是过滤后的DataFrame
        # 这样可以确保流程中的其他步骤能够访问到所有数据
        return df
    
    def process_pipeline(self, input_path: str = None, output_path: str = None) -> pd.DataFrame:
        """执行完整的清洗流程"""
        logger.info("开始执行利润表数据清洗流程")
        
        # 确定输入路径
        if input_path is None:
            input_path = self.config.get('input_path', 'data/norm/income_statement/merged/income_y.parquet')
        
        # 读取数据
        logger.info(f"读取输入文件: {input_path}")
        df = pd.read_parquet(input_path)
        logger.info(f"原始数据形状: {df.shape}")
        
        # 获取清洗流程配置
        pipeline_config = self.config.get('cleaning_pipeline', [])
        
        # 按顺序执行清洗步骤
        for step in pipeline_config:
            function_name = step.get('function')
            description = step.get('description', '')
            
            if function_name in self.cleaning_functions:
                logger.info(f"执行步骤: {function_name} - {description}")
                
                try:
                    if function_name == 'output_parquet_file':
                        # 输出文件步骤需要传递输出路径
                        df = self.cleaning_functions[function_name](df, output_path)
                    else:
                        df = self.cleaning_functions[function_name](df)
                    
                    logger.info(f"步骤完成，当前数据形状: {df.shape}")
                    
                except Exception as e:
                    logger.error(f"执行步骤 {function_name} 时发生错误: {e}")
                    raise
            else:
                logger.warning(f"未找到清洗函数: {function_name}")
        
        logger.info("利润表数据清洗流程执行完成")
        
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
            {'function': 'validate_total_revenue'},
            {'function': 'validate_total_opcost'},
            {'function': 'validate_operate_profit'},
            {'function': 'validate_net_income'},
            {'function': 'validate_unique_records'},
            {'function': 'rename_fixed_columns'},
            {'function': 'output_parquet_file'}
        ]
    }
    
    processor = FinISProcessor(test_config)
    
    # 执行清洗流程
    result_df = processor.process_pipeline()
    
    print(f"清洗完成，最终数据形状: {result_df.shape}")


if __name__ == "__main__":
    main()