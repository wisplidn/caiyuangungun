"""
基础数据清洗服务模块

提供简化的数据清洗接口，基于配置文件驱动
"""

import os
import json
import logging
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime

from ..core.path_manager import PathManager
from ..core.config_manager import ConfigManager
from ..processors.common.universal_data_cleaner import UniversalDataCleaner

logger = logging.getLogger(__name__)


class DataCleaningService:
    """基础数据清洗服务类"""
    
    def __init__(self, base_path: str = None, config_dir: str = None):
        """
        初始化数据清洗服务
        
        Args:
            base_path: 数据基础路径
            config_dir: 配置文件目录
        """
        # 初始化配置管理器
        self.config_manager = ConfigManager()
        
        # 从配置文件获取base_path
        base_path = base_path or self.config_manager.get_norm_base_path()
        
        # 设置路径
        self.base_path = base_path
        self.config_dir = config_dir or os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(base_path)))), "data", "config")
        
        # 加载清洗管道配置
        self.pipeline_config = self._load_pipeline_config()
        
        # 初始化路径管理器和数据清洗器 - 使用raw数据路径
        raw_data_path = os.path.join(os.path.dirname(base_path), "raw")
        self.path_manager = PathManager(raw_data_path)
        
        # 获取股票基础信息和BSE映射字典
        try:
            stock_basic_df = self.config_manager.get_stock_basic_info()
            bse_mapping = self.config_manager.get_bse_code_mapping()
            self.data_cleaner = UniversalDataCleaner(stock_basic_df=stock_basic_df, bse_mapping=bse_mapping)
        except Exception as e:
            raise RuntimeError(f"初始化UniversalDataCleaner失败，请确保股票基础信息可用: {e}")
        
    def _load_pipeline_config(self) -> Dict:
        """加载清洗管道配置文件"""
        config_manager = ConfigManager()
        return config_manager.load_config("basic_cleaning_pipeline_config.json")
    
    def get_available_pipelines(self) -> List[str]:
        """获取可用的清洗流水线列表"""
        return list(self.pipeline_config.keys())
    
    def clean_data_by_pipeline(self, pipeline_name: str, 
                              data_source: str = None,
                              max_files: Optional[int] = None) -> pd.DataFrame:
        """
        按流水线清洗数据
        
        Args:
            pipeline_name: 流水线名称
            data_source: 指定数据源
            max_files: 最大处理文件数
            
        Returns:
            清洗后的DataFrame
        """
        if pipeline_name not in self.pipeline_config:
            raise ValueError(f"未找到流水线配置: {pipeline_name}")
        
        config = self.pipeline_config[pipeline_name]
        logger.info(f"开始执行清洗流水线: {pipeline_name}")
        
        # 获取清洗管道配置
        cleaning_pipelines = config.get('cleaning_pipelines', {})
        
        if data_source and data_source not in cleaning_pipelines:
            logger.warning(f"未找到数据源配置: {data_source}")
            return pd.DataFrame()
        
        all_cleaned_data = []
        
        # 处理指定数据源或所有数据源
        sources_to_process = [data_source] if data_source else list(cleaning_pipelines.keys())
        
        for ds_name in sources_to_process:
            if ds_name not in cleaning_pipelines:
                continue
                
            logger.info(f"处理数据源: {ds_name}")
            
            try:
                # 加载数据
                df = self._load_data_source(ds_name, max_files)
                if df.empty:
                    logger.warning(f"数据源 {ds_name} 无数据")
                    continue
                
                # 执行清洗流水线
                pipeline_config = cleaning_pipelines[ds_name]
                
                # 处理新格式（带enabled和pipeline字段）和旧格式
                if isinstance(pipeline_config, dict) and 'pipeline' in pipeline_config:
                    # 新格式：检查是否启用
                    if not pipeline_config.get('enabled', True):
                        logger.info(f"数据源 {ds_name} 已禁用，跳过")
                        continue
                    pipeline_steps = pipeline_config['pipeline']
                else:
                    # 旧格式：直接是步骤列表
                    pipeline_steps = pipeline_config
                
                cleaned_df = self._execute_cleaning_steps(df, pipeline_steps)
                
                if not cleaned_df.empty:
                    all_cleaned_data.append(cleaned_df)
                    logger.info(f"数据源 {ds_name} 清洗完成: {cleaned_df.shape}")
                
            except Exception as e:
                logger.error(f"处理数据源 {ds_name} 失败: {e}")
                continue
        
        # 合并所有清洗后的数据
        if all_cleaned_data:
            merged_df = pd.concat(all_cleaned_data, ignore_index=True)
            logger.info(f"流水线 {pipeline_name} 执行完成: {merged_df.shape}")
            return merged_df
        else:
            logger.warning(f"流水线 {pipeline_name} 无数据输出")
            return pd.DataFrame()
    
    def _load_data_source(self, data_source: str, max_files: Optional[int] = None) -> pd.DataFrame:
        """加载指定数据源的数据"""
        try:
            file_pairs = self.path_manager.get_method_file_paths(data_source)
            if not file_pairs:
                return pd.DataFrame()
            
            if max_files and len(file_pairs) > max_files:
                file_pairs = file_pairs[:max_files]
            
            # 提取parquet文件路径（file_pairs是(parquet_path, json_path)的元组列表）
            parquet_files = [pair[0] for pair in file_pairs]
            
            return self.data_cleaner.load_parquet_files(parquet_files)
            
        except Exception as e:
            logger.error(f"加载数据源 {data_source} 失败: {e}")
            return pd.DataFrame()
    
    def _execute_cleaning_steps(self, df: pd.DataFrame, steps: List[Dict]) -> pd.DataFrame:
        """执行清洗步骤"""
        result_df = df.copy()
        initial_shape = result_df.shape
        
        logger.info(f"开始执行清洗流水线，共 {len(steps)} 个步骤，初始数据形状: {initial_shape}")
        
        for i, step in enumerate(steps, 1):
            function_name = step.get('function')
            if not function_name:
                logger.warning(f"步骤 {i}: 缺少function参数，跳过")
                continue
            
            # 记录步骤开始
            step_start_time = datetime.now()
            before_shape = result_df.shape
            
            # 处理新格式（带params字段）和旧格式
            if 'params' in step:
                # 新格式：参数在params字段中
                params = step['params']
                field_name = params.get('field_name', '')
                source_field = params.get('source_field', '')
            else:
                # 旧格式：参数直接在step中
                field_name = step.get('field_name', '')
                source_field = step.get('source_field', '')
                params = {k: v for k, v in step.items() if k != 'function'}
            
            logger.info(f"步骤 {i}/{len(steps)}: 执行 {function_name} - 目标字段: {field_name}, 源字段: {source_field}")
                
            try:
                # 使用统一的apply_cleaning_function接口，传递所有参数
                result_df = self.data_cleaner.apply_cleaning_function(
                    result_df, 
                    function_name, 
                    **params
                )
                
                # 记录步骤完成
                step_end_time = datetime.now()
                step_duration = (step_end_time - step_start_time).total_seconds()
                after_shape = result_df.shape
                
                # 计算数据变化
                row_change = after_shape[0] - before_shape[0]
                col_change = after_shape[1] - before_shape[1]
                
                logger.info(f"步骤 {i} 完成: {function_name} - 耗时: {step_duration:.3f}s, "
                          f"数据变化: {before_shape} -> {after_shape} "
                          f"(行{'+' if row_change >= 0 else ''}{row_change}, 列{'+' if col_change >= 0 else ''}{col_change})")
                
            except Exception as e:
                step_end_time = datetime.now()
                step_duration = (step_end_time - step_start_time).total_seconds()
                logger.error(f"步骤 {i} 失败: {function_name} - 耗时: {step_duration:.3f}s, 错误: {e}")
                continue
        
        final_shape = result_df.shape
        total_row_change = final_shape[0] - initial_shape[0]
        total_col_change = final_shape[1] - initial_shape[1]
        
        logger.info(f"清洗流水线执行完成: {initial_shape} -> {final_shape} "
                   f"(总计: 行{'+' if total_row_change >= 0 else ''}{total_row_change}, "
                   f"列{'+' if total_col_change >= 0 else ''}{total_col_change})")
        
        return result_df
    
    def save_cleaned_data(self, df: pd.DataFrame, pipeline_name: str, 
                         data_source: str = None, format: str = 'parquet') -> str:
        """保存清洗后的数据"""
        try:
            config = self.pipeline_config.get(pipeline_name, {})
            output_path = config.get('output_path', f'{pipeline_name}/cleaned')
            
            # 构建完整输出路径
            full_output_path = os.path.join(self.base_path, output_path)
            os.makedirs(full_output_path, exist_ok=True)
            
            # 根据数据源命名文件
            if data_source:
                filename = f'{data_source}.{format}'
            else:
                filename = f'cleaned_data.{format}'
            
            # 保存文件
            file_path = os.path.join(full_output_path, filename)
            if format == 'parquet':
                df.to_parquet(file_path, index=False)
            else:
                df.to_csv(file_path, index=False)
            
            logger.info(f"数据已保存到: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
            return ""
    
    def clean_data_by_period(self, method_name: str, start_date: str = None, end_date: str = None, 
                           output_format: str = 'parquet') -> str:
        """
        按时间段清洗数据的通用方法
        
        Args:
            method_name: 数据方法名称，同时也是pipeline名称
            start_date: 开始日期，格式YYYYMMDD，为空则获取全部数据
            end_date: 结束日期，格式YYYYMMDD，为空则获取全部数据
            output_format: 输出格式，默认parquet
            
        Returns:
            输出文件路径
        """
        logger.info(f"开始清洗{method_name}数据，时间范围: {start_date or '全部'} 到 {end_date or '全部'}")
        
        try:
            # 验证日期参数
            if (start_date is None) != (end_date is None):
                raise ValueError("start_date和end_date必须同时为空或同时不为空")
            
            # 获取文件路径
            if start_date and end_date:
                # 按日期范围获取文件
                file_pairs = self.path_manager.get_method_file_paths_by_date_range(
                    method_name, start_date, end_date
                )
            else:
                # 获取全部文件
                file_pairs = self.path_manager.get_method_file_paths(method_name)
            
            if not file_pairs:
                logger.warning(f"未找到{method_name}数据文件")
                return ""
            
            logger.info(f"找到{len(file_pairs)}个数据文件")
            
            # 使用UniversalDataCleaner加载和合并数据
            parquet_files = [pair[0] for pair in file_pairs]
            df = self.data_cleaner.load_parquet_files(parquet_files)
            
            if df.empty:
                logger.warning(f"合并后的{method_name}数据为空")
                return ""
            
            # 执行数据清洗管道
            if method_name in self.pipeline_config:
                pipeline_steps = self.pipeline_config[method_name].get('cleaning_pipelines', {})
                
                # 对每个数据源执行相应的清洗步骤
                for source_name, steps in pipeline_steps.items():
                    if steps:
                        logger.info(f"执行{source_name}清洗管道")
                        df = self._execute_cleaning_steps(df, steps)
            
            # 构建输出路径 - 使用配置中的output_path
            # 首先查找method_name对应的配置
            method_config = None
            for pipeline_name, config in self.pipeline_config.items():
                if method_name in config.get('cleaning_pipelines', {}):
                    method_config = config
                    break
            
            if method_config:
                output_path_config = method_config.get('output_path', f'norm/{method_name}/merged')
            else:
                output_path_config = f'norm/{method_name}/merged'
            
            # 构建输出路径 - 从raw目录回到data目录，然后进入norm目录
            data_dir = os.path.dirname(self.path_manager.base_path)  # 从raw回到data目录
            output_dir = os.path.join(data_dir, output_path_config)
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 输出文件名简化为method_name.format
            output_filename = f"{method_name}.{output_format}"
            output_file_path = os.path.join(output_dir, output_filename)
            
            # 保存数据
            if output_format.lower() == 'parquet':
                df.to_parquet(output_file_path, index=False)
            elif output_format.lower() == 'csv':
                df.to_csv(output_file_path, index=False)
            else:
                raise ValueError(f"不支持的输出格式: {output_format}")
            
            logger.info(f"数据已保存到: {output_file_path}")
            logger.info(f"{method_name}数据清洗完成，输出文件: {output_file_path}")
            
            return output_file_path
            
        except Exception as e:
            logger.error(f"清洗{method_name}数据时发生错误: {str(e)}")
            raise