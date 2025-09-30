"""
通用数据清洗器
提供统一的数据清洗函数接口，支持配置化的清洗流程
"""

import os
import json
import pandas as pd
import re
from typing import List, Tuple, Dict, Optional, Any, Callable
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UniversalDataCleaner:
    """通用数据清洗器类"""
    
    def __init__(self, stock_basic_df: pd.DataFrame = None, bse_mapping: Dict[str, str] = None):
        """
        初始化通用数据清洗器
        
        Args:
            stock_basic_df: 股票基础信息DataFrame
            bse_mapping: BSE代码映射字典
        """
        self.stock_basic_df = stock_basic_df if stock_basic_df is not None else pd.DataFrame()
        self.bse_mapping = bse_mapping if bse_mapping is not None else {}
        
        # 注册所有清洗函数
        self._register_cleaning_functions()
        
    def _register_cleaning_functions(self):
        """注册所有可用的清洗函数"""
        self.cleaning_functions = {
            'convert_date_format': self.convert_date_format,
            'apply_bse_mapping': self.apply_bse_mapping,
            'add_ts_code': self.add_ts_code,
            'extract_end_date_from_path': self.extract_end_date_from_path,
            'drop_duplicates': self.drop_duplicates,
            'rename_column': self.rename_column,
            'fill_na': self.fill_na,
            'filter_data': self.filter_data
        }
    
    def get_available_functions(self) -> List[str]:
        """获取所有可用的清洗函数名称"""
        return list(self.cleaning_functions.keys())
    
    def apply_cleaning_function(self, df: pd.DataFrame, function_name: str, 
                              field_name: str = None, source_field: str = None, 
                              **kwargs) -> pd.DataFrame:
        """
        应用清洗函数的统一接口
        
        Args:
            df: 待清洗的DataFrame
            function_name: 清洗函数名称
            field_name: 目标字段名
            source_field: 源字段名
            **kwargs: 传递给清洗函数的额外参数
            
        Returns:
            清洗后的DataFrame
        """
        if function_name not in self.cleaning_functions:
            raise ValueError(f"未知的清洗函数: {function_name}")
        
        # 自动判断操作类型：如果field_name与source_field相同或者没有source_field，则为替换操作
        # 特殊函数drop_duplicates始终为update操作
        if function_name == 'drop_duplicates':
            operation = 'update'
        elif field_name and source_field and field_name == source_field:
            operation = 'replace'
        elif field_name and not source_field:
            operation = 'update'
        else:
            operation = 'add'
        
        cleaning_func = self.cleaning_functions[function_name]
        
        # 记录函数执行开始
        func_start_time = datetime.now()
        before_shape = df.shape
        
        try:
            # 调用清洗函数
            result_df = cleaning_func(df, field_name=field_name, source_field=source_field, 
                                    operation=operation, **kwargs)
            
            # 记录函数执行完成
            func_end_time = datetime.now()
            func_duration = (func_end_time - func_start_time).total_seconds()
            after_shape = result_df.shape
            
            # 计算数据变化
            row_change = after_shape[0] - before_shape[0]
            col_change = after_shape[1] - before_shape[1]
            
            logger.info(f"清洗函数 {function_name}({operation}) 执行成功 - 耗时: {func_duration:.3f}s, "
                       f"数据变化: {before_shape} -> {after_shape} "
                       f"(行{'+' if row_change >= 0 else ''}{row_change}, 列{'+' if col_change >= 0 else ''}{col_change})")
            
            return result_df
        except Exception as e:
            func_end_time = datetime.now()
            func_duration = (func_end_time - func_start_time).total_seconds()
            logger.error(f"清洗函数 {function_name} 执行失败 - 耗时: {func_duration:.3f}s, 错误: {e}")
            return df
    
    def convert_date_format(self, df: pd.DataFrame, field_name: str = None, 
                          operation: str = 'add', target_field: str = None, 
                          source_field: str = None, **kwargs) -> pd.DataFrame:
        """
        日期格式转换函数
        
        Args:
            df: DataFrame
            field_name: 目标字段名
            operation: 操作类型
            target_field: 目标字段名（优先级高于field_name）
            source_field: 源字段名
            
        Returns:
            处理后的DataFrame
        """
        result_df = df.copy()
        
        # 确定源字段和目标字段
        if source_field is None:
            source_field = field_name
        if target_field is None:
            target_field = field_name
            
        if source_field not in df.columns:
            logger.warning(f"源字段 {source_field} 不存在")
            return result_df
        
        def _convert_single_date(date_str: Any) -> Optional[str]:
            """转换单个日期值"""
            try:
                if pd.isna(date_str) or date_str is None:
                    return None
                
                # 如果是datetime对象，直接转换
                if hasattr(date_str, 'strftime'):
                    return date_str.strftime('%Y%m%d')
                
                date_str = str(date_str).strip()
                
                # 处理ISO格式：2008-06-24T00:00:00.000Z
                if 'T' in date_str:
                    date_part = date_str.split('T')[0]
                    return date_part.replace('-', '')
                
                # 处理带时间的格式：20230426 00:00:00 或 2025-08-26 00:00:00
                elif ' ' in date_str and ':' in date_str:
                    date_part = date_str.split(' ')[0]
                    if '-' in date_part:
                        return date_part.replace('-', '')
                    else:
                        return date_part
                
                # 处理纯日期格式：20230426 或 2023-04-26
                else:
                    if '-' in date_str:
                        return date_str.replace('-', '')
                    else:
                        return date_str
                        
            except Exception as e:
                logger.warning(f"日期格式转换失败: {date_str}, 错误: {e}")
                return None
        
        # 应用日期转换
        if operation == 'replace':
            result_df[source_field] = result_df[source_field].apply(_convert_single_date)
        else:  # add or update
            result_df[target_field] = result_df[source_field].apply(_convert_single_date)
        
        return result_df
    
    def apply_bse_mapping(self, df: pd.DataFrame, field_name: str = None, 
                         operation: str = 'add', target_field: str = None, 
                         source_field: str = None, **kwargs) -> pd.DataFrame:
        """
        应用BSE代码映射，参考add_ts_code的标准化处理方式
        
        Args:
            df: DataFrame
            field_name: 目标字段名
            operation: 操作类型
            target_field: 目标字段名
            source_field: 源字段名
            
        Returns:
            处理后的DataFrame
        """
        result_df = df.copy()
        
        # 确定源字段和目标字段
        if source_field is None:
            source_field = field_name
        if target_field is None:
            if operation == 'replace':
                target_field = source_field
            else:
                target_field = field_name if field_name else f"{source_field}_mapped"
            
        if source_field not in df.columns:
            logger.warning(f"源字段 {source_field} 不存在")
            return result_df
        
        if not self.bse_mapping:
            logger.warning("BSE映射字典为空")
            # 即使映射为空，也创建目标字段，使用原始值
            result_df[target_field] = result_df[source_field]
            return result_df
        
        # 应用BSE映射，参考add_ts_code的标准化处理方式
        def apply_bse_mapping_to_code(ts_code):
            if pd.isna(ts_code):
                return ts_code
            
            ts_code_str = str(ts_code)
            
            # 如果包含交易所后缀（如 833943.BJ），需要分别处理代码和后缀
            if '.' in ts_code_str:
                code_part, exchange_part = ts_code_str.split('.', 1)
                # 使用标准化的纯数字代码进行映射
                normalized_code = self._normalize_stock_code(code_part)
                mapped_code = self.bse_mapping.get(normalized_code)
                if mapped_code:
                    return f"{mapped_code}.{exchange_part}"
                else:
                    return ts_code
            else:
                # 没有交易所后缀，直接使用标准化代码进行映射
                normalized_code = self._normalize_stock_code(ts_code_str)
                return self.bse_mapping.get(normalized_code, ts_code)
        
        result_df[target_field] = result_df[source_field].apply(apply_bse_mapping_to_code)
        
        return result_df
    
    def _normalize_stock_code(self, code: str) -> str:
        """
        标准化股票代码，将各种格式转换为纯数字格式
        
        支持的格式：
        - 000001.SZ -> 000001
        - 000001 -> 000001  
        - SZ000001 -> 000001
        - SH600000 -> 600000
        
        Args:
            code: 原始股票代码
            
        Returns:
            标准化后的纯数字股票代码
        """
        if pd.isna(code):
            return code
            
        code_str = str(code).strip()
        
        # 格式1: 000001.SZ 或 000001.SH
        if '.' in code_str:
            return code_str.split('.')[0]
        
        # 格式2: SZ000001 或 SH600000
        if code_str.startswith(('SZ', 'SH')):
            return code_str[2:]
        
        # 格式3: 纯数字 000001
        return code_str

    def add_ts_code(self, df: pd.DataFrame, field_name: str = None, 
                   operation: str = 'add', source_field: str = None, **kwargs) -> pd.DataFrame:
        """
        添加ts_code字段，同时添加list_status、list_date和delist_date字段
        
        Args:
            df: DataFrame
            field_name: 目标字段名（通常是'ts_code'）
            operation: 操作类型
            source_field: 源字段名（股票代码字段或ts_code字段）
            
        Returns:
            处理后的DataFrame
        """
        result_df = df.copy()
        
        if source_field is None:
            logger.warning("必须指定source_field参数")
            return result_df
            
        if source_field not in df.columns:
            logger.warning(f"源字段 {source_field} 不存在")
            return result_df
        
        target_field = field_name if field_name else 'ts_code'
        
        if self.stock_basic_df is None or self.stock_basic_df.empty:
            raise ValueError("股票基础信息DataFrame为空，请通过config管理器正确初始化UniversalDataCleaner")
        
        # 创建标准化的股票代码映射
        # 将stock_basic中的symbol和ts_code都标准化为纯数字格式作为key
        stock_mapping = {}
        list_status_mapping = {}
        list_date_mapping = {}
        delist_date_mapping = {}
        
        for _, row in self.stock_basic_df.iterrows():
            # 使用symbol（纯数字）作为标准化key
            normalized_key = self._normalize_stock_code(row['symbol'])
            stock_mapping[normalized_key] = row['ts_code']
            list_status_mapping[normalized_key] = row['list_status']
            list_date_mapping[normalized_key] = row['list_date']
            delist_date_mapping[normalized_key] = row['delist_date']
        
        # 标准化输入数据的股票代码并进行映射
        normalized_codes = result_df[source_field].apply(self._normalize_stock_code)
        
        # 应用映射
        result_df[target_field] = normalized_codes.map(stock_mapping)
        result_df['list_status'] = normalized_codes.map(list_status_mapping)
        result_df['list_date'] = normalized_codes.map(list_date_mapping)
        result_df['delist_date'] = normalized_codes.map(delist_date_mapping)
        
        return result_df
    
    def extract_end_date_from_path(self, df: pd.DataFrame, field_name: str = None, 
                                  operation: str = 'add', source_field: str = 'file_path', 
                                  **kwargs) -> pd.DataFrame:
        """
        从文件路径中提取end_date
        
        Args:
            df: DataFrame
            field_name: 目标字段名
            operation: 操作类型
            source_field: 源字段名（文件路径字段）
            
        Returns:
            处理后的DataFrame
        """
        result_df = df.copy()
        
        if source_field not in df.columns:
            logger.warning(f"源字段 {source_field} 不存在")
            return result_df
        
        def _extract_end_date(file_path):
            """从文件路径提取季度信息并转换为end_date"""
            try:
                # 提取季度信息，如2007Q1
                match = re.search(r'(\d{4})Q(\d)', str(file_path))
                if match:
                    year = match.group(1)
                    quarter = match.group(2)
                    # 根据季度计算end_date
                    quarter_end_dates = {'1': '0331', '2': '0630', '3': '0930', '4': '1231'}
                    return year + quarter_end_dates.get(quarter, '1231')
                return None
            except Exception as e:
                logger.warning(f"从路径提取end_date失败: {file_path}, 错误: {e}")
                return None
        
        target_field = field_name if field_name else 'end_date'
        result_df[target_field] = result_df[source_field].apply(_extract_end_date)
        
        return result_df
    
    def drop_duplicates(self, df: pd.DataFrame, field_name: str = None, 
                       operation: str = 'update', subset: List[str] = None, 
                       **kwargs) -> pd.DataFrame:
        """
        去重函数
        
        Args:
            df: DataFrame
            field_name: 不使用（保持接口一致性）
            operation: 操作类型
            subset: 去重依据的列名列表
            
        Returns:
            去重后的DataFrame
        """
        result_df = df.copy()
        
        if subset:
            # 检查指定的列是否存在
            existing_cols = [col for col in subset if col in df.columns]
            if existing_cols:
                result_df = result_df.drop_duplicates(subset=existing_cols)
            else:
                logger.warning(f"指定的去重列都不存在: {subset}")
        else:
            # 全字段去重
            result_df = result_df.drop_duplicates()
        
        return result_df
    
    def rename_column(self, df: pd.DataFrame, field_name: str = None, 
                     operation: str = 'update', old_name: str = None, 
                     new_name: str = None, **kwargs) -> pd.DataFrame:
        """
        重命名列
        
        Args:
            df: DataFrame
            field_name: 新列名（优先级低于new_name）
            operation: 操作类型
            old_name: 旧列名
            new_name: 新列名
            
        Returns:
            重命名后的DataFrame
        """
        result_df = df.copy()
        
        if old_name is None or (new_name is None and field_name is None):
            logger.warning("重命名列需要指定old_name和new_name参数")
            return result_df
        
        if old_name not in df.columns:
            logger.warning(f"要重命名的列 {old_name} 不存在")
            return result_df
        
        target_name = new_name if new_name else field_name
        result_df = result_df.rename(columns={old_name: target_name})
        
        return result_df
    
    def fill_na(self, df: pd.DataFrame, field_name: str = None, 
               operation: str = 'update', fill_value: Any = None, 
               method: str = None, **kwargs) -> pd.DataFrame:
        """
        填充缺失值
        
        Args:
            df: DataFrame
            field_name: 字段名
            operation: 操作类型
            fill_value: 填充值
            method: 填充方法 ('ffill', 'bfill', etc.)
            
        Returns:
            填充后的DataFrame
        """
        result_df = df.copy()
        
        if field_name and field_name in df.columns:
            # 填充指定列
            if fill_value is not None:
                result_df[field_name] = result_df[field_name].fillna(fill_value)
            elif method:
                result_df[field_name] = result_df[field_name].fillna(method=method)
        else:
            # 填充所有列
            if fill_value is not None:
                result_df = result_df.fillna(fill_value)
            elif method:
                result_df = result_df.fillna(method=method)
        
        return result_df
    
    def filter_data(self, df: pd.DataFrame, field_name: str = None, 
                   operation: str = 'update', condition: str = None, 
                   **kwargs) -> pd.DataFrame:
        """
        数据过滤
        
        Args:
            df: DataFrame
            field_name: 字段名
            operation: 操作类型
            condition: 过滤条件
            
        Returns:
            过滤后的DataFrame
        """
        result_df = df.copy()
        
        if condition and field_name and field_name in df.columns:
            try:
                # 简单的条件过滤，可以扩展更复杂的条件
                if condition.startswith('=='):
                    value = condition[2:].strip()
                    result_df = result_df[result_df[field_name] == value]
                elif condition.startswith('!='):
                    value = condition[2:].strip()
                    result_df = result_df[result_df[field_name] != value]
                elif condition.startswith('>='):
                    value = float(condition[2:].strip())
                    result_df = result_df[result_df[field_name] >= value]
                elif condition.startswith('<='):
                    value = float(condition[2:].strip())
                    result_df = result_df[result_df[field_name] <= value]
                elif condition.startswith('>'):
                    value = float(condition[1:].strip())
                    result_df = result_df[result_df[field_name] > value]
                elif condition.startswith('<'):
                    value = float(condition[1:].strip())
                    result_df = result_df[result_df[field_name] < value]
                elif condition == 'notna':
                    result_df = result_df[result_df[field_name].notna()]
                elif condition == 'isna':
                    result_df = result_df[result_df[field_name].isna()]
            except Exception as e:
                logger.warning(f"过滤条件应用失败: {condition}, 错误: {e}")
        
        return result_df
    
    def apply_cleaning_pipeline(self, df: pd.DataFrame, 
                              pipeline_config: List[Dict]) -> pd.DataFrame:
        """
        应用清洗流水线
        
        Args:
            df: 待清洗的DataFrame
            pipeline_config: 清洗流水线配置列表
            
        Returns:
            清洗后的DataFrame
        """
        result_df = df.copy()
        initial_shape = result_df.shape
        pipeline_start_time = datetime.now()
        
        logger.info(f"开始执行清洗流水线，共 {len(pipeline_config)} 个配置步骤，初始数据形状: {initial_shape}")
        
        for i, step_config in enumerate(pipeline_config, 1):
            function_name = step_config.get('function')
            if not function_name:
                logger.warning(f"配置步骤 {i}: 缺少function参数")
                continue
            
            # 提取参数
            params = step_config.copy()
            params.pop('function', None)
            
            logger.info(f"配置步骤 {i}/{len(pipeline_config)}: 准备执行 {function_name}")
            
            # 应用清洗函数
            result_df = self.apply_cleaning_function(result_df, function_name, **params)
        
        # 记录流水线执行完成
        pipeline_end_time = datetime.now()
        pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()
        final_shape = result_df.shape
        
        total_row_change = final_shape[0] - initial_shape[0]
        total_col_change = final_shape[1] - initial_shape[1]
        
        logger.info(f"清洗流水线执行完成 - 总耗时: {pipeline_duration:.3f}s, "
                   f"数据变化: {initial_shape} -> {final_shape} "
                   f"(总计: 行{'+' if total_row_change >= 0 else ''}{total_row_change}, "
                   f"列{'+' if total_col_change >= 0 else ''}{total_col_change})")
        
        return result_df


    def load_parquet_files(self, file_paths: List[str], include_md5: bool = True, max_workers: int = 4) -> pd.DataFrame:
        """
        加载多个parquet文件（支持进度显示、批量处理、从JSON读取MD5和并行处理）
        
        Args:
            file_paths: 文件路径列表
            include_md5: 是否从JSON文件读取MD5信息
            max_workers: 并行处理的最大线程数
            
        Returns:
            合并后的DataFrame
        """
        if not file_paths:
            logger.warning("文件路径列表为空")
            return pd.DataFrame()
        
        total_files = len(file_paths)
        logger.info(f"开始加载 {total_files} 个parquet文件{'（含MD5读取）' if include_md5 else ''}...")
        
        all_data = []
        successful_count = 0
        failed_count = 0
        
        # 批量处理，每100个文件显示一次进度
        batch_size = 100
        
        # 线程安全的计数器
        count_lock = threading.Lock()
        
        def _read_md5_from_json(file_path):
            """从JSON文件读取MD5信息"""
            try:
                json_path = Path(file_path).with_suffix('.json')
                if json_path.exists():
                    with open(json_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                        return json_data.get('data_md5')
                else:
                    logger.warning(f"JSON文件不存在: {json_path}")
                    return None
            except Exception as e:
                logger.warning(f"读取JSON文件失败 {json_path}: {e}")
                return None
        
        def _process_single_file(file_path):
            """处理单个文件（包括读取parquet和从JSON读取MD5）"""
            nonlocal successful_count, failed_count
            
            try:
                if not os.path.exists(file_path):
                    with count_lock:
                        failed_count += 1
                    logger.warning(f"文件不存在: {file_path}")
                    return None
                
                # 读取parquet文件
                df = pd.read_parquet(file_path)
                
                # 添加文件路径信息
                df['file_path'] = file_path
                
                # 如果需要MD5，从JSON文件读取
                if include_md5:
                    file_md5 = _read_md5_from_json(file_path)
                    df['file_md5'] = file_md5
                
                with count_lock:
                    successful_count += 1
                
                return df
                
            except Exception as e:
                with count_lock:
                    failed_count += 1
                logger.error(f"处理文件失败 {file_path}: {e}")
                return None
        
        # 使用线程池并行处理文件
        if max_workers > 1 and len(file_paths) > 10:  # 文件数量较少时不使用并行
            logger.info(f"使用 {max_workers} 个线程并行处理文件")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_path = {executor.submit(_process_single_file, path): path for path in file_paths}
                
                # 收集结果并显示进度
                processed_count = 0
                for future in as_completed(future_to_path):
                    result = future.result()
                    if result is not None:
                        all_data.append(result)
                    
                    processed_count += 1
                    
                    # 每100个文件或最后一个文件时显示进度
                    if processed_count % batch_size == 0 or processed_count == total_files:
                        progress_pct = (processed_count / total_files) * 100
                        logger.info(f"进度: {processed_count}/{total_files} ({progress_pct:.1f}%) - 成功: {successful_count}, 失败: {failed_count}")
                        
                        # 如果累积的数据过多，先进行一次合并以节省内存
                        if len(all_data) >= 500:  # 每500个文件合并一次
                            logger.info("执行中间合并以节省内存...")
                            if all_data:
                                temp_df = pd.concat(all_data, ignore_index=True)
                                all_data = [temp_df]
        else:
            # 串行处理
            logger.info("使用串行方式处理文件")
            for i, file_path in enumerate(file_paths):
                result = _process_single_file(file_path)
                if result is not None:
                    all_data.append(result)
                
                # 每100个文件或最后一个文件时显示进度
                if (i + 1) % batch_size == 0 or (i + 1) == total_files:
                    progress_pct = ((i + 1) / total_files) * 100
                    logger.info(f"进度: {i + 1}/{total_files} ({progress_pct:.1f}%) - 成功: {successful_count}, 失败: {failed_count}")
                    
                    # 如果累积的数据过多，先进行一次合并以节省内存
                    if len(all_data) >= 500:  # 每500个文件合并一次
                        logger.info("执行中间合并以节省内存...")
                        if all_data:
                            temp_df = pd.concat(all_data, ignore_index=True)
                            all_data = [temp_df]
        
        # 最终合并所有数据
        if all_data:
            logger.info(f"合并 {len(all_data)} 个数据块...")
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"数据加载完成: {final_df.shape[0]} 行, {final_df.shape[1]} 列")
            logger.info(f"成功: {successful_count}, 失败: {failed_count}")
            return final_df
        else:
            logger.warning("没有成功加载任何数据")
            return pd.DataFrame()
    
    def _calculate_file_md5_fast(self, file_path: str) -> Optional[str]:
        """
        快速计算文件MD5（优化版本）
        
        Args:
            file_path: 文件路径
            
        Returns:
            MD5哈希值或None
        """
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                # 使用更大的缓冲区以提高I/O效率
                for chunk in iter(lambda: f.read(65536), b""):  # 64KB chunks
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.warning(f"计算MD5失败: {file_path}, 错误: {e}")
            return None