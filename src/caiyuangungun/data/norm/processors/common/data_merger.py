#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据合并处理器

职责：
1. 读取配置管理器中启用的数据定义
2. 根据存储类型执行不同的合并策略：
   - SNAPSHOT: 直接处理，无需合并
   - DAILY: 按自然年合并
   - MONTHLY: 按全量合并
3. 使用路径管理器构建读取和保存路径
4. 将合并结果保存到stage1路径，使用压缩parquet格式
"""

import os
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import logging

from ...core.config_manager import get_config_manager
from ...core.path_manager import get_path_manager
from .....contracts import DataSource
from .data_cleaner import create_data_cleaner


class DataMerger:
    """数据合并处理器
    
    负责根据配置自动合并不同存储类型的数据，并保存到Norm层stage1阶段。
    """
    
    def __init__(self):
        """初始化数据合并处理器"""
        self.config_manager = get_config_manager()
        self.path_manager = get_path_manager()
        self.data_cleaner = create_data_cleaner()  # 初始化数据清洗器
        self.logger = logging.getLogger(__name__)
        
        # 设置日志格式
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def get_enabled_data_definitions(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """获取所有启用的数据定义
        
        Returns:
            按数据源分组的启用数据定义字典
        """
        enabled_definitions = {}
        
        # 遍历所有数据源
        for source in DataSource:
            source_config = self.config_manager.get_data_source_config(source)
            if not source_config or not source_config.enabled:
                continue
            
            # 获取启用的数据定义
            enabled_interfaces = {}
            for interface_name, interface_config in source_config.data_definitions.items():
                if interface_config.get('enabled', False):
                    enabled_interfaces[interface_name] = interface_config
            
            if enabled_interfaces:
                enabled_definitions[source.value] = enabled_interfaces
        
        return enabled_definitions
    
    def merge_data(self, source: DataSource, interface_name: str, 
                  time_range: Optional[Union[str, Dict[str, Any]]] = None,
                  dedup_columns: Optional[List[str]] = None) -> bool:
        """统一的数据合并方法
        
        Args:
            source: 数据源
            interface_name: 接口名称
            time_range: 时间范围参数
                - 对于DAILY类型: "202410"(年月)或{"year": 2024, "month": 10}
                - 对于MONTHLY类型: "2024"(年份)或{"year": 2024}
                - 对于SNAPSHOT类型: 忽略此参数
            dedup_columns: 去重时使用的列名列表，如果为None则使用所有列去重
            
        Returns:
            处理是否成功
        """
        try:
            # 获取存储类型
            storage_type = self.config_manager.get_storage_type(source, interface_name)
            if not storage_type:
                raise ValueError(f"No storage type found for {source.value}.{interface_name}")
            
            self.logger.info(f"Merging {storage_type} data: {source.value}.{interface_name}")
            
            # 使用path_manager获取文件路径列表
            file_paths = self.path_manager.get_raw_file_paths(
                source=source,
                data_type=interface_name,
                time_range=time_range
            )
            
            if not file_paths:
                self.logger.warning(f"No files found for {source.value}.{interface_name}")
                return False
            
            # 读取并合并所有数据文件
            self.logger.info(f"Found {len(file_paths)} files to merge")
            dataframes = []
            
            for file_path in file_paths:
                try:
                    df = pd.read_parquet(file_path)
                    dataframes.append(df)
                except Exception as e:
                    self.logger.warning(f"Failed to read {file_path}: {e}")
            
            if not dataframes:
                self.logger.error(f"No valid data files found for {source.value}.{interface_name}")
                return False
            
            # 记录源文件信息
            source_files_info = []
            for i, df in enumerate(dataframes):
                source_files_info.append({
                    "file_path": file_paths[i],
                    "record_count": len(df),
                    "columns": df.columns.tolist()
                })
            
            # 合并数据
            merged_df = pd.concat(dataframes, ignore_index=True)
            original_count = len(merged_df)
            
            # 去重处理
            dedup_method = "specified_columns" if dedup_columns else "all_columns"
            if dedup_columns:
                # 使用指定列去重
                merged_df = merged_df.drop_duplicates(subset=dedup_columns)
            else:
                # 使用所有列去重
                merged_df = merged_df.drop_duplicates()
            
            deduped_count = len(merged_df)
            self.logger.info(f"Deduplication: {original_count} -> {deduped_count} records")
            
            # 数据清洗处理
            self.logger.info(f"Starting data cleaning for {interface_name}")
            cleaned_df = self.data_cleaner.clean_data(merged_df, interface_name)
            final_count = len(cleaned_df)
            self.logger.info(f"Data cleaning completed for {interface_name}")
            
            # 获取stage1保存路径
            year = None
            if storage_type == "DAILY" and time_range:
                # 为DAILY类型提取年份参数
                if isinstance(time_range, str) and len(time_range) >= 4:
                    year = int(time_range[:4])
                elif isinstance(time_range, dict) and 'year' in time_range:
                    year = time_range['year']
            
            stage1_path = self.path_manager.get_norm_path(
                source=source,
                data_interface=interface_name,
                stage="stage_1_merge",
                create_dirs=True,
                year=year
            )
            
            # 创建元数据信息
            metadata = {
                "merge_info": {
                    "source": source.value,
                    "interface": interface_name,
                    "storage_type": storage_type,
                    "time_range": time_range,
                    "merge_timestamp": datetime.now().isoformat(),
                    "year": year
                },
                "source_files": [
                    {
                        "file_path": info["file_path"],
                        "record_count": info["record_count"]
                    } for info in source_files_info
                ],
                "processing_stats": {
                    "total_source_files": len(file_paths),
                    "total_source_records": sum(info["record_count"] for info in source_files_info),
                    "merged_records": original_count,
                    "deduped_records": deduped_count,
                    "final_records": final_count,
                    "dedup_method": dedup_method,
                    "dedup_columns": dedup_columns,
                    "records_removed_by_dedup": original_count - deduped_count,
                    "records_changed_by_cleaning": deduped_count - final_count
                },
                "data_cleaning": {
                    "bse_mapping_applied": len(self.data_cleaner.ts_code_mapping) > 0,
                    "bse_mapping_count": len(self.data_cleaner.ts_code_mapping),
                    "ts_code_format_converted": True,
                    "columns_renamed": True,
                    "data_type": interface_name
                },
                "output_info": {
                    "file_path": str(stage1_path),
                    "compression": "snappy",
                    "format": "parquet",
                    "final_shape": list(cleaned_df.shape)
                }
            }
            
            # 保存压缩parquet
            cleaned_df.to_parquet(
                stage1_path,
                compression='snappy',
                index=False
            )
            
            # 保存元数据json文件
            metadata_path = str(stage1_path).replace('.parquet', '_metadata.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Data merged and saved to: {stage1_path} ({final_count} records)")
            self.logger.info(f"Metadata saved to: {metadata_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error merging {source.value}.{interface_name}: {e}")
            return False
    

    
    def process_interface(self, source: DataSource, interface_name: str, 
                         time_range: Optional[Union[str, Dict[str, Any]]] = None,
                         dedup_columns: Optional[List[str]] = None) -> bool:
        """处理单个数据接口
        
        Args:
            source: 数据源
            interface_name: 接口名称
            time_range: 时间范围参数
            dedup_columns: 去重时使用的列名列表
            
        Returns:
            处理是否成功
        """
        return self.merge_data(source, interface_name, time_range, dedup_columns)
    
    def run_merge_all(self, time_range: Optional[Union[str, Dict[str, Any]]] = None,
                         dedup_columns: Optional[List[str]] = None) -> Dict[str, Dict[str, bool]]:
        """运行所有启用数据定义的合并处理
        
        Args:
            time_range: 时间范围参数，应用于所有数据接口
            dedup_columns: 去重时使用的列名列表
            
        Returns:
            处理结果字典，格式为 {source: {interface: success}}
        """
        self.logger.info("Starting data merge process for all enabled definitions")
        
        enabled_definitions = self.get_enabled_data_definitions()
        results = {}
        
        total_processed = 0
        total_success = 0
        
        for source_name, interfaces in enabled_definitions.items():
            try:
                source = DataSource(source_name)
            except ValueError:
                self.logger.error(f"Invalid data source: {source_name}")
                continue
            
            source_results = {}
            
            for interface_name, interface_config in interfaces.items():
                total_processed += 1
                success = self.process_interface(source, interface_name, time_range, dedup_columns)
                source_results[interface_name] = success
                
                if success:
                    total_success += 1
            
            results[source_name] = source_results
        
        # 输出汇总结果
        self.logger.info(f"Data merge completed: {total_success}/{total_processed} successful")
        
        return results
    
    def run_merge_specific(self, source: DataSource, interface_name: str,
                          time_range: Optional[Union[str, Dict[str, Any]]] = None,
                          dedup_columns: Optional[List[str]] = None) -> bool:
        """运行特定数据接口的合并处理
        
        Args:
            source: 数据源
            interface_name: 接口名称
            time_range: 时间范围参数
            dedup_columns: 去重时使用的列名列表
            
        Returns:
            处理是否成功
        """
        # 检查接口是否启用
        source_config = self.config_manager.get_data_source_config(source)
        if not source_config or not source_config.enabled:
            self.logger.error(f"Data source {source.value} is not enabled")
            return False
        
        interface_config = source_config.data_definitions.get(interface_name)
        if not interface_config or not interface_config.get('enabled', False):
            self.logger.error(f"Interface {source.value}.{interface_name} is not enabled")
            return False
        
        return self.process_interface(source, interface_name, time_range, dedup_columns)


def main():
    """主函数 - 用于测试和独立运行"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据合并处理器')
    parser.add_argument('--source', type=str, help='数据源名称')
    parser.add_argument('--interface', type=str, help='接口名称')
    parser.add_argument('--time-range', type=str, help='时间范围 (如: 202410 或 2024)')
    parser.add_argument('--dedup-columns', type=str, nargs='*', help='去重列名列表')
    
    args = parser.parse_args()
    
    merger = DataMerger()
    
    if args.source and args.interface:
        # 运行特定接口的合并
        try:
            source = DataSource(args.source)
            success = merger.run_merge_specific(
                source=source,
                interface_name=args.interface,
                time_range=args.time_range,
                dedup_columns=args.dedup_columns
            )
            print(f"合并结果: {'成功' if success else '失败'}")
        except ValueError as e:
            print(f"错误: {e}")
    else:
        # 运行所有启用的数据合并
        results = merger.run_merge_all(
            time_range=args.time_range,
            dedup_columns=args.dedup_columns
        )
        
        # 输出详细结果
        print("\n=== 数据合并结果 ===")
        for source_name, source_results in results.items():
            print(f"\n数据源: {source_name}")
            for interface_name, success in source_results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {interface_name}")


if __name__ == "__main__":
    main()