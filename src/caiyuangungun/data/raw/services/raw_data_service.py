#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原始数据服务

提供统一的原始数据获取和管理服务，支持：
1. 历史数据回填 - 根据配置的start_date作为起点，自动跳过已存在的数据文件
2. 标准数据更新 - 仅更新最新的一份数据，不跳过已存在文件
3. 数据更新含回溯 - 最新数据 + lookback_periods 的数据份数
4. 数据更新含回溯_三倍 - 最新数据 + lookback_periods*3 的数据份数
5. 指定期间的数据获取 - 获取指定时间范围的数据

基于storage_type进行数据分类管理：
- SNAPSHOT: 快照数据（如股票基本信息、交易日历）
- DAILY: 日频数据（如日线行情、每日指标）
- MONTHLY: 月频数据（如月度财务报表）
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
import pandas as pd
import logging
import time

# 导入新的工具模块
from ..utils.data_service_utils import (
    RunMode, DateUtils, ParameterBuilder, 
    DataDefinitionProcessor, ResultProcessor
)
from ..utils.db_logger import DatabaseLogger

# 计算正确的core目录路径
current_file = Path(__file__).resolve()
raw_dir = current_file.parent.parent  # services -> raw
core_dir = raw_dir / 'core'

print(f"Current file: {current_file}")
print(f"Raw dir: {raw_dir}")
print(f"Core dir: {core_dir}")
print(f"Core dir exists: {core_dir.exists()}")

sys.path.insert(0, str(core_dir))

# 使用importlib动态导入
import importlib.util

# 修复导入问题：使用绝对路径导入core模块
try:
    # 使用importlib.util直接从文件路径导入模块
    def import_from_path(module_name, file_path):
        spec = importlib.util.spec_from_file_location(module_name, str(file_path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    # 导入ConfigManager
    config_manager_module = import_from_path("config_manager", core_dir / "config_manager.py")
    ConfigManager = config_manager_module.ConfigManager
    
    # 导入DataSourceManager
    data_source_manager_module = import_from_path("data_source_manager", core_dir / "data_source_manager.py")
    DataSourceManager = data_source_manager_module.DataSourceManager
    
    # 导入UniversalArchiver
    universal_archiver_module = import_from_path("universal_archiver", core_dir / "universal_archiver.py")
    UniversalArchiver = universal_archiver_module.UniversalArchiver
    
    print("Successfully imported core modules using absolute paths")
    
    # 导入DTO验证模块（如果失败则使用简化版本）
    try:
        from ..dto.dto_validation import get_validator, validate_data, is_valid_data, get_validation_errors
    except ImportError:
        print("DTO validation module not available, using simplified implementation")
        def get_validator(): return None
        def validate_data(data): return []
        def is_valid_data(data): return True
        def get_validation_errors(data): return []
        
except Exception as e:
    print(f"Core module import failed: {e}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    print(f"Full traceback: {traceback.format_exc()}")
    print("Falling back to simplified implementation...")
    
    # 只导入ConfigManager，其他功能我们自己实现
    spec = importlib.util.spec_from_file_location("config_manager", str(core_dir / "config_manager.py"))
    config_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_manager_module)
    ConfigManager = config_manager_module.ConfigManager
    
    # DTO验证的简化实现
    def get_validator(): return None
    def validate_data(data): return []
    def is_valid_data(data): return True
    def get_validation_errors(data): return []
    
    # 如果核心模块导入失败，抛出异常而不是使用简化实现
    raise ImportError(f"核心模块导入失败: {e}。请检查依赖模块是否正确安装和配置。")


class RawDataService:
    """原始数据服务
    
    提供统一的数据获取、处理和归档功能
    """
    
    def __init__(self):
        """初始化原始数据服务"""
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化核心组件
        self.config_manager = ConfigManager()
        self.data_source_manager = DataSourceManager(self.config_manager)
        
        # 初始化DTO验证器
        try:
            self.dto_validator = get_validator()
        except Exception as e:
            self.logger.warning(f"DTO验证器初始化失败: {e}，将跳过数据验证")
            self.dto_validator = None
        
        # 获取统一数据配置
        all_config = self.config_manager.get_all_config()
        self.unified_config = all_config.get('unified_data_config', {})
        self.data_sources_config = self.unified_config.get('data_sources', {})
        
        # 初始化归档器缓存
        self._archivers = {}
        
        # 初始化数据库记录器
        try:
            self.db_logger = DatabaseLogger(self.config_manager)
            if self.db_logger.enabled:
                self.logger.info("数据库记录器初始化成功")
            else:
                self.logger.info("数据库记录器已禁用")
        except Exception as e:
            self.logger.warning(f"数据库记录器初始化失败: {e}")
            self.db_logger = None
        
        self.logger.info(f"RawDataService初始化完成，加载{len(self.data_sources_config)}个数据源配置")
    
    def get_archiver(self, source_name: str) -> UniversalArchiver:
        """获取指定数据源的归档器
        
        Args:
            source_name: 数据源名称
            
        Returns:
            UniversalArchiver实例
        """
        if source_name not in self._archivers:
            self._archivers[source_name] = UniversalArchiver(source_name)
        return self._archivers[source_name]
    
    def get_data_definitions_by_storage_type(self, storage_type: str, 
                                           source_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """根据存储类型获取数据定义
        
        Args:
            storage_type: 存储类型 (SNAPSHOT, DAILY, MONTHLY)
            source_name: 数据源名称，None表示所有数据源
            
        Returns:
            符合条件的数据定义字典
        """
        result = {}
        
        for src_name, src_config in self.data_sources_config.items():
            # 如果指定了数据源名称，则过滤
            if source_name and src_name != source_name:
                continue
                
            data_definitions = src_config.get('data_definitions', {})
            for data_type, definition in data_definitions.items():
                if definition.get('storage_type') == storage_type:
                    # 添加源信息
                    definition_with_source = definition.copy()
                    definition_with_source['source_name'] = src_name
                    result[f"{src_name}_{data_type}"] = definition_with_source
        
        return result
    
    def get_data_definition(self, source_name: str, data_type: str) -> Optional[Dict[str, Any]]:
        """获取指定数据源和数据类型的定义
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            
        Returns:
            数据定义，如果不存在返回None
        """
        source_config = self.data_sources_config.get(source_name, {})
        data_definitions = source_config.get('data_definitions', {})
        definition = data_definitions.get(data_type)
        
        if definition:
            definition_with_source = definition.copy()
            definition_with_source['source_name'] = source_name
            return definition_with_source
        
        return None
    
    # 注意：原有的辅助方法已移至utils模块中，通过DateUtils类提供相同功能
    
    def fetch_and_archive_data(self, source_name: str, data_type: str, 
                             date_param: Optional[str] = None, 
                             skip_existing: bool = True) -> Dict[str, Any]:
        """获取并归档数据
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            date_param: 日期参数
            skip_existing: 是否跳过已存在的文件
            
        Returns:
            处理结果
        """
        # 初始化数据库记录
        db_record = None
        if self.db_logger and self.db_logger.enabled:
            db_record = self.db_logger.record_fetch_start(
                source_name=source_name,
                data_type=data_type
            )
        
        try:
            # 获取数据定义
            definition = self.get_data_definition(source_name, data_type)
            if not definition:
                error_msg = f"未找到数据定义: {source_name}.{data_type}"
                if db_record:
                    self.db_logger.record_fetch_failed(db_record, error_msg)
                raise ValueError(error_msg)
            
            storage_type = definition.get('storage_type')
            method = definition.get('method')
            required_params = definition.get('required_params', [])
            
            # 获取归档器
            archiver = self.get_archiver(source_name)
            
            # 如果需要跳过已存在文件，先检查文件是否存在
            if skip_existing:
                archive_type = storage_type.upper()  # 归档器要求大写
                archive_info = archiver.get_archive_info(
                    data_type=data_type,
                    archive_type=archive_type,
                    date_param=date_param
                )
                
                if archive_info.get('exists', False):
                    self.logger.info(f"文件已存在，跳过: {source_name}.{data_type} {date_param or ''}")
                    # 如果error信息是文件已存在，不需要写入数据库
                    # if db_record:
                    #     self.db_logger.record_fetch_skipped(db_record, "文件已存在")
                    return {
                        'status': 'skipped',
                        'reason': 'file_exists',
                        'source_name': source_name,
                        'data_type': data_type,
                        'date_param': date_param
                    }
            
            # 获取数据源实例
            data_source = self.data_source_manager.get_instance(source_name)
            if not data_source:
                raise ValueError(f"无法获取数据源实例: {source_name}")
            
            # 使用工具函数构建获取数据的参数
            fetch_params = ParameterBuilder.build_fetch_params(
                data_type, date_param, required_params
            )
            
            # 保存程序构造的参数（用于归档）
            constructed_params = {
                'source_name': source_name,
                'data_type': data_type,
                'date_param': date_param,
                'storage_type': storage_type,
                'method': method,
                'required_params': required_params
            }
            
            # 准备API调用参数
            api_call_params = fetch_params.copy()
            api_call_params['data_type'] = data_type
            
            # 更新数据库记录的构造参数信息
            if db_record:
                db_record.constructed_params = constructed_params
                db_record.archive_type = storage_type.upper()
                db_record.date_param = date_param
            
            # 获取数据
            self.logger.info(f"获取数据: {source_name}.{data_type} {date_param or ''}")
            
            data = data_source.fetch_data(**api_call_params)
            
            # 获取数据源处理后的真实API参数
            if hasattr(data_source, '_last_processed_params'):
                # 如果数据源提供了最后处理的参数，使用它
                real_api_params = data_source._last_processed_params.copy()
            else:
                # 否则尝试模拟数据源的参数处理过程
                try:
                    # 获取API配置
                    api_config = data_source.get_api_config(data_type)
                    # 调用数据源的参数处理方法
                    real_api_params = data_source._process_params(api_call_params, api_config)
                except Exception as e:
                    self.logger.warning(f"无法获取真实API参数: {e}，使用原始参数")
                    real_api_params = api_call_params.copy()
            
            # 更新数据库记录的真实API参数
            if db_record:
                db_record.api_params = real_api_params
            
            if data is None or data.empty:
                self.logger.warning(f"获取到空数据: {source_name}.{data_type} {date_param or ''}")
                if db_record:
                    self.db_logger.record_fetch_failed(db_record, "获取到空数据")
                return {
                    'status': 'empty',
                    'source_name': source_name,
                    'data_type': data_type,
                    'date_param': date_param
                }
            
            # 数据验证
            if self.dto_validator and isinstance(data, dict) and data:
                if not is_valid_data(data):
                    validation_errors = get_validation_errors(data)
                    error_msg = f"数据验证失败: {validation_errors}"
                    self.logger.warning(f"数据验证失败: {source_name}.{data_type} {date_param or ''} - {validation_errors}")
                    if db_record:
                        self.db_logger.record_fetch_failed(db_record, error_msg)
                    return {
                        'status': 'validation_failed',
                        'source_name': source_name,
                        'data_type': data_type,
                        'date_param': date_param,
                        'validation_errors': validation_errors
                    }
            
            # 归档数据
            archive_type = storage_type.upper()  # 归档器要求大写
            
            # 使用工具函数构建归档参数
            archive_kwargs = ParameterBuilder.build_archive_params(
                source_name, data_type, data, date_param, archive_type,
                constructed_params, real_api_params
            )
            
            archive_result = archiver.archive_data(**archive_kwargs)
            
            # 处理需要重试的情况
            if archive_result.get('action') == 'retry_required':
                retry_reason = archive_result.get('retry_reason', '数据行数减少')
                self.logger.warning(f"[Service] {source_name}.{data_type} {date_param or ''} - {retry_reason}，开始二次验证")
                
                # 进行二次数据获取
                try:
                    self.logger.info(f"[Service] 开始二次数据获取: {source_name}.{data_type}")
                    retry_data = data_source.fetch_data(**api_params)
                    
                    # 比较两次数据是否一致
                    if retry_data.equals(data):
                        self.logger.info(f"[Service] 二次验证通过，数据一致，继续保存")
                        # 强制保存新数据
                        archive_kwargs_retry = archive_kwargs.copy()
                        archive_kwargs_retry['force_update'] = True
                        archive_result = archiver.archive_data(**archive_kwargs_retry)
                    else:
                        # 两次数据不一致，标记失败
                        error_msg = f"二次验证失败：两次获取的数据不一致。原因：{retry_reason}"
                        self.logger.error(f"[Service] {error_msg}")
                        
                        if db_record:
                            self.db_logger.record_fetch_failed(db_record, error_msg)
                        
                        return ResultProcessor.create_error_result(
                            source_name, data_type, date_param, error_msg
                        )
                        
                except Exception as retry_e:
                    error_msg = f"二次验证过程中发生错误：{str(retry_e)}。原因：{retry_reason}"
                    self.logger.error(f"[Service] {error_msg}")
                    
                    if db_record:
                        self.db_logger.record_fetch_failed(db_record, error_msg)
                    
                    return ResultProcessor.create_error_result(
                        source_name, data_type, date_param, error_msg
                    )
            
            # 记录数据库成功状态
            if db_record:
                # 从归档结果中获取MD5和data_shape信息
                current_md5 = archive_result.get('data_md5')
                previous_md5 = archive_result.get('previous_md5')
                previous_data_shape = archive_result.get('previous_data_shape')
                
                if archive_result['action'] == 'skipped_duplicate':
                    self.db_logger.record_fetch_skipped(db_record, "数据未变更", 
                                                      data_md5=current_md5, data_shape=tuple(data.shape))
                else:
                    self.db_logger.record_fetch_success(
                        record=db_record,
                        data_md5=current_md5,
                        data_shape=tuple(data.shape),
                        previous_md5=previous_md5,
                        previous_data_shape=tuple(previous_data_shape) if previous_data_shape else None,
                        archive_type=storage_type.upper(),
                        date_param=date_param
                    )
            
            # 使用工具函数创建成功结果
            result = ResultProcessor.create_success_result(
                source_name, data_type, date_param, list(data.shape), archive_result
            )
            
            # 简化日志输出，详细信息由数据源和归档器提供
            action_desc = {'created': '新建', 'updated': '更新', 'skipped_duplicate': '跳过'}.get(archive_result['action'], archive_result['action'])
            self.logger.info(f"[Service] {source_name}.{data_type} {date_param or ''} - {action_desc}")
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"数据处理失败: {source_name}.{data_type} {date_param or ''} - {error_msg}")
            
            # 记录数据库失败状态
            if db_record:
                self.db_logger.record_fetch_failed(db_record, error_msg)
            
            # 使用工具函数创建错误结果
            return ResultProcessor.create_error_result(
                source_name, data_type, date_param, error_msg
            )
    
    def historical_backfill(self, source_name: Optional[str] = None, 
                          storage_type: Optional[str] = None,
                          data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """历史数据回填
        
        根据配置的start_date作为起点，自动跳过已存在的数据文件
        
        Args:
            source_name: 数据源名称，None表示所有数据源
            storage_type: 存储类型，None表示所有类型
            data_type: 数据类型，None表示所有类型
            
        Returns:
            处理结果列表
        """
        # 使用统一运行函数
        return self.execute_unified_run(
            run_mode=RunMode.HISTORICAL_BACKFILL,
            source_name=source_name,
            storage_type=storage_type,
            data_type=data_type
        )
    
    def standard_update(self, source_name: Optional[str] = None,
                       storage_type: Optional[str] = None,
                       data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """标准数据更新
        
        仅更新最新的一份数据，不跳过已存在文件
        
        Args:
            source_name: 数据源名称
            storage_type: 存储类型
            data_type: 数据类型
            
        Returns:
            处理结果列表
        """
        # 使用统一运行函数
        return self.execute_unified_run(
            run_mode=RunMode.STANDARD_UPDATE,
            source_name=source_name,
            storage_type=storage_type,
            data_type=data_type
        )
    
    def update_with_lookback(self, source_name: Optional[str] = None,
                           storage_type: Optional[str] = None,
                           data_type: Optional[str] = None,
                           multiplier: int = 1) -> List[Dict[str, Any]]:
        """数据更新含回溯
        
        最新数据 + lookback_periods * multiplier 的数据份数
        
        Args:
            source_name: 数据源名称
            storage_type: 存储类型
            data_type: 数据类型
            multiplier: 回溯倍数（1=标准回溯，3=三倍回溯）
            
        Returns:
            处理结果列表
        """
        # 使用统一运行函数
        run_mode = RunMode.UPDATE_WITH_TRIPLE_LOOKBACK if multiplier == 3 else RunMode.UPDATE_WITH_LOOKBACK
        return self.execute_unified_run(
            run_mode=run_mode,
            source_name=source_name,
            storage_type=storage_type,
            data_type=data_type,
            multiplier=multiplier
        )
    
    def update_with_triple_lookback(self, source_name: Optional[str] = None,
                                  storage_type: Optional[str] = None,
                                  data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """数据更新含回溯_三倍
        
        最新数据 + lookback_periods*3 的数据份数
        
        Args:
            source_name: 数据源名称
            storage_type: 存储类型
            data_type: 数据类型
            
        Returns:
            处理结果列表
        """
        # 使用统一运行函数
        return self.execute_unified_run(
            run_mode=RunMode.UPDATE_WITH_TRIPLE_LOOKBACK,
            source_name=source_name,
            storage_type=storage_type,
            data_type=data_type
        )
    
    def fetch_period_data(self, start_date: str, end_date: str,
                         source_name: Optional[str] = None,
                         storage_type: Optional[str] = None,
                         data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """指定期间的数据获取
        
        获取指定时间范围的数据，不跳过已存在文件
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            source_name: 数据源名称
            storage_type: 存储类型
            data_type: 数据类型
            
        Returns:
            处理结果列表
        """
        # 使用统一运行函数
        return self.execute_unified_run(
            run_mode=RunMode.FETCH_PERIOD_DATA,
            source_name=source_name,
            storage_type=storage_type,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date
        )
    
    def execute_unified_run(self, 
                          run_mode: RunMode,
                          source_name: Optional[str] = None,
                          storage_type: Optional[str] = None,
                          data_type: Optional[str] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None,
                          multiplier: int = 1) -> List[Dict[str, Any]]:
        """统一的数据获取执行函数
        
        支持所有5种运行模式：
        1. HISTORICAL_BACKFILL - 历史数据回填，根据配置的start_date作为起点，自动跳过已存在的数据文件
        2. STANDARD_UPDATE - 标准数据更新，仅更新最新的一份数据，不跳过已存在文件
        3. UPDATE_WITH_LOOKBACK - 数据更新含回溯，最新数据 + lookback_periods 的数据份数
        4. UPDATE_WITH_TRIPLE_LOOKBACK - 数据更新含回溯_三倍，最新数据 + lookback_periods*3 的数据份数
        5. FETCH_PERIOD_DATA - 指定期间的数据获取，获取指定时间范围的数据
        
        Args:
            run_mode: 运行模式
            source_name: 数据源名称
            storage_type: 存储类型
            data_type: 数据类型
            start_date: 开始日期（用于FETCH_PERIOD_DATA模式）
            end_date: 结束日期（用于FETCH_PERIOD_DATA模式）
            multiplier: 回溯倍数（用于UPDATE_WITH_LOOKBACK模式）
            
        Returns:
            处理结果列表
        """
        # 获取过滤后的数据定义
        definitions = DataDefinitionProcessor.get_filtered_definitions(
            self.data_sources_config, source_name, storage_type, data_type
        )
        
        if not definitions:
            self.logger.warning(f"未找到匹配的数据定义: source={source_name}, storage={storage_type}, data_type={data_type}")
            return []
        
        # 生成任务列表
        tasks = DataDefinitionProcessor.generate_task_list(
            definitions, run_mode, start_date, end_date, multiplier
        )
        
        mode_names = {
            RunMode.HISTORICAL_BACKFILL: "历史数据回填",
            RunMode.STANDARD_UPDATE: "标准数据更新",
            RunMode.UPDATE_WITH_LOOKBACK: "数据更新含回溯",
            RunMode.UPDATE_WITH_TRIPLE_LOOKBACK: "数据更新含三倍回溯",
            RunMode.FETCH_PERIOD_DATA: "指定期间数据获取"
        }
        
        mode_name = mode_names.get(run_mode, str(run_mode))
        self.logger.info(f"开始{mode_name}，共{len(tasks)}个任务")
        
        # 执行任务
        results = []
        for task in tasks:
            result = self.fetch_and_archive_data(
                source_name=task['source_name'],
                data_type=task['data_type'],
                date_param=task['date_param'],
                skip_existing=task['skip_existing']
            )
            results.append(result)
            
            # 添加延迟避免请求过于频繁
            if len(tasks) > 1:
                time.sleep(0.1)
        
        # 汇总结果
        summary = ResultProcessor.summarize_results(results)
        self.logger.info(f"[Service] {mode_name}完成: 成功{summary['success']}, 跳过{summary['skipped']}, 错误{summary['error']}")
        
        return results
    
    def get_service_status(self) -> Dict[str, Any]:
        """获取服务状态
        
        Returns:
            服务状态信息
        """
        # 获取数据源状态
        sources = self.data_source_manager.list_sources()
        
        # 统计数据定义
        snapshot_count = len(self.get_data_definitions_by_storage_type('SNAPSHOT'))
        daily_count = len(self.get_data_definitions_by_storage_type('DAILY'))
        monthly_count = len(self.get_data_definitions_by_storage_type('MONTHLY'))
        
        # 获取DTO验证器状态
        dto_validator_status = {
            'enabled': self.dto_validator is not None,
            'rules_count': len(self.dto_validator.get_all_rules()) if self.dto_validator else 0
        }
        
        return {
            'service_name': 'RawDataService',
            'status': 'running',
            'data_sources': {
                'total': len(sources),
                'enabled': sum(1 for s in sources if s['enabled']),
                'with_instances': sum(1 for s in sources if s['instance_created'])
            },
            'data_definitions': {
                'snapshot': snapshot_count,
                'daily': daily_count,
                'monthly': monthly_count,
                'total': snapshot_count + daily_count + monthly_count
            },
            'dto_validator': dto_validator_status,
            'archivers': list(self._archivers.keys())
        }


if __name__ == '__main__':
    """测试和演示脚本"""
    import logging
    
    # 配置日志 - 设置为INFO级别
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 设置特定模块的日志级别
    logging.getLogger('RawDataService').setLevel(logging.INFO)
    logging.getLogger('DataSourceManager').setLevel(logging.INFO)
    logging.getLogger('UniversalArchiver').setLevel(logging.INFO)
    
    # 创建服务实例
    service = RawDataService()
    
    print("=== RawDataService 演示 ===")
    
    # 2. 获取24年11月最后几天daily数据 (测试)
    print("\n2. 获取24年11月最后几天daily数据")
    daily_results = service.fetch_period_data(
        start_date='20241125',
        end_date='20241130',
        storage_type='DAILY'
    )
    print(f"Daily数据获取完成，处理了{len(daily_results)}个任务")
    
    # 3. 获取24年4月monthly数据 (测试)
    print("\n3. 获取24年4月monthly数据")
    monthly_results = service.fetch_period_data(
        start_date='202404',
        end_date='202404',
        storage_type='MONTHLY'
    )
    print(f"Monthly数据获取完成，处理了{len(monthly_results)}个任务")
    
    # 4. 获取所有快照数据
    print("\n4. 获取所有快照数据")
    snapshot_results = service.standard_update(storage_type='SNAPSHOT')
    print(f"Snapshot数据获取完成，处理了{len(snapshot_results)}个任务")
    
    # 显示服务状态
    print("\n=== 服务状态 ===")
    status = service.get_service_status()
    print(f"数据源: {status['data_sources']}")
    print(f"数据定义: {status['data_definitions']}")
    print(f"归档器: {status['archivers']}")