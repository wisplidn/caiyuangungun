#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw数据服务
负责原始数据的采集、处理和保存
包含任务生成、任务执行、数据保存等核心功能模块
"""

import logging
import time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import calendar
import json
import hashlib

# 导入项目内部模块 - 使用相对导入
try:
    from config_manager import get_config_manager
    from base_data_source import BaseDataSource
    from universal_archiver import UniversalArchiver
    from placeholder_generator import PlaceholderGenerator
    from path_generator import PathGenerator
except ImportError:
    # 备用导入方式
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / 'core'))
    sys.path.insert(0, str(project_root / 'utils'))
    
    from config_manager import get_config_manager
    from base_data_source import BaseDataSource
    from universal_archiver import UniversalArchiver
    from placeholder_generator import PlaceholderGenerator
    from path_generator import PathGenerator

# 导入数据库记录管理器
try:
    from ..database.task_record_manager import TaskRecordManager
except ImportError:
    # 备用导入方式
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / 'database'))
    from task_record_manager import TaskRecordManager

# 确保pandas已导入用于数据处理
try:
    import pandas as pd
except ImportError:
    pd = None


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"      # 待执行
    RUNNING = "running"      # 执行中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"        # 执行失败
    CANCELLED = "cancelled"  # 已取消


class TaskPriority(Enum):
    """任务优先级枚举"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4


@dataclass
class DataTask:
    """数据采集任务"""
    task_id: str
    source_name: str
    data_type: str  # 用于路径生成的配置键名
    endpoint: str   # 用于API调用的真实endpoint
    method: str
    params: Dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.MEDIUM
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3





class TaskGenerator:
    """任务生成器
    
    负责根据配置和规则生成数据采集任务
    """
    
    def __init__(self, config_manager=None, force_update=False):
        self.config_manager = config_manager or get_config_manager()
        self.logger = logging.getLogger(__name__ + '.TaskGenerator')
        self.placeholder_generator = PlaceholderGenerator()
        self.force_update = force_update
    
    def convert_task_list_to_blocks(self, task_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """将任务列表转换为任务块
        
        处理包含列表参数的任务配置，将其拆分为多个独立的任务块。
        特别处理monthlyrange方法生成的一一对应的两个key。
        集成路径生成、文件检查和过滤逻辑。
        
        Args:
            task_config: 包含列表参数的任务配置
            
        Returns:
            List[Dict[str, Any]]: 拆分后的任务块列表（已过滤存在文件的块，除非强制更新）
        """
        required_params = task_config.get('required_params', {})
        
        # 处理required_params可能是列表的情况
        if isinstance(required_params, list):
            # 如果是空列表，转换为空字典
            if not required_params:
                required_params = {}
            else:
                # 如果是非空列表，这可能是配置错误，记录警告并返回原任务
                self.logger.warning(f"required_params是非空列表，无法处理: {required_params}")
                return [task_config]
        
        # 检查是否包含列表参数
        list_params = {}
        non_list_params = {}
        
        for key, value in required_params.items():
            if isinstance(value, list) and len(value) > 0:
                list_params[key] = value
            else:
                non_list_params[key] = value
        
        # 如果没有列表参数，直接处理单个任务
        if not list_params:
            task_blocks = [task_config]
        else:
            # 验证所有列表参数长度是否一致（一一对应）
            list_lengths = [len(v) for v in list_params.values()]
            if len(set(list_lengths)) > 1:
                self.logger.error(f"列表参数长度不一致: {dict(zip(list_params.keys(), list_lengths))}")
                raise ValueError(f"列表参数长度必须一致，当前长度: {dict(zip(list_params.keys(), list_lengths))}")
            
            task_count = list_lengths[0]
            self.logger.info(f"检测到 {len(list_params)} 个列表参数，将生成 {task_count} 个任务块")
            
            # 生成任务块
            task_blocks = []
            for i in range(task_count):
                # 复制基础任务配置
                block_config = task_config.copy()
                
                # 构建当前任务块的参数
                block_params = non_list_params.copy()
                for key, value_list in list_params.items():
                    block_params[key] = value_list[i]
                
                block_config['required_params'] = block_params
                
                # 为任务块添加索引标识
                block_config['block_index'] = i
                block_config['total_blocks'] = task_count
                
                task_blocks.append(block_config)
                
                self.logger.debug(f"生成任务块 {i+1}/{task_count}: {block_params}")
        
        # 为每个任务块生成路径并检查文件存在性
        # 需要从配置管理器获取PathGenerator实例
        try:
            from ..core.path_generator import PathGenerator, ConfigDTO, ArchiveTypeConfig, FileConfig, PathsConfig
        except ImportError:
            from path_generator import PathGenerator, ConfigDTO, ArchiveTypeConfig, FileConfig, PathsConfig
        
        enhanced_blocks = []
        
        for block in task_blocks:
            try:
                # 从任务块参数中提取路径生成所需的信息
                required_params = block.get('required_params', {})
                data_source = block.get('data_source', '')
                endpoint = block.get('endpoint', '')
                storage_type = block.get('storage_type', '')
                
                # 构建PathGenerator所需的参数
                # 优先使用配置键名(config_key)，如果没有则使用endpoint
                config_key = block.get('config_key', endpoint)
                path_params = {
                    'source_name': data_source,
                    'data_type': config_key,  # 使用配置键名作为data_type
                    'archive_type': storage_type.upper()  # storage_type作为archive_type
                }
                
                # 添加日期相关参数 - 统一使用date参数
                storage_type_upper = storage_type.upper()
                
                # 对于需要日期的archive_type，统一查找date参数
                if storage_type_upper in ['DAILY', 'MONTHLY', 'QUARTERLY']:
                    date_value = None
                    # 支持多种日期参数名称映射到统一的date参数
                    # 添加 'period' 参数支持，用于处理 fina_indicator_vip 等接口的季度日期
                    # 添加 'ann_date' 参数支持，用于处理 dividend 等接口的公告日期
                    for date_key in ['daily_date', 'monthly_date', 'quarterly_date', 'trade_date', 'month', 'quarter', 'date', 'period','end_date', 'ann_date']:
                        if date_key in required_params:
                            date_value = required_params[date_key]
                            break
                    
                    # 如果没有找到直接的date参数，尝试从start_date生成
                    if date_value is None and 'start_date' in required_params:
                        start_date = required_params['start_date']
                        if storage_type_upper == 'MONTHLY':
                            # 对于MONTHLY类型，从start_date提取年月（前6位）
                            if len(start_date) >= 6:
                                date_value = start_date[:6]  # 取前6位作为年月
                        elif storage_type_upper == 'DAILY':
                            # 对于DAILY类型，直接使用start_date
                            date_value = start_date
                        elif storage_type_upper == 'QUARTERLY':
                            # 对于QUARTERLY类型，需要转换为财报日期
                            if len(start_date) >= 6:
                                year = start_date[:4]
                                month = start_date[4:6]
                                # 根据月份确定财报日期
                                if month <= '03':
                                    date_value = f"{year}0331"  # Q1
                                elif month <= '06':
                                    date_value = f"{year}0630"  # Q2
                                elif month <= '09':
                                    date_value = f"{year}0930"  # Q3
                                else:
                                    date_value = f"{year}1231"  # Q4
                    
                    if date_value is not None:
                        if isinstance(date_value, list) and 'block_index' in block:
                            path_params['date'] = date_value[block['block_index']] if block['block_index'] < len(date_value) else date_value[0]
                        else:
                            path_params['date'] = date_value
                
                # 添加symbol相关参数 - 对于SYMBOL类型的archive_type
                if storage_type_upper == 'SYMBOL':
                    symbol_value = None
                    # 查找symbol参数
                    if 'symbol' in required_params:
                        symbol_value = required_params['symbol']
                        if isinstance(symbol_value, list) and 'block_index' in block:
                            path_params['symbol'] = symbol_value[block['block_index']] if block['block_index'] < len(symbol_value) else symbol_value[0]
                        else:
                            path_params['symbol'] = symbol_value
                
                # 从配置管理器获取PathGenerator配置
                # 直接从path_generator_config获取配置
                path_gen_config = self.config_manager.get('path_generator_config', {})
                
                # 如果path_generator_config存在且包含path_generator，则提取嵌套配置
                if path_gen_config and 'path_generator' in path_gen_config:
                    actual_config = path_gen_config['path_generator']
                else:
                    # 否则直接使用path_generator_config作为配置
                    actual_config = path_gen_config
                
                # 如果还是没有配置，尝试从path_generator直接获取
                if not actual_config:
                    actual_config = self.config_manager.get('path_generator', {})
                
                print(f"DEBUG: 获取到的PathGenerator配置: {json.dumps(actual_config, indent=2, ensure_ascii=False) if actual_config else 'None'}")
                
                # 构建archive_types配置
                archive_types = {}
                config_archive_types = actual_config.get('archive_types', {})
                for type_name, type_config in config_archive_types.items():
                    if type_config.get('enabled', True):
                        archive_types[type_name] = ArchiveTypeConfig(
                            value=type_config.get('value', type_name.lower()),
                            description=type_config.get('description', ''),
                            path_pattern=type_config.get('path_pattern', ''),
                            enabled=type_config.get('enabled', True),
                            validation_rules=type_config.get('validation_rules', {})
                        )
                
                # 构建file_config
                file_config_data = actual_config.get('file_config', {})
                file_config = FileConfig(
                    filename_template=file_config_data.get('filename_template', 'data.{file_type}'),
                    supported_formats=file_config_data.get('supported_formats', ['parquet', 'json']),
                    default_format=file_config_data.get('default_format', 'parquet')
                )
                
                # 构建paths_config
                paths_config_data = actual_config.get('paths', {})
                paths_config = PathsConfig(
                    landing_subpath=paths_config_data.get('landing_subpath', 'landing'),
                    archive_subpath=paths_config_data.get('archive_subpath', 'archive')
                )
                
                # 创建ConfigDTO
                config_dto = ConfigDTO(
                    base_path=actual_config.get('base_path', '/tmp/data'),
                    archive_types=archive_types,
                    file_config=file_config,
                    paths=paths_config
                )
                
                # 创建PathGenerator实例
                path_generator = PathGenerator(config_dto)
                
                # 生成路径信息
                path_result = path_generator.get_path_info(**path_params)
                
                if path_result.get('success', False):
                    # 获取默认格式的路径信息
                    default_format = path_result.get('default_format', 'parquet')
                    file_paths = path_result.get('file_paths', {})
                    
                    if default_format in file_paths:
                        path_info = file_paths[default_format]
                        landing_dir = path_info.get('directory', {}).get('landing')
                        archive_dir = path_info.get('directory', {}).get('archive')
                        
                        # 检查landing和archive目录下的data.parquet文件是否存在
                        landing_file_exists = False
                        archive_file_exists = False
                        
                        if landing_dir:
                            landing_data_file = Path(landing_dir) / 'data.parquet'
                            landing_file_exists = landing_data_file.exists()
                        
                        if archive_dir:
                            archive_data_file = Path(archive_dir) / 'data.parquet'
                            archive_file_exists = archive_data_file.exists()
                        
                        # 添加目录信息和文件存在性到任务块
                        block['landing_dir'] = landing_dir
                        block['archive_dir'] = archive_dir
                        block['data_file_exists'] = landing_file_exists or archive_file_exists
                        block['landing_file_exists'] = landing_file_exists
                        block['archive_file_exists'] = archive_file_exists
                        
                        if landing_file_exists:
                            self.logger.debug(f"检测到已存在landing文件: {landing_data_file}")
                        if archive_file_exists:
                            self.logger.debug(f"检测到已存在archive文件: {archive_data_file}")
                    else:
                        # 路径生成成功但格式不匹配
                        self.logger.warning(f"未找到默认格式 {default_format} 的路径信息")
                        block['landing_dir'] = None
                        block['archive_dir'] = None
                        block['data_file_exists'] = False
                        block['landing_file_exists'] = False
                        block['archive_file_exists'] = False
                else:
                    # 路径生成失败
                    errors = path_result.get('errors', [])
                    self.logger.warning(f"路径生成失败: {'; '.join(errors)}")
                    # 添加详细的调试信息
                    print(f"DEBUG: 路径生成失败 - 参数: {path_params}")
                    print(f"DEBUG: PathGenerator返回结果: {json.dumps(path_result, indent=2, ensure_ascii=False)}")
                    block['landing_dir'] = None
                    block['archive_dir'] = None
                    block['data_file_exists'] = False
                    block['landing_file_exists'] = False
                    block['archive_file_exists'] = False
                
                enhanced_blocks.append(block)
                
            except Exception as e:
                self.logger.warning(f"处理任务块路径时出错: {e}")
                # 即使出错也保留任务块，但标记为路径生成失败
                block['landing_dir'] = None
                block['archive_dir'] = None
                block['data_file_exists'] = False
                block['landing_file_exists'] = False
                block['archive_file_exists'] = False
                enhanced_blocks.append(block)
        
        # 根据force_update标志过滤任务块
        if self.force_update:
            # 强制更新模式，返回所有任务块
            filtered_blocks = enhanced_blocks
            self.logger.info(f"强制更新模式，保留所有 {len(filtered_blocks)} 个任务块")
        else:
            # 非强制更新模式，过滤掉已存在文件的任务块
            filtered_blocks = [block for block in enhanced_blocks if not block.get('data_file_exists', False)]
            removed_count = len(enhanced_blocks) - len(filtered_blocks)
            if removed_count > 0:
                self.logger.info(f"过滤掉 {removed_count} 个已存在文件的任务块，剩余 {len(filtered_blocks)} 个任务块")
            else:
                self.logger.info(f"无需过滤，保留所有 {len(filtered_blocks)} 个任务块")
        
        return filtered_blocks
        
    def generate_tasks(self, 
                      data_sources: str = None,
                      methods: str = None,
                      storage_types: str = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      lookback_multiplier: int = 0) -> Dict[str, Any]:
        """生成任务列表
        
        Args:
            data_sources: 数据源列表，None或空列表表示全部
            methods: 方法列表，None或空列表表示全部
            storage_types: 存储类型列表，None或空列表表示全部
            start_date: 开始日期，None表示使用配置中的默认值
            end_date: 结束日期，None表示使用当天
            lookback_multiplier: 回看周期扩展倍数
            
        Returns:
            Dict[str, Any]: 任务列表JSON格式
        """
        self.logger.info(f"开始生成任务 - data_sources: {data_sources}, methods: {methods}, start_date: {start_date}, end_date: {end_date}")
        
        # 获取配置
        all_config = self.config_manager.get_all_config()
        self.logger.info(f"获取到配置键: {list(all_config.keys())}")
        
        # 从unified_data_config中获取data_sources配置
        if 'unified_data_config' not in all_config:
            raise ValueError("配置文件中缺少unified_data_config配置")
        
        config = all_config['unified_data_config']
        if 'data_sources' not in config:
            raise ValueError("配置文件中缺少data_sources配置")
        
        self.logger.info(f"配置中的数据源: {list(config['data_sources'].keys())}")
        
        # 设置默认值
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        self.logger.info(f"使用日期范围: {start_date} 到 {end_date}")
        
        # 验证和过滤配置
        filtered_config = self._filter_config(config, data_sources, methods, storage_types)
        self.logger.info(f"过滤后的数据源: {list(filtered_config['data_sources'].keys())}")
        
        # 生成任务列表
        task_list = {"data_sources": {}}
        
        for source_name, source_config in filtered_config['data_sources'].items():
            self.logger.info(f"处理数据源: {source_name}, enabled: {source_config.get('enabled', False)}")
            if not source_config.get('enabled', False):
                self.logger.warning(f"跳过未启用的数据源: {source_name}")
                continue
                
            task_list["data_sources"][source_name] = {"methods": {}}
            self.logger.info(f"数据源 {source_name} 的方法: {list(source_config['methods'].keys())}")
            
            for method_name, method_config in source_config['methods'].items():
                self.logger.info(f"处理方法: {method_name}, enable: {method_config.get('enable', False)}")
                if not method_config.get('enable', False):
                    self.logger.warning(f"跳过未启用的方法: {method_name}")
                    continue
                
                # 处理日期
                method_start_date = start_date if start_date else method_config.get('start_date')
                method_end_date = end_date
                
                self.logger.info(f"方法 {method_name} 使用日期: {method_start_date} 到 {method_end_date}")
                
                # 确定截断模式：如果明确传入了start_date参数，则禁用截断
                truncate_mode = start_date is None
                
                # 创建任务配置
                task_config = {
                    "data_source": method_config['data_source'],
                    "endpoint": method_config['endpoint'],
                    "description": method_config['description'],
                    "storage_type": method_config['storage_type'],
                    "required_params": method_config['required_params'],
                    "start_date": method_start_date,
                    "end_date": method_end_date,
                    "lookback_periods": method_config['lookback_periods'],
                    "lookback_multiplier": lookback_multiplier,
                    "truncate_mode": truncate_mode,
                    "enable": method_config['enable']
                }
                
                self.logger.info(f"任务配置创建完成: {source_name}.{method_name}")
                self.logger.info(f"原始参数: {method_config['required_params']}")
                
                # 处理占位符
                if method_config['storage_type'] != 'SNAPSHOT':
                    task_config = self._process_placeholders(task_config, truncate_mode)
                    self.logger.info(f"处理占位符后的参数: {task_config['required_params']}")
                
                task_list["data_sources"][source_name]["methods"][method_name] = task_config
                self.logger.info(f"任务添加完成: {source_name}.{method_name}")
        
        self.logger.info(f"最终生成的任务列表结构: {task_list}")
        return task_list
    
    def generate_tasks_with_validation(self, 
                                     data_sources: List[str] = None,
                                     methods: List[str] = None,
                                     storage_types: List[str] = None,
                                     start_date: Optional[str] = None,
                                     end_date: Optional[str] = None,
                                     lookback_multiplier: int = 0,
                                     force_update: Optional[bool] = None,
                                     **kwargs) -> Dict[str, Any]:
        """生成任务列表并进行配置规则校验
        
        这个函数合并了TaskGenerator类的功能，在调用generate_tasks之前
        先进行配置规则校验，确保输入的参数都存在于配置文件中。
        
        Args:
            data_sources: 数据源列表，None表示全部
            methods: 方法列表，None表示全部  
            storage_types: 存储类型列表，None表示全部
            start_date: 开始日期，None表示使用配置中的默认值
            end_date: 结束日期，None表示使用当天
            lookback_multiplier: 回看周期扩展倍数
            force_update: 强制更新标志，None时使用实例的force_update属性
            
        Returns:
            Dict[str, Any]: 任务列表JSON格式
            
        Raises:
            ValueError: 当输入参数不存在于配置中时抛出异常
        """
        # 确定是否强制更新
        effective_force_update = force_update if force_update is not None else self.force_update
        self.logger.info(f"开始生成任务并进行配置校验 - data_sources: {data_sources}, methods: {methods}, storage_types: {storage_types}, force_update: {effective_force_update}")
        
        try:
            # 获取配置
            all_config = self.config_manager.get_all_config()
            if 'unified_data_config' not in all_config:
                error_msg = "配置文件中缺少unified_data_config配置"
                self.logger.error(error_msg)
                return {
                    'success': False,
                    'task_config': {},
                    'task_blocks': [],
                    'message': error_msg
                }
            
            config = all_config['unified_data_config']
            if 'data_sources' not in config:
                error_msg = "配置文件中缺少data_sources配置"
                self.logger.error(error_msg)
                return {
                    'success': False,
                    'task_config': {},
                    'task_blocks': [],
                    'message': error_msg
                }
            
            # 进行配置规则校验
            self._validate_config_parameters(config, data_sources, methods, storage_types)
            
            # 调用原有的generate_tasks方法
            task_config = self.generate_tasks(
                data_sources=data_sources,
                methods=methods, 
                storage_types=storage_types,
                start_date=start_date,
                end_date=end_date,
                lookback_multiplier=lookback_multiplier
            )
        except Exception as e:
            error_msg = f"任务生成失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'task_config': {},
                'task_blocks': [],
                'message': error_msg
            }
        
        # 转换为块格式
        task_blocks = []
        
        # 遍历每个数据源和方法，为每个方法生成任务块
        for source_name, source_data in task_config.get('data_sources', {}).items():
            for method_name, method_config in source_data.get('methods', {}).items():
                # 创建基础任务配置
                base_task_config = {
                    'data_source': source_name,
                    'endpoint': method_config.get('endpoint', method_name),
                    'config_key': method_name,  # 添加配置键名
                    'description': method_config.get('description', ''),
                    'storage_type': method_config.get('storage_type', ''),
                    'required_params': method_config.get('required_params', {}),
                    'start_date': method_config.get('start_date'),
                    'end_date': method_config.get('end_date'),
                    'lookback_periods': method_config.get('lookback_periods', 1),
                    'lookback_multiplier': method_config.get('lookback_multiplier', 0),
                    'truncate_mode': method_config.get('truncate_mode', False),
                    'enable': method_config.get('enable', True)
                }
                
                # 使用convert_task_list_to_blocks处理列表参数
                method_task_blocks = self.convert_task_list_to_blocks(base_task_config)
                
                # 将生成的任务块添加到总列表中
                task_blocks.extend(method_task_blocks)
        
        # 如果不是强制更新，检查已存在的文件并移除对应的任务块
        if not effective_force_update:
            original_count = len(task_blocks)
            task_blocks = self._remove_existing_tasks(task_blocks)
            removed_count = original_count - len(task_blocks)
            if removed_count > 0:
                self.logger.info(f"移除了 {removed_count} 个已存在文件对应的任务块")
        
        return {
            'success': True,
            'task_config': task_config,
            'task_blocks': task_blocks,
            'message': f'成功生成 {len(task_blocks)} 个任务块'
        }
    
    def _remove_existing_tasks(self, task_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """移除已存在文件对应的任务块
        
        Args:
            task_blocks: 任务块列表
            
        Returns:
            List[Dict[str, Any]]: 过滤后的任务块列表
        """
        filtered_blocks = []
        
        for block in task_blocks:
            # 检查文件是否存在 - 使用convert_task_list_to_blocks方法设置的data_file_exists字段
            data_file_exists = block.get('data_file_exists', False)
            if data_file_exists:
                # 获取任务块的标识信息用于日志
                params = block.get('required_params', {})
                symbol = params.get('symbol', 'unknown')
                data_source = block.get('data_source', 'unknown')
                endpoint = block.get('endpoint', 'unknown')
                self.logger.debug(f"文件已存在，跳过任务块: {data_source}.{endpoint} symbol={symbol}")
                continue
            
            # 如果文件不存在，保留任务块
            filtered_blocks.append(block)
        
        return filtered_blocks
    
    def _validate_config_parameters(self, config: Dict[str, Any], 
                                  data_sources: Optional[List[str]], 
                                  methods: Optional[List[str]], 
                                  storage_types: Optional[List[str]]) -> None:
        """验证配置参数
        
        Args:
            config: 配置字典
            data_sources: 指定的数据源列表
            methods: 指定的方法列表
            storage_types: 指定的存储类型列表
            
        Raises:
            ValueError: 当参数不存在于配置中时抛出异常
        """
        # 获取所有可用的配置选项
        available_sources = []
        available_methods = set()
        available_storage_types = set()
        
        for source_name, source_config in config['data_sources'].items():
            if source_config.get('enabled', False):
                available_sources.append(source_name)
                
                for method_name, method_config in source_config.get('methods', {}).items():
                    if method_config.get('enable', False):
                        # 同时添加配置键名和endpoint作为可用方法
                        available_methods.add(method_name)  # 配置键名
                        available_methods.add(method_config.get('endpoint', ''))  # endpoint
                        available_storage_types.add(method_config.get('storage_type', ''))
        
        # 验证data_sources
        if data_sources and data_sources != ['all']:
            invalid_sources = set(data_sources) - set(available_sources)
            if invalid_sources:
                self.logger.error(f"无效的数据源: {invalid_sources}")
                self.logger.info(f"可用的数据源: {available_sources}")
                raise ValueError(f"无效的数据源: {list(invalid_sources)}。可用的数据源: {available_sources}")
        
        # 验证methods
        if methods and methods != ['all']:
            invalid_methods = set(methods) - available_methods
            if invalid_methods:
                self.logger.error(f"无效的方法: {invalid_methods}")
                self.logger.info(f"可用的方法: {sorted(list(available_methods))}")
                raise ValueError(f"无效的方法: {list(invalid_methods)}。可用的方法: {sorted(list(available_methods))}")
        
        # 验证storage_types
        if storage_types and storage_types != ['all']:
            invalid_storage_types = set(storage_types) - available_storage_types
            if invalid_storage_types:
                self.logger.error(f"无效的存储类型: {invalid_storage_types}")
                self.logger.info(f"可用的存储类型: {sorted(list(available_storage_types))}")
                raise ValueError(f"无效的存储类型: {list(invalid_storage_types)}。可用的存储类型: {sorted(list(available_storage_types))}")
        
        self.logger.info("配置参数校验通过")
    
    def _filter_config(self, config: Dict[str, Any], 
                      data_sources: Optional[List[str]], 
                      methods: Optional[List[str]], 
                      storage_types: Optional[List[str]]) -> Dict[str, Any]:
        """过滤配置
        
        Args:
            config: 原始配置
            data_sources: 指定的数据源列表
            methods: 指定的方法列表
            storage_types: 指定的存储类型列表
            
        Returns:
            Dict[str, Any]: 过滤后的配置
        """
        filtered_config = {"data_sources": {}}
        
        # 获取所有可用的数据源、方法和存储类型
        available_sources = list(config['data_sources'].keys())
        available_methods = set()
        available_storage_types = set()
        
        for source_config in config['data_sources'].values():
            if source_config.get('enabled', False):
                for method_name, method_config in source_config.get('methods', {}).items():
                    if method_config.get('enable', False):
                        # 同时添加配置键名和endpoint作为可用方法
                        available_methods.add(method_name)  # 配置键名
                        available_methods.add(method_config.get('endpoint', ''))  # endpoint
                        available_storage_types.add(method_config.get('storage_type', ''))
        
        # 验证输入参数
        if data_sources and data_sources != ['all']:
            invalid_sources = set(data_sources) - set(available_sources)
            if invalid_sources:
                raise ValueError(f"无效的数据源: {invalid_sources}")
        
        if methods and methods != ['all']:
            invalid_methods = set(methods) - available_methods
            if invalid_methods:
                raise ValueError(f"无效的方法: {invalid_methods}")
        
        if storage_types and storage_types != ['all']:
            invalid_storage_types = set(storage_types) - available_storage_types
            if invalid_storage_types:
                raise ValueError(f"无效的存储类型: {invalid_storage_types}")
        
        # 过滤数据源
        for source_name, source_config in config['data_sources'].items():
            if not source_config.get('enabled', False):
                continue
                
            if data_sources and data_sources != ['all'] and source_name not in data_sources:
                continue
            
            filtered_source = dict(source_config)
            filtered_source['methods'] = {}
            
            # 过滤方法
            for method_name, method_config in source_config.get('methods', {}).items():
                if not method_config.get('enable', False):
                    continue
                
                # 检查方法过滤 - 支持配置键名和endpoint
                if methods and methods != ['all']:
                    if method_name not in methods and method_config.get('endpoint') not in methods:
                        continue
                
                # 检查存储类型过滤
                if storage_types and storage_types != ['all'] and method_config.get('storage_type') not in storage_types:
                    continue
                
                # 验证方法与数据源的匹配
                if method_config.get('data_source') != source_name:
                    self.logger.warning(f"方法 {method_name} 的data_source与所属数据源 {source_name} 不匹配")
                    continue
                
                filtered_source['methods'][method_name] = method_config
            
            if filtered_source['methods']:  # 只有当有有效方法时才添加数据源
                filtered_config['data_sources'][source_name] = filtered_source
        
        return filtered_config
    
    def _process_placeholders(self, task_config: Dict[str, Any], truncate_mode: bool = True) -> Dict[str, Any]:
        """处理占位符
        
        Args:
            task_config: 任务配置
            truncate_mode: 是否启用截断模式，默认为True
            
        Returns:
            Dict[str, Any]: 处理后的任务配置
        """
        required_params = task_config['required_params']
        
        if isinstance(required_params, dict):
            # 直接传递整个字典给PlaceholderGenerator处理
            try:
                processed_params = self.placeholder_generator.process_params_dict(
                    params_dict=required_params,
                    start_date=task_config['start_date'],
                    end_date=task_config['end_date'],
                    lookback_periods=task_config['lookback_periods'],
                    lookback_multiplier=task_config['lookback_multiplier'],
                    truncate_mode=truncate_mode
                )
                task_config['required_params'] = processed_params
            except Exception as e:
                self.logger.error(f"处理参数字典时出错: {e}")
                # 发生错误时保持原始参数不变
        
        return task_config
    
    def generate_daily_tasks(self) -> List[DataTask]:
        """生成日常数据采集任务
        
        Returns:
            List[DataTask]: 日常任务列表
        """
        self.logger.info("生成日常数据采集任务")
        
        # 生成所有启用的任务
        task_list = self.generate_tasks()
        
        # 转换为DataTask对象列表
        data_tasks = []
        
        for source_name, source_data in task_list['data_sources'].items():
            for method_name, method_config in source_data['methods'].items():
                # 为每个方法创建任务
                task_id = f"{source_name}_{method_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                task = DataTask(
                    task_id=task_id,
                    source_name=source_name,
                    data_type=method_name,  # 使用配置键名而不是endpoint
                    endpoint=method_config.get('endpoint', method_name),  # 添加真实的endpoint
                    method='fetch_data',
                    params=method_config['required_params']
                )
                
                data_tasks.append(task)
        
        return data_tasks
    
    def generate_on_demand_task(self, source_name: str, data_type: str, 
                               method: str, params: Dict[str, Any], endpoint: str = None) -> DataTask:
        """生成按需数据采集任务
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型（用于路径生成）
            method: 方法名称
            params: 参数字典
            endpoint: API端点（如果不提供，使用data_type）
            
        Returns:
            DataTask: 数据采集任务
        """
        task_id = f"{source_name}_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return DataTask(
            task_id=task_id,
            source_name=source_name,
            data_type=data_type,
            endpoint=endpoint or data_type,
            method=method,
            params=params
        )
    
    def generate_batch_tasks(self, task_configs: List[Dict[str, Any]]) -> List[DataTask]:
        """批量生成数据采集任务
        
        Args:
            task_configs: 任务配置列表
            
        Returns:
            List[DataTask]: 数据采集任务列表
        """
        self.logger.info(f"批量生成 {len(task_configs)} 个数据采集任务")
        
        tasks = []
        for config in task_configs:
            task = self.generate_on_demand_task(
                source_name=config['source_name'],
                data_type=config['data_type'],
                method=config.get('method', 'fetch_data'),
                params=config.get('params', {})
            )
            tasks.append(task)
        
        return tasks
    

class ConfigManager:
    """配置管理器
    
    负责管理数据源配置、任务配置等各种配置信息
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.ConfigManager')
        self._config_cache = {}
        self._data_source_configs = {}
        self._task_configs = {}
        self.config_manager = None
        self._initialize_config_manager()
        
    def _initialize_config_manager(self):
        """初始化配置管理器"""
        try:
            from config_manager import get_config_manager
            self.config_manager = get_config_manager()
            self.logger.info("配置管理器初始化成功")
        except ImportError as e:
            self.logger.error(f"配置管理器导入失败: {e}")
            print("程序终止：无法导入配置管理器")
            import sys
            sys.exit(1)
        
    def load_data_source_config(self, config_path: str) -> Dict[str, Any]:
        """加载数据源配置
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            Dict[str, Any]: 数据源配置字典
        """
        try:
            if config_path in self._config_cache:
                return self._config_cache[config_path]
                
            # 这里可以根据实际需要实现配置文件加载逻辑
            # 例如从JSON、YAML或其他格式文件加载
            config = {}
            self._config_cache[config_path] = config
            return config
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {config_path}, 错误: {e}")
            raise
    
    def get_data_source_configs(self):
        """获取数据源配置信息
        
        Returns:
            dict: 数据源配置字典
        """
        try:
            # 获取所有配置
            all_config = self.config_manager.get_all_config()
            
            if 'unified_data_config' not in all_config:
                raise ValueError("配置文件中缺少unified_data_config配置")
            
            config = all_config['unified_data_config']
            if 'data_sources' not in config:
                raise ValueError("配置文件中缺少data_sources配置")
            
            return config['data_sources']
            
        except Exception as e:
            raise RuntimeError(f"获取数据源配置失败: {e}")
    
    def validate_data_source_config(self, config):
        """验证数据源配置的完整性
        
        Args:
            config (dict): 数据源配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ['name', 'source_type']
        
        for field in required_fields:
            if field not in config:
                print(f"❌ 数据源配置缺少必需字段: {field}")
                return False
        
        return True
    
    def get_data_source_config(self, source_name: str) -> Dict[str, Any]:
        """获取指定数据源的配置
        
        Args:
            source_name: 数据源名称
            
        Returns:
            Dict[str, Any]: 数据源配置
        """
        return self._data_source_configs.get(source_name, {})
    
    def set_data_source_config(self, source_name: str, config: Dict[str, Any]):
        """设置数据源配置
        
        Args:
            source_name: 数据源名称
            config: 配置字典
        """
        self._data_source_configs[source_name] = config
        self.logger.info(f"设置数据源配置: {source_name}")
    
    def get_task_config(self, task_type: str) -> Dict[str, Any]:
        """获取任务配置
        
        Args:
            task_type: 任务类型
            
        Returns:
            Dict[str, Any]: 任务配置
        """
        return self._task_configs.get(task_type, {})
    
    def set_task_config(self, task_type: str, config: Dict[str, Any]):
        """设置任务配置
        
        Args:
            task_type: 任务类型
            config: 配置字典
        """
        self._task_configs[task_type] = config
        self.logger.info(f"设置任务配置: {task_type}")
    
    def clear_cache(self):
        """清空配置缓存"""
        self._config_cache.clear()
        self.logger.info("配置缓存已清空")


class TaskExecutor:
    """任务执行器
    
    负责执行数据采集任务，调用相应的数据源获取数据
    """
    
    def __init__(self, config_manager=None):
        self.config_manager = config_manager or get_config_manager()
        self.logger = logging.getLogger(__name__ + '.TaskExecutor')
        self.data_sources: Dict[str, BaseDataSource] = {}
        self.task_status: Dict[str, DataTask] = {}
        self.registered_sources = {}
        
        # 初始化DataSourceManager
        try:
            from data_source_manager import DataSourceManager
        except ImportError:
            import sys
            from pathlib import Path
            project_root = Path(__file__).parent.parent
            sys.path.insert(0, str(project_root / 'core'))
            from data_source_manager import DataSourceManager
        
        # 获取数据源配置并传递给DataSourceManager
        try:
            data_sources_config = self.config_manager.get_all_config().get('unified_data_config', {}).get('data_sources', {})
            self.data_source_manager = DataSourceManager({'data_sources': data_sources_config})
        except Exception as e:
            self.logger.warning(f"获取数据源配置失败: {e}，使用空配置")
            self.data_source_manager = DataSourceManager()
        
        # 使用传入的ConfigManager
        self.config_mgr = ConfigManager()
        self.config_mgr.config_manager = self.config_manager
        
    def register_data_source(self, name: str, data_source: BaseDataSource):
        """注册数据源
        
        Args:
            name: 数据源名称
            data_source: 数据源实例
        """
        self.data_sources[name] = data_source
        self.logger.info(f"注册数据源: {name}")
    
    def execute_task(self, task: DataTask) -> Tuple[bool, Any]:
        """执行单个任务
        
        Args:
            task: 数据采集任务
            
        Returns:
            Tuple[bool, Any]: (是否成功, 数据或错误信息)
        """
        self.logger.info(f"开始执行任务: {task.task_id}")
        self.logger.info(f"任务详情: source_name={task.source_name}, data_type={task.data_type}, params={task.params}")
        
        try:
            # 更新任务状态为运行中
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            self.task_status[task.task_id] = task
            
            # 获取数据源实例
            self.logger.info(f"尝试获取数据源实例: {task.source_name}")
            data_source = self._get_data_source_instance(task.source_name)
            if not data_source:
                error_msg = f"未找到数据源: {task.source_name}"
                self.logger.error(f"{error_msg}")
                task.status = TaskStatus.FAILED
                task.error_message = error_msg
                task.completed_at = datetime.now()
                return False, error_msg
            
            self.logger.info(f"成功获取数据源实例: {type(data_source).__name__}")
            
            # 连接数据源
            self.logger.info(f"检查数据源连接状态")
            if not data_source.is_connected():
                self.logger.info(f"数据源未连接，尝试连接")
                if not data_source.connect():
                    error_msg = f"数据源连接失败: {task.source_name}"
                    self.logger.error(f"{error_msg}")
                    task.status = TaskStatus.FAILED
                    task.error_message = error_msg
                    task.completed_at = datetime.now()
                    return False, error_msg
                self.logger.info(f"数据源连接成功")
            else:
                self.logger.info(f"数据源已连接")
            
            # 确保task.params是字典类型
            if not isinstance(task.params, dict):
                self.logger.warning(f"task.params不是字典类型: {type(task.params)}, 值: {task.params}，转换为空字典")
                task.params = {}
            
            # 处理tushare数据源的limitmax配置
            if task.source_name.lower() == 'tushare':
                self.logger.info(f"处理tushare数据源的limitmax配置")
                limitmax_config = self._load_tushare_limitmax_config()
                if limitmax_config and task.endpoint in limitmax_config.get('endpoint_limits', {}):  # 使用endpoint
                    endpoint_config = limitmax_config['endpoint_limits'][task.endpoint]
                    task.params['limitmax'] = endpoint_config['limitmax']
                    self.logger.info(f"为tushare任务 {task.endpoint} 设置limitmax: {endpoint_config['limitmax']}")
            
            # 处理参数：将列表参数转换为字符串（针对单值列表）
            processed_params = {}
            for key, value in task.params.items():
                if isinstance(value, list) and len(value) == 1:
                    # 单值列表转换为字符串
                    processed_params[key] = str(value[0])
                    self.logger.info(f"参数 {key} 从列表 {value} 转换为字符串 {processed_params[key]}")
                elif isinstance(value, list) and len(value) > 1:
                    # 多值列表保持不变，但记录警告
                    processed_params[key] = value
                    self.logger.warning(f"参数 {key} 是多值列表 {value}，保持原样传递")
                else:
                    processed_params[key] = value
            
            # 执行数据获取
            self.logger.info(f"开始执行数据获取: data_type={task.data_type}, endpoint={task.endpoint}, params={processed_params}")
            data = data_source.fetch_data(task.endpoint, **processed_params)  # 使用endpoint而不是data_type
            self.logger.info(f"数据获取完成，结果类型: {type(data)}, 是否为空: {data is None or (hasattr(data, 'empty') and data.empty)}")
            if data is not None and hasattr(data, 'shape'):
                self.logger.info(f"数据形状: {data.shape}")
            
            # 如果是tushare数据源且limitmax有更新，保存配置
            if task.source_name.lower() == 'tushare' and hasattr(data_source, 'limitmax'):
                original_limitmax = task.params.get('limitmax', 3000)
                updated_limitmax = data_source.limitmax
                if updated_limitmax > original_limitmax:
                    self._update_tushare_limitmax_config(task.endpoint, updated_limitmax)  # 使用endpoint
                    self.logger.info(f"更新tushare limitmax配置: {task.endpoint} {original_limitmax} -> {updated_limitmax}")
            
            if data is None or (hasattr(data, 'empty') and data.empty):
                error_msg = f"获取到空数据: {task.source_name}.{task.data_type}"
                self.logger.warning(error_msg)
                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.now()
                return True, data  # 空数据也算成功
            
            # 任务执行成功
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            self.logger.info(f"任务执行成功: {task.task_id}, 数据行数: {len(data) if hasattr(data, '__len__') else 'N/A'}")
            
            return True, data
            
        except Exception as e:
            import traceback
            error_msg = f"任务执行失败: {task.task_id}, 错误: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(f"详细错误堆栈: {traceback.format_exc()}")
            
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            task.completed_at = datetime.now()
            task.retry_count += 1
            
            return False, str(e)
    
    def execute_task_from_config(self, task_config: Dict[str, Any]) -> Tuple[bool, Any]:
        """从任务配置执行任务
        
        Args:
            task_config: 任务配置字典，包含data_source, endpoint, required_params等
            
        Returns:
            Tuple[bool, Any]: (是否成功, 数据或错误信息)
        """
        self.logger.info(f"[DEBUG] 从配置执行任务: {task_config.get('data_source', 'unknown')}.{task_config.get('endpoint', 'unknown')}")
        self.logger.info(f"[DEBUG] 任务配置: {task_config}")
        
        # 从配置创建DataTask
        # 优先使用配置键名，如果没有则报错
        config_key = task_config.get('config_key', task_config.get('endpoint', 'unknown'))
        if config_key == 'unknown':
            raise ValueError(f"任务配置缺少config_key或endpoint: {task_config}")
        task_id = f"{task_config['data_source']}_{config_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        task = DataTask(
            task_id=task_id,
            source_name=task_config['data_source'],
            data_type=config_key,  # 使用配置键名而不是endpoint
            endpoint=task_config.get('endpoint', config_key),  # 添加真实的endpoint
            method='fetch_data',
            params=task_config.get('required_params', {})
        )
        
        self.logger.info(f"[DEBUG] 创建的DataTask: task_id={task.task_id}, source_name={task.source_name}, data_type={task.data_type}, params={task.params}")
        
        return self.execute_task(task)
    
    def _get_data_source_instance(self, source_name: str) -> Optional[BaseDataSource]:
        """获取数据源实例
        
        Args:
            source_name: 数据源名称
            
        Returns:
            Optional[BaseDataSource]: 数据源实例
        """
        # 首先检查已注册的数据源
        if source_name in self.data_sources:
            return self.data_sources[source_name]
        
        # 尝试从DataSourceManager获取
        try:
            instance = self.data_source_manager.get_instance(source_name)
            if instance:
                self.data_sources[source_name] = instance  # 缓存实例
                return instance
        except Exception as e:
            self.logger.error(f"从DataSourceManager获取数据源失败: {source_name}, 错误: {e}")
        
        return None
    
    def update_task_status(self, task_id: str, status: TaskStatus, error_message: str = None) -> bool:
        """更新任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            error_message: 错误信息（可选）
            
        Returns:
            bool: 是否更新成功
        """
        if task_id in self.task_status:
            task = self.task_status[task_id]
            task.status = status
            if error_message:
                task.error_message = error_message
            if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.completed_at = datetime.now()
            self.logger.info(f"任务状态更新: {task_id} -> {status.value}")
            return True
        else:
            self.logger.warning(f"未找到任务: {task_id}")
            return False
    
    def get_task_status(self, task_id: str) -> Optional[DataTask]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            Optional[DataTask]: 任务对象
        """
        return self.task_status.get(task_id)
    
    def list_registered_sources(self) -> List[str]:
        """列出已注册的数据源
        
        Returns:
            List[str]: 数据源名称列表
        """
        return list(self.data_sources.keys())
    
    def get_source_info(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取数据源信息
        
        Args:
            source_name: 数据源名称
            
        Returns:
            Optional[Dict[str, Any]]: 数据源信息
        """
        data_source = self._get_data_source_instance(source_name)
        if data_source:
            return data_source.get_source_info()
        return None
    
    def setup_data_sources(self):
        """从配置文件设置数据源配置（按需注册）
        
        Returns:
            dict: 可用的数据源配置字典
        """
        try:
            # 从ConfigManager获取数据源配置
            print("📋 正在从配置文件加载数据源配置...")
            raw_configs = self.config_mgr.get_data_source_configs()
            
            # 导入DataSourceConfig类
            try:
                from base_data_source import DataSourceConfig
            except ImportError:
                # 创建简单的替代类
                class DataSourceConfig:
                    def __init__(self, name, source_type, connection_params):
                        self.name = name
                        self.source_type = source_type
                        self.connection_params = connection_params
            
            # 构建数据源配置对象
            data_source_configs = {}
            enabled_sources = 0
            
            for source_name, source_config in raw_configs.items():
                if source_config.get('enabled', False):
                    try:
                        data_source_configs[source_name] = DataSourceConfig(
                            name=source_config['name'],
                            source_type=source_config['source_type'],
                            connection_params=source_config.get('connection_params', {})
                        )
                        enabled_sources += 1
                        print(f"✅ 从配置文件加载数据源: {source_name}")
                    except KeyError as e:
                        print(f"❌ 数据源 {source_name} 配置不完整，缺少字段: {e}")
                        raise ValueError(f"数据源配置格式错误: {e}")
            
            if enabled_sources == 0:
                raise ValueError("没有启用的数据源")
            
            print(f"✅ 成功加载 {enabled_sources} 个数据源配置")
            self.available_configs = data_source_configs
            return data_source_configs
            
        except Exception as e:
            print(f"❌ 从配置文件加载数据源失败: {e}")
            raise RuntimeError(f"数据源配置加载失败: {e}")
    
    def register_data_source_on_demand(self, source_name):
        """按需注册数据源
        
        Args:
            source_name (str): 数据源名称
            
        Returns:
            bool: 注册是否成功
        """
        # 如果已经注册，直接返回成功
        if source_name in self.registered_sources:
            return True
        
        # 如果没有可用配置，先加载配置
        if not hasattr(self, 'available_configs') or not self.available_configs:
            self.setup_data_sources()
        
        # 检查配置是否存在
        if source_name not in self.available_configs:
            print(f"❌ 数据源 {source_name} 不在可用配置中")
            return False
        
        try:
            config = self.available_configs[source_name]
            print(f"🔧 按需注册数据源: {source_name}")
            
            # 创建数据源实例
            data_source = self._create_data_source_instance(config)
            if data_source is None:
                return False
            
            # 注册到TaskExecutor
            self.register_data_source(config.name, data_source)
            self.registered_sources[config.name] = data_source
            print(f"✅ 成功注册数据源: {config.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ 注册数据源 {source_name} 失败: {e}")
            return False
    
    def _create_data_source_instance(self, config):
        """根据配置创建数据源实例
        
        Args:
            config: 数据源配置对象
            
        Returns:
            数据源实例或None
        """
        try:
            if config.source_type == 'tushare':
                from tushare_source import TushareDataSource
                return TushareDataSource(config)
            elif config.source_type == 'akshare':
                try:
                    from akshare_source import AkshareDataSource
                except ImportError:
                    # 尝试从sources模块导入
                    from sources.akshare_source import AkshareDataSource
                return AkshareDataSource(config)
            else:
                print(f"❌ 不支持的数据源类型: {config.source_type}")
                print("程序终止：数据源类型不支持")
                import sys
                sys.exit(1)
                
        except ImportError as e:
            print(f"❌ 导入数据源类失败: {e}")
            print(f"程序终止：无法导入 {config.source_type} 数据源")
            import sys
            sys.exit(1)
        except Exception as e:
            print(f"❌ 创建数据源实例失败: {e}")
            print(f"程序终止：{config.name} 数据源实例化失败")
            import sys
            sys.exit(1)
    
    def get_registered_sources_dict(self):
        """获取已注册的数据源字典
        
        Returns:
            dict: 已注册的数据源字典
        """
        return self.registered_sources
    
    def is_data_source_registered(self, source_name):
        """检查数据源是否已注册
        
        Args:
            source_name (str): 数据源名称
            
        Returns:
            bool: 是否已注册
        """
        return source_name in self.registered_sources
    
    def _load_tushare_limitmax_config(self) -> Optional[Dict[str, Any]]:
        """加载tushare limitmax配置文件
        
        Returns:
            Optional[Dict[str, Any]]: 配置字典，如果加载失败返回None
        """
        config_path = Path(__file__).parent.parent.parent.parent.parent.parent / 'data' / 'config' / 'tushare_limitmax_config.json'
        try:
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                self.logger.warning(f"tushare limitmax配置文件不存在: {config_path}")
                return None
        except Exception as e:
            self.logger.error(f"加载tushare limitmax配置失败: {e}")
            return None
    
    def _update_tushare_limitmax_config(self, endpoint: str, new_limitmax: int) -> bool:
        """更新tushare limitmax配置文件
        
        Args:
            endpoint: 端点名称
            new_limitmax: 新的limitmax值
            
        Returns:
            bool: 是否更新成功
        """
        config_path = Path(__file__).parent.parent.parent.parent.parent.parent / 'data' / 'config' / 'tushare_limitmax_config.json'
        try:
            # 加载现有配置
            config = self._load_tushare_limitmax_config()
            if config is None:
                # 如果配置文件不存在，创建新的配置结构
                config = {"endpoint_limits": {}}
            
            # 更新配置
            if 'endpoint_limits' not in config:
                config['endpoint_limits'] = {}
            
            config['endpoint_limits'][endpoint] = {
                'limitmax': new_limitmax,
                'updated_at': datetime.now().isoformat()
            }
            
            # 保存配置
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"成功更新tushare limitmax配置: {endpoint} = {new_limitmax}")
            return True
            
        except Exception as e:
            self.logger.error(f"更新tushare limitmax配置失败: {e}")
            return False


class DataSaver:
    """数据保存器 - 简化版本，专注于基本保存功能
    
    负责将采集到的数据保存到指定位置，支持多种存储格式
    使用归档器进行MD5检查和归档功能
    """
    
    def __init__(self, config_manager=None):
        self.config_manager = config_manager or get_config_manager()
        self.logger = logging.getLogger(__name__ + '.DataSaver')
        
        # 初始化路径生成器（使用ConfigManager加载配置）
        try:
            from ..core.path_generator import ConfigDTO, ArchiveTypeConfig, FileConfig, PathsConfig
        except ImportError:
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent / 'core'))
            from path_generator import ConfigDTO, ArchiveTypeConfig, FileConfig, PathsConfig
        
        # 从ConfigManager获取路径生成器配置
        path_config = config_manager.get_section('path_generator_config')['path_generator']
        
        # 转换为ConfigDTO格式
        archive_types = {}
        for name, config in path_config['archive_types'].items():
            archive_types[name] = ArchiveTypeConfig(
                value=config['value'],
                description=config.get('description', ''),
                path_pattern=config['path_pattern'],
                enabled=config.get('enabled', True),
                validation_rules=config.get('validation_rules', {})
            )
        
        config_dto = ConfigDTO(
            base_path=path_config['base_path'],
            archive_types=archive_types,
            file_config=FileConfig(
                filename_template=path_config['file_config']['filename_template'],
                supported_formats=path_config['file_config']['supported_formats'],
                default_format=path_config['file_config']['default_format']
            ),
            paths=PathsConfig(
                landing_subpath=path_config['paths']['landing_subpath'],
                archive_subpath=path_config['paths']['archive_subpath']
            )
        )
        
        self.path_generator = PathGenerator(config_dto)
        
        # 初始化归档器（用于MD5检查和归档功能）
        from pathlib import Path as PathLib
        self.archiver = UniversalArchiver(base_path=PathLib.cwd())
        

    def save_data_auto(self, data: pd.DataFrame, source_name: str, data_type: str, 
                      method: str = 'fetch_data', task_id: Optional[str] = None, 
                      api_params: Optional[Dict[str, Any]] = None, 
                      file_format: str = 'parquet',
                      verification_callback: Optional[Callable[[], pd.DataFrame]] = None,
                      predefined_paths: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """自动化数据保存接口 - 最简单易用的保存方法
        
        接收TaskExecutor的输出数据和基本信息，自动处理：
        1. 路径生成（基于source_name, data_type和api_params）
        2. 数据保存和MD5验证
        3. 文件归档（如果数据有变化）
        4. 返回详细的执行结果
        
        Args:
            data: 要保存的DataFrame数据
            source_name: 数据源名称（如'tushare', 'akshare'）
            data_type: 数据类型（如'stock_basic', 'daily'）
            method: API方法名（默认'fetch_data'）
            task_id: 任务ID（可选，自动生成）
            api_params: API参数（用于路径生成和元数据）
            file_format: 文件格式（默认'parquet'）
            
        Returns:
            Dict[str, Any]: 执行结果，包含：
            - success: bool - 是否成功
            - save_method: str - 保存方式（'new', 'update', 'skip'）
            - file_path: str - 保存的文件路径
            - archive_path: str - 归档路径（如果有归档）
            - message: str - 详细信息
            - data_info: dict - 数据信息（行数、列数、MD5等）
            - errors: list - 错误信息（如果有）
        """
        try:
            # 1. 参数预处理和验证
            if data is None or data.empty:
                return {
                    'success': False,
                    'save_method': 'skip',
                    'file_path': None,
                    'archive_path': None,
                    'message': 'DataFrame为空，跳过保存',
                    'data_info': {'rows': 0, 'columns': 0},
                    'errors': ['数据为空']
                }
            
            # 生成task_id（如果未提供）
            if not task_id:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                task_id = f"{source_name}_{data_type}_{timestamp}"
            
            # 处理api_params
            if api_params is None:
                api_params = {}
            
            # 2. 确定archive_type（实际上就是storage_type）
            archive_type = api_params.get('storage_type', api_params.get('archive_type', ''))
            # 确保archive_type在api_params中，供路径生成器使用
            if 'archive_type' not in api_params:
                api_params['archive_type'] = archive_type
            
            # 3. 获取文件路径和归档路径
            try:
                # 检查预定义路径是否有效（不为None且不为空）
                use_predefined = (predefined_paths and 
                                'landing_dir' in predefined_paths and 
                                'archive_dir' in predefined_paths and
                                predefined_paths['landing_dir'] is not None and
                                predefined_paths['archive_dir'] is not None and
                                predefined_paths['landing_dir'].strip() != '' and
                                predefined_paths['archive_dir'].strip() != '')
                
                if use_predefined:
                    # 使用预定义的路径信息，跳过路径生成
                    landing_dir = predefined_paths['landing_dir']
                    archive_dir = predefined_paths['archive_dir']
                    
                    file_path = Path(landing_dir) / f'data.{file_format}'
                    archive_path = Path(archive_dir) / f'data.{file_format}'
                    self.logger.info(f"使用预定义路径: landing={file_path}, archive={archive_path}")
                else:
                    # 使用PathGenerator生成路径
                    self.logger.info(f"预定义路径无效，使用PathGenerator生成路径")
                    path_result = self.path_generator.generate_paths(
                        source_name=source_name,
                        data_type=data_type,
                        **api_params
                    )
                    
                    if not path_result.get('success', False):
                        raise ValueError(f"路径生成失败: {path_result.get('errors', ['未知错误'])}")
                    
                    # 从返回结果中获取文件路径和归档路径
                    file_paths = path_result.get('file_paths', {})
                    if file_format not in file_paths:
                        raise ValueError(f"不支持的文件格式: {file_format}")
                    
                    file_path = Path(file_paths[file_format]['landing_path'])
                    archive_path = Path(file_paths[file_format]['archive_path'])
                
            except Exception as e:
                return {
                    'success': False,
                    'save_method': 'new',
                    'file_path': None,
                    'archive_path': None,
                    'message': f'路径处理失败: {str(e)}',
                    'data_info': {'rows': len(data), 'columns': len(data.columns)},
                    'errors': [f'路径处理错误: {str(e)}']
                }
            
            # 4. 确保目录存在
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 5. 准备JSON元数据信息
            # 从api_params中提取实际传递给数据接口的参数（排除程序内部参数）
            interface_params = {k: v for k, v in api_params.items() 
                              if k not in ['archive_type', 'storage_type']}
            
            json_info = {
                'source_name': source_name,
                'data_type': data_type,
                'task_id': task_id,
                'api_params': interface_params,
                'file_format': file_format,
                'archive_type': archive_type
            }
            
            # 6. 使用简化的归档器接口进行保存
            if file_format == 'parquet':
                result = self.archiver.archive_data_simple(
                    data=data,
                    json_info=json_info,
                    save_path=str(file_path),
                    archive_path=str(archive_path),
                    verification_callback=verification_callback
                )
                
                # 处理归档器结果
                if result['save_status'] == 'success':
                    save_method = result['save_method']
                    message = result['message']
                    
                    # 获取实际的归档路径（如果有的话）
                    actual_archive_path = result.get('actual_archived_path')
                    
                    # 记录日志
                    if save_method == 'skip':
                        self.logger.info(f"数据未变化，跳过保存: {file_path} - {message}")
                    elif save_method == 'new':
                        self.logger.info(f"新数据保存成功: {file_path} - {message}")
                    elif save_method == 'update':
                        self.logger.info(f"数据更新成功: {file_path} - {message}")
                        if actual_archive_path:
                            self.logger.info(f"原数据已归档至: {actual_archive_path}")
                    
                    # 从json_info中获取previous信息
                    json_info = result.get('json_info', {})
                    previous_info = json_info.get('previous', {})
                    
                    return {
                        'success': True,
                        'save_method': save_method,
                        'file_path': str(file_path),
                        'archive_path': actual_archive_path if save_method == 'update' else None,
                        'message': message,
                        'data_info': {
                            'rows': len(data),
                            'columns': len(data.columns),
                            'md5': json_info.get('data_md5', ''),
                            'shape': json_info.get('data_shape', [len(data), len(data.columns)]),
                            'original_rows': previous_info.get('rows') if previous_info else None
                        },
                        'archive_info': {
                            'previous_md5': previous_info.get('md5') if previous_info else None
                        },
                        'errors': []
                    }
                else:
                    return {
                        'success': False,
                        'save_method': 'new',
                        'file_path': str(file_path),
                        'archive_path': None,
                        'message': result['message'],
                        'data_info': {'rows': len(data), 'columns': len(data.columns)},
                        'errors': [result['message']]
                    }
            else:
                # 对于非parquet格式，暂不支持
                return {
                    'success': False,
                    'save_method': 'failed',
                    'file_path': None,
                    'archive_path': None,
                    'message': f'暂不支持{file_format}格式，请使用parquet格式',
                    'data_info': {'rows': len(data), 'columns': len(data.columns)},
                    'errors': [f'不支持的文件格式: {file_format}']
                }
                
        except Exception as e:
            self.logger.error(f"自动化数据保存异常: {e}")
            return {
                'success': False,
                'save_method': 'failed',
                'file_path': None,
                'archive_path': None,
                'message': f'保存异常: {str(e)}',
                'data_info': {'rows': len(data) if data is not None else 0, 'columns': len(data.columns) if data is not None else 0},
                'errors': [f'异常: {str(e)}']
            }


class RawDataService:
    """原始数据服务类
    
    整合TaskGenerator、TaskExecutor和DataSaver的功能，
    提供完整的数据采集流水线：输入参数 -> 生成任务 -> 执行任务 -> 保存数据
    """
    
    def __init__(self, config_manager=None, force_update=False):
        """初始化RawDataService
        
        Args:
            config_manager: 配置管理器实例
            force_update: 强制更新标志，传递给TaskGenerator
        """
        self.config_manager = config_manager or get_config_manager()
        self.logger = logging.getLogger(__name__ + '.RawDataService')
        
        # 初始化三个核心组件
        self.task_generator = TaskGenerator(config_manager=self.config_manager, force_update=force_update)
        self.task_executor = TaskExecutor(config_manager=self.config_manager)
        self.data_saver = DataSaver(config_manager=self.config_manager)
        
        # 初始化数据库记录管理器
        try:
            self.task_record_manager = TaskRecordManager(config_manager=self.config_manager)
            self.task_record_manager.create_table_if_not_exists()
            self.logger.info("数据库记录管理器初始化完成")
        except Exception as e:
            self.logger.warning(f"数据库记录管理器初始化失败: {e}，将跳过数据库记录功能")
            self.task_record_manager = None
        
        self.logger.info("RawDataService初始化完成")
    
    def generate_tasks_with_validation(self, data_sources: List[str], methods: List[str], 
                                     storage_types: List[str], start_date: str, end_date: str,
                                     **kwargs) -> Dict[str, Any]:
        """生成并验证任务块
        
        Args:
            data_sources: 数据源列表，如['tushare', 'akshare']
            methods: 方法列表，如['stock_basic', 'daily']
            storage_types: 存储类型列表，如['SNAPSHOT', 'PERIOD']
            start_date: 开始日期，格式'YYYYMMDD'
            end_date: 结束日期，格式'YYYYMMDD'
            **kwargs: 其他参数
            
        Returns:
            Dict[str, Any]: 包含任务列表和任务块的结果
        """
        return self.task_generator.generate_tasks_with_validation(
            data_sources=data_sources,
            methods=methods,
            storage_types=storage_types,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
    
    def execute_data_pipeline(self, data_sources: List[str], methods: List[str],
                            storage_types: List[str], start_date: str, end_date: str,
                            **kwargs) -> Dict[str, Any]:
        """执行完整的数据采集流水线
        
        Args:
            data_sources: 数据源列表
            methods: 方法列表
            storage_types: 存储类型列表
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数
            
        Returns:
            Dict[str, Any]: 执行结果，包含成功/失败任务数和详细结果
        """
        self.logger.info(f"开始执行数据流水线: sources={data_sources}, methods={methods}, dates={start_date}-{end_date}")
        
        try:
            # 1. 生成任务块
            task_generation_result = self.generate_tasks_with_validation(
                data_sources=data_sources,
                methods=methods,
                storage_types=storage_types,
                start_date=start_date,
                end_date=end_date,
                **kwargs
            )
            
            if not task_generation_result.get('success', False):
                return {
                    'success': False,
                    'successful_tasks': 0,
                    'failed_tasks': 0,
                    'task_results': [],
                    'errors': [f"任务生成失败: {task_generation_result.get('message', '未知错误')}"]
                }
            
            task_blocks = task_generation_result.get('task_blocks', [])
            if not task_blocks:
                return {
                    'success': True,
                    'successful_tasks': 0,
                    'failed_tasks': 0,
                    'task_results': [],
                    'message': '没有生成任何任务块'
                }
            
            self.logger.info(f"生成了 {len(task_blocks)} 个任务块")
            
            # 应用max_tasks限制（如果指定）
            max_tasks = kwargs.get('max_tasks')
            if max_tasks is not None and max_tasks > 0 and len(task_blocks) > max_tasks:
                original_count = len(task_blocks)
                task_blocks = task_blocks[:max_tasks]
                self.logger.info(f"应用任务数上限 {max_tasks}，从 {original_count} 个任务块减少到 {len(task_blocks)} 个")
            
            # 2. 执行任务块并保存数据
            successful_tasks = 0
            failed_tasks = 0
            task_results = []
            errors = []
            
            for i, task_block in enumerate(task_blocks):
                self.logger.info(f"执行任务块 {i+1}/{len(task_blocks)}: {task_block.get('data_source', 'unknown')}.{task_block.get('endpoint', 'unknown')}")
                
                try:
                    # 记录任务开始时间
                    start_time = datetime.now()
                    
                    # 执行任务
                    success, data = self.task_executor.execute_task_from_config(task_block)
                    
                    task_result = {
                        'task_index': i + 1,
                        'data_source': task_block.get('data_source', 'unknown'),
                        'endpoint': task_block.get('endpoint', 'unknown'),
                        'method_name': f"{task_block.get('data_source', 'unknown')}.{task_block.get('endpoint', 'unknown')}",
                        'success': success,
                        'params': task_block.get('required_params', {}),
                        'start_time': start_time
                    }
                    
                    if success and data is not None:
                        # 准备api_params，包含required_params和archive_type
                        api_params = task_block.get('required_params', {}).copy()
                        if 'storage_type' not in api_params and 'storage_type' in task_block:
                            api_params['storage_type'] = task_block['storage_type']
                        
                        # 创建二次验证回调函数
                        def verification_callback():
                            """二次验证回调：当新数据行数小于原数据时重新请求数据"""
                            self.logger.info(f"触发二次验证：重新请求 {task_block.get('data_source', 'unknown')}.{task_block.get('endpoint', 'unknown')} 数据")
                            try:
                                # 重新执行任务获取数据
                                verification_success, verification_data = self.execute_task_block(task_block)
                                if verification_success and verification_data is not None:
                                    self.logger.info(f"二次验证成功，获取到 {len(verification_data)} 行数据")
                                    return verification_data
                                else:
                                    self.logger.warning(f"二次验证失败，返回原数据")
                                    return data
                            except Exception as e:
                                self.logger.error(f"二次验证异常: {e}")
                                return data
                        
                        # 保存数据（带二次验证回调）
                        # 从任务块中提取预定义的路径信息
                        predefined_paths = None
                        if 'landing_dir' in task_block and 'archive_dir' in task_block:
                            predefined_paths = {
                                'landing_dir': task_block.get('landing_dir'),
                                'archive_dir': task_block.get('archive_dir')
                            }
                            self.logger.info(f"使用任务块中的预定义路径: {predefined_paths}")
                        
                        save_result = self.save_data_auto(
                            data=data,
                            source_name=task_block.get('data_source', 'unknown'),
                            data_type=task_block.get('config_key', task_block.get('endpoint', 'unknown')),
                            api_params=api_params,
                            verification_callback=verification_callback,
                            predefined_paths=predefined_paths
                        )
                        
                        task_result.update({
                            'data_rows': len(data) if hasattr(data, '__len__') else 0,
                            'save_result': save_result
                        })
                        
                        if save_result.get('success', False):
                            successful_tasks += 1
                            self.logger.info(f"任务块 {i+1} 执行并保存成功")
                            
                            # 根据save_method确定execution_status
                            save_method = save_result.get('save_method', 'unknown')
                            if save_method == 'skip':
                                execution_status = 'SKIPPED'
                            else:
                                execution_status = 'SUCCESS'
                            
                            # 记录任务到数据库
                            self._record_task_to_database(
                                task_block=task_block,
                                task_result=task_result,
                                save_result=save_result,
                                execution_status=execution_status,
                                start_time=task_result.get('start_time'),
                                end_time=datetime.now()
                            )
                        else:
                            failed_tasks += 1
                            error_msg = f"任务块 {i+1} 数据保存失败: {save_result.get('message', '未知错误')}"
                            errors.append(error_msg)
                            task_result['error'] = error_msg
                            
                            # 记录失败任务到数据库
                            self._record_task_to_database(
                                task_block=task_block,
                                task_result=task_result,
                                save_result=save_result,
                                execution_status='FAILED',
                                error_message=error_msg,
                                start_time=task_result.get('start_time'),
                                end_time=datetime.now()
                            )
                    else:
                        failed_tasks += 1
                        error_msg = f"任务块 {i+1} 执行失败: {data if isinstance(data, str) else '数据获取失败'}"
                        errors.append(error_msg)
                        task_result['error'] = error_msg
                        
                        # 记录执行失败任务到数据库
                        self._record_task_to_database(
                            task_block=task_block,
                            task_result=task_result,
                            save_result=None,
                            execution_status='FAILED',
                            error_message=error_msg,
                            start_time=start_time,
                            end_time=datetime.now()
                        )
                    
                    task_results.append(task_result)
                    
                except Exception as e:
                    failed_tasks += 1
                    error_msg = f"任务块 {i+1} 执行异常: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg, exc_info=True)
                    
                    exception_task_result = {
                        'task_index': i + 1,
                        'data_source': task_block.get('data_source', 'unknown'),
                        'endpoint': task_block.get('endpoint', 'unknown'),
                        'method_name': f"{task_block.get('data_source', 'unknown')}.{task_block.get('endpoint', 'unknown')}",
                        'success': False,
                        'error': error_msg,
                        'start_time': locals().get('start_time', datetime.now())
                    }
                    
                    # 记录异常任务到数据库
                    self._record_task_to_database(
                        task_block=task_block,
                        task_result=exception_task_result,
                        save_result=None,
                        execution_status='FAILED',
                        error_message=error_msg,
                        start_time=exception_task_result['start_time'],
                        end_time=datetime.now()
                    )
                    
                    task_results.append(exception_task_result)
            
            # 3. 返回执行结果
            result = {
                'success': successful_tasks > 0,
                'successful_tasks': successful_tasks,
                'failed_tasks': failed_tasks,
                'task_results': task_results,
                'total_tasks': len(task_blocks)
            }
            
            if errors:
                result['errors'] = errors
            
            self.logger.info(f"数据流水线执行完成: 成功 {successful_tasks}, 失败 {failed_tasks}")
            return result
            
        except Exception as e:
            error_msg = f"数据流水线执行异常: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'successful_tasks': 0,
                'failed_tasks': 0,
                'task_results': [],
                'errors': [error_msg]
            }
    
    def save_data_auto(self, data, source_name: str, data_type: str, 
                      method: str = 'fetch_data', task_id: str = None,
                      api_params: Dict[str, Any] = None, file_format: str = 'parquet',
                      verification_callback: Optional[Callable[[], pd.DataFrame]] = None,
                      predefined_paths: Dict[str, str] = None) -> Dict[str, Any]:
        """自动保存数据
        
        Args:
            data: 要保存的数据
            source_name: 数据源名称
            data_type: 数据类型
            method: 方法名
            task_id: 任务ID
            api_params: API参数
            file_format: 文件格式
            
        Returns:
            Dict[str, Any]: 保存结果
        """
        return self.data_saver.save_data_auto(
            data=data,
            source_name=source_name,
            data_type=data_type,
            method=method,
            task_id=task_id,
            api_params=api_params or {},
            file_format=file_format,
            verification_callback=verification_callback,
            predefined_paths=predefined_paths
        )
    
    def collect_data(self, data_sources: List[str], methods: List[str],
                    storage_types: List[str], start_date: str, end_date: str,
                    **kwargs) -> Dict[str, Any]:
        """数据采集主入口方法
        
        这是最终用户调用的主要方法，实现：输入参数 -> 完成采集任务
        
        Args:
            data_sources: 数据源列表
            methods: 方法列表
            storage_types: 存储类型列表
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数
            
        Returns:
            Dict[str, Any]: 采集结果
        """
        self.logger.info(f"开始数据采集任务: {data_sources} -> {methods} ({start_date} to {end_date})")
        
        # 直接调用execute_data_pipeline，它已经包含了完整的流程
        result = self.execute_data_pipeline(
            data_sources=data_sources,
            methods=methods,
            storage_types=storage_types,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        self.logger.info(f"数据采集任务完成: 成功 {result.get('successful_tasks', 0)}, 失败 {result.get('failed_tasks', 0)}")
        return result
    
    def _record_task_to_database(self, task_block: Dict[str, Any], task_result: Dict[str, Any], 
                                save_result: Optional[Dict[str, Any]], execution_status: str,
                                error_message: Optional[str] = None, 
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> None:
        """记录任务执行结果到数据库
        
        Args:
            task_block: 任务块配置
            task_result: 任务执行结果
            save_result: 数据保存结果
            execution_status: 执行状态 ('SUCCESS', 'FAILED', 'PARTIAL')
            error_message: 错误信息
            start_time: 开始时间
            end_time: 结束时间
        """
        if not self.task_record_manager:
            return
            
        try:
            # 生成任务ID
            task_id = f"{task_block.get('data_source', 'unknown')}_{task_block.get('endpoint', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 计算执行时长
            duration_ms = None
            if start_time and end_time:
                duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # 获取数据信息
            data_rows = task_result.get('data_rows', 0)
            data_md5 = None
            previous_md5 = None
            original_rows = None
            row_difference = None
            original_filename = None
            file_path = None
            archive_path = None
            
            if save_result:
                data_info = save_result.get('data_info', {})
                data_md5 = data_info.get('md5')
                original_rows = data_info.get('original_rows')
                if data_rows and original_rows:
                    row_difference = data_rows - original_rows
                
                # 获取文件路径信息
                file_path = save_result.get('file_path')
                archive_path = save_result.get('archive_path')
                
                # 获取归档文件路径作为原数据文件名（用于结合archive_path快速查找被归档的文件）
                if archive_path:
                    original_filename = Path(archive_path).name
                elif file_path:
                    # 如果没有归档路径，则使用文件路径作为备选
                    original_filename = Path(file_path).name
                    
                # 获取previous_md5（如果有归档信息）
                archive_info = save_result.get('archive_info', {})
                if isinstance(archive_info, dict):
                    previous_md5 = archive_info.get('previous_md5')
            
            # 插入数据库记录
            record_id = self.task_record_manager.insert_task_record(
                task_id=task_id,
                source_name=task_block.get('data_source', 'unknown'),
                data_type=task_block.get('config_key', task_block.get('endpoint', 'unknown')),
                data_md5=data_md5,
                previous_md5=previous_md5,
                duration_ms=duration_ms,
                data_rows=data_rows,
                original_rows=original_rows,
                row_difference=row_difference,
                error_message=error_message,
                api_params=task_block.get('required_params', {}),
                original_filename=original_filename,
                file_path=file_path,
                archive_path=archive_path,
                execution_status=execution_status
            )
            
            self.logger.info(f"任务执行记录已写入数据库，记录ID: {record_id}")
            
        except Exception as e:
            self.logger.error(f"写入数据库记录失败: {e}", exc_info=True)
 
    