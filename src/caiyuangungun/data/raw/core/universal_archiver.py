#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
universal数据归档器
支持结构化配置参数和完整的元数据记录
"""

import json
import hashlib
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field

# 检查pandas可用性
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None


@dataclass
class ArchiveConfig:
    """归档配置参数"""
    source_name: str  # 数据源名称，如"tushare"
    data_type: str    # 数据类型，如"stock_basic"
    archive_type: str # 归档类型，如"SNAPSHOT", "DAILY", "MONTHLY"
    date_param: Optional[str] = None  # 日期参数
    method: Optional[str] = None      # API方法名
    required_params: list = field(default_factory=list)  # 必需参数列表
    
    def __post_init__(self):
        """参数验证"""
        if not self.source_name:
            raise ValueError("source_name不能为空")
        if not self.data_type:
            raise ValueError("data_type不能为空")
        if not self.archive_type:
            raise ValueError("archive_type不能为空")

@dataclass
class PathInfo:
    """路径信息"""
    landing_path: str    # 落地路径
    archive_path: str    # 归档路径
    data_filename: str   # 数据文件名
    config_filename: str # 配置文件名


class UniversalArchiver:
    """Pandas数据归档器
    
    专门用于pandas DataFrame的归档和管理，支持：
    - parquet格式保存
    - 完整的元数据记录
    - 数据一致性验证
    - 自动归档管理
    """
    
    def __init__(self, base_path: Union[str, Path]):
        """初始化归档器
        
        Args:
            base_path: 基础路径
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas未安装，无法使用UniversalArchiver")
            
        self.base_path = Path(base_path)
        self.logger = logging.getLogger(f"UniversalArchiver")
        
        # 确保基础路径存在
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def calculate_md5(self, data_path: Path) -> str:
        """计算文件MD5值"""
        hash_md5 = hashlib.md5()
        with open(data_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def calculate_dataframe_md5(self, df: pd.DataFrame) -> str:
        """计算DataFrame的MD5值"""
        # 将DataFrame转换为字节串计算MD5
        df_bytes = df.to_csv(index=False).encode('utf-8')
        return hashlib.md5(df_bytes).hexdigest()
    
    def _write_dataframe_atomically(self, file_path: Path, df: pd.DataFrame):
        """原子性写入DataFrame到parquet文件"""
        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 使用临时文件进行原子写入
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet', 
                                       dir=file_path.parent) as tmp_file:
            tmp_path = Path(tmp_file.name)
            
        try:
            # 写入临时文件
            df.to_parquet(tmp_path, index=False)
            # 原子性移动到目标位置
            tmp_path.replace(file_path)
        except Exception:
            # 清理临时文件
            if tmp_path.exists():
                tmp_path.unlink()
            raise
    
    def _create_config_file(self, data_path: Path, config: ArchiveConfig, 
                          data_md5: str, data_shape: tuple, 
                          api_params: Dict[str, Any] = None,
                          previous_md5: str = None,
                          previous_shape: tuple = None,
                          archived_config: str = None,
                          archived_data: str = None,
                          path_info: PathInfo = None) -> Path:
        """创建配置文件
        
        Args:
            data_path: 数据文件路径
            config: 归档配置
            data_md5: 数据MD5值
            data_shape: 数据形状
            api_params: API参数
            previous_md5: 之前的MD5值
            previous_shape: 之前的数据形状
            archived_config: 归档的配置文件名
            archived_data: 归档的数据文件名
            
        Returns:
            配置文件路径
        """
        # 生成配置文件路径
        if path_info and path_info.config_filename:
            config_filename = path_info.config_filename
        else:
            config_filename = data_path.name.replace('.parquet', '_config.json')
        config_path = data_path.parent / config_filename
        
        # 准备配置数据（按照用户提供的格式）
        config_data = {
            'source_name': config.source_name,
            'data_type': config.data_type,
            'archive_type': config.archive_type,
            'date_param': config.date_param,
            'created_at': datetime.now().isoformat(),
            'data_md5': data_md5,
            'data_shape': list(data_shape),
            'constructed_params': {
                'source_name': config.source_name,
                'data_type': config.data_type,
                'date_param': config.date_param,
                'storage_type': config.archive_type,
                'method': config.method,
                'required_params': config.required_params
            },
            'api_params': api_params or {}
        }
        
        # 添加可选的归档信息
        if previous_md5:
            config_data['previous_md5'] = previous_md5
        if previous_shape:
            config_data['previous_data_shape'] = list(previous_shape)
        if archived_config:
            config_data['archived_config'] = archived_config
        if archived_data:
            config_data['archived_data'] = archived_data
            config_data['updated_at'] = datetime.now().isoformat()
        
        # 写入配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)
        
        return config_path
    
    def _archive_existing_files(self, data_path: Path, config_path: Path, path_info: PathInfo = None) -> tuple:
        """归档现有文件到archive目录
        
        Returns:
            (archived_data_filename, archived_config_filename)
        """
        # 创建archive目录
        if path_info and path_info.archive_path:
            archive_dir = Path(path_info.archive_path)
        else:
            archive_dir = data_path.parent / 'archive'
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成带时间戳的归档文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        data_stem = data_path.stem
        config_stem = config_path.stem
        
        archived_data_name = f"{data_stem}_{timestamp}.parquet"
        archived_config_name = f"{config_stem}_{timestamp}.json"
        
        # 移动文件到archive目录
        archived_data_path = archive_dir / archived_data_name
        archived_config_path = archive_dir / archived_config_name
        
        data_path.rename(archived_data_path)
        config_path.rename(archived_config_path)
        
        return archived_data_name, archived_config_name
    
    def archive_data(self, df: pd.DataFrame, path_info: PathInfo, 
                    config: ArchiveConfig, api_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """归档pandas DataFrame数据
        
        Args:
            df: 要归档的DataFrame
            path_info: 路径信息
            config: 归档配置
            api_params: API参数
            
        Returns:
            归档结果信息
        """
        if df is None or df.empty:
            raise ValueError("DataFrame不能为空")
        
        # 构建完整路径
        data_path = Path(path_info.landing_path) / path_info.data_filename
        
        log_prefix = f"[{config.source_name}:{config.data_type}]"
        
        # 1. 计算新数据的MD5
        new_data_md5 = self.calculate_dataframe_md5(df)
        self.logger.info(f"{log_prefix} 1.开始归档 - 目标路径:{data_path}")
        self.logger.info(f"{log_prefix} 2.数据MD5计算完成: {new_data_md5[:8]}...")
        
        # 2. 检查文件是否存在
        file_existed_before = data_path.exists()
        config_path = data_path.parent / path_info.config_filename
        
        previous_md5 = None
        previous_shape = None
        archived_data = None
        archived_config = None
        
        if file_existed_before:
            self.logger.info(f"{log_prefix} 3.文件检查 - 文件已存在，进行MD5比较")
            # 读取现有文件并计算DataFrame的MD5
            try:
                existing_df = pd.read_parquet(data_path)
                existing_md5 = self.calculate_dataframe_md5(existing_df)
            except Exception as e:
                self.logger.warning(f"{log_prefix} 读取现有文件失败: {e}，将重新写入")
                existing_md5 = None
            
            if existing_md5 and existing_md5 == new_data_md5:
                self.logger.info(f"{log_prefix} 3.文件检查 - MD5相同，跳过写入 (MD5:{existing_md5[:8]}...)")
                # 确保配置文件存在
                if not config_path.exists():
                    config_path = self._create_config_file(
                        data_path, config, existing_md5, df.shape, api_params, path_info=path_info
                    )
                    self.logger.info(f"{log_prefix} 4.配置文件 - 已存在:{config_path}")
                
                return {
                    'success': True,
                    'action': 'skipped',
                    'message': f'文件已存在且内容相同，跳过写入: {data_path}',
                    'file_path': str(data_path),
                    'config_path': str(config_path),
                    'md5': existing_md5
                }
            else:
                self.logger.info(f"{log_prefix} 3.文件检查 - MD5不同，需要更新")
                
                # 读取现有配置获取之前的信息
                if config_path.exists():
                    try:
                        with open(config_path, 'r', encoding='utf-8') as f:
                            old_config = json.load(f)
                            previous_md5 = old_config.get('data_md5')
                            previous_shape = tuple(old_config.get('data_shape', []))
                    except Exception as e:
                        self.logger.warning(f"{log_prefix} 读取旧配置失败: {e}")
                else:
                    # 如果配置文件不存在，使用现有DataFrame的MD5
                    previous_md5 = existing_md5
                    if existing_md5:
                        try:
                            existing_df = pd.read_parquet(data_path)
                            previous_shape = existing_df.shape
                        except Exception:
                            previous_shape = None
                
                # 检查数据行数是否减少（二次验证机制）
                if previous_shape and len(previous_shape) >= 2:
                    old_rows = previous_shape[0]
                    new_rows = df.shape[0]
                    if new_rows < old_rows:
                        self.logger.warning(f"{log_prefix} 数据行数减少: {old_rows} -> {new_rows}，触发二次验证")
                        raise ValueError(f"数据行数从{old_rows}减少到{new_rows}，请确认数据完整性")
                
                # 归档现有文件
                archived_data, archived_config = self._archive_existing_files(data_path, config_path, path_info)
                self.logger.info(f"{log_prefix} 4.文件归档 - 已归档到archive目录")
        else:
            self.logger.info(f"{log_prefix} 3.文件检查 - 文件不存在，需要创建")
        
        # 3. 原子性数据写入
        self.logger.info(f"{log_prefix} 5.数据写入 - 开始原子写入")
        try:
            self._write_dataframe_atomically(data_path, df)
            action = 'updated' if file_existed_before else 'created'
            
            self.logger.info(f"{log_prefix} 5.数据写入 - 完成 (MD5:{new_data_md5[:8]}..., 操作:{action})")
            
            # 4. 创建配置文件
            config_path = self._create_config_file(
                data_path, config, new_data_md5, df.shape, api_params,
                previous_md5, previous_shape, archived_config, archived_data, path_info
            )
            self.logger.info(f"{log_prefix} 6.配置文件 - 已创建:{config_path}")
            
            return {
                'success': True,
                'action': action,
                'message': f'数据已成功归档到: {data_path}',
                'file_path': str(data_path),
                'config_path': str(config_path),
                'md5': new_data_md5,
                'archived_files': [archived_data, archived_config] if archived_data else []
            }
            
        except IOError as e:
            self.logger.error(f"{log_prefix} 数据写入失败: {str(e)}")
            raise
    
    def get_archive_info(self, file_path: str) -> Dict[str, Any]:
        """获取归档信息
        
        Args:
            file_path: 数据文件路径
            
        Returns:
            归档信息字典
        """
        data_path = Path(file_path)
        
        if not data_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 生成配置文件路径，优先查找同名.json文件
        base_name = data_path.stem  # 获取不带扩展名的文件名
        config_path1 = data_path.parent / f"{base_name}.json"  # 同名.json文件
        config_path2 = data_path.parent / f"{base_name}_config.json"  # 默认_config.json文件
        
        # 优先使用同名.json文件，如果不存在则使用_config.json
        if config_path1.exists():
            config_path = config_path1
        elif config_path2.exists():
            config_path = config_path2
        else:
            raise FileNotFoundError(f"配置文件不存在: {config_path1} 或 {config_path2}")
        
        # 读取配置信息
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # 添加文件统计信息
        config_data['file_size'] = data_path.stat().st_size
        config_data['file_modified'] = datetime.fromtimestamp(
            data_path.stat().st_mtime
        ).isoformat()
        
        return config_data
    
    def archive_data_simple(self, data: pd.DataFrame, json_info: Dict[str, Any], 
                           save_path: str, archive_path: str, 
                           verification_callback: Optional[callable] = None) -> Dict[str, Any]:
        """简化的数据归档接口
        
        Args:
            data: 要保存的DataFrame数据
            json_info: JSON元数据信息，包含source_name, data_type等
            save_path: 保存路径（完整文件路径）
            archive_path: 存档路径（目录路径）
            verification_callback: 数据行数减少时的二次验证回调函数
            
        Returns:
            Dict包含:
            - save_method: 保存方式 ('new', 'update', 'skip')
            - save_status: 保存状态 ('success', 'failed')
            - json_info: 更新后的JSON信息（包含MD5等）
            - message: 详细信息
        """
        if data is None or data.empty:
            return {
                'save_method': 'skip',
                'save_status': 'failed',
                'json_info': json_info,
                'message': 'DataFrame为空，跳过保存'
            }
        
        try:
            save_file_path = Path(save_path)
            archive_dir_path = Path(archive_path)
            
            # 1. 计算新数据的MD5
            new_data_md5 = self.calculate_dataframe_md5(data)
            
            # 2. 检查文件是否存在并比较MD5
            file_exists = save_file_path.exists()
            save_method = 'new'
            previous_info = None
            
            if file_exists:
                # 查找并读取现有的JSON配置文件
                existing_json_info = self._find_and_load_json_config(save_file_path)
                
                if existing_json_info:
                    existing_md5 = existing_json_info.get('data_md5')
                    existing_shape = existing_json_info.get('data_shape', [])
                    
                    # 准备previous信息
                    previous_info = {
                        'rows': existing_shape[0] if len(existing_shape) >= 1 else 0,
                        'columns': existing_shape[1] if len(existing_shape) >= 2 else 0,
                        'md5': existing_md5,
                        'filename': save_file_path.name,
                        'updated_at': existing_json_info.get('updated_at', existing_json_info.get('created_at'))
                    }
                    
                    if existing_md5 == new_data_md5:
                        # MD5相同，跳过保存
                        return {
                            'save_method': 'skip',
                            'save_status': 'success',
                            'json_info': existing_json_info,
                            'message': f'数据MD5相同({new_data_md5[:8]}...)，跳过保存'
                        }
                    else:
                        # 检查数据行数是否减少
                        old_rows = existing_shape[0] if len(existing_shape) >= 1 else 0
                        new_rows = data.shape[0]
                        
                        if new_rows < old_rows and verification_callback:
                            self.logger.warning(f"数据行数减少: {old_rows} -> {new_rows}，触发二次验证")
                            
                            # 执行二次验证
                            verification_data = verification_callback()
                            if verification_data is None or verification_data.empty:
                                raise ValueError(f"二次验证失败：无法获取验证数据")
                            
                            verification_rows = verification_data.shape[0]
                            if verification_rows != new_rows:
                                raise ValueError(f"二次验证失败：验证数据行数({verification_rows})与原数据行数({new_rows})不一致")
                            
                            # 验证MD5是否一致
                            verification_md5 = self.calculate_dataframe_md5(verification_data)
                            if verification_md5 != new_data_md5:
                                raise ValueError(f"二次验证失败：验证数据MD5({verification_md5[:8]}...)与原数据MD5({new_data_md5[:8]}...)不一致")
                            
                            self.logger.info(f"二次验证通过：确认数据行数从{old_rows}减少到{new_rows}")
                        
                        # MD5不同，需要归档现有文件并更新
                        actual_archived_path = self._archive_existing_file_simple(save_file_path, archive_dir_path)
                        save_method = 'update'
                else:
                    # 文件存在但没有配置，直接计算现有文件MD5比较
                    try:
                        existing_df = pd.read_parquet(save_file_path)
                        existing_md5 = self.calculate_dataframe_md5(existing_df)
                        existing_shape = existing_df.shape
                        
                        # 准备previous信息
                        previous_info = {
                            'rows': existing_shape[0],
                            'columns': existing_shape[1],
                            'md5': existing_md5,
                            'filename': save_file_path.name,
                            'updated_at': datetime.fromtimestamp(save_file_path.stat().st_mtime).isoformat()
                        }
                        
                        if existing_md5 == new_data_md5:
                            # 创建配置文件并跳过
                            updated_json_info = self._create_json_info(json_info, new_data_md5, data.shape, previous_info)
                            self._save_json_config(save_file_path, updated_json_info)
                            return {
                                'save_method': 'skip',
                                'save_status': 'success',
                                'json_info': updated_json_info,
                                'message': f'数据MD5相同({new_data_md5[:8]}...)，跳过保存并创建配置文件'
                            }
                        else:
                            # 检查数据行数是否减少
                            old_rows = existing_shape[0]
                            new_rows = data.shape[0]
                            
                            if new_rows < old_rows and verification_callback:
                                self.logger.warning(f"数据行数减少: {old_rows} -> {new_rows}，触发二次验证")
                                
                                # 执行二次验证
                                verification_data = verification_callback()
                                if verification_data is None or verification_data.empty:
                                    raise ValueError(f"二次验证失败：无法获取验证数据")
                                
                                verification_rows = verification_data.shape[0]
                                if verification_rows != new_rows:
                                    raise ValueError(f"二次验证失败：验证数据行数({verification_rows})与原数据行数({new_rows})不一致")
                                
                                # 验证MD5是否一致
                                verification_md5 = self.calculate_dataframe_md5(verification_data)
                                if verification_md5 != new_data_md5:
                                    raise ValueError(f"二次验证失败：验证数据MD5({verification_md5[:8]}...)与原数据MD5({new_data_md5[:8]}...)不一致")
                                
                                self.logger.info(f"二次验证通过：确认数据行数从{old_rows}减少到{new_rows}")
                            
                            # 归档现有文件
                            actual_archived_path = self._archive_existing_file_simple(save_file_path, archive_dir_path)
                            save_method = 'update'
                    except Exception as e:
                        self.logger.warning(f"读取现有文件失败: {e}，将重新保存")
                        actual_archived_path = self._archive_existing_file_simple(save_file_path, archive_dir_path)
                        save_method = 'update'
            
            # 只有在需要保存的情况下才执行保存操作（new或update）
            # 3. 保存数据文件
            self._write_dataframe_atomically(save_file_path, data)
            
            # 4. 创建/更新JSON配置文件
            updated_json_info = self._create_json_info(json_info, new_data_md5, data.shape, previous_info)
            self._save_json_config(save_file_path, updated_json_info)
            
            # 准备返回结果
            result = {
                'save_method': save_method,
                'save_status': 'success',
                'json_info': updated_json_info,
                'message': f'数据保存成功，MD5: {new_data_md5[:8]}...'
            }
            
            # 如果有归档操作，添加实际的归档路径
            if save_method == 'update' and 'actual_archived_path' in locals():
                result['actual_archived_path'] = actual_archived_path
            
            return result
            
        except Exception as e:
            self.logger.error(f"数据归档失败: {e}")
            return {
                'save_method': 'new',
                'save_status': 'failed',
                'json_info': json_info,
                'message': f'保存失败: {str(e)}'
            }
    
    def _find_and_load_json_config(self, data_file_path: Path) -> Optional[Dict[str, Any]]:
        """查找并加载JSON配置文件
        
        优先查找同名.json文件，然后查找_config.json文件
        """
        base_name = data_file_path.stem
        
        # 可能的配置文件路径
        possible_config_paths = [
            data_file_path.parent / f"{base_name}.json",
            data_file_path.parent / f"{base_name}_config.json"
        ]
        
        for config_path in possible_config_paths:
            if config_path.exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    self.logger.warning(f"读取配置文件失败 {config_path}: {e}")
                    continue
        
        return None
    
    def _archive_existing_file_simple(self, file_path: Path, archive_path: Path) -> str:
        """简单的文件归档功能
        
        Args:
            file_path: 要归档的文件路径
            archive_path: 归档文件的完整路径（包含文件名）
        
        Returns:
            str: 实际的归档文件路径
        """
        # 从archive_path中提取目录路径
        archive_dir = archive_path.parent
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # 使用原始文件名加时间戳
        archived_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        archived_path = archive_dir / archived_name
        
        # 移动数据文件到归档目录
        file_path.rename(archived_path)
        
        # 查找并归档对应的JSON配置文件
        base_name = file_path.stem
        possible_config_paths = [
            file_path.parent / f"{base_name}.json",
            file_path.parent / f"{base_name}_config.json"
        ]
        
        for config_path in possible_config_paths:
            if config_path.exists():
                archived_config_name = f"{config_path.stem}_{timestamp}{config_path.suffix}"
                archived_config_path = archive_dir / archived_config_name
                config_path.rename(archived_config_path)
                break
        
        return str(archived_path)
    
    def _create_json_info(self, original_json_info: Dict[str, Any], 
                         data_md5: str, data_shape: tuple, 
                         previous_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """创建更新后的JSON信息"""
        updated_info = original_json_info.copy()
        
        # 更新核心信息
        updated_info['data_md5'] = data_md5
        updated_info['data_shape'] = list(data_shape)
        updated_info['updated_at'] = datetime.now().isoformat()
        
        # 如果没有创建时间，添加创建时间
        if 'created_at' not in updated_info:
            updated_info['created_at'] = datetime.now().isoformat()
        
        # 添加previous信息
        if previous_info:
            updated_info['previous'] = previous_info
        
        return updated_info
    
    def _save_json_config(self, data_file_path: Path, json_info: Dict[str, Any]):
        """保存JSON配置文件
        
        优先保存为同名.json文件
        """
        base_name = data_file_path.stem
        config_path = data_file_path.parent / f"{base_name}.json"
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(json_info, f, ensure_ascii=False, indent=2)