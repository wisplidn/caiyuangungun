"""通用数据归档器

提供通用的数据归档功能，支持任何数据源的数据归档。
实现文件存在性检查、MD5验证、文件归档和配置更新等功能。
"""

import json
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import pandas as pd
import logging

# 使用绝对路径导入避免相对导入问题
import importlib.util
from pathlib import Path as PathLib

# 获取当前文件所在目录
current_dir = PathLib(__file__).parent

# 导入PathGenerator
path_generator_spec = importlib.util.spec_from_file_location("path_generator", str(current_dir / "path_generator.py"))
path_generator_module = importlib.util.module_from_spec(path_generator_spec)
path_generator_spec.loader.exec_module(path_generator_module)
PathGenerator = path_generator_module.PathGenerator

# 导入ConfigManager
config_manager_spec = importlib.util.spec_from_file_location("config_manager", str(current_dir / "config_manager.py"))
config_manager_module = importlib.util.module_from_spec(config_manager_spec)
config_manager_spec.loader.exec_module(config_manager_module)
ConfigManager = config_manager_module.ConfigManager


class UniversalArchiver:
    """通用数据归档器
    
    提供通用的数据归档功能，支持任何数据源。
    实现完整的归档流程：文件检查、MD5验证、归档和配置更新。
    """
    
    def __init__(self, source_name: str, config_name: str = "path_generator"):
        """初始化通用归档器
        
        Args:
            source_name: 数据源名称（如tushare、wind、bloomberg等）
            config_name: 配置名称，用于PathGenerator
        """
        self.source_name = source_name
        self.path_generator = PathGenerator(config_name)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def calculate_md5(self, file_path: Path) -> str:
        """计算文件MD5值
        
        Args:
            file_path: 文件路径
            
        Returns:
            MD5哈希值
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def calculate_data_md5(self, data: pd.DataFrame) -> str:
        """计算DataFrame的MD5值
        
        Args:
            data: 数据DataFrame
            
        Returns:
            MD5哈希值
        """
        # 将DataFrame转换为字符串并计算MD5
        data_str = data.to_string().encode('utf-8')
        return hashlib.md5(data_str).hexdigest()
    
    def create_timestamp_filename(self, original_path: Path) -> str:
        """创建带时间戳的文件名
        
        Args:
            original_path: 原始文件路径
            
        Returns:
            带时间戳的文件名
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = original_path.stem
        suffix = original_path.suffix
        return f"{stem}_{timestamp}{suffix}"
    
    def load_config_file(self, config_path: Path) -> Dict[str, Any]:
        """加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def save_config_file(self, config_path: Path, config_data: Dict[str, Any]) -> None:
        """保存配置文件
        
        Args:
            config_path: 配置文件路径
            config_data: 配置数据
        """
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)
    
    def archive_existing_files(self, landing_dir: Path, archive_dir: Path, 
                              config_filename: str, data_filename: str) -> Tuple[str, str]:
        """归档现有文件到archive目录
        
        Args:
            landing_dir: landing目录路径
            archive_dir: archive目录路径
            config_filename: 配置文件名
            data_filename: 数据文件名
            
        Returns:
            归档后的配置文件名和数据文件名
        """
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        # 归档配置文件
        config_src = landing_dir / config_filename
        archived_config_name = self.create_timestamp_filename(config_src)
        config_dst = archive_dir / archived_config_name
        shutil.move(str(config_src), str(config_dst))
        
        # 归档数据文件
        data_src = landing_dir / data_filename
        archived_data_name = self.create_timestamp_filename(data_src)
        data_dst = archive_dir / archived_data_name
        shutil.move(str(data_src), str(data_dst))
        
        self.logger.info(f"已归档文件: {config_filename} -> {archived_config_name}")
        self.logger.info(f"已归档文件: {data_filename} -> {archived_data_name}")
        
        return archived_config_name, archived_data_name
    
    def archive_data(self, data: pd.DataFrame, data_type: str, archive_type: str, 
                    date_param: Optional[str] = None, 
                    constructed_params: Optional[Dict[str, Any]] = None,
                    api_params: Optional[Dict[str, Any]] = None,
                    force_update: bool = False,
                    **kwargs) -> Dict[str, Any]:
        """归档数据
        
        Args:
            data: 要归档的数据DataFrame
            data_type: 数据类型
            archive_type: 归档类型（snapshot, daily, monthly等）
            date_param: 日期参数
            **kwargs: 其他参数
            
        Returns:
            归档结果信息
        """
        # 准备参数
        params = {
            'source_name': self.source_name,
            'data_type': data_type,
            'archive_type': archive_type
        }
        
        # 添加日期参数
        if date_param:
            if archive_type.upper() == 'DAILY':
                params['daily_date'] = date_param
            elif archive_type.upper() == 'MONTHLY':
                params['monthly_date'] = date_param
            elif archive_type.upper() == 'QUARTERLY':
                params['quarterly_date'] = date_param
        
        # 添加其他参数
        params.update(kwargs)
        
        # 生成路径
        path_result = self.path_generator.get_path_info(**params)
        
        if not path_result.get('success', False):
            raise ValueError(f"路径生成失败: {path_result.get('errors', ['未知错误'])}")
        
        # 获取默认格式的路径信息
        default_format = path_result['default_format']
        file_paths = path_result['file_paths'][default_format]
        
        landing_dir = Path(file_paths['directory']['landing'])
        archive_dir = Path(file_paths['directory']['archive'])
        data_filename = file_paths['filename']
        
        # 配置文件名（将parquet改为json）
        config_filename = data_filename.replace('.parquet', '.json').replace('.csv', '.json')
        
        config_path = landing_dir / config_filename
        data_path = landing_dir / data_filename
        
        # 6. 准备写入，执行MD5检查
        new_data_md5 = self.calculate_data_md5(data)
        self.logger.info(f"[{self.source_name}:{data_type}] 6.准备写入 - 计算数据MD5: {new_data_md5[:8]}...")
        
        # 准备配置数据
        config_data = {
            'source_name': self.source_name,
            'data_type': data_type,
            'archive_type': archive_type,
            'date_param': date_param,
            'created_at': datetime.now().isoformat(),
            'data_md5': new_data_md5,
            'data_shape': list(data.shape),
            'constructed_params': constructed_params or {},
            'api_params': api_params or {}
        }
        
        result = {
            'action': 'unknown',
            'landing_dir': str(landing_dir),
            'archive_dir': str(archive_dir),
            'config_filename': config_filename,
            'data_filename': data_filename,
            'data_md5': new_data_md5
        }
        
        # 检查文件是否存在
        if not config_path.exists() or not data_path.exists():
            # 文件不存在，直接写入
            self.logger.info(f"[{self.source_name}:{data_type}] 6.MD5检查 - 新文件，准备保存")
            landing_dir.mkdir(parents=True, exist_ok=True)
            
            # 8. 保存成功
            self.save_config_file(config_path, config_data)
            data.to_parquet(data_path, index=False)
            
            result['action'] = 'created'
            self.logger.info(f"[{self.source_name}:{data_type}] 8.保存成功 - 创建新文件: {config_filename}, {data_filename}")
            
        else:
            # 文件存在，验证MD5
            existing_config = self.load_config_file(config_path)
            existing_md5 = existing_config.get('data_md5', '')
            
            if existing_md5 == new_data_md5:
                # MD5相同，不写入
                result['action'] = 'skipped_duplicate'
                self.logger.info(f"[{self.source_name}:{data_type}] 6.MD5检查 - 数据未变更，跳过保存")
                
            else:
                # 7. 检查到发生变更，进行数据行数比较
                existing_data_shape = existing_config.get('data_shape', [0, 0])
                existing_rows = existing_data_shape[0] if existing_data_shape else 0
                new_rows = data.shape[0]
                
                self.logger.info(f"[{self.source_name}:{data_type}] 7.检查到发生变更 - MD5不同({existing_md5[:8]}... -> {new_data_md5[:8]}...)")
                self.logger.info(f"[{self.source_name}:{data_type}] 数据行数比较 - 原有:{existing_rows}行 -> 新数据:{new_rows}行")
                
                # 如果强制更新，跳过行数检查
                if force_update:
                    self.logger.info(f"[{self.source_name}:{data_type}] 强制更新模式，跳过行数检查")
                # 如果新数据行数小于原有数据行数，进行二次验证（非强制更新模式）
                elif new_rows < existing_rows:
                    self.logger.warning(f"[{self.source_name}:{data_type}] 新数据行数减少，启动二次验证")
                    
                    # 标记需要二次验证
                    result['requires_retry'] = True
                    result['retry_reason'] = f'数据行数减少: {existing_rows} -> {new_rows}'
                    result['action'] = 'retry_required'
                    
                    self.logger.info(f"[{self.source_name}:{data_type}] 要求调用方进行二次验证")
                    return result
                
                # 正常更新流程：转移文件
                update_reason = "强制更新" if force_update else "数据行数正常"
                self.logger.info(f"[{self.source_name}:{data_type}] {update_reason}，转移现有文件")
                
                # 归档现有文件
                archived_config, archived_data = self.archive_existing_files(
                    landing_dir, archive_dir, config_filename, data_filename
                )
                
                # 更新配置数据，记录原MD5信息
                config_data['previous_md5'] = existing_md5
                config_data['previous_data_shape'] = existing_data_shape
                config_data['archived_config'] = archived_config
                config_data['archived_data'] = archived_data
                config_data['updated_at'] = datetime.now().isoformat()
                
                # 8. 保存成功
                self.save_config_file(config_path, config_data)
                data.to_parquet(data_path, index=False)
                
                result['action'] = 'updated'
                result['archived_config'] = archived_config
                result['archived_data'] = archived_data
                result['previous_md5'] = existing_md5
                result['previous_data_shape'] = existing_data_shape
                
                self.logger.info(f"[{self.source_name}:{data_type}] 8.保存成功 - 更新文件: {config_filename}, {data_filename}")
        
        return result
    
    def get_archive_info(self, data_type: str, archive_type: str, 
                        date_param: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """获取归档信息
        
        Args:
            data_type: 数据类型
            archive_type: 归档类型
            date_param: 日期参数
            **kwargs: 其他参数
            
        Returns:
            归档信息
        """
        # 准备参数
        params = {
            'source_name': self.source_name,
            'data_type': data_type,
            'archive_type': archive_type
        }
        
        # 添加日期参数
        if date_param:
            if archive_type.upper() == 'DAILY':
                params['daily_date'] = date_param
            elif archive_type.upper() == 'MONTHLY':
                params['monthly_date'] = date_param
            elif archive_type.upper() == 'QUARTERLY':
                params['quarterly_date'] = date_param
        
        # 添加其他参数
        params.update(kwargs)
        
        # 生成路径
        path_result = self.path_generator.get_path_info(**params)
        
        if not path_result.get('success', False):
            raise ValueError(f"路径生成失败: {path_result.get('errors', ['未知错误'])}")
        
        # 获取默认格式的路径信息
        default_format = path_result['default_format']
        file_paths = path_result['file_paths'][default_format]
        
        landing_dir = Path(file_paths['directory']['landing'])
        data_filename = file_paths['filename']
        
        # 配置文件名（将parquet改为json）
        config_filename = data_filename.replace('.parquet', '.json').replace('.csv', '.json')
        config_path = landing_dir / config_filename
        
        if config_path.exists():
            config_data = self.load_config_file(config_path)
            config_data['exists'] = True
            config_data['config_path'] = str(config_path)
            return config_data
        else:
            return {
                'exists': False,
                'config_path': str(config_path)
            }