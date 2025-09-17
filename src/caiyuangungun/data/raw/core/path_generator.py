"""通用数据路径生成器模块

基于JSON配置的参数验证和路径生成器。
输入必要参数，进行有效性验证，返回路径信息的JSON结果。
"""

import json
import re
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    errors: List[str]
    
    def add_error(self, error: str):
        """添加错误信息"""
        self.is_valid = False
        self.errors.append(error)


@dataclass
class ArchiveTypeConfig:
    """归档类型配置"""
    value: str
    description: str
    path_pattern: str
    enabled: bool = True
    validation_rules: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileConfig:
    """文件配置"""
    filename_template: str = 'data.{file_type}'
    supported_formats: List[str] = field(default_factory=lambda: ['parquet', 'json'])
    default_format: str = 'parquet'


@dataclass
class PathsConfig:
    """路径配置"""
    landing_subpath: str = 'landing'
    archive_subpath: str = 'archive'


@dataclass
class ConfigDTO:
    """配置数据传输对象，替代ConfigManager依赖"""
    base_path: str
    archive_types: Dict[str, ArchiveTypeConfig]
    file_config: FileConfig
    paths: PathsConfig
    allowed_archive_types: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.allowed_archive_types:
            self.allowed_archive_types = [name for name, config in self.archive_types.items() if config.enabled]
    
    def get_archive_types(self) -> List[str]:
        """获取允许的归档类型列表"""
        return self.allowed_archive_types


class PathGenerator:
    """通用数据路径生成器
    
    功能：
    1. 输入参数验证
    2. 路径生成
    3. 返回JSON格式的路径信息
    """
    
    def __init__(self, config_dto: ConfigDTO):
        """初始化路径生成器
        
        Args:
            config_dto: 配置数据传输对象，包含所有必要的配置信息
        """
        self.config_dto = config_dto
        

    
    def validate_field(self, field_name: str, value: Any) -> ValidationResult:
        """验证单个字段
        
        Args:
            field_name: 字段名称
            value: 字段值
            
        Returns:
            ValidationResult: 验证结果
        """
        result = ValidationResult(is_valid=True, errors=[])
        
        # 基本验证：检查必需字段是否为空
        if not value or (isinstance(value, str) and not value.strip()):
            result.add_error(f"{field_name} 不能为空")
            # 添加正确示例
            if field_name == 'source_name':
                result.add_error("正确示例: source_name='tushare' 或 'akshare'")
            elif field_name == 'data_type':
                result.add_error("正确示例: data_type='stock_basic' 或 'daily'")
            elif field_name == 'archive_type':
                result.add_error(f"正确示例: archive_type='{self.config_dto.get_archive_types()[0] if self.config_dto.get_archive_types() else 'DAILY'}'")
            return result
        
        # 基本类型验证
        if not isinstance(value, str):
            result.add_error(f"{field_name} 必须是字符串类型")
            return result
        
        # 简单的格式验证
        if field_name == 'archive_type':
            # 从ConfigDTO获取允许的归档类型
            allowed_values = self.config_dto.get_archive_types()
            if value not in allowed_values:
                result.add_error(f"{field_name} 必须是以下值之一: {allowed_values}")
                result.add_error(f"正确示例: archive_type='{allowed_values[0] if allowed_values else 'DAILY'}'")
        
        return result
    
    def _validate_input_completeness(self, **params) -> ValidationResult:
        """验证输入参数的完整性
        
        Args:
            **params: 输入参数
            
        Returns:
            ValidationResult: 验证结果
        """
        result = ValidationResult(is_valid=True, errors=[])
        
        # 检查必需的基础参数
        required_fields = ['source_name', 'data_type', 'archive_type']
        missing_fields = []
        
        for field in required_fields:
            if field not in params or not params[field]:
                missing_fields.append(field)
        
        if missing_fields:
            result.add_error(f"缺少必需参数: {', '.join(missing_fields)}")
            result.add_error("完整示例: {")
            result.add_error("  'source_name': 'tushare',")
            result.add_error("  'data_type': 'stock_basic',")
            result.add_error("  'archive_type': 'DAILY',")
            result.add_error("  'date': '20241231'  # 当archive_type为DAILY或MONTHLY时必需")
            result.add_error("}")
            return result
        
        # 根据归档类型检查日期参数
        archive_type = params.get('archive_type', '').upper()
        if archive_type == 'DAILY':
            if 'date' not in params or not params['date']:
                result.add_error("DAILY类型需要提供date参数")
                result.add_error("正确示例: date='20241231'")
        elif archive_type == 'MONTHLY':
            if 'date' not in params or not params['date']:
                result.add_error("MONTHLY类型需要提供date参数")
                result.add_error("正确示例: date='202412' 或 '20241231'")
        elif archive_type == 'QUARTERLY':
            if 'date' not in params or not params['date']:
                result.add_error("QUARTERLY类型需要提供date参数")
                result.add_error("正确示例: date='20240331'、'20240630'、'20240930'、'20241231'")
        elif archive_type == 'symbol':
            if 'symbol' not in params or not params['symbol']:
                result.add_error("symbol类型需要提供symbol参数")
                result.add_error("正确示例: symbol='SH600519'")
        # SNAPSHOT类型不需要date参数
        
        return result
    
    def validate_params(self, **params) -> ValidationResult:
        """验证输入参数
        
        Args:
            **params: 输入参数
            
        Returns:
            ValidationResult: 验证结果
        """
        result = ValidationResult(is_valid=True, errors=[])
        
        # 验证基础字段
        for field in ['source_name', 'data_type', 'archive_type']:
            field_result = self.validate_field(field, params.get(field))
            if not field_result.is_valid:
                result.errors.extend(field_result.errors)
                result.is_valid = False
        
        if not result.is_valid:
            return result
        
        # 获取归档类型
        archive_type = params.get('archive_type', '').upper()
        
        # 验证日期参数
        if archive_type == 'SNAPSHOT':
            # SNAPSHOT类型不需要日期参数，直接忽略
            pass
        elif archive_type == 'DAILY':
            # DAILY类型要求8位日期
            date_value = params.get('date')
            if not date_value:
                result.add_error("DAILY类型需要提供date参数")
                result.add_error("正确示例: date='20241231'")
                result.is_valid = False
            elif not re.match(r'^\d{8}$', date_value):
                result.add_error("DAILY类型的date必须是8位数字日期格式")
                result.add_error("正确示例: date='20241231'")
                result.is_valid = False
        elif archive_type == 'MONTHLY':
            # MONTHLY类型支持6位或8位日期
            date_value = params.get('date')
            if not date_value:
                result.add_error("MONTHLY类型需要提供date参数")
                result.add_error("正确示例: date='202412' 或 '20241231'")
                result.is_valid = False
            elif not re.match(r'^\d{6}$', date_value) and not re.match(r'^\d{8}$', date_value):
                result.add_error("MONTHLY类型的date必须是6位年月格式或8位日期格式")
                result.add_error("正确示例: date='202412' 或 '20241231'")
                result.is_valid = False
        elif archive_type == 'QUARTERLY':
            # QUARTERLY类型要求8位日期且必须是财报日期
            date_value = params.get('date')
            if not date_value:
                result.add_error("QUARTERLY类型需要提供date参数")
                result.add_error("正确示例: date='20240331'、'20240630'、'20240930'、'20241231'")
                result.is_valid = False
            elif not re.match(r'^\d{8}$', date_value):
                result.add_error("QUARTERLY类型的date必须是8位数字日期格式")
                result.add_error("正确示例: date='20240331'、'20240630'、'20240930'、'20241231'")
                result.is_valid = False
            else:
                # 验证是否为财报日期（每年的3月31日、6月30日、9月30日、12月31日）
                month_day = date_value[4:8]
                if month_day not in ['0331', '0630', '0930', '1231']:
                    result.add_error("QUARTERLY类型的date必须是财报日期")
                    result.add_error("正确示例: date='20240331'（Q1）、'20240630'（Q2）、'20240930'（Q3）、'20241231'（Q4）")
                    result.is_valid = False
        elif archive_type == 'symbol':
            # symbol类型要求股票代码格式
            symbol_value = params.get('symbol')
            if not symbol_value:
                result.add_error("symbol类型需要提供symbol参数")
                result.add_error("正确示例: symbol='SH600519'")
                result.is_valid = False
            elif not re.match(r'^(SH|SZ)\d{6}$', symbol_value):
                result.add_error("symbol类型的symbol必须是AK格式的股票代码")
                result.add_error("正确示例: symbol='SH600519'（上海）或 'SZ000001'（深圳）")
                result.is_valid = False
        
        return result
    
    def generate_paths(self, **params) -> Dict[str, Any]:
        """生成路径信息
        
        Args:
            **params: 输入参数
            
        Returns:
            Dict[str, Any]: 包含路径信息的JSON结果
        """
        # 入口完整性验证
        validation_result = self._validate_input_completeness(**params)
        if not validation_result.is_valid:
            return {
                "success": False,
                "errors": validation_result.errors,
                "message": "参数验证失败，请检查输入参数的完整性和准确性"
            }
        
        # 详细参数验证
        validation_result = self.validate_params(**params)
        if not validation_result.is_valid:
            return {
                "success": False,
                "errors": validation_result.errors,
                "message": "参数格式验证失败"
            }
        
        try:
            # 获取归档类型配置
            archive_type = params['archive_type']
            archive_config = None
            
            # 从ConfigDTO获取归档类型配置
            if archive_type in self.config_dto.archive_types:
                archive_config = self.config_dto.archive_types[archive_type]
            else:
                # 如果直接查找失败，尝试通过value字段匹配
                for type_name, type_config in self.config_dto.archive_types.items():
                    if type_config.value == archive_type:
                        archive_config = type_config
                        break
            
            if not archive_config:
                return {
                    "success": False,
                    "errors": [f"未找到归档类型 '{archive_type}' 的配置"],
                    "message": "归档类型配置错误"
                }
            
            # 生成路径变量
            path_vars = {
                'source_name': params['source_name'],
                'data_type': params['data_type']
            }
            
            # 处理日期参数
            if archive_type == 'DAILY' and 'date' in params:
                date_value = params['date']
                path_vars.update({
                    'year_month': date_value[:6],
                    'day': date_value[6:8],
                    'year': date_value[:4],
                    'month': date_value[4:6]
                })
            elif archive_type == 'MONTHLY' and 'date' in params:
                date_value = params['date']
                if len(date_value) == 8:
                    # 8位日期，取前6位作为年月
                    year_month = date_value[:6]
                else:
                    # 6位年月格式
                    year_month = date_value
                path_vars.update({
                    'year_month': year_month,
                    'year': year_month[:4],
                    'month': year_month[4:6]
                })
            elif archive_type == 'QUARTERLY' and 'date' in params:
                date_value = params['date']
                year = date_value[:4]
                month_day = date_value[4:8]
                # 根据月日确定季度
                if month_day == '0331':
                    quarter = 'Q1'
                elif month_day == '0630':
                    quarter = 'Q2'
                elif month_day == '0930':
                    quarter = 'Q3'
                elif month_day == '1231':
                    quarter = 'Q4'
                else:
                    quarter = 'Q1'  # 默认值，理论上不会到达这里
                
                path_vars.update({
                    'year_quarter': f"{year}{quarter}",
                    'year': year,
                    'quarter': quarter
                })
            elif archive_type == 'symbol' and 'symbol' in params:
                # 处理股票代码参数
                symbol_value = params['symbol']
                path_vars.update({
                    'symbol': symbol_value
                })
            # SNAPSHOT类型不需要处理日期参数
            
            # 生成子路径
            path_pattern = archive_config.path_pattern
            sub_path = path_pattern.format(**path_vars)
            
            # 获取基础路径配置
            base_path = Path(self.config_dto.base_path)
            landing_subpath = self.config_dto.paths.landing_subpath
            archive_subpath = self.config_dto.paths.archive_subpath
            
            # 生成完整路径
            landing_dir = base_path / landing_subpath / sub_path
            archive_dir = base_path / archive_subpath / sub_path
            
            # 获取文件配置
            filename_template = self.config_dto.file_config.filename_template
            supported_formats = self.config_dto.file_config.supported_formats
            default_format = self.config_dto.file_config.default_format
            
            # 生成文件路径信息
            file_paths = {}
            for file_format in supported_formats:
                filename = filename_template.format(file_type=file_format)
                file_paths[file_format] = {
                    "landing_path": str(landing_dir / filename),
                    "archive_path": str(archive_dir / filename),
                    "filename": filename,
                    "directory": {
                        "landing": str(landing_dir),
                        "archive": str(archive_dir)
                    }
                }
            
            return {
                "success": True,
                "archive_type": archive_type,
                "data_type": params['data_type'],
                "base_path": str(base_path),
                "sub_path": sub_path,
                "supported_formats": supported_formats,
                "default_format": default_format,
                "file_paths": file_paths,
                "params": params
            }
            
        except Exception as e:
            return {
                "success": False,
                "errors": [f"路径生成失败: {str(e)}"]
            }
    
    def get_path_info(self, source_name: str, data_type: str, archive_type: str, date: Optional[str] = None, symbol: Optional[str] = None) -> Dict[str, Any]:
        """获取路径信息的便捷方法
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            archive_type: 归档类型
            date: 日期参数（SNAPSHOT类型可忽略，DAILY要求8位，MONTHLY支持6位或8位）
            symbol: 股票代码参数（symbol类型需要）
            
        Returns:
            Dict[str, Any]: 路径信息JSON
        """
        params = {
            'source_name': source_name,
            'data_type': data_type,
            'archive_type': archive_type
        }
        if date is not None:
            params['date'] = date
        if symbol is not None:
            params['symbol'] = symbol
        return self.generate_paths(**params)
    
    def get_config_info(self) -> Dict[str, Any]:
        """获取配置信息
        
        Returns:
            Dict[str, Any]: 配置信息
        """
        return {
            "archive_types": {
                name: {
                    "value": config.value,
                    "description": config.description,
                    "path_pattern": config.path_pattern,
                    "enabled": config.enabled,
                    "required_fields": config.validation_rules.get('required_fields', []),
                    "optional_fields": config.validation_rules.get('optional_fields', [])
                }
                for name, config in self.config_dto.archive_types.items()
                if config.enabled
            },
            "file_config": {
                "filename_template": self.config_dto.file_config.filename_template,
                "supported_formats": self.config_dto.file_config.supported_formats,
                "default_format": self.config_dto.file_config.default_format
            },
            "base_path": self.config_dto.base_path,
            "paths": {
                "landing_subpath": self.config_dto.paths.landing_subpath,
                "archive_subpath": self.config_dto.paths.archive_subpath
            }
        }