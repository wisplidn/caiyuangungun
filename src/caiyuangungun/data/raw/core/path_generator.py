"""通用数据路径生成器模块

基于JSON配置的参数验证和路径生成器。
输入必要参数，进行有效性验证，返回路径信息的JSON结果。
"""

import json
import re
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
from dataclasses import dataclass

# 使用绝对路径导入避免相对导入问题
import importlib.util
from pathlib import Path as PathLib

# 获取当前文件所在目录
current_dir = PathLib(__file__).parent

# 导入ConfigManager
config_manager_spec = importlib.util.spec_from_file_location("config_manager", str(current_dir / "config_manager.py"))
config_manager_module = importlib.util.module_from_spec(config_manager_spec)
config_manager_spec.loader.exec_module(config_manager_module)
ConfigManager = config_manager_module.ConfigManager


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    errors: List[str]
    
    def add_error(self, error: str):
        """添加错误信息"""
        self.is_valid = False
        self.errors.append(error)


class PathGenerator:
    """通用数据路径生成器
    
    功能：
    1. 输入参数验证
    2. 路径生成
    3. 返回JSON格式的路径信息
    """
    
    def __init__(self, config_name: str = "path_generator"):
        """初始化路径生成器
        
        Args:
            config_name: 配置名称，用于从配置管理器加载配置
        """
        self.config_manager = ConfigManager()
        self.config = self.config_manager.get_archiver_config(config_name)
        

    
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
            return result
        
        # 基本类型验证
        if not isinstance(value, str):
            result.add_error(f"{field_name} 必须是字符串类型")
            return result
        
        # 简单的格式验证
        if field_name == 'archive_type':
            # 从配置管理器获取允许的归档类型
            allowed_values = self.config_manager.get_archive_types()
            if value not in allowed_values:
                result.add_error(f"{field_name} 必须是以下值之一: {allowed_values}")
        elif field_name == 'daily_date':
            if not re.match(r'^\d{8}$', value):
                result.add_error(f"{field_name} 格式不正确，应为8位数字日期格式，如20241231")
        elif field_name == 'monthly_date':
            if not re.match(r'^\d{6}$', value):
                result.add_error(f"{field_name} 格式不正确，应为6位数字年月格式，如202412")
        elif field_name == 'quarterly_date':
            if not re.match(r'^\d{8}$', value):
                result.add_error(f"{field_name} 格式不正确，应为8位数字日期格式，如20231231")
        
        return result
    
    def validate_params(self, **params) -> ValidationResult:
        """验证输入参数
        
        Args:
            **params: 输入参数
            
        Returns:
            ValidationResult: 验证结果
        """
        result = ValidationResult(is_valid=True, errors=[])
        
        # 获取归档类型
        archive_type = params.get('archive_type')
        if not archive_type:
            result.add_error("缺少必需参数: archive_type")
            return result
        
        # 验证归档类型
        archive_type_result = self.validate_field('archive_type', archive_type)
        if not archive_type_result.is_valid:
            result.errors.extend(archive_type_result.errors)
            result.is_valid = False
            return result
        
        # 获取对应归档类型的验证规则
        archive_types_config = self.config.get('archive_types', {})
        archive_config = None
        
        # 直接通过归档类型名称查找配置
        if archive_type in archive_types_config:
            archive_config = archive_types_config[archive_type]
        else:
            # 如果直接查找失败，尝试通过value字段匹配
            for type_name, type_config in archive_types_config.items():
                if type_config.get('value') == archive_type:
                    archive_config = type_config
                    break
        
        if not archive_config:
            result.add_error(f"未找到归档类型 {archive_type} 的配置")
            return result
        
        # 验证必需字段
        validation_rules = archive_config.get('validation_rules', {})
        required_fields = validation_rules.get('required_fields', [])
        
        for field in required_fields:
            if field not in params:
                result.add_error(f"缺少必需参数: {field}")
                continue
            
            # 验证字段值
            field_result = self.validate_field(field, params[field])
            if not field_result.is_valid:
                result.errors.extend(field_result.errors)
        
        # 注意：已移除可选字段验证逻辑
        
        # 如果有错误，标记为无效
        if result.errors:
            result.is_valid = False
        
        return result
    
    def generate_paths(self, **params) -> Dict[str, Any]:
        """生成路径信息
        
        Args:
            **params: 输入参数
            
        Returns:
            Dict[str, Any]: 包含路径信息的JSON结果
        """
        # 先验证参数
        validation_result = self.validate_params(**params)
        if not validation_result.is_valid:
            return {
                "success": False,
                "errors": validation_result.errors
            }
        
        try:
            # 获取归档类型配置
            archive_type = params['archive_type']
            archive_types_config = self.config.get('archive_types', {})
            archive_config = None
            
            # 直接通过归档类型名称查找配置
            if archive_type in archive_types_config:
                archive_config = archive_types_config[archive_type]
            else:
                # 如果直接查找失败，尝试通过value字段匹配
                for type_name, type_config in archive_types_config.items():
                    if type_config.get('value') == archive_type:
                        archive_config = type_config
                        break
            
            # 生成路径变量
            path_vars = {
                'source_name': params['source_name'],
                'data_type': params['data_type']
            }
            
            # 处理日期参数
            if 'daily_date' in params:
                daily_date = params['daily_date']
                path_vars.update({
                    'year_month': daily_date[:6],
                    'day': daily_date[6:8],
                    'year': daily_date[:4],
                    'month': daily_date[4:6]
                })
            elif 'monthly_date' in params:
                monthly_date = params['monthly_date']
                path_vars.update({
                    'year_month': monthly_date,
                    'year': monthly_date[:4],
                    'month': monthly_date[4:6]
                })
            elif 'quarterly_date' in params:
                quarterly_date = params['quarterly_date']
                # 季报日期转换为年季度格式
                year = quarterly_date[:4]
                month = quarterly_date[4:6]
                # 根据月份确定季度
                if month in ['03']:
                    quarter = 'Q1'
                elif month in ['06']:
                    quarter = 'Q2'
                elif month in ['09']:
                    quarter = 'Q3'
                elif month in ['12']:
                    quarter = 'Q4'
                else:
                    quarter = 'Q1'  # 默认值
                
                path_vars.update({
                    'year_quarter': f"{year}{quarter}",
                    'year': year,
                    'quarter': quarter
                })
            
            # 生成子路径
            path_pattern = archive_config.get('path_pattern', '{data_type}')
            sub_path = path_pattern.format(**path_vars)
            
            # 获取基础路径配置
            base_path = Path(self.config.get('base_path', '/Users/daishun/个人文档/caiyuangungun/data/raw'))
            paths_config = self.config.get('paths', {})
            landing_subpath = paths_config.get('landing_subpath', 'landing')
            archive_subpath = paths_config.get('archive_subpath', 'archive')
            
            # 生成完整路径
            landing_dir = base_path / landing_subpath / sub_path
            archive_dir = base_path / archive_subpath / sub_path
            
            # 获取文件配置
            file_config = self.config.get('file_config', {})
            filename_template = file_config.get('filename_template', 'data.{file_type}')
            supported_formats = file_config.get('supported_formats', ['parquet', 'json'])
            default_format = file_config.get('default_format', 'parquet')
            
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
    
    def get_path_info(self, source_name: str, data_type: str, archive_type: str, **kwargs) -> Dict[str, Any]:
        """获取路径信息的便捷方法
        
        Args:
            source_name: 数据源名称
            data_type: 数据类型
            archive_type: 归档类型
            **kwargs: 其他参数（如daily_date, monthly_date等）
            
        Returns:
            Dict[str, Any]: 路径信息JSON
        """
        params = {
            'source_name': source_name,
            'data_type': data_type,
            'archive_type': archive_type,
            **kwargs
        }
        return self.generate_paths(**params)
    
    def get_config_info(self) -> Dict[str, Any]:
        """获取配置信息
        
        Returns:
            Dict[str, Any]: 配置信息
        """
        return {
            "archive_types": {
                name: {
                    "value": config.get('value'),
                    "description": config.get('description'),
                    "required_fields": config.get('validation_rules', {}).get('required_fields', []),
                    "optional_fields": config.get('validation_rules', {}).get('optional_fields', [])
                }
                for name, config in self.config.get('archive_types', {}).items()
                if config.get('enabled', True)
            },

            "file_config": self.config.get('file_config', {}),
            "base_path": self.config.get('base_path'),
            "paths": self.config.get('paths', {})
        }


# 使用示例
if __name__ == "__main__":
    # 创建路径生成器实例
    generator = PathGenerator()
    
    # 示例1: SNAPSHOT类型
    result1 = generator.get_path_info(
        source_name="example_source",
        data_type="stock_basic",
        archive_type="SNAPSHOT"
    )
    print("SNAPSHOT示例:")
    print(json.dumps(result1, indent=2, ensure_ascii=False))
    
    # 示例2: DAILY类型
    result2 = generator.get_path_info(
        source_name="example_source",
        data_type="daily",
        archive_type="DAILY",
        daily_date="20241231"
    )
    print("\nDAILY示例:")
    print(json.dumps(result2, indent=2, ensure_ascii=False))
    
    # 示例3: MONTHLY类型
    result3 = generator.get_path_info(
        source_name="example_source",
        data_type="monthly_return",
        archive_type="MONTHLY",
        monthly_date="202412"
    )
    print("\nMONTHLY示例:")
    print(json.dumps(result3, indent=2, ensure_ascii=False))
    
    # 示例4: 错误情况
    result4 = generator.get_path_info(
        source_name="example_source",
        data_type="",  # 空字符串，应该失败
        archive_type="DAILY"
    )
    print("\n错误示例:")
    print(json.dumps(result4, indent=2, ensure_ascii=False))