"""DTO验证模块

基于配置文件动态生成验证规则，提供简洁的调用逻辑来满足数据验证需求。
使用ConfigManager从dto_validation_config.json中加载验证规则。
"""

import re
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from ..core.config_manager import get_config_manager


@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    field_name: str
    value: Any
    error_message: Optional[str] = None


class DTOValidator:
    """DTO验证器
    
    基于配置文件动态生成验证规则，支持：
    - 字符串模式匹配
    - 枚举值验证
    - 数值范围验证
    - 数组类型验证
    """
    
    def __init__(self):
        """初始化验证器，从配置管理器加载验证规则"""
        self.config_manager = get_config_manager()
        self._load_validation_rules()
    
    def _load_validation_rules(self) -> None:
        """从配置文件加载验证规则"""
        # 获取DTO验证配置
        dto_config = self.config_manager._dto_validation_config
        if dto_config and 'field_validations' in dto_config:
            self.validation_rules = dto_config['field_validations']
        else:
            self.validation_rules = {}
    
    def validate_field(self, field_name: str, value: Any) -> ValidationResult:
        """验证单个字段
        
        Args:
            field_name: 字段名称
            value: 字段值
            
        Returns:
            ValidationResult: 验证结果
        """
        if field_name not in self.validation_rules:
            return ValidationResult(
                is_valid=True,
                field_name=field_name,
                value=value
            )
        
        rule = self.validation_rules[field_name]
        
        # 类型验证
        if not self._validate_type(value, rule):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                value=value,
                error_message=rule.get('error_message', f'{field_name}类型不匹配')
            )
        
        # 模式验证
        if not self._validate_pattern(value, rule):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                value=value,
                error_message=rule.get('error_message', f'{field_name}格式不正确')
            )
        
        # 枚举值验证
        if not self._validate_enum(value, rule):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                value=value,
                error_message=rule.get('error_message', f'{field_name}值不在允许范围内')
            )
        
        # 数值范围验证
        if not self._validate_range(value, rule):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                value=value,
                error_message=rule.get('error_message', f'{field_name}值超出允许范围')
            )
        
        # 数组验证
        if not self._validate_array(value, rule):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                value=value,
                error_message=rule.get('error_message', f'{field_name}数组格式不正确')
            )
        
        return ValidationResult(
            is_valid=True,
            field_name=field_name,
            value=value
        )
    
    def _validate_type(self, value: Any, rule: Dict[str, Any]) -> bool:
        """验证数据类型"""
        expected_type = rule.get('type')
        if not expected_type:
            return True
        
        type_mapping = {
            'string': str,
            'integer': int,
            'float': float,
            'boolean': bool,
            'array': list
        }
        
        expected_python_type = type_mapping.get(expected_type)
        if expected_python_type:
            return isinstance(value, expected_python_type)
        
        return True
    
    def _validate_pattern(self, value: Any, rule: Dict[str, Any]) -> bool:
        """验证正则表达式模式"""
        pattern = rule.get('pattern')
        if not pattern or not isinstance(value, str):
            return True
        
        try:
            return bool(re.match(pattern, value))
        except re.error:
            return False
    
    def _validate_enum(self, value: Any, rule: Dict[str, Any]) -> bool:
        """验证枚举值"""
        enum_values = rule.get('enum')
        if not enum_values:
            return True
        
        return value in enum_values
    
    def _validate_range(self, value: Any, rule: Dict[str, Any]) -> bool:
        """验证数值范围"""
        if not isinstance(value, (int, float)):
            return True
        
        minimum = rule.get('minimum')
        maximum = rule.get('maximum')
        
        if minimum is not None and value < minimum:
            return False
        
        if maximum is not None and value > maximum:
            return False
        
        return True
    
    def _validate_array(self, value: Any, rule: Dict[str, Any]) -> bool:
        """验证数组类型"""
        if rule.get('type') != 'array' or not isinstance(value, list):
            return True
        
        items_rule = rule.get('items')
        if not items_rule:
            return True
        
        # 验证数组中每个元素
        for item in value:
            if not self._validate_type(item, items_rule):
                return False
        
        return True
    
    def validate_multiple(self, data: Dict[str, Any]) -> List[ValidationResult]:
        """批量验证多个字段
        
        Args:
            data: 包含多个字段的数据字典
            
        Returns:
            List[ValidationResult]: 所有字段的验证结果列表
        """
        results = []
        for field_name, value in data.items():
            result = self.validate_field(field_name, value)
            results.append(result)
        return results
    
    def is_valid(self, data: Dict[str, Any]) -> bool:
        """检查数据是否全部有效
        
        Args:
            data: 要验证的数据字典
            
        Returns:
            bool: 如果所有字段都有效则返回True，否则返回False
        """
        results = self.validate_multiple(data)
        return all(result.is_valid for result in results)
    
    def get_errors(self, data: Dict[str, Any]) -> List[str]:
        """获取所有验证错误信息
        
        Args:
            data: 要验证的数据字典
            
        Returns:
            List[str]: 错误信息列表
        """
        results = self.validate_multiple(data)
        return [result.error_message for result in results if not result.is_valid and result.error_message]
    
    def get_field_rule(self, field_name: str) -> Optional[Dict[str, Any]]:
        """获取指定字段的验证规则
        
        Args:
            field_name: 字段名称
            
        Returns:
            Optional[Dict[str, Any]]: 验证规则字典，如果不存在则返回None
        """
        return self.validation_rules.get(field_name)
    
    def get_all_rules(self) -> Dict[str, Any]:
        """获取所有验证规则
        
        Returns:
            Dict[str, Any]: 所有验证规则的字典
        """
        return self.validation_rules.copy()
    
    def reload_rules(self) -> None:
        """重新加载验证规则"""
        self._load_validation_rules()


# 全局验证器实例
_global_validator: Optional[DTOValidator] = None


def get_validator() -> DTOValidator:
    """获取全局验证器实例"""
    global _global_validator
    if _global_validator is None:
        _global_validator = DTOValidator()
    return _global_validator


# 便捷函数
def validate(field_name: str, value: Any) -> ValidationResult:
    """验证单个字段的便捷函数
    
    Args:
        field_name: 字段名称
        value: 字段值
        
    Returns:
        ValidationResult: 验证结果
    """
    return get_validator().validate_field(field_name, value)


def validate_data(data: Dict[str, Any]) -> List[ValidationResult]:
    """批量验证数据的便捷函数
    
    Args:
        data: 要验证的数据字典
        
    Returns:
        List[ValidationResult]: 验证结果列表
    """
    return get_validator().validate_multiple(data)


def is_valid_data(data: Dict[str, Any]) -> bool:
    """检查数据是否有效的便捷函数
    
    Args:
        data: 要验证的数据字典
        
    Returns:
        bool: 数据是否有效
    """
    return get_validator().is_valid(data)


def get_validation_errors(data: Dict[str, Any]) -> List[str]:
    """获取验证错误的便捷函数
    
    Args:
        data: 要验证的数据字典
        
    Returns:
        List[str]: 错误信息列表
    """
    return get_validator().get_errors(data)


# 特定字段验证的便捷函数
def validate_monthly_date(value: str) -> ValidationResult:
    """验证月度日期格式"""
    return validate('monthly_date', value)


def validate_trade_date(value: str) -> ValidationResult:
    """验证交易日期格式"""
    return validate('trade_date', value)


def validate_start_date(value: str) -> ValidationResult:
    """验证开始日期格式"""
    return validate('start_date', value)


def validate_end_date(value: str) -> ValidationResult:
    """验证结束日期格式"""
    return validate('end_date', value)


def validate_archive_types(value: str) -> ValidationResult:
    """验证归档类型"""
    return validate('archive_types', value)


def validate_storage_type(value: str) -> ValidationResult:
    """验证存储类型"""
    return validate('storage_type', value)


def validate_data_source(value: str) -> ValidationResult:
    """验证数据源"""
    return validate('data_source', value)


def validate_lookback_periods(value: int) -> ValidationResult:
    """验证回溯周期"""
    return validate('lookback_periods', value)


def validate_required_params(value: List[str]) -> ValidationResult:
    """验证必需参数列表"""
    return validate('required_params', value)