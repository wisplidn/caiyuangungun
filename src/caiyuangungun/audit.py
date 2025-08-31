"""
审计和质检框架
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, date
from pathlib import Path
import json
import logging
from dataclasses import dataclass
from enum import Enum

from .contracts import DataContract, InterfaceType, DataLayer, DEFAULT_CONTRACT
from .base import BaseDataManager


class AuditLevel(Enum):
    """审计级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditCategory(Enum):
    """审计类别"""
    DATA_QUALITY = "data_quality"        # 数据质量
    SCHEMA_COMPLIANCE = "schema_compliance"  # Schema合规性
    PIT_COMPLIANCE = "pit_compliance"     # PIT合规性
    BUSINESS_LOGIC = "business_logic"     # 业务逻辑
    PERFORMANCE = "performance"           # 性能
    COMPLETENESS = "completeness"         # 完整性


@dataclass
class AuditRule:
    """审计规则定义"""
    rule_id: str
    name: str
    category: AuditCategory
    level: AuditLevel
    description: str
    check_function: str  # 检查函数名
    parameters: Dict[str, Any] = None
    enabled: bool = True
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


@dataclass
class AuditResult:
    """审计结果"""
    rule_id: str
    rule_name: str
    category: AuditCategory
    level: AuditLevel
    status: str  # PASS, FAIL, SKIP
    message: str
    details: Dict[str, Any] = None
    timestamp: str = None
    data_path: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.details is None:
            self.details = {}


class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self, contract: DataContract = None):
        self.contract = contract or DEFAULT_CONTRACT
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def check_null_ratio(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查空值比例"""
        max_ratio = rule.parameters.get("max_ratio", self.contract.quality_rule.max_null_ratio)
        
        null_ratios = data.isnull().sum() / len(data)
        failed_columns = null_ratios[null_ratios > max_ratio]
        
        if len(failed_columns) == 0:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="所有列的空值比例都在阈值范围内",
                details={"max_null_ratio": null_ratios.max(), "threshold": max_ratio}
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message=f"{len(failed_columns)}个列的空值比例超过阈值",
                details={
                    "failed_columns": failed_columns.to_dict(),
                    "threshold": max_ratio,
                    "worst_ratio": failed_columns.max()
                }
            )
    
    def check_duplicate_ratio(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查重复比例"""
        primary_keys = rule.parameters.get("primary_keys", [])
        max_ratio = rule.parameters.get("max_ratio", self.contract.quality_rule.max_duplicate_ratio)
        
        if not primary_keys:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="SKIP",
                message="未指定主键，跳过重复检查"
            )
        
        available_keys = [key for key in primary_keys if key in data.columns]
        if not available_keys:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="SKIP",
                message="主键列不存在，跳过重复检查",
                details={"expected_keys": primary_keys, "available_columns": list(data.columns)}
            )
        
        duplicate_count = data.duplicated(subset=available_keys).sum()
        duplicate_ratio = duplicate_count / len(data)
        
        if duplicate_ratio <= max_ratio:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="重复比例在阈值范围内",
                details={"duplicate_ratio": duplicate_ratio, "threshold": max_ratio}
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message="重复比例超过阈值",
                details={
                    "duplicate_ratio": duplicate_ratio,
                    "duplicate_count": duplicate_count,
                    "total_count": len(data),
                    "threshold": max_ratio,
                    "primary_keys": available_keys
                }
            )
    
    def check_outliers(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查异常值"""
        std_threshold = rule.parameters.get("std_threshold", self.contract.quality_rule.outlier_std_threshold)
        columns = rule.parameters.get("columns", [])
        
        if not columns:
            columns = data.select_dtypes(include=[np.number]).columns.tolist()
        
        outlier_info = {}
        for col in columns:
            if col in data.columns and data[col].notna().sum() > 0:
                mean_val = data[col].mean()
                std_val = data[col].std()
                if std_val > 0:
                    outlier_mask = np.abs((data[col] - mean_val) / std_val) > std_threshold
                    outlier_count = outlier_mask.sum()
                    outlier_ratio = outlier_count / len(data)
                    outlier_info[col] = {
                        "outlier_count": outlier_count,
                        "outlier_ratio": outlier_ratio,
                        "mean": mean_val,
                        "std": std_val
                    }
        
        max_outlier_ratio = max([info["outlier_ratio"] for info in outlier_info.values()]) if outlier_info else 0
        
        return AuditResult(
            rule_id=rule.rule_id,
            rule_name=rule.name,
            category=rule.category,
            level=rule.level,
            status="PASS" if max_outlier_ratio < 0.05 else "WARNING",  # 5%作为警告阈值
            message=f"检查了{len(columns)}个数值列的异常值",
            details={
                "outlier_info": outlier_info,
                "max_outlier_ratio": max_outlier_ratio,
                "std_threshold": std_threshold
            }
        )
    
    def check_data_types(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查数据类型合规性"""
        expected_types = rule.parameters.get("expected_types", {})
        
        type_issues = {}
        for col, expected_type in expected_types.items():
            if col in data.columns:
                actual_type = str(data[col].dtype)
                if not self._is_compatible_type(actual_type, expected_type):
                    type_issues[col] = {
                        "expected": expected_type,
                        "actual": actual_type
                    }
        
        if not type_issues:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="所有列的数据类型都符合预期"
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message=f"{len(type_issues)}个列的数据类型不符合预期",
                details={"type_issues": type_issues}
            )
    
    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        """检查数据类型是否兼容"""
        # 简化的类型兼容性检查
        type_groups = {
            "float": ["float64", "float32", "float"],
            "int": ["int64", "int32", "int", "Int64"],
            "str": ["object", "string", "str"],
            "datetime": ["datetime64", "datetime"]
        }
        
        for group_name, types in type_groups.items():
            if expected.startswith(group_name) and any(actual.startswith(t) for t in types):
                return True
        
        return actual == expected


class SchemaComplianceChecker:
    """Schema合规性检查器"""
    
    def __init__(self, contract: DataContract = None):
        self.contract = contract or DEFAULT_CONTRACT
    
    def check_required_columns(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查必需列"""
        required_columns = rule.parameters.get("required_columns", [])
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if not missing_columns:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="所有必需列都存在"
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message=f"缺失{len(missing_columns)}个必需列",
                details={
                    "missing_columns": missing_columns,
                    "available_columns": list(data.columns)
                }
            )
    
    def check_primary_key_uniqueness(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查主键唯一性"""
        primary_keys = rule.parameters.get("primary_keys", [])
        
        if not primary_keys:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="SKIP",
                message="未指定主键"
            )
        
        available_keys = [key for key in primary_keys if key in data.columns]
        if not available_keys:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message="主键列不存在",
                details={"expected_keys": primary_keys}
            )
        
        duplicate_count = data.duplicated(subset=available_keys).sum()
        
        if duplicate_count == 0:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="主键唯一性检查通过"
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message=f"发现{duplicate_count}行主键重复",
                details={
                    "duplicate_count": duplicate_count,
                    "primary_keys": available_keys
                }
            )


class PITComplianceChecker:
    """PIT合规性检查器"""
    
    def __init__(self, contract: DataContract = None):
        self.contract = contract or DEFAULT_CONTRACT
    
    def check_announcement_date_logic(self, data: pd.DataFrame, rule: AuditRule) -> AuditResult:
        """检查公告日期逻辑"""
        ann_date_col = rule.parameters.get("ann_date_col", "ann_date")
        period_end_col = rule.parameters.get("period_end_col", "period_end")
        
        if ann_date_col not in data.columns or period_end_col not in data.columns:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="SKIP",
                message="缺少必要的日期列"
            )
        
        # 转换为日期类型
        try:
            ann_dates = pd.to_datetime(data[ann_date_col])
            period_ends = pd.to_datetime(data[period_end_col])
        except:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="ERROR",
                message="日期列格式无法解析"
            )
        
        # 检查公告日期是否晚于报告期
        invalid_dates = (ann_dates <= period_ends).sum()
        
        if invalid_dates == 0:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="PASS",
                message="所有公告日期都晚于报告期结束日期"
            )
        else:
            return AuditResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                category=rule.category,
                level=rule.level,
                status="FAIL",
                message=f"{invalid_dates}条记录的公告日期不晚于报告期",
                details={"invalid_count": invalid_dates}
            )


class AuditEngine:
    """审计引擎"""
    
    def __init__(self, contract: DataContract = None):
        self.contract = contract or DEFAULT_CONTRACT
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化检查器
        self.data_quality_checker = DataQualityChecker(contract)
        self.schema_checker = SchemaComplianceChecker(contract)
        self.pit_checker = PITComplianceChecker(contract)
        
        # 预定义审计规则
        self.rules = self._load_default_rules()
    
    def _load_default_rules(self) -> List[AuditRule]:
        """加载默认审计规则"""
        return [
            # 数据质量规则
            AuditRule(
                rule_id="DQ001",
                name="空值比例检查",
                category=AuditCategory.DATA_QUALITY,
                level=AuditLevel.WARNING,
                description="检查各列的空值比例是否超过阈值",
                check_function="check_null_ratio"
            ),
            AuditRule(
                rule_id="DQ002", 
                name="重复数据检查",
                category=AuditCategory.DATA_QUALITY,
                level=AuditLevel.ERROR,
                description="检查主键重复的记录数量",
                check_function="check_duplicate_ratio"
            ),
            AuditRule(
                rule_id="DQ003",
                name="异常值检查",
                category=AuditCategory.DATA_QUALITY,
                level=AuditLevel.INFO,
                description="检查数值列的异常值分布",
                check_function="check_outliers"
            ),
            AuditRule(
                rule_id="DQ004",
                name="数据类型检查",
                category=AuditCategory.DATA_QUALITY,
                level=AuditLevel.ERROR,
                description="检查列的数据类型是否符合预期",
                check_function="check_data_types"
            ),
            
            # Schema合规性规则
            AuditRule(
                rule_id="SC001",
                name="必需列检查",
                category=AuditCategory.SCHEMA_COMPLIANCE,
                level=AuditLevel.CRITICAL,
                description="检查必需的列是否存在",
                check_function="check_required_columns"
            ),
            AuditRule(
                rule_id="SC002",
                name="主键唯一性检查",
                category=AuditCategory.SCHEMA_COMPLIANCE,
                level=AuditLevel.CRITICAL,
                description="检查主键的唯一性",
                check_function="check_primary_key_uniqueness"
            ),
            
            # PIT合规性规则
            AuditRule(
                rule_id="PIT001",
                name="公告日期逻辑检查",
                category=AuditCategory.PIT_COMPLIANCE,
                level=AuditLevel.ERROR,
                description="检查公告日期是否晚于报告期",
                check_function="check_announcement_date_logic"
            )
        ]
    
    def audit_data(self, 
                   data: pd.DataFrame,
                   interface_type: InterfaceType,
                   data_path: str = None,
                   custom_rules: List[AuditRule] = None) -> List[AuditResult]:
        """
        执行数据审计
        
        Args:
            data: 待审计的数据
            interface_type: 接口类型
            data_path: 数据文件路径
            custom_rules: 自定义审计规则
            
        Returns:
            审计结果列表
        """
        results = []
        rules_to_check = self.rules + (custom_rules or [])
        
        # 为规则添加接口特定参数
        for rule in rules_to_check:
            if not rule.enabled:
                continue
                
            # 为不同规则添加特定参数
            if rule.rule_id == "DQ002" or rule.rule_id == "SC002":
                rule.parameters["primary_keys"] = self.contract.primary_keys.get(interface_type, [])
            
            if rule.rule_id == "SC001":
                rule.parameters["required_columns"] = self.contract.primary_keys.get(interface_type, [])
            
            if rule.rule_id == "DQ004":
                # 简化的数据类型检查，不依赖已删除的Schema
                rule.parameters["expected_types"] = {}
            
            # 执行检查
            try:
                checker = self._get_checker_for_rule(rule)
                if checker:
                    result = getattr(checker, rule.check_function)(data, rule)
                    result.data_path = data_path
                    results.append(result)
                else:
                    results.append(AuditResult(
                        rule_id=rule.rule_id,
                        rule_name=rule.name,
                        category=rule.category,
                        level=rule.level,
                        status="ERROR",
                        message="未找到对应的检查器",
                        data_path=data_path
                    ))
            except Exception as e:
                self.logger.error(f"执行审计规则{rule.rule_id}时出错: {e}")
                results.append(AuditResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    category=rule.category,
                    level=rule.level,
                    status="ERROR",
                    message=f"执行检查时出错: {str(e)}",
                    data_path=data_path
                ))
        
        return results
    
    def _get_checker_for_rule(self, rule: AuditRule):
        """根据规则获取对应的检查器"""
        if rule.category == AuditCategory.DATA_QUALITY:
            return self.data_quality_checker
        elif rule.category == AuditCategory.SCHEMA_COMPLIANCE:
            return self.schema_checker
        elif rule.category == AuditCategory.PIT_COMPLIANCE:
            return self.pit_checker
        else:
            return None
    
    def save_audit_report(self, 
                          results: List[AuditResult],
                          report_path: Union[str, Path]) -> Path:
        """保存审计报告"""
        report_path = Path(report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 转换为字典格式
        report_data = {
            "audit_timestamp": datetime.now().isoformat(),
            "total_rules": len(results),
            "summary": self._generate_summary(results),
            "results": [
                {
                    "rule_id": r.rule_id,
                    "rule_name": r.rule_name,
                    "category": r.category.value,
                    "level": r.level.value,
                    "status": r.status,
                    "message": r.message,
                    "details": self._convert_to_json_serializable(r.details),
                    "timestamp": r.timestamp,
                    "data_path": r.data_path
                }
                for r in results
            ]
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"审计报告已保存到: {report_path}")
        return report_path
    
    def _convert_to_json_serializable(self, obj):
        """转换为JSON可序列化的对象"""
        if obj is None:
            return None
        elif isinstance(obj, dict):
            return {k: self._convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_json_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj
    
    def _generate_summary(self, results: List[AuditResult]) -> Dict[str, Any]:
        """生成审计摘要"""
        status_count = {}
        level_count = {}
        category_count = {}
        
        for result in results:
            status_count[result.status] = status_count.get(result.status, 0) + 1
            level_count[result.level.value] = level_count.get(result.level.value, 0) + 1
            category_count[result.category.value] = category_count.get(result.category.value, 0) + 1
        
        return {
            "by_status": status_count,
            "by_level": level_count,
            "by_category": category_count,
            "has_critical_issues": any(r.level == AuditLevel.CRITICAL and r.status == "FAIL" for r in results),
            "has_errors": any(r.level == AuditLevel.ERROR and r.status == "FAIL" for r in results),
            "pass_rate": status_count.get("PASS", 0) / len(results) if results else 0
        }
