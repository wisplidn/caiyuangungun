#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Qlib数据转换服务

提供统一的服务接口，用于管理和执行Qlib数据转换：
- 加载配置文件
- 注册转换器
- 执行单个或批量转换
- 生成转换报告
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..core.config_manager import ConfigManager
from ..converter.daily_qlib_converter import DailyQlibConverter
from ..converter.pit_qlib_converter import PITQlibConverter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('qlib_service')


class QlibConversionService:
    """
    Qlib数据转换服务
    
    职责：
    - 加载和管理配置
    - 注册和管理转换器
    - 执行转换任务
    - 生成转换报告
    """
    
    # 转换器类型映射
    CONVERTER_TYPES = {
        'daily': DailyQlibConverter,
        'pit': PITQlibConverter,
    }
    
    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化转换服务
        
        Args:
            config_manager: 配置管理器实例，如果为None则创建新实例
        """
        self.config_manager = config_manager or ConfigManager()
        self.project_root = self.config_manager.project_root
        
        # 加载配置
        self.full_config = self.config_manager.get_all_qlib_conversion_configs()
        
        # 注册的转换器
        self._converters = {}
        
        logger.info(f"QlibConversionService 初始化完成")
        logger.info(f"配置目录: {self.config_manager.config_dir}")
        logger.info(f"项目根目录: {self.project_root}")
    
    
    def get_available_conversions(self) -> List[str]:
        """
        获取所有可用的转换配置
        
        Returns:
            转换配置名称列表
        """
        conversions = self.config_manager.get_qlib_converter_names()
        logger.info(f"可用转换: {conversions}")
        return conversions
    
    def register_converter(self, name: str, 
                          converter_type: str,
                          config: Dict[str, Any],
                          limit_symbols: Optional[int] = None):
        """
        注册转换器
        
        Args:
            name: 转换器名称
            converter_type: 转换器类型 ('daily' 或 'pit')
            config: 转换器配置
            limit_symbols: 限制股票数量
        """
        if converter_type not in self.CONVERTER_TYPES:
            raise ValueError(f"未知的转换器类型: {converter_type}")
        
        # 添加项目根目录到配置
        config['project_root'] = self.project_root
        
        # 创建转换器实例
        converter_class = self.CONVERTER_TYPES[converter_type]
        converter = converter_class(config, limit_symbols)
        
        self._converters[name] = {
            'type': converter_type,
            'instance': converter,
            'config': config
        }
        
        logger.info(f"✅ 已注册转换器: {name} (类型: {converter_type})")
    
    def register_from_config(self, config_key: str, 
                            converter_type: str,
                            limit_symbols: Optional[int] = None):
        """
        从配置文件注册转换器
        
        Args:
            config_key: 配置键名
            converter_type: 转换器类型
            limit_symbols: 限制股票数量
        """
        config = self.config_manager.get_qlib_conversion_config(config_key)
        if not config:
            raise KeyError(f"配置中不存在: {config_key}")
        
        self.register_converter(config_key, converter_type, config, limit_symbols)
    
    def run_converter(self, name: str, validate: bool = True) -> Dict[str, Any]:
        """
        运行单个转换器
        
        Args:
            name: 转换器名称
            validate: 是否执行验证
            
        Returns:
            执行结果
        """
        if name not in self._converters:
            raise KeyError(f"转换器未注册: {name}")
        
        logger.info("="*80)
        logger.info(f"运行转换器: {name}")
        logger.info("="*80)
        
        converter_info = self._converters[name]
        converter = converter_info['instance']
        
        # 执行转换
        result = converter.run()
        
        # 执行验证（如果需要）
        if validate and result['success']:
            try:
                logger.info("\n执行数据验证...")
                converter.validate()
                result['validation'] = 'passed'
            except Exception as e:
                logger.error(f"验证失败: {e}")
                result['validation'] = 'failed'
                result['validation_error'] = str(e)
        
        return result
    
    def run_all_converters(self, validate: bool = True) -> Dict[str, Any]:
        """
        运行所有注册的转换器
        
        Args:
            validate: 是否执行验证
            
        Returns:
            所有转换器的执行结果
        """
        logger.info("="*80)
        logger.info(f"批量运行转换器（共 {len(self._converters)} 个）")
        logger.info("="*80)
        
        results = {
            'total': len(self._converters),
            'success_count': 0,
            'failed_count': 0,
            'details': {}
        }
        
        for name in self._converters.keys():
            try:
                result = self.run_converter(name, validate)
                results['details'][name] = result
                
                if result['success']:
                    results['success_count'] += 1
                else:
                    results['failed_count'] += 1
                    
            except Exception as e:
                logger.error(f"❌ 转换器 {name} 执行异常: {e}")
                results['details'][name] = {
                    'success': False,
                    'error': str(e)
                }
                results['failed_count'] += 1
        
        # 生成总结
        logger.info("="*80)
        logger.info("批量转换完成")
        logger.info(f"  总数: {results['total']}")
        logger.info(f"  成功: {results['success_count']}")
        logger.info(f"  失败: {results['failed_count']}")
        logger.info("="*80)
        
        return results
    
    def generate_report(self, results: Dict[str, Any], 
                       output_path: Optional[Path] = None) -> str:
        """
        生成转换报告
        
        Args:
            results: 转换结果
            output_path: 报告输出路径
            
        Returns:
            报告内容
        """
        report_lines = [
            "# Qlib数据转换报告",
            "",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 总体情况",
            "",
            f"- 总转换任务: {results.get('total', 0)}",
            f"- 成功: {results.get('success_count', 0)}",
            f"- 失败: {results.get('failed_count', 0)}",
            "",
            "## 详细结果",
            ""
        ]
        
        # 添加每个转换器的详细结果
        for name, detail in results.get('details', {}).items():
            status = "✅ 成功" if detail.get('success') else "❌ 失败"
            report_lines.extend([
                f"### {name}",
                "",
                f"- 状态: {status}",
            ])
            
            if detail.get('success'):
                stages = detail.get('stages', {})
                if 'load' in stages:
                    report_lines.append(f"- 加载: {stages['load'].get('rows', 0):,} 行")
                if 'convert' in stages:
                    report_lines.append(f"- 转换: {stages['convert'].get('rows', 0):,} 行")
                if 'validation' in detail:
                    report_lines.append(f"- 验证: {detail['validation']}")
            else:
                error = detail.get('error', '未知错误')
                report_lines.append(f"- 错误: {error}")
            
            report_lines.append("")
        
        report = "\n".join(report_lines)
        
        # 保存报告
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"✅ 报告已保存: {output_path}")
        
        return report


def create_daily_converter_service(config_manager: ConfigManager = None,
                                   limit_symbols: Optional[int] = None) -> QlibConversionService:
    """
    创建日频数据转换服务（便捷函数）
    
    Args:
        config_manager: 配置管理器实例，如果为None则创建新实例
        limit_symbols: 限制股票数量
        
    Returns:
        配置好的服务实例
    """
    service = QlibConversionService(config_manager)
    service.register_from_config('daily_quotes', 'daily', limit_symbols)
    return service


def create_pit_converter_service(config_manager: ConfigManager = None,
                                 data_types: Optional[List[str]] = None,
                                 limit_symbols: Optional[int] = None) -> QlibConversionService:
    """
    创建PIT数据转换服务（便捷函数）
    
    Args:
        config_manager: 配置管理器实例，如果为None则创建新实例
        data_types: 数据类型列表（如 ['income_statement', 'balance_sheet']）
        limit_symbols: 限制股票数量
        
    Returns:
        配置好的服务实例
    """
    service = QlibConversionService(config_manager)
    
    # 默认数据类型
    if data_types is None:
        data_types = [
            'income_statement',
            'balance_sheet',
            'cash_flow',
            'financial_indicator',
            'dividend'
        ]
    
    # 注册每个数据类型的转换器
    for data_type in data_types:
        if data_type in service.full_config:
            service.register_from_config(data_type, 'pit', limit_symbols)
        else:
            logger.warning(f"配置中不存在: {data_type}")
    
    return service


def create_full_converter_service(config_manager: ConfigManager = None,
                                  limit_symbols: Optional[int] = None) -> QlibConversionService:
    """
    创建完整转换服务（日频+PIT）（便捷函数）
    
    Args:
        config_manager: 配置管理器实例，如果为None则创建新实例
        limit_symbols: 限制股票数量
        
    Returns:
        配置好的服务实例
    """
    service = QlibConversionService(config_manager)
    
    # 注册日频转换器
    if 'daily_quotes' in service.full_config:
        service.register_from_config('daily_quotes', 'daily', limit_symbols)
    
    # 注册所有PIT转换器
    pit_types = [
        'income_statement',
        'balance_sheet', 
        'cash_flow',
        'financial_indicator',
        'dividend'
    ]
    
    for data_type in pit_types:
        if data_type in service.full_config:
            service.register_from_config(data_type, 'pit', limit_symbols)
    
    return service

