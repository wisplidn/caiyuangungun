"""
通用处理器服务模块

提供通用化的处理器服务，根据配置文件自动寻找processor和具体模块，并依次执行
支持动态加载和执行各种数据处理器
"""

import os
import sys
import logging
import importlib
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
from datetime import datetime

from ..core.config_manager import ConfigManager

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('processor_service')


class ProcessorService:
    """通用处理器服务类"""
    
    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化处理器服务
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager or ConfigManager()
        self.project_root = self.config_manager.project_root
        self._processor_cache = {}
        
    def get_available_processors(self) -> List[str]:
        """
        获取所有可用的处理器列表
        
        Returns:
            处理器名称列表
        """
        try:
            all_configs = self.config_manager.get_all_processor_configs()
            return list(all_configs.keys())
        except Exception as e:
            logger.error(f"获取处理器列表失败: {e}")
            return []
    
    def _load_processor(self, processor_name: str) -> Any:
        """
        动态加载处理器类
        
        Args:
            processor_name: 处理器名称
            
        Returns:
            处理器类实例
        """
        if processor_name in self._processor_cache:
            return self._processor_cache[processor_name]
        
        try:
            # 首先尝试从配置文件获取模块路径和类名
            processor_config = self.config_manager.get_processor_config(processor_name)
            
            if processor_config and 'module_path' in processor_config and 'class_name' in processor_config:
                module_path = processor_config['module_path']
                class_name = processor_config['class_name']
                logger.info(f"从配置文件加载处理器: {processor_name} from {module_path}")
            else:
                # 回退到推断模式
                module_path = self._get_processor_module_path(processor_name)
                class_name = self._get_processor_class_name(processor_name)
                logger.info(f"推断加载处理器: {processor_name} from {module_path}")
            
            # 动态导入模块
            module = importlib.import_module(module_path)
            processor_class = getattr(module, class_name)
            
            # 创建实例 - 传递完整的processor配置给处理器
            processor_config = self.config_manager.get_processor_config(processor_name)
            processor_instance = processor_class(config=processor_config)
            
            # 缓存实例
            self._processor_cache[processor_name] = processor_instance
            
            return processor_instance
            
        except Exception as e:
            logger.error(f"加载处理器失败 {processor_name}: {e}")
            raise
    
    def _get_processor_module_path(self, processor_name: str) -> str:
        """
        根据处理器名称推断模块路径
        
        Args:
            processor_name: 处理器名称，如 'fin_is_processor'
            
        Returns:
            模块路径字符串
        """
        # 处理器名称到模块路径的映射规则
        if processor_name.startswith('fin_'):
            # 财务数据处理器
            return f"caiyuangungun.data.norm.processors.financials.{processor_name}"
        elif processor_name.endswith('_quotes_processor'):
            # 行情数据处理器
            return f"caiyuangungun.data.norm.processors.quotes.{processor_name}"
        elif processor_name.endswith('_calendar_processor'):
            # 日历数据处理器
            return f"caiyuangungun.data.norm.processors.calendar.{processor_name}"
        elif processor_name.endswith('_ref_processor'):
            # 参考数据处理器
            return f"caiyuangungun.data.norm.processors.ref.{processor_name}"
        else:
            # 默认路径
            return f"caiyuangungun.data.norm.processors.{processor_name}"
    
    def _get_processor_class_name(self, processor_name: str) -> str:
        """
        根据处理器名称推断类名
        
        Args:
            processor_name: 处理器名称，如 'fin_is_processor'
            
        Returns:
            类名字符串
        """
        # 将下划线命名转换为驼峰命名
        parts = processor_name.split('_')
        class_name = ''.join(word.capitalize() for word in parts)
        return class_name
    
    def _resolve_path(self, path: str) -> str:
        """
        解析相对路径为绝对路径
        
        Args:
            path: 相对或绝对路径
            
        Returns:
            绝对路径
        """
        if os.path.isabs(path):
            return path
        return str(self.project_root / path)
    
    def execute_processor(self, processor_name: str, input_path: str = None, output_path: str = None) -> Dict[str, Any]:
        """
        执行指定的处理器
        
        Args:
            processor_name: 处理器名称
            input_path: 输入路径（可选，会从配置中获取）
            output_path: 输出路径（可选，会从配置中获取）
            
        Returns:
            执行结果字典
        """
        start_time = datetime.now()
        logger.info(f"开始执行处理器: {processor_name}")
        
        try:
            # 获取处理器配置
            processor_config = self.config_manager.get_processor_config(processor_name)
            paths_config = self.config_manager.get_processor_paths(processor_name)
            pipeline_config = self.config_manager.get_processor_pipeline(processor_name)
            
            # 解析路径
            input_path = input_path or self._resolve_path(paths_config["input_path"])
            output_path = output_path or self._resolve_path(paths_config["output_path"])
            
            logger.info(f"输入路径: {input_path}")
            logger.info(f"输出路径: {output_path}")
            
            # 检查输入文件
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"输入文件不存在: {input_path}")
            
            # 加载处理器
            processor = self._load_processor(processor_name)
            
            # 读取输入数据
            logger.info("读取输入数据...")
            df = pd.read_parquet(input_path)
            initial_shape = df.shape
            logger.info(f"输入数据形状: {initial_shape}")
            
            # 执行处理流水线
            df = self._execute_pipeline(processor, df, pipeline_config)
            
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # 检查是否有output_parquet_file步骤，如果有则调用它来保存结果
            has_output_step = any(step.get("function") == "output_parquet_file" for step in pipeline_config)
            
            if has_output_step and hasattr(processor, "output_parquet_file"):
                # 使用处理器的output_parquet_file方法保存，确保字段过滤
                logger.info(f"使用output_parquet_file方法保存结果到: {output_path}")
                processor.output_parquet_file(df, output_path)
            else:
                # 如果没有output_parquet_file步骤，则直接保存
                logger.info(f"直接保存结果到: {output_path}")
                df.to_parquet(output_path, index=False)
            
            # 计算执行时间
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # 验证输出文件
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
                verify_df = pd.read_parquet(output_path)
                final_shape = verify_df.shape
            else:
                raise RuntimeError("输出文件创建失败")
            
            result = {
                "processor_name": processor_name,
                "success": True,
                "status": "success",
                "input_path": input_path,
                "output_path": output_path,
                "input_shape": initial_shape,
                "output_shape": final_shape,
                "initial_shape": initial_shape,
                "final_shape": final_shape,
                "execution_time": execution_time,
                "file_size_mb": file_size,
                "pipeline_steps": len(pipeline_config)
            }
            
            logger.info(f"处理器执行成功: {processor_name}")
            logger.info(f"数据形状变化: {initial_shape} -> {final_shape}")
            logger.info(f"执行时间: {execution_time:.2f}秒")
            logger.info(f"输出文件大小: {file_size:.2f}MB")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            error_result = {
                "processor_name": processor_name,
                "success": False,
                "status": "error",
                "error": str(e),
                "error_message": str(e),
                "execution_time": execution_time
            }
            
            logger.error(f"处理器执行失败: {processor_name}, 错误: {e}")
            return error_result
    
    def _execute_pipeline(self, processor: Any, df: pd.DataFrame, pipeline_config: List[Dict]) -> pd.DataFrame:
        """
        执行处理流水线
        
        Args:
            processor: 处理器实例
            df: 输入数据
            pipeline_config: 流水线配置
            
        Returns:
            处理后的数据
        """
        logger.info(f"开始执行处理流水线，共{len(pipeline_config)}个步骤")
        
        for i, step in enumerate(pipeline_config, 1):
            function_name = step.get("function")
            description = step.get("description", "")
            category = step.get("category", "")
            
            logger.info(f"步骤{i}: {function_name} ({category}) - {description}")
            
            try:
                # 记录步骤开始时的数据状态
                before_shape = df.shape
                step_start_time = datetime.now()
                
                # 执行处理函数
                if hasattr(processor, function_name):
                    method = getattr(processor, function_name)
                    if function_name == "output_parquet_file":
                        # output_parquet_file在主流程中处理，这里跳过
                        logger.info(f"跳过{function_name}，将在主流程中处理")
                        continue
                    else:
                        df = method(df)
                else:
                    logger.warning(f"处理器中未找到方法: {function_name}")
                    continue
                
                # 记录步骤执行结果
                after_shape = df.shape
                step_end_time = datetime.now()
                step_time = (step_end_time - step_start_time).total_seconds()
                
                logger.info(f"步骤{i}完成: {before_shape} -> {after_shape}, 耗时: {step_time:.2f}秒")
                
            except Exception as e:
                logger.error(f"步骤{i}执行失败: {function_name}, 错误: {e}")
                raise
        
        logger.info("处理流水线执行完成")
        return df
    
    def get_processor_info(self, processor_name: str) -> Dict[str, Any]:
        """
        获取处理器信息
        
        Args:
            processor_name: 处理器名称
            
        Returns:
            处理器信息字典
        """
        try:
            config = self.config_manager.get_processor_config(processor_name)
            paths = self.config_manager.get_processor_paths(processor_name)
            pipeline = self.config_manager.get_processor_pipeline(processor_name)
            
            # 获取模块路径和类名
            module_path = config.get("module_path") or self._get_processor_module_path(processor_name)
            class_name = config.get("class_name") or self._get_processor_class_name(processor_name)
            
            return {
                "name": processor_name,
                "description": config.get("description", ""),
                "module_path": module_path,
                "class_name": class_name,
                "input_path": paths["input_path"],
                "output_path": paths["output_path"],
                "pipeline_steps": len(pipeline),
                "categories": list(set(step.get("category", "") for step in pipeline))
            }
            
        except Exception as e:
            logger.error(f"获取处理器信息失败: {processor_name}, 错误: {e}")
            return None
    
    def list_processors(self) -> None:
        """
        列出所有可用的处理器及其信息
        """
        processors = self.get_available_processors()
        
        if not processors:
            print("未找到可用的处理器")
            return
        
        print(f"可用处理器列表 (共{len(processors)}个):")
        print("-" * 80)
        
        for processor_name in processors:
            info = self.get_processor_info(processor_name)
            if info:
                print(f"名称: {processor_name}")
                print(f"描述: {info.get('description', 'N/A')}")
                print(f"输入: {info.get('input_path', 'N/A')}")
                print(f"输出: {info.get('output_path', 'N/A')}")
                print(f"步骤: {info.get('pipeline_steps', 0)}个")
                print(f"类别: {', '.join(info.get('categories', []))}")
                print("-" * 80)


def create_processor_service() -> ProcessorService:
    """
    创建处理器服务实例的便捷函数
    
    Returns:
        ProcessorService实例
    """
    return ProcessorService()