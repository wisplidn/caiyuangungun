#!/usr/bin/env python3
"""
数据清洗服务示例
支持任意指定数据源的数据清洗，参考example_processor_service.py的设计模式
支持运行指定数据源或全部启用数据源的清洗
"""

import sys
import os
import logging
from pathlib import Path
from typing import Optional, Dict, List

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('example_data_cleaning_service')

# 动态添加项目路径到sys.path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.norm.services.basic_data_cleaning_service import DataCleaningService
import pandas as pd


class DataCleaningServiceExample:
    """数据清洗服务示例类"""
    
    def __init__(self):
        self.service = DataCleaningService()
        self.enabled_sources = self._get_enabled_data_sources()
    
    def _get_enabled_data_sources(self) -> Dict[str, List[str]]:
        """
        获取所有enabled=true的数据源
        
        Returns:
            Dict[str, List[str]]: 按数据类型分组的启用数据源
        """
        config = self.service.config_manager.get_basic_cleaning_config()
        
        enabled_sources = {}
        
        for data_type, type_config in config.items():
            if 'cleaning_pipelines' in type_config:
                enabled_list = []
                for source_name, source_config in type_config['cleaning_pipelines'].items():
                    if source_config.get('enabled', True):  # 默认为True
                        enabled_list.append(source_name)
                
                if enabled_list:
                    enabled_sources[data_type] = enabled_list
        
        return enabled_sources
    
    def list_available_data_sources(self):
        """列出所有可用的数据源"""
        logger.info("=== 可用的数据源列表 ===")
        
        if not self.enabled_sources:
            logger.warning("❌ 没有找到启用的数据源")
            return
        
        total_sources = 0
        for data_type, sources in self.enabled_sources.items():
            logger.info(f"\n📊 {data_type}:")
            for source in sources:
                logger.info(f"  ✓ {source}")
                total_sources += 1
        
        logger.info(f"\n总计: {total_sources} 个数据源")
        return self.enabled_sources
    
    def get_data_source_info(self, data_type: str, data_source: str = None) -> Optional[Dict]:
        """获取数据源详细信息"""
        if data_type not in self.enabled_sources:
            logger.error(f"数据类型 {data_type} 不存在或未启用")
            return None
        
        if data_source and data_source not in self.enabled_sources[data_type]:
            logger.error(f"数据源 {data_source} 在 {data_type} 中不存在或未启用")
            return None
        
        config = self.service.config_manager.get_basic_cleaning_config()
        type_config = config.get(data_type, {})
        
        info = {
            'data_type': data_type,
            'description': type_config.get('description', ''),
            'output_path': type_config.get('output_path', ''),
            'available_sources': self.enabled_sources[data_type]
        }
        
        if data_source:
            source_config = type_config.get('cleaning_pipelines', {}).get(data_source, {})
            info['data_source'] = data_source
            info['enabled'] = source_config.get('enabled', True)
            info['pipeline_steps'] = len(source_config.get('pipeline', []))
            info['pipeline'] = source_config.get('pipeline', [])
        
        return info
    
    def clean_specific_data_source(self, data_type: str, data_source: str, max_files: Optional[int] = None) -> bool:
        """清洗指定的数据源"""
        logger.info(f"=== 开始清洗数据源: {data_type}/{data_source} ===")
        
        # 检查数据源是否存在
        if data_type not in self.enabled_sources:
            logger.error(f"✗ 数据类型 {data_type} 不存在或未启用")
            logger.info(f"可用的数据类型: {', '.join(self.enabled_sources.keys())}")
            return False
        
        if data_source not in self.enabled_sources[data_type]:
            logger.error(f"✗ 数据源 {data_source} 在 {data_type} 中不存在或未启用")
            logger.info(f"可用的数据源: {', '.join(self.enabled_sources[data_type])}")
            return False
        
        try:
            # 显示处理信息
            files_info = f"前{max_files}个文件" if max_files else "全部文件"
            logger.info(f"处理范围: {files_info}")
            
            # 执行清洗
            result_df = self.service.clean_data_by_pipeline(
                pipeline_name=data_type,
                data_source=data_source,
                max_files=max_files
            )
            
            if result_df is not None and not result_df.empty:
                # 保存数据
                file_path = self.service.save_cleaned_data(
                    df=result_df,
                    pipeline_name=data_type,
                    data_source=data_source
                )
                
                if file_path:
                    logger.info(f"✓ 数据清洗成功")
                    logger.info(f"  数据形状: {result_df.shape}")
                    logger.info(f"  输出文件: {file_path}")
                    
                    # 检查输出文件大小
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                        logger.info(f"  文件大小: {file_size:.2f} MB")
                    
                    logger.info(f"  列名: {list(result_df.columns)[:10]}{'...' if len(result_df.columns) > 10 else ''}")
                    return True
                else:
                    logger.error(f"✗ 数据保存失败")
                    return False
            else:
                logger.error(f"✗ 清洗后数据为空")
                return False
                
        except Exception as e:
            logger.error(f"✗ 清洗数据源失败: {str(e)}")
            return False
    
    def clean_all_data_sources_in_type(self, data_type: str, max_files: Optional[int] = None) -> Dict[str, bool]:
        """清洗指定数据类型下的所有数据源"""
        logger.info(f"=== 开始清洗数据类型: {data_type} ===")
        
        if data_type not in self.enabled_sources:
            logger.error(f"✗ 数据类型 {data_type} 不存在或未启用")
            return {}
        
        sources = self.enabled_sources[data_type]
        results = {}
        
        logger.info(f"将要处理 {len(sources)} 个数据源:")
        for source in sources:
            logger.info(f"  ✓ {source}")
        
        for data_source in sources:
            logger.info(f"\n--- 处理数据源: {data_source} ---")
            success = self.clean_specific_data_source(data_type, data_source, max_files)
            results[data_source] = success
        
        # 输出汇总结果
        logger.info(f"\n=== {data_type} 清洗结果汇总 ===")
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        for source, success in results.items():
            status = "✓ 成功" if success else "✗ 失败"
            logger.info(f"  {source}: {status}")
        
        logger.info(f"\n总计: 成功 {success_count}/{total_count} 个数据源")
        return results
    
    def clean_all_enabled_data_sources(self, max_files: Optional[int] = None) -> Dict[str, Dict[str, bool]]:
        """清洗所有启用的数据源"""
        files_info = f"前{max_files}个文件" if max_files else "全部文件"
        logger.info(f"\n{'='*60}")
        logger.info(f"开始清洗所有启用的数据源 - 处理{files_info}")
        logger.info(f"{'='*60}")
        
        if not self.enabled_sources:
            logger.error("❌ 没有找到启用的数据源")
            return {}
        
        # 显示将要处理的数据源
        self.list_available_data_sources()
        logger.info(f"{'='*60}")
        
        all_results = {}
        
        # 按数据类型处理
        for data_type in self.enabled_sources.keys():
            type_results = self.clean_all_data_sources_in_type(data_type, max_files)
            all_results[data_type] = type_results
        
        # 打印总体汇总结果
        logger.info(f"\n{'='*60}")
        logger.info("总体清洗结果汇总:")
        logger.info(f"{'='*60}")
        
        total_success = 0
        total_failed = 0
        
        for data_type, type_results in all_results.items():
            logger.info(f"\n📊 {data_type}:")
            for source, success in type_results.items():
                if success:
                    logger.info(f"  ✓ {source}: 成功")
                    total_success += 1
                else:
                    logger.info(f"  ✗ {source}: 失败")
                    total_failed += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"总计: 成功 {total_success} 个, 失败 {total_failed} 个")
        logger.info(f"{'='*60}")
        
        return all_results


def run_specific_data_source(data_type: str = None, data_source: str = None, max_files: Optional[int] = None):
    """运行指定的数据源清洗"""
    example = DataCleaningServiceExample()
    
    if data_type is None:
        # 如果没有指定，列出所有可用的数据源
        example.list_available_data_sources()
        
        # 默认运行dividend数据源
        data_type = 'dividend'
        data_source = 'dividend'
        logger.info(f"\n默认运行: {data_type}/{data_source}")
    
    if data_source is None:
        # 如果只指定了数据类型，清洗该类型下的所有数据源
        logger.info(f"=== 开始运行数据类型: {data_type} ===")
        
        try:
            results = example.clean_all_data_sources_in_type(data_type, max_files)
            
            success_count = sum(1 for success in results.values() if success)
            total_count = len(results)
            
            if success_count == total_count:
                logger.info(f"\n🎯 {data_type} 清洗完成! 所有 {total_count} 个数据源都成功")
                logger.info("💡 输出文件已生成，可以查看结果")
                return True
            else:
                logger.error(f"\n⚠️  {data_type} 清洗完成，但有 {total_count - success_count} 个数据源失败")
                return False
                
        except Exception as e:
            logger.error(f"{data_type} 清洗异常: {e}")
            return False
    else:
        # 清洗指定的数据源
        logger.info(f"=== 开始运行数据源: {data_type}/{data_source} ===")
        
        try:
            success = example.clean_specific_data_source(data_type, data_source, max_files)
            
            if success:
                logger.info(f"\n🎯 {data_type}/{data_source} 清洗成功!")
                logger.info("💡 输出文件已生成，可以查看结果")
            else:
                logger.error(f"\n⚠️  {data_type}/{data_source} 清洗失败")
                
            return success
                
        except Exception as e:
            logger.error(f"{data_type}/{data_source} 清洗异常: {e}")
            return False


def run_all_enabled_data_sources(max_files: Optional[int] = None):
    """运行所有启用的数据源清洗"""
    logger.info("=== 开始运行所有启用数据源清洗 ===")
    
    try:
        example = DataCleaningServiceExample()
        results = example.clean_all_enabled_data_sources(max_files)
        
        # 计算总体成功率
        total_success = 0
        total_count = 0
        
        for type_results in results.values():
            for success in type_results.values():
                if success:
                    total_success += 1
                total_count += 1
        
        if total_success == total_count:
            logger.info(f"\n🎯 所有数据源清洗成功! 共处理 {total_count} 个数据源")
            logger.info("💡 所有输出文件已生成，可以查看结果")
            return True
        else:
            logger.error(f"\n⚠️  数据源清洗完成，成功 {total_success}/{total_count} 个")
            return False
            
    except Exception as e:
        logger.error(f"清洗所有数据源异常: {e}")
        return False


def main():
    """主函数 - 支持命令行参数"""
    import sys
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            # 运行所有启用的数据源清洗
            max_files = None
            if len(sys.argv) > 2:
                try:
                    max_files = int(sys.argv[2])
                except ValueError:
                    if sys.argv[2].lower() != 'all':
                        logger.error("文件数量参数错误，请输入数字或'all'")
                        return
            
            run_all_enabled_data_sources(max_files)
            
        elif sys.argv[1] == "--list":
            # 列出所有可用的数据源
            example = DataCleaningServiceExample()
            example.list_available_data_sources()
            
        elif sys.argv[1] == "--info":
            # 获取数据源信息
            if len(sys.argv) < 3:
                logger.error("请指定数据类型: --info <data_type> [data_source]")
                return
            
            data_type = sys.argv[2]
            data_source = sys.argv[3] if len(sys.argv) > 3 else None
            
            example = DataCleaningServiceExample()
            info = example.get_data_source_info(data_type, data_source)
            
            if info:
                logger.info(f"数据类型: {info['data_type']}")
                logger.info(f"描述: {info['description']}")
                logger.info(f"输出路径: {info['output_path']}")
                logger.info(f"可用数据源: {', '.join(info['available_sources'])}")
                
                if 'data_source' in info:
                    logger.info(f"数据源: {info['data_source']}")
                    logger.info(f"启用状态: {info['enabled']}")
                    logger.info(f"流水线步骤数: {info['pipeline_steps']}")
            
        else:
            # 解析数据类型和数据源
            parts = sys.argv[1].split('/')
            data_type = parts[0]
            data_source = parts[1] if len(parts) > 1 else None
            
            # 解析文件数量参数
            max_files = None
            if len(sys.argv) > 2:
                try:
                    max_files = int(sys.argv[2])
                except ValueError:
                    if sys.argv[2].lower() != 'all':
                        logger.error("文件数量参数错误，请输入数字或'all'")
                        return
            
            run_specific_data_source(data_type, data_source, max_files)
    else:
        # 显示使用说明
        logger.info("💡 数据清洗服务使用说明:")
        logger.info("  python example_data_cleaning_service.py                           # 默认运行dividend/dividend")
        logger.info("  python example_data_cleaning_service.py dividend                 # 运行dividend类型下所有数据源")
        logger.info("  python example_data_cleaning_service.py dividend/dividend       # 运行指定数据源")
        logger.info("  python example_data_cleaning_service.py dividend/dividend 10    # 运行指定数据源，处理前10个文件")
        logger.info("  python example_data_cleaning_service.py --all                   # 运行所有启用的数据源")
        logger.info("  python example_data_cleaning_service.py --all 10                # 运行所有启用的数据源，每个处理前10个文件")
        logger.info("  python example_data_cleaning_service.py --list                  # 列出所有可用数据源")
        logger.info("  python example_data_cleaning_service.py --info dividend         # 获取数据类型信息")
        logger.info("  python example_data_cleaning_service.py --info dividend dividend # 获取数据源详细信息")
        logger.info("")
        
        # 默认运行dividend数据源
        run_specific_data_source()


if __name__ == "__main__":
    main()
