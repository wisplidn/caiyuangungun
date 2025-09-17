#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取并保存stock_basic和trade_cal数据

功能：
1. 使用DataSourceManager管理Tushare数据源
2. 获取stock_basic（股票基础信息）和trade_cal（交易日历）数据
3. 使用UniversalArchiver保存数据到本地
"""

import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime

# 添加项目路径 - 使用绝对路径
project_root = Path("/Users/daishun/个人文档/caiyuangungun/src/caiyuangungun/data/raw")
core_path = project_root / 'core'
sources_path = project_root / 'sources'
# 添加项目根目录以支持caiyuangungun模块导入
project_src_root = Path("/Users/daishun/个人文档/caiyuangungun/src")

# 确保路径存在并添加到sys.path
if core_path.exists():
    sys.path.insert(0, str(core_path))
if sources_path.exists():
    sys.path.insert(0, str(sources_path))
sys.path.insert(0, str(project_root))
# 添加src目录以支持caiyuangungun模块
sys.path.insert(0, str(project_src_root))

print(f"Core path: {core_path}, exists: {core_path.exists()}")
print(f"Sources path: {sources_path}, exists: {sources_path.exists()}")
print(f"Project src root: {project_src_root}, exists: {project_src_root.exists()}")
print(f"Python path: {sys.path[:4]}")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def load_config():
    """加载配置文件"""
    # 修正配置文件路径
    config_path = Path("/Users/daishun/个人文档/caiyuangungun/src/caiyuangungun/data/raw/config/unified_data_config.json")
    
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    logger.info(f"成功加载配置文件: {config_path}")
    return config

def main():
    """主函数"""
    try:
        # 1. 加载配置
        logger.info("=== 开始获取和保存stock_basic、trade_cal数据 ===")
        config = load_config()
        
        # 2. 初始化数据源管理器
        logger.info("初始化数据源管理器...")
        try:
            from data_source_manager import DataSourceManager
        except ImportError:
            # 尝试其他导入方式
            try:
                from core.data_source_manager import DataSourceManager
            except ImportError:
                import importlib.util
                spec = importlib.util.spec_from_file_location(
                    "data_source_manager", 
                    core_path / "data_source_manager.py"
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                DataSourceManager = module.DataSourceManager
        
        # 直接创建TushareDataSource实例，不使用DataSourceManager
        logger.info("创建TushareDataSource实例...")
        
        # 导入必要的类
        try:
            from caiyuangungun.data.raw.core.base_data_source import DataSourceConfig
            from caiyuangungun.data.raw.sources.tushare_source import TushareDataSource
        except ImportError as e:
            logger.error(f"导入失败: {e}")
            # 尝试直接导入
            try:
                import sys
                sys.path.insert(0, str(project_root / 'core'))
                sys.path.insert(0, str(project_root / 'sources'))
                from base_data_source import DataSourceConfig
                from tushare_source import TushareDataSource
            except ImportError as e2:
                logger.error(f"直接导入也失败: {e2}")
                raise
        
        # 创建DataSourceConfig对象
        tushare_config_data = config['data_sources']['tushare']
        data_source_config = DataSourceConfig(
            name=tushare_config_data['name'],
            source_type=tushare_config_data['source_type'],
            connection_params=tushare_config_data['connection_params'],
            timeout=tushare_config_data['connection_params'].get('timeout', 30),
            retry_count=tushare_config_data['connection_params'].get('retry_count', 3)
        )
        
        # 创建TushareDataSource实例
        tushare_source = TushareDataSource(data_source_config)
        
        # 3. 连接Tushare数据源
        logger.info("连接Tushare数据源...")
        if not tushare_source.connect():
            raise Exception("Tushare数据源连接失败")
        
        if not tushare_source:
            raise RuntimeError("无法获取Tushare数据源实例")
        
        logger.info("Tushare数据源实例获取成功")
        
        # 4. 初始化归档器
        logger.info("初始化数据归档器...")
        try:
            from universal_archiver import PandasArchiver, ArchiveConfig
        except ImportError:
            try:
                from core.universal_archiver import PandasArchiver, ArchiveConfig
            except ImportError:
                import importlib.util
                spec = importlib.util.spec_from_file_location(
                    "universal_archiver", 
                    core_path / "universal_archiver.py"
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                PandasArchiver = module.PandasArchiver
                ArchiveConfig = module.ArchiveConfig
        
        # 设置基础保存路径
        base_save_path = Path("/Users/daishun/个人文档/caiyuangungun/data/raw/landing/tushare")
        
        # 创建子目录
        stock_basic_path = base_save_path / "stock_basic"
        trade_cal_path = base_save_path / "trade_cal"
        
        stock_basic_path.mkdir(parents=True, exist_ok=True)
        trade_cal_path.mkdir(parents=True, exist_ok=True)
        archiver = PandasArchiver(str(base_save_path))
        
        # 5. 获取stock_basic数据
        logger.info("\n=== 获取stock_basic数据 ===")
        try:
            stock_basic_data = tushare_source.fetch_data('stock_basic')
            
            if not stock_basic_data.empty:
                logger.info(f"获取到stock_basic数据: {len(stock_basic_data)}行")
                
                # 保存数据
                config_obj = ArchiveConfig(
                    source_name='tushare',
                    data_type='stock_basic',
                    archive_type='SNAPSHOT'
                )
                
                from core.universal_archiver import PathInfo
                path_info = PathInfo(
                    landing_path=str(stock_basic_path),
                    archive_path=str(stock_basic_path / "archive"),
                    data_filename="data.parquet",
                    config_filename="data.json"
                )
                
                result = archiver.archive_data(stock_basic_data, path_info, config_obj, {})
                if result.success:
                    logger.info(f"stock_basic数据保存成功: {result.file_path}")
                else:
                    logger.error(f"stock_basic数据保存失败: {result.error_message}")
            else:
                logger.warning("stock_basic数据为空")
                
        except Exception as e:
            logger.error(f"获取stock_basic数据失败: {e}")
        
        # 6. 获取trade_cal数据
        logger.info("\n=== 获取trade_cal数据 ===")
        try:
            # 获取交易日历数据，设置日期范围
            trade_cal_data = tushare_source.fetch_data(
                'trade_cal',
                start_date='20000101',
                end_date='20991231'
            )
            
            if not trade_cal_data.empty:
                logger.info(f"获取到trade_cal数据: {len(trade_cal_data)}行")
                
                # 保存数据
                config_obj = ArchiveConfig(
                    source_name='tushare',
                    data_type='trade_cal',
                    archive_type='SNAPSHOT'
                )
                
                path_info = PathInfo(
                    landing_path=str(trade_cal_path),
                    archive_path=str(trade_cal_path / "archive"),
                    data_filename="data.parquet",
                    config_filename="data.json"
                )
                
                result = archiver.archive_data(trade_cal_data, path_info, config_obj, {})
                if result.success:
                    logger.info(f"trade_cal数据保存成功: {result.file_path}")
                else:
                    logger.error(f"trade_cal数据保存失败: {result.error_message}")
            else:
                logger.warning("trade_cal数据为空")
                
        except Exception as e:
            logger.error(f"获取trade_cal数据失败: {e}")
        
        # 7. 显示保存的文件
        logger.info("\n=== 数据保存完成 ===")
        if base_save_path.exists():
            logger.info("保存的文件列表:")
            for file_path in base_save_path.rglob('*'):
                if file_path.is_file():
                    logger.info(f"  - {file_path.relative_to(base_save_path)} ({file_path.stat().st_size} bytes)")
        
        logger.info("数据获取和保存任务完成！")
        
    except Exception as e:
        logger.error(f"执行失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ 数据获取和保存成功完成！")
    else:
        print("\n❌ 数据获取和保存失败！")
        sys.exit(1)