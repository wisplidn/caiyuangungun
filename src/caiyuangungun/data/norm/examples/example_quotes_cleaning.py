#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
行情数据清洗示例脚本
运行daily、adj_factor、daily_basic三个处理器
"""

import os
import sys
import logging
from pathlib import Path

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('example_quotes_cleaning')

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from caiyuangungun.data.norm.services.processor_service import ProcessorService


def run_quotes_processors():
    """运行所有行情数据处理器"""
    logger.info("=" * 80)
    logger.info("开始清洗行情数据")
    logger.info("=" * 80)
    
    # 定义处理器列表
    processors = [
        'daily_processor',
        'adj_factor_processor',
        'daily_basic_processor'
    ]
    
    # 创建服务
    service = ProcessorService()
    
    # 执行结果统计
    results = []
    
    for processor_name in processors:
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"开始运行: {processor_name}")
        logger.info("=" * 80)
        
        try:
            # 执行处理器
            result = service.execute_processor(processor_name)
            
            if result.get('success'):
                logger.info(f"✓ {processor_name} 执行成功")
                logger.info(f"  处理时间: {result['execution_time']:.2f}秒")
                logger.info(f"  输入数据形状: {result['input_shape']}")
                logger.info(f"  输出数据形状: {result['output_shape']}")
                logger.info(f"  输出文件: {result['output_path']}")
                
                # 检查输出文件
                if os.path.exists(result['output_path']):
                    file_size = os.path.getsize(result['output_path']) / (1024 * 1024)  # MB
                    logger.info(f"  输出文件大小: {file_size:.2f} MB")
                
                results.append((processor_name, True, result))
            else:
                logger.error(f"✗ {processor_name} 执行失败: {result.get('error', '未知错误')}")
                results.append((processor_name, False, result))
                
        except Exception as e:
            logger.error(f"✗ 执行 {processor_name} 时发生异常: {e}")
            results.append((processor_name, False, {'error': str(e)}))
    
    # 输出总结
    logger.info("")
    logger.info("=" * 80)
    logger.info("清洗总结")
    logger.info("=" * 80)
    
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    
    for processor_name, success, result in results:
        status = "✓ 成功" if success else "✗ 失败"
        logger.info(f"{processor_name}: {status}")
        
        if success:
            logger.info(f"  输出文件: {result['output_path']}")
    
    logger.info("")
    logger.info(f"总计: {passed}/{total} 个处理器成功")
    
    if passed == total:
        logger.info("🎉 所有行情数据清洗完成！")
        return True
    else:
        logger.error(f"❌ {total - passed} 个处理器失败")
        return False


def main():
    """主函数"""
    try:
        success = run_quotes_processors()
        
        if success:
            logger.info("\n✅ 行情数据清洗全部完成")
            logger.info("📁 清洗后的数据文件位于: data/norm/daily_data/cleaned/")
        else:
            logger.error("\n⚠️  部分行情数据清洗失败，请检查日志")
        
        return success
        
    except Exception as e:
        logger.error(f"主函数执行失败: {e}")
        return False


if __name__ == "__main__":
    main()

