"""
测试ProcessorService的功能
验证通用化处理器服务的完整流程
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
logger = logging.getLogger('example_processor_service')

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from caiyuangungun.data.norm.services.processor_service import ProcessorService
from caiyuangungun.data.norm.core.config_manager import ConfigManager


def test_list_processors():
    """测试列出所有可用处理器"""
    logger.info("=== 测试：列出所有可用处理器 ===")
    
    try:
        service = ProcessorService()
        service.list_processors()
        logger.info("✓ 列出处理器功能正常")
        return True
    except Exception as e:
        logger.error(f"✗ 列出处理器失败: {e}")
        return False


def test_get_processor_info():
    """测试获取处理器详细信息"""
    logger.info("=== 测试：获取处理器详细信息 ===")
    
    try:
        service = ProcessorService()
        info = service.get_processor_info('fin_is_processor')
        
        if info:
            logger.info("✓ 获取处理器信息成功")
            logger.info(f"  处理器名称: {info['name']}")
            logger.info(f"  描述: {info['description']}")
            logger.info(f"  模块路径: {info['module_path']}")
            logger.info(f"  类名: {info['class_name']}")
            logger.info(f"  流水线步骤数: {info['pipeline_steps']}")
            return True
        else:
            logger.error("✗ 未能获取处理器信息")
            return False
            
    except Exception as e:
        logger.error(f"✗ 获取处理器信息失败: {e}")
        return False


def test_execute_processor(processor_name: str):
    """测试执行指定处理器"""
    logger.info(f"=== 测试：执行 {processor_name} ===")
    
    try:
        service = ProcessorService()
        
        # 执行处理器
        result = service.execute_processor(processor_name)
        
        if result.get('success'):
            logger.info(f"✓ {processor_name} 执行成功")
            logger.info(f"  处理时间: {result['execution_time']:.2f}秒")
            logger.info(f"  输入数据形状: {result['input_shape']}")
            logger.info(f"  输出数据形状: {result['output_shape']}")
            logger.info(f"  输出文件: {result['output_path']}")
            
            # 检查输出文件是否存在
            if os.path.exists(result['output_path']):
                file_size = os.path.getsize(result['output_path']) / (1024 * 1024)  # MB
                logger.info(f"  输出文件大小: {file_size:.2f} MB")
                logger.info("✓ 输出文件创建成功")
            else:
                logger.warning("⚠ 输出文件未找到")
            
            return True
        else:
            logger.error(f"✗ {processor_name} 执行失败: {result.get('error', '未知错误')}")
            return False
            
    except Exception as e:
        logger.error(f"✗ 执行 {processor_name} 时发生异常: {e}")
        return False


def test_execute_fin_is_processor():
    """测试执行fin_is_processor"""
    return test_execute_processor('fin_is_processor')


def test_execute_fin_bs_processor():
    """测试执行fin_bs_processor"""
    return test_execute_processor('fin_bs_processor')


def test_execute_fin_cf_processor():
    """测试执行fin_cf_processor"""
    return test_execute_processor('fin_cf_processor')


def test_execute_fin_indicator_processor():
    """测试执行fin_indicator_processor"""
    return test_execute_processor('fin_indicator_processor')


def test_config_manager_integration():
    """测试配置管理器集成"""
    logger.info("=== 测试：配置管理器集成 ===")
    
    try:
        config_manager = ConfigManager()
        
        # 测试获取处理器配置
        processor_config = config_manager.get_processor_config('fin_is_processor')
        if processor_config:
            logger.info("✓ 获取处理器配置成功")
            logger.info(f"  配置描述: {processor_config.get('description', 'N/A')}")
        else:
            logger.error("✗ 未能获取处理器配置")
            return False
        
        # 测试获取处理流水线
        pipeline = config_manager.get_processor_pipeline('fin_is_processor')
        if pipeline:
            logger.info(f"✓ 获取处理流水线成功，共 {len(pipeline)} 个步骤")
        else:
            logger.error("✗ 未能获取处理流水线")
            return False
        
        # 测试获取路径配置
        paths = config_manager.get_processor_paths('fin_is_processor')
        if paths:
            logger.info("✓ 获取路径配置成功")
            logger.info(f"  输入路径: {paths.get('input_path', 'N/A')}")
            logger.info(f"  输出路径: {paths.get('output_path', 'N/A')}")
        else:
            logger.error("✗ 未能获取路径配置")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 配置管理器集成测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行ProcessorService完整测试套件")
    logger.info("=" * 60)
    
    tests = [
        ("配置管理器集成", test_config_manager_integration),
        ("列出处理器", test_list_processors),
        ("获取处理器信息", test_get_processor_info),
        ("执行fin_is_processor", test_execute_fin_is_processor),
        ("执行fin_bs_processor", test_execute_fin_bs_processor),
        ("执行fin_cf_processor", test_execute_fin_cf_processor),
        ("执行fin_indicator_processor", test_execute_fin_indicator_processor),
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n开始测试: {test_name}")
        logger.info("-" * 40)
        
        try:
            success = test_func()
            results.append((test_name, success))
            
            if success:
                logger.info(f"✓ {test_name} 测试通过")
            else:
                logger.error(f"✗ {test_name} 测试失败")
                
        except Exception as e:
            logger.error(f"✗ {test_name} 测试异常: {e}")
            results.append((test_name, False))
        
        logger.info("-" * 40)
    
    # 输出测试总结
    logger.info("\n" + "=" * 60)
    logger.info("测试总结")
    logger.info("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✓ 通过" if success else "✗ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总计: {passed}/{total} 个测试通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！ProcessorService 功能正常")
        return True
    else:
        logger.error(f"❌ {total - passed} 个测试失败，请检查相关功能")
        return False


def main():
    """主函数"""
    try:
        success = run_all_tests()
        
        if success:
            logger.info("\n🎯 测试结论: ProcessorService 已准备就绪，可以投入使用")
            logger.info("💡 使用方法: 输入 'fin_is_processor' 即可直接输出最终的parquet文件")
        else:
            logger.error("\n⚠️  测试结论: ProcessorService 存在问题，需要进一步调试")
            
    except Exception as e:
        logger.error(f"测试主函数执行失败: {e}")


if __name__ == "__main__":
    main()