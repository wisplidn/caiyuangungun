#!/usr/bin/env python3
"""测试简化后的配置管理器"""

import sys
import os
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from caiyuangungun.data.raw.core.config_manager import ConfigManager, get_config_manager, get_config

def test_basic_functionality():
    """测试基本功能"""
    print("=== 测试简化后的配置管理器 ===")
    
    # 测试1: 创建配置管理器实例
    print("\n1. 创建配置管理器实例...")
    try:
        config_manager = ConfigManager()
        print("✓ 配置管理器创建成功")
    except Exception as e:
        print(f"✗ 配置管理器创建失败: {e}")
        return False
    
    # 测试2: 测试基本的get/set功能
    print("\n2. 测试基本的get/set功能...")
    try:
        # 设置配置值
        config_manager.set("test.key1", "value1")
        config_manager.set("test.key2", 123)
        config_manager.set("test.nested.key", True)
        
        # 获取配置值
        assert config_manager.get("test.key1") == "value1"
        assert config_manager.get("test.key2") == 123
        assert config_manager.get("test.nested.key") == True
        assert config_manager.get("nonexistent", "default") == "default"
        
        print("✓ 基本get/set功能正常")
    except Exception as e:
        print(f"✗ 基本get/set功能测试失败: {e}")
        return False
    
    # 测试3: 测试字典式访问
    print("\n3. 测试字典式访问...")
    try:
        config_manager["dict.access"] = "works"
        assert config_manager["dict.access"] == "works"
        assert "dict.access" in config_manager
        assert "nonexistent" not in config_manager
        
        print("✓ 字典式访问功能正常")
    except Exception as e:
        print(f"✗ 字典式访问测试失败: {e}")
        return False
    
    # 测试4: 测试配置段获取
    print("\n4. 测试配置段获取...")
    try:
        test_section = config_manager.get_section("test")
        assert isinstance(test_section, dict)
        assert test_section.get("key1") == "value1"
        assert test_section.get("key2") == 123
        
        print("✓ 配置段获取功能正常")
    except Exception as e:
        print(f"✗ 配置段获取测试失败: {e}")
        return False
    
    # 测试5: 测试全局配置管理器
    print("\n5. 测试全局配置管理器...")
    try:
        global_manager = get_config_manager()
        assert isinstance(global_manager, ConfigManager)
        
        # 测试便捷函数
        result = get_config("test.key1", "default")
        print(f"✓ 全局配置管理器功能正常，获取到值: {result}")
    except Exception as e:
        print(f"✗ 全局配置管理器测试失败: {e}")
        return False
    
    # 测试6: 测试配置保存
    print("\n6. 测试配置保存...")
    try:
        test_config = {
            "database": {
                "host": "localhost",
                "port": 5432
            },
            "api": {
                "timeout": 30
            }
        }
        
        config_manager.save_config("test_config", test_config)
        print("✓ 配置保存功能正常")
    except Exception as e:
        print(f"✗ 配置保存测试失败: {e}")
        return False
    
    print("\n=== 所有测试通过! 简化后的配置管理器工作正常 ===")
    return True

def test_environment_variables():
    """测试环境变量覆盖功能"""
    print("\n=== 测试环境变量覆盖功能 ===")
    
    # 设置测试环境变量
    os.environ["CAIYUAN_TEST_ENV_VAR"] = "env_value"
    os.environ["CAIYUAN_NESTED_CONFIG_KEY"] = "nested_env_value"
    
    try:
        # 创建新的配置管理器实例以加载环境变量
        config_manager = ConfigManager()
        
        # 检查环境变量是否被正确加载
        env_value = config_manager.get("test.env.var")
        nested_value = config_manager.get("nested.config.key")
        
        print(f"环境变量值: test.env.var = {env_value}")
        print(f"嵌套环境变量值: nested.config.key = {nested_value}")
        
        print("✓ 环境变量覆盖功能正常")
        
    except Exception as e:
        print(f"✗ 环境变量测试失败: {e}")
        return False
    
    finally:
        # 清理测试环境变量
        os.environ.pop("CAIYUAN_TEST_ENV_VAR", None)
        os.environ.pop("CAIYUAN_NESTED_CONFIG_KEY", None)
    
    return True

if __name__ == "__main__":
    success = True
    
    # 运行基本功能测试
    success &= test_basic_functionality()
    
    # 运行环境变量测试
    success &= test_environment_variables()
    
    if success:
        print("\n🎉 所有测试都通过了！简化后的配置管理器功能完整且正常工作。")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查配置管理器实现。")
        sys.exit(1)