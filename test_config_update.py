#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试配置部分更新功能
"""

import sys
import os
from pathlib import Path

# 添加项目路径到sys.path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from caiyuangungun.data.raw.core.config_manager import ConfigManager
import json

def test_config_update():
    """测试配置部分更新功能"""
    print("=== 测试配置部分更新功能 ===")
    
    try:
        # 初始化ConfigManager
        config_manager = ConfigManager()
        
        # 读取当前tushare_source_config.json的内容
        config_file = config_manager.config_dir / "tushare_source_config.json"
        
        if not config_file.exists():
            print(f"❌ 配置文件不存在: {config_file}")
            return False
            
        print(f"📁 配置文件路径: {config_file}")
        
        # 读取原始配置
        with open(config_file, 'r', encoding='utf-8') as f:
            original_config = json.load(f)
            
        # 检查是否有api_endpoints配置
        if 'api_endpoints' not in original_config:
            print("❌ 配置文件中没有找到api_endpoints")
            return False
            
        # 找到第一个端点进行测试
        endpoint_names = list(original_config['api_endpoints'].keys())
        if not endpoint_names:
            print("❌ 没有找到任何API端点配置")
            return False
            
        test_endpoint = endpoint_names[0]
        print(f"🎯 测试端点: {test_endpoint}")
        
        # 获取原始limitmax值
        original_limitmax = original_config['api_endpoints'][test_endpoint].get('limitmax', 5000)
        print(f"📊 原始limitmax值: {original_limitmax}")
        
        # 设置新的测试值
        test_limitmax = original_limitmax + 1000
        print(f"🔄 测试更新limitmax为: {test_limitmax}")
        
        # 使用部分更新方法
        field_path = f"api_endpoints.{test_endpoint}.limitmax"
        config_manager.update_config_field('tushare_source_config', field_path, test_limitmax)
        
        # 验证更新结果
        with open(config_file, 'r', encoding='utf-8') as f:
            updated_config = json.load(f)
            
        updated_limitmax = updated_config['api_endpoints'][test_endpoint]['limitmax']
        
        if updated_limitmax == test_limitmax:
            print(f"✅ 更新成功! 新值: {updated_limitmax}")
            
            # 检查其他配置是否保持不变
            other_unchanged = True
            for endpoint_name in endpoint_names:
                if endpoint_name != test_endpoint:
                    original_val = original_config['api_endpoints'][endpoint_name].get('limitmax')
                    updated_val = updated_config['api_endpoints'][endpoint_name].get('limitmax')
                    if original_val != updated_val:
                        print(f"⚠️  其他端点 {endpoint_name} 的limitmax意外改变: {original_val} -> {updated_val}")
                        other_unchanged = False
                        
            if other_unchanged:
                print("✅ 其他配置保持不变")
            
            # 恢复原始值
            print(f"🔄 恢复原始值: {original_limitmax}")
            config_manager.update_config_field('tushare_source_config', field_path, original_limitmax)
            print("✅ 已恢复原始配置")
            
            return True
        else:
            print(f"❌ 更新失败! 期望: {test_limitmax}, 实际: {updated_limitmax}")
            return False
            
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_config_update()
    if success:
        print("\n🎉 测试通过! 部分更新功能正常工作")
    else:
        print("\n💥 测试失败! 请检查配置")
        sys.exit(1)