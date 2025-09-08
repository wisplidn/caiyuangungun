#!/usr/bin/env python3
"""
直接测试TushareDataSource的limitmax更新逻辑
不依赖pipeline模块
"""

import sys
from pathlib import Path

# 添加项目路径到sys.path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

try:
    from caiyuangungun.data.raw.sources.tushare_source import TushareDataSource
    from caiyuangungun.data.raw.core.config_manager import ConfigManager
    
    def test_limitmax_logic():
        """测试limitmax更新逻辑"""
        print("=== 测试TushareDataSource的limitmax更新逻辑 ===")
        
        # 初始化ConfigManager
        config_manager = ConfigManager()
        
        # 创建TushareDataSource实例
        tushare_source = TushareDataSource()
        
        # 连接到Tushare
        if not tushare_source.connect():
            print("❌ Tushare连接失败")
            return False
            
        print("✅ Tushare连接成功")
        
        # 测试adj_factor数据获取
        print("\n--- 测试adj_factor数据获取 ---")
        try:
            data = tushare_source.fetch_data(
                endpoint_name='adj_factor',
                trade_date='20241129'
            )
            print(f"获取到 {len(data)} 行adj_factor数据")
            
            # 检查limitmax配置
            limitmax_config = config_manager.get_limitmax_config('adj_factor')
            if limitmax_config:
                print(f"adj_factor当前limitmax配置: {limitmax_config}")
            else:
                print("adj_factor无limitmax配置")
                
        except Exception as e:
            print(f"❌ adj_factor数据获取失败: {e}")
            
        # 测试daily_basic数据获取
        print("\n--- 测试daily_basic数据获取 ---")
        try:
            # 获取更新前的limitmax配置
            limitmax_config_before = config_manager.get_limitmax_config('daily_basic')
            print(f"更新前daily_basic limitmax配置: {limitmax_config_before}")
            
            data = tushare_source.fetch_data(
                endpoint_name='daily_basic',
                trade_date='20241129'
            )
            print(f"获取到 {len(data)} 行daily_basic数据")
            
            # 获取更新后的limitmax配置
            limitmax_config_after = config_manager.get_limitmax_config('daily_basic')
            print(f"更新后daily_basic limitmax配置: {limitmax_config_after}")
            
            # 比较前后配置
            if limitmax_config_before and limitmax_config_after:
                before_limitmax = limitmax_config_before.get('limitmax', 0)
                after_limitmax = limitmax_config_after.get('limitmax', 0)
                data_count = len(data)
                
                print(f"\n分析结果:")
                print(f"  数据行数: {data_count}")
                print(f"  更新前limitmax: {before_limitmax}")
                print(f"  更新后limitmax: {after_limitmax}")
                
                if data_count > before_limitmax:
                    if after_limitmax > before_limitmax:
                        print(f"  ✅ 正确: 数据行数({data_count}) > 原limitmax({before_limitmax})，limitmax已更新为{after_limitmax}")
                    else:
                        print(f"  ❌ 错误: 数据行数({data_count}) > 原limitmax({before_limitmax})，但limitmax未更新")
                elif data_count == before_limitmax:
                    if after_limitmax == before_limitmax:
                        print(f"  ✅ 正确: 数据行数({data_count}) = 原limitmax({before_limitmax})，limitmax未更新")
                    else:
                        print(f"  ❌ 错误: 数据行数({data_count}) = 原limitmax({before_limitmax})，但limitmax被意外更新")
                else:
                    if after_limitmax == before_limitmax:
                        print(f"  ✅ 正确: 数据行数({data_count}) < 原limitmax({before_limitmax})，limitmax未更新")
                    else:
                        print(f"  ❌ 错误: 数据行数({data_count}) < 原limitmax({before_limitmax})，但limitmax被意外更新")
                
        except Exception as e:
            print(f"❌ daily_basic数据获取失败: {e}")
            
        # 断开连接
        tushare_source.disconnect()
        print("\n✅ 测试完成")
        return True
        
    if __name__ == "__main__":
        test_limitmax_logic()
        
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    print("请检查项目路径和模块结构")
except Exception as e:
    print(f"❌ 运行测试失败: {e}")
    import traceback
    traceback.print_exc()