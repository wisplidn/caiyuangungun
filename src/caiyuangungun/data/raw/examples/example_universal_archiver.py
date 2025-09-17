#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UniversalArchiver 全面测试脚本
测试功能包括：
1. 基本功能测试
2. 重复文件跳过测试
3. 归档功能测试
4. 错误参数处理测试
"""

import sys
import os
import json
import shutil
import logging
from pathlib import Path
from datetime import datetime

# 添加项目路径 - tests目录的父目录是raw，core是raw的子目录
core_path = Path(__file__).parent / "../core"
sys.path.insert(0, str(core_path.resolve()))

try:
    import pandas as pd
    from universal_archiver import UniversalArchiver, ArchiveConfig, PathInfo
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保pandas已安装且universal_archiver.py在正确位置")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"Core路径: {core_path.absolute()}")
    print(f"Core路径存在: {core_path.exists()}")
    print(f"universal_archiver.py存在: {(core_path / 'universal_archiver.py').exists()}")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class UniversalArchiverTester:
    """UniversalArchiver测试类"""
    
    def __init__(self):
        self.test_base_path = "/Users/daishun/个人文档/caiyuangungun/data/raw/landing/tushare/test"
        self.archiver = UniversalArchiver(self.test_base_path)
        self.test_results = []
        
    def log_test_result(self, test_name: str, success: bool, message: str = ""):
        """记录测试结果"""
        result = {
            'test_name': test_name,
            'success': success,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}: {message}")
        
    def create_sample_dataframe(self, rows: int = 100, seed: int = 42) -> pd.DataFrame:
        """创建示例DataFrame"""
        import numpy as np
        np.random.seed(seed)
        
        return pd.DataFrame({
            'ts_code': [f'00000{i%10}.SZ' for i in range(rows)],
            'symbol': [f'股票{i%10}' for i in range(rows)],
            'name': [f'公司{i}' for i in range(rows)],
            'area': np.random.choice(['深圳', '上海', '北京'], rows),
            'industry': np.random.choice(['制造业', '金融业', '科技业'], rows),
            'market': np.random.choice(['主板', '中小板', '创业板'], rows),
            'list_date': pd.date_range('2020-01-01', periods=rows, freq='D').strftime('%Y%m%d')
        })
    
    def test_1_basic_functionality(self):
        """测试1: 基本功能测试"""
        print("\n=== 测试1: 基本功能测试 ===")
        
        try:
            # 创建测试数据
            df = self.create_sample_dataframe(100)
            
            # 配置参数
            config = ArchiveConfig(
                source_name="tushare",
                data_type="stock_basic",
                archive_type="SNAPSHOT",
                date_param=None,
                method="stock_basic",
                required_params=[]
            )
            
            path_info = PathInfo(
                landing_path=self.test_base_path,
                archive_path="/Users/daishun/个人文档/caiyuangungun/data/raw/archive/tushare/test",
                data_filename="data.parquet",
                config_filename="data.json"
            )
            
            api_params = {
                "exchange": "",
                "list_status": "L",
                "fields": "ts_code,symbol,name,area,industry,market,list_date"
            }
            
            # 执行归档
            result = self.archiver.archive_data(df, path_info, config, api_params)
            
            # 验证结果
            if result['success'] and result['action'] in ['created', 'updated']:
                # 验证文件是否存在
                data_path = Path(result['file_path'])
                config_path = Path(result['config_path'])
                
                if data_path.exists() and config_path.exists():
                    # 验证数据完整性
                    loaded_df = pd.read_parquet(data_path)
                    if len(loaded_df) == len(df) and list(loaded_df.columns) == list(df.columns):
                        self.log_test_result("基本功能测试", True, "数据文件和配置文件创建成功，数据完整")
                    else:
                        self.log_test_result("基本功能测试", False, "数据完整性验证失败")
                else:
                    self.log_test_result("基本功能测试", False, "文件创建失败")
            else:
                self.log_test_result("基本功能测试", False, f"归档失败: {result}")
                
        except Exception as e:
            self.log_test_result("基本功能测试", False, f"异常: {str(e)}")
    
    def test_2_duplicate_skip(self):
        """测试2: 重复文件跳过测试"""
        print("\n=== 测试2: 重复文件跳过测试 ===")
        
        try:
            # 使用相同的数据再次归档
            df = self.create_sample_dataframe(100)  # 相同的seed，相同的数据
            
            config = ArchiveConfig(
                source_name="tushare",
                data_type="stock_basic",
                archive_type="SNAPSHOT",
                method="stock_basic"
            )
            
            path_info = PathInfo(
                landing_path=self.test_base_path,
                archive_path="/Users/daishun/个人文档/caiyuangungun/data/raw/archive/tushare/test",
                data_filename="data.parquet",
                config_filename="data.json"
            )
            
            # 执行归档
            result = self.archiver.archive_data(df, path_info, config)
            
            # 验证是否跳过
            if result['success'] and result['action'] == 'skipped':
                self.log_test_result("重复文件跳过测试", True, "成功跳过重复数据")
            else:
                self.log_test_result("重复文件跳过测试", False, f"未能跳过重复数据: {result['action']}")
                
        except Exception as e:
            self.log_test_result("重复文件跳过测试", False, f"异常: {str(e)}")
    
    def test_3_archive_functionality(self):
        """测试3: 归档功能测试"""
        print("\n=== 测试3: 归档功能测试 ===")
        
        try:
            # 创建不同的数据（更多行数）
            df_new = self.create_sample_dataframe(100, seed=123)  # 不同的seed，相同行数
            
            config = ArchiveConfig(
                source_name="tushare",
                data_type="stock_basic",
                archive_type="SNAPSHOT",
                method="stock_basic"
            )
            
            path_info = PathInfo(
                landing_path=self.test_base_path,
                archive_path="/Users/daishun/个人文档/caiyuangungun/data/raw/archive/tushare/test",
                data_filename="data.parquet",
                config_filename="data.json"
            )
            
            # 检查archive目录是否存在
            archive_dir = Path("/Users/daishun/个人文档/caiyuangungun/data/raw/archive/tushare/test")
            files_before = list(archive_dir.glob("*")) if archive_dir.exists() else []
            
            # 执行归档
            result = self.archiver.archive_data(df_new, path_info, config)
            
            # 验证归档结果
            if result['success'] and result['action'] == 'updated':
                # 检查是否有归档文件信息
                if 'archived_files' in result and result['archived_files']:
                    # 验证新数据是否正确保存
                    data_path = Path(result['file_path'])
                    loaded_df = pd.read_parquet(data_path)
                    
                    if len(loaded_df) == 100:
                        self.log_test_result("归档功能测试", True, f"旧文件已归档({len(result['archived_files'])}个文件)，新数据已保存")
                    else:
                        self.log_test_result("归档功能测试", False, f"新数据保存不正确，期望100行，实际{len(loaded_df)}行")
                else:
                    self.log_test_result("归档功能测试", False, "旧文件未被归档")
            else:
                self.log_test_result("归档功能测试", False, f"归档失败: {result}")
                
        except Exception as e:
            self.log_test_result("归档功能测试", False, f"异常: {str(e)}")
    
    def test_4_data_reduction_protection(self):
        """测试4: 数据减少保护测试"""
        print("\n=== 测试4: 数据减少保护测试 ===")
        
        try:
            # 创建更少行数的数据，应该触发保护机制
            df_reduced = self.create_sample_dataframe(50, seed=456)  # 只有50行
            
            config = ArchiveConfig(
                source_name="tushare",
                data_type="stock_basic",
                archive_type="SNAPSHOT",
                method="stock_basic"
            )
            
            path_info = PathInfo(
                landing_path=self.test_base_path,
                archive_path="/Users/daishun/个人文档/caiyuangungun/data/raw/archive/tushare/test",
                data_filename="data.parquet",
                config_filename="data.json"
            )
            
            # 执行归档，应该抛出异常
            try:
                result = self.archiver.archive_data(df_reduced, path_info, config)
                self.log_test_result("数据减少保护测试", False, "未能检测到数据减少")
            except ValueError as ve:
                if "数据行数" in str(ve) and "减少" in str(ve):
                    self.log_test_result("数据减少保护测试", True, "成功检测并阻止数据减少")
                else:
                    self.log_test_result("数据减少保护测试", False, f"异常信息不符合预期: {ve}")
                    
        except Exception as e:
            self.log_test_result("数据减少保护测试", False, f"异常: {str(e)}")
    
    def test_5_error_parameter_handling(self):
        """测试5: 错误参数处理测试"""
        print("\n=== 测试5: 错误参数处理测试 ===")
        
        # 测试5.1: 空DataFrame
        try:
            empty_df = pd.DataFrame()
            config = ArchiveConfig("tushare", "test", "SNAPSHOT")
            path_info = PathInfo(self.test_base_path, "", "test.parquet", "test_config.json")
            
            try:
                self.archiver.archive_data(empty_df, path_info, config)
                self.log_test_result("错误参数-空DataFrame", False, "未能检测到空DataFrame")
            except ValueError:
                self.log_test_result("错误参数-空DataFrame", True, "成功检测空DataFrame")
        except Exception as e:
            self.log_test_result("错误参数-空DataFrame", False, f"异常: {str(e)}")
        
        # 测试5.2: None DataFrame
        try:
            config = ArchiveConfig("tushare", "test", "SNAPSHOT")
            path_info = PathInfo(self.test_base_path, "", "test.parquet", "test_config.json")
            
            try:
                self.archiver.archive_data(None, path_info, config)
                self.log_test_result("错误参数-None DataFrame", False, "未能检测到None DataFrame")
            except ValueError:
                self.log_test_result("错误参数-None DataFrame", True, "成功检测None DataFrame")
        except Exception as e:
            self.log_test_result("错误参数-None DataFrame", False, f"异常: {str(e)}")
        
        # 测试5.3: 无效配置参数
        try:
            df = self.create_sample_dataframe(10)
            
            try:
                # 空的source_name
                config = ArchiveConfig("", "test", "SNAPSHOT")
                self.log_test_result("错误参数-空source_name", False, "未能检测到空source_name")
            except ValueError:
                self.log_test_result("错误参数-空source_name", True, "成功检测空source_name")
                
            try:
                # 空的data_type
                config = ArchiveConfig("tushare", "", "SNAPSHOT")
                self.log_test_result("错误参数-空data_type", False, "未能检测到空data_type")
            except ValueError:
                self.log_test_result("错误参数-空data_type", True, "成功检测空data_type")
                
            try:
                # 空的archive_type
                config = ArchiveConfig("tushare", "test", "")
                self.log_test_result("错误参数-空archive_type", False, "未能检测到空archive_type")
            except ValueError:
                self.log_test_result("错误参数-空archive_type", True, "成功检测空archive_type")
                
        except Exception as e:
            self.log_test_result("错误参数-配置验证", False, f"异常: {str(e)}")
    
    def test_6_get_archive_info(self):
        """测试6: 获取归档信息测试"""
        print("\n=== 测试6: 获取归档信息测试 ===")
        
        try:
            # 获取已存在文件的归档信息
            data_path = f"{self.test_base_path}/data.parquet"
            
            if Path(data_path).exists():
                info = self.archiver.get_archive_info(data_path)
                
                # 验证信息完整性
                required_fields = ['source_name', 'data_type', 'archive_type', 'created_at', 'data_md5', 'data_shape']
                missing_fields = [field for field in required_fields if field not in info]
                
                if not missing_fields:
                    self.log_test_result("获取归档信息测试", True, "成功获取完整的归档信息")
                else:
                    self.log_test_result("获取归档信息测试", False, f"缺少字段: {missing_fields}")
            else:
                self.log_test_result("获取归档信息测试", False, "测试文件不存在")
                
        except Exception as e:
            self.log_test_result("获取归档信息测试", False, f"异常: {str(e)}")
    
    def run_all_tests(self):
        """运行所有测试"""
        print("开始运行UniversalArchiver全面测试...")
        print(f"测试基础路径: {self.test_base_path}")
        
        # 确保测试目录存在
        Path(self.test_base_path).mkdir(parents=True, exist_ok=True)
        
        # 运行所有测试
        self.test_1_basic_functionality()
        self.test_2_duplicate_skip()
        self.test_3_archive_functionality()
        self.test_4_data_reduction_protection()
        self.test_5_error_parameter_handling()
        self.test_6_get_archive_info()
        
        # 输出测试总结
        self.print_test_summary()
    
    def print_test_summary(self):
        """打印测试总结"""
        print("\n" + "="*60)
        print("测试总结")
        print("="*60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])
        failed_tests = total_tests - passed_tests
        
        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests} ✅")
        print(f"失败: {failed_tests} ❌")
        print(f"成功率: {passed_tests/total_tests*100:.1f}%")
        
        if failed_tests > 0:
            print("\n失败的测试:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['test_name']}: {result['message']}")
        
        # 保存测试结果到文件
        result_file = Path(self.test_base_path) / "test_results.json"
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump({
                'summary': {
                    'total_tests': total_tests,
                    'passed_tests': passed_tests,
                    'failed_tests': failed_tests,
                    'success_rate': passed_tests/total_tests*100
                },
                'details': self.test_results
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细测试结果已保存到: {result_file}")
        
        # 显示创建的文件
        print("\n创建的测试文件:")
        test_path = Path(self.test_base_path)
        for file_path in test_path.rglob("*"):
            if file_path.is_file():
                print(f"  - {file_path}")

if __name__ == "__main__":
    tester = UniversalArchiverTester()
    tester.run_all_tests()