#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QLIB数据处理器 - 命令行接口模块

该模块提供QLIB数据处理的命令行接口类，用于将NORM层数据转换为QLIB-READY格式。

主要功能:
- 数据处理: 将NORM层数据转换为QLIB-READY格式
- 数据验证: 验证已处理数据的格式和完整性
- 股票列表: 列出可用的股票代码
- 数据清理: 清理指定接口的数据

使用方式:
该模块通过主项目的统一CLI入口调用，不建议直接运行。
请使用: python cli.py qlib --help

"""

import argparse
import os
import sys
import pandas as pd
import gc
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 相对导入
from ..processors.quotes.manager import QlibReadyDataManager
from ..processors.quotes.processor import QlibDataProcessor
from ..core.validator import QlibFormatValidator
from ...base import DataLayer
from ...contracts import InterfaceType


class QlibDataProcessorCLI:
    """QLIB数据处理器命令行接口"""
    
    def __init__(self, data_root: str = None):
        """初始化处理器
        
        Args:
            data_root: 数据根目录，默认为项目根目录下的data文件夹
        """
        if data_root is None:
            # 从当前文件位置向上找到项目根目录
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent.parent.parent  # 回到项目根目录
            data_root = str(project_root / 'data')
        
        self.data_root = data_root
        self.norm_data_root = os.path.join(data_root, 'norm')
        self.qlib_ready_root = os.path.join(data_root, 'qlib_ready')
        
        # 初始化组件
        self.manager = QlibReadyDataManager(data_root=data_root)
        self.processor = QlibDataProcessor()
        self.validator = QlibFormatValidator()
        
        # 接口类型映射
        self.interface_map = {
            'quotes_daily': InterfaceType.QUOTES_DAILY,
            'quotes_with_adj': InterfaceType.QUOTES_WITH_ADJ,
            'quotes_with_basic': InterfaceType.QUOTES_WITH_BASIC,
        }
    
    def _get_norm_data_path(self, interface: str, year: int = None) -> str:
        """获取NORM层数据路径
        
        Args:
            interface: 接口类型
            year: 年份
            
        Returns:
            数据文件路径
        """
        interface_dir = os.path.join(self.norm_data_root, interface)
        
        if year:
            filename = f"{interface}_normal_{year}.parquet"
            return os.path.join(interface_dir, filename)
        else:
            # 返回目录，让调用者自己处理
            return interface_dir
    
    def _load_norm_data(self, interface: str, year: int = None, 
                       start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """加载NORM层数据
        
        Args:
            interface: 接口类型
            year: 年份
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            数据DataFrame
        """
        if year:
            data_path = self._get_norm_data_path(interface, year)
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"数据文件不存在: {data_path}")
            
            print(f"加载数据文件: {data_path}")
            df = pd.read_parquet(data_path)
            
        else:
            # 加载多个文件或日期范围
            interface_dir = self._get_norm_data_path(interface)
            if not os.path.exists(interface_dir):
                raise FileNotFoundError(f"数据目录不存在: {interface_dir}")
            
            # 查找所有parquet文件
            parquet_files = list(Path(interface_dir).glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(f"在目录 {interface_dir} 中未找到parquet文件")
            
            print(f"找到 {len(parquet_files)} 个数据文件")
            dfs = []
            for file_path in sorted(parquet_files):
                print(f"  加载: {file_path.name}")
                df_temp = pd.read_parquet(file_path)
                dfs.append(df_temp)
            
            df = pd.concat(dfs, ignore_index=True)
        
        # 日期过滤
        if start_date or end_date:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            
            if start_date:
                start_dt = pd.to_datetime(start_date)
                df = df[df['trade_date'] >= start_dt]
            
            if end_date:
                end_dt = pd.to_datetime(end_date)
                df = df[df['trade_date'] <= end_dt]
            
            # 转换回原格式
            df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d').astype(int)
        
        return df
    
    def _process_data_batch(self, symbols: List[str], df: pd.DataFrame, 
                           interface_type: InterfaceType, batch_size: int = 100) -> Dict[str, int]:
        """批量处理股票数据
        
        Args:
            symbols: 股票代码列表
            df: 数据DataFrame
            interface_type: 接口类型
            batch_size: 批处理大小
            
        Returns:
            处理结果统计
        """
        results = {'success': 0, 'failed': 0}
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"\n处理批次 {batch_num}/{total_batches} ({len(batch_symbols)} 个股票)")
            
            for symbol in batch_symbols:
                try:
                    # 提取单个股票数据
                    symbol_data = df[df['symbol'] == symbol].copy()
                    if len(symbol_data) == 0:
                        continue
                    
                    # 保存数据
                    self.manager.save_data(
                        data=symbol_data,
                        interface_type=interface_type,
                        symbol=symbol
                    )
                    results['success'] += 1
                    
                except Exception as e:
                    print(f"  ✗ 处理 {symbol} 失败: {e}")
                    results['failed'] += 1
            
            print(f"  批次结果: 成功 {len(batch_symbols) - (results['failed'] - (i // batch_size) * batch_size)}, 失败 {results['failed'] - (i // batch_size) * batch_size}")
            print(f"  累计进度: {results['success'] + results['failed']}/{len(symbols)} ({(results['success'] + results['failed'])/len(symbols)*100:.1f}%)")
            
            # 定期垃圾回收
            if batch_num % 5 == 0:
                gc.collect()
        
        return results
    
    def process_data(self, interface: str, year: int = None, 
                    start_date: str = None, end_date: str = None, 
                    batch_size: int = 100) -> None:
        """处理数据
        
        Args:
            interface: 接口类型
            year: 年份
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批处理大小
        """
        print(f"=== QLIB数据处理器 ===")
        print(f"开始时间: {datetime.now()}")
        print(f"接口类型: {interface}")
        
        if interface not in self.interface_map:
            raise ValueError(f"不支持的接口类型: {interface}。支持的类型: {list(self.interface_map.keys())}")
        
        interface_type = self.interface_map[interface]
        
        try:
            # 1. 加载数据
            print(f"\n=== 1. 数据加载 ===")
            df = self._load_norm_data(interface, year, start_date, end_date)
            print(f"✓ 成功加载数据，形状: {df.shape}")
            print(f"  数据时间范围: {df['trade_date'].min()} - {df['trade_date'].max()}")
            print(f"  股票数量: {df['ts_code'].nunique()}")
            
            # 2. 数据预处理
            print(f"\n=== 2. 数据预处理 ===")
            print(f"原始数据: {len(df)} 条")
            
            # 过滤正常数据
            if 'execution_mode' in df.columns:
                df_clean = df[df['execution_mode'] == '正常'].copy()
                print(f"清洗后数据: {len(df_clean)} 条 (移除了 {len(df) - len(df_clean)} 条异常数据)")
            else:
                df_clean = df.copy()
                print(f"数据无需清洗: {len(df_clean)} 条")
            
            del df
            gc.collect()
            
            # 数据预处理
            print("正在进行数据预处理...")
            processed_data = self.processor._preprocess_quotes(df_clean)
            print(f"✓ 数据预处理完成，处理后数据形状: {processed_data.shape}")
            
            del df_clean
            gc.collect()
            
            # 3. 获取股票列表
            print(f"\n=== 3. 数据分组 ===")
            all_symbols = processed_data['symbol'].unique().tolist()
            print(f"✓ 获取股票列表完成，共 {len(all_symbols)} 个股票")
            
            # 4. 数据验证（样本）
            print(f"\n=== 4. 数据验证 (样本) ===")
            validation_count = 0
            validation_success = 0
            
            for symbol in all_symbols[:3]:  # 只验证前3个股票
                validation_count += 1
                try:
                    symbol_data = processed_data[processed_data['symbol'] == symbol].copy()
                    is_valid, errors = self.validator.validate_symbol_data_format(symbol_data, symbol)
                    if is_valid:
                        validation_success += 1
                        print(f"✓ {symbol} 格式验证通过 (数据形状: {symbol_data.shape})")
                    else:
                        print(f"✗ {symbol} 格式验证失败:")
                        for error in errors:
                            print(f"    - {error}")
                except Exception as e:
                    print(f"✗ {symbol} 验证异常: {e}")
            
            print(f"验证结果: {validation_success}/{validation_count} 个股票通过验证")
            
            # 5. 批量数据保存
            print(f"\n=== 5. 批量数据保存 ===")
            results = self._process_data_batch(all_symbols, processed_data, interface_type, batch_size)
            
            print(f"\n✓ 数据保存完成")
            print(f"  总计: 成功 {results['success']}, 失败 {results['failed']}")
            print(f"  成功率: {results['success']/(results['success']+results['failed'])*100:.1f}%")
            
        except Exception as e:
            print(f"错误: 数据处理失败 - {e}")
            import traceback
            traceback.print_exc()
            return
        
        print(f"\n=== 处理完成 ===")
        print(f"结束时间: {datetime.now()}")
        print(f"输出目录: {os.path.join(self.data_root, 'qlib_ready', interface)}")
    
    def validate_data(self, interface: str) -> None:
        """验证已处理的数据
        
        Args:
            interface: 接口类型
        """
        print(f"=== 数据验证 ===")
        print(f"接口类型: {interface}")
        
        if interface not in self.interface_map:
            raise ValueError(f"不支持的接口类型: {interface}")
        
        interface_type = self.interface_map[interface]
        
        try:
            # 获取已保存的股票列表
            saved_symbols = self.manager.list_available_symbols(interface_type)
            print(f"已保存股票数量: {len(saved_symbols)}")
            
            if not saved_symbols:
                print("没有找到已保存的数据")
                return
            
            # 验证前几个股票
            validation_count = min(10, len(saved_symbols))
            validation_success = 0
            
            print(f"\n验证前 {validation_count} 个股票:")
            for symbol in saved_symbols[:validation_count]:
                try:
                    # 加载数据
                    data = self.manager.load_data(interface_type, symbol)
                    
                    # 验证格式
                    is_valid, errors = self.validator.validate_symbol_data_format(data, symbol)
                    if is_valid:
                        validation_success += 1
                        print(f"✓ {symbol}: 数据形状 {data.shape}, 时间范围 {data.index.min()} - {data.index.max()}")
                    else:
                        print(f"✗ {symbol}: 验证失败")
                        for error in errors:
                            print(f"    - {error}")
                            
                except Exception as e:
                    print(f"✗ {symbol}: 加载失败 - {e}")
            
            print(f"\n验证结果: {validation_success}/{validation_count} 个股票通过验证")
            
        except Exception as e:
            print(f"错误: 数据验证失败 - {e}")
    
    def list_symbols(self, interface: str) -> None:
        """列出可用的股票代码
        
        Args:
            interface: 接口类型
        """
        print(f"=== 股票列表 ===")
        print(f"接口类型: {interface}")
        
        if interface not in self.interface_map:
            raise ValueError(f"不支持的接口类型: {interface}")
        
        interface_type = self.interface_map[interface]
        
        try:
            symbols = self.manager.list_available_symbols(interface_type)
            print(f"\n共找到 {len(symbols)} 个股票:")
            
            if symbols:
                # 按交易所分组显示
                sh_symbols = [s for s in symbols if s.startswith('SH')]
                sz_symbols = [s for s in symbols if s.startswith('SZ')]
                bj_symbols = [s for s in symbols if s.startswith('BJ')]
                other_symbols = [s for s in symbols if not any(s.startswith(p) for p in ['SH', 'SZ', 'BJ'])]
                
                if sh_symbols:
                    print(f"\n上海证券交易所 ({len(sh_symbols)} 个):")
                    print(f"  {', '.join(sh_symbols[:10])}{'...' if len(sh_symbols) > 10 else ''}")
                
                if sz_symbols:
                    print(f"\n深圳证券交易所 ({len(sz_symbols)} 个):")
                    print(f"  {', '.join(sz_symbols[:10])}{'...' if len(sz_symbols) > 10 else ''}")
                
                if bj_symbols:
                    print(f"\n北京证券交易所 ({len(bj_symbols)} 个):")
                    print(f"  {', '.join(bj_symbols[:10])}{'...' if len(bj_symbols) > 10 else ''}")
                
                if other_symbols:
                    print(f"\n其他 ({len(other_symbols)} 个):")
                    print(f"  {', '.join(other_symbols[:10])}{'...' if len(other_symbols) > 10 else ''}")
            
        except Exception as e:
            print(f"错误: 获取股票列表失败 - {e}")
    
    def clean_data(self, interface: str) -> None:
        """清理指定接口的数据
        
        Args:
            interface: 接口类型
        """
        print(f"=== 数据清理 ===")
        print(f"接口类型: {interface}")
        
        if interface not in self.interface_map:
            raise ValueError(f"不支持的接口类型: {interface}")
        
        # 确认操作
        confirm = input(f"确认要删除 {interface} 接口的所有数据吗？(yes/no): ")
        if confirm.lower() != 'yes':
            print("操作已取消")
            return
        
        try:
            interface_dir = os.path.join(self.qlib_ready_root, interface)
            if os.path.exists(interface_dir):
                import shutil
                shutil.rmtree(interface_dir)
                print(f"✓ 已删除目录: {interface_dir}")
            else:
                print(f"目录不存在: {interface_dir}")
                
        except Exception as e:
            print(f"错误: 数据清理失败 - {e}")


def main():
    parser = argparse.ArgumentParser(
        description='QLIB数据处理器 - 将NORM层数据转换为QLIB-READY格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  %(prog)s --mode process --year 2024 --interface quotes_daily
  %(prog)s --mode process --start-date 2024-01-01 --end-date 2024-12-31 --interface quotes_daily
  %(prog)s --mode validate --interface quotes_daily
  %(prog)s --mode list --interface quotes_daily
  %(prog)s --mode clean --interface quotes_daily
        """
    )
    
    parser.add_argument('--mode', 
                       choices=['process', 'validate', 'list', 'clean'], 
                       required=True,
                       help='运行模式')
    
    parser.add_argument('--interface', 
                       choices=['quotes_daily', 'quotes_with_adj', 'quotes_with_basic'],
                       required=True,
                       help='接口类型')
    
    parser.add_argument('--year', 
                       type=int,
                       help='处理指定年份的数据')
    
    parser.add_argument('--start-date', 
                       help='开始日期 (YYYY-MM-DD格式)')
    
    parser.add_argument('--end-date', 
                       help='结束日期 (YYYY-MM-DD格式)')
    
    parser.add_argument('--batch-size', 
                       type=int, 
                       default=100,
                       help='批处理大小 (默认: 100)')
    
    parser.add_argument('--data-root', 
                       help='数据根目录 (默认: ./data)')
    
    args = parser.parse_args()
    
    # 初始化处理器
    try:
        processor = QlibDataProcessorCLI(data_root=args.data_root)
    except Exception as e:
        print(f"错误: 初始化处理器失败 - {e}")
        return 1
    
    # 执行相应操作
    try:
        if args.mode == 'process':
            processor.process_data(
                interface=args.interface,
                year=args.year,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size
            )
        elif args.mode == 'validate':
            processor.validate_data(args.interface)
        elif args.mode == 'list':
            processor.list_symbols(args.interface)
        elif args.mode == 'clean':
            processor.clean_data(args.interface)
            
    except Exception as e:
        print(f"错误: 操作失败 - {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())