#!/usr/bin/env python3
"""
Raw数据管理CLI工具

统一的数据获取、更新、补全工具，支持分页和多种运行模式。

使用示例：
# 1. 按配置表更新（现有的update模式）
python -m caiyuangungun.data.raw.cli update

# 2. 强制更新历史数据（无论文件是否存在）
python -m caiyuangungun.data.raw.cli force-update --data-types income,balancesheet,cashflow --periods 20240630,20240930

# 3. 补全历史数据（自动寻路补全缺失的数据）
python -m caiyuangungun.data.raw.cli backfill --data-types daily,daily_basic --lookback-months 6

# 4. 定向强制更新（指定数据类型和分区）
python -m caiyuangungun.data.raw.cli targeted-update --data-type income --periods 20240630,20240930
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import pandas as pd

# 获取项目根目录
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from caiyuangungun.data.raw.archivers.period_archiver import PeriodArchiver
from caiyuangungun.data.raw.archivers.trade_date_archiver import TradeDateArchiver
from caiyuangungun.data.raw.archivers.snapshot_archiver import SnapshotArchiver
from caiyuangungun.data.raw.manifest import DATA_ASSETS
from caiyuangungun.data.raw.ip_check import ensure_shanghai_ip

class RawDataCLI:
    """Raw数据管理CLI工具"""
    
    # 支持的数据类型配置
    SUPPORTED_DATA_TYPES = {
        # 财务报表数据 (PeriodArchiver)
        'income': {
            'archiver': PeriodArchiver,
            'type': 'period',
            'description': '利润表'
        },
        'balancesheet': {
            'archiver': PeriodArchiver,
            'type': 'period',
            'description': '资产负债表'
        },
        'cashflow': {
            'archiver': PeriodArchiver,
            'type': 'period',
            'description': '现金流量表'
        },
        
        # 交易日数据 (TradeDateArchiver)
        'daily': {
            'archiver': TradeDateArchiver,
            'type': 'trade_date',
            'description': '日交易数据'
        },
        'daily_basic': {
            'archiver': TradeDateArchiver,
            'type': 'trade_date',
            'description': '日基本信息'
        },
        'adj_factor': {
            'archiver': TradeDateArchiver,
            'type': 'trade_date',
            'description': '复权因子'
        },
        
        # 快照数据 (SnapshotArchiver)
        'stock_basic': {
            'archiver': SnapshotArchiver,
            'type': 'snapshot',
            'description': '股票基本信息'
        },
        'index_basic': {
            'archiver': SnapshotArchiver,
            'type': 'snapshot',
            'description': '指数基本信息'
        },
        'index_classify': {
            'archiver': SnapshotArchiver,
            'type': 'snapshot',
            'description': '指数分类'
        },
        'trade_cal': {
            'archiver': SnapshotArchiver,
            'type': 'snapshot',
            'description': '交易日历'
        }
    }
    
    def __init__(self):
        # 首先进行IP地区检测
        ensure_shanghai_ip()
        
        self.project_root = project_root
        self.data_root = project_root / "data"
        print(f"🚀 Raw数据管理CLI已初始化")
        print(f"📁 项目根目录: {self.project_root}")
        print(f"💾 数据根目录: {self.data_root}")
    
    def _generate_periods(self, lookback_months: int) -> List[str]:
        """生成财务报表期间列表"""
        from dateutil.relativedelta import relativedelta
        
        periods = []
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=lookback_months)
        
        current_date = start_date
        while current_date <= end_date:
            year = current_date.year
            quarter = (current_date.month - 1) // 3 + 1
            month_day = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[quarter]
            periods.append(f"{year}{month_day}")
            current_date += relativedelta(months=3)
        
        return sorted(list(set(periods)))
    
    def _generate_trade_dates(self, lookback_days: int) -> List[str]:
        """生成交易日期列表"""
        # 这里简化处理，实际应该读取交易日历
        dates = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        
        current_date = start_date
        while current_date <= end_date:
            # 跳过周末
            if current_date.weekday() < 5:
                dates.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        return dates
    
    def _get_existing_partitions(self, data_type: str) -> Set[str]:
        """获取已存在的数据分区"""
        landing_path = self.data_root / "raw" / "landing" / "tushare" / data_type
        if not landing_path.exists():
            return set()
        
        partitions = set()
        for partition_dir in landing_path.iterdir():
            if partition_dir.is_dir():
                partition_name = partition_dir.name.split('=')[1] if '=' in partition_dir.name else partition_dir.name
                partitions.add(partition_name)
        
        return partitions
    
    def _process_with_retry(self, archiver, partition: str, max_retries: int = 3, enable_pagination: bool = True) -> bool:
        """带重试机制的数据处理"""
        config = self.SUPPORTED_DATA_TYPES[archiver.data_type]
        
        for attempt in range(max_retries):
            try:
                print(f"  📡 尝试 {attempt + 1}/{max_retries}: 处理 {partition}...")
                
                if config['type'] == 'period':
                    if enable_pagination and hasattr(archiver, '_process_period_with_pagination'):
                        archiver._process_period_with_pagination(partition)
                    else:
                        archiver._process_period(partition)
                elif config['type'] == 'trade_date':
                    if enable_pagination and hasattr(archiver, '_process_day_with_pagination'):
                        archiver._process_day_with_pagination(partition)
                    else:
                        archiver._process_day(partition)
                elif config['type'] == 'snapshot':
                    archiver.update()
                
                print(f"  ✅ 成功处理 {partition}")
                return True
                
            except Exception as e:
                print(f"  ❌ 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    print(f"  ⏳ 等待 {attempt + 1} 秒后重试...")
                    time.sleep(attempt + 1)
                else:
                    print(f"  🚫 达到最大重试次数，放弃处理 {partition}")
        
        return False
    
    def update_mode(self, data_types: List[str] = None):
        """按配置表更新模式（现有的update模式）"""
        print(f"\n🔄 按配置表更新模式")
        
        # 如果没有指定数据类型，使用配置表中的所有支持类型
        if not data_types:
            data_types = [asset['name'] for asset in DATA_ASSETS 
                         if asset['name'] in self.SUPPORTED_DATA_TYPES]
        
        print(f"📊 处理数据类型: {', '.join(data_types)}")
        
        success_count = 0
        total_count = len(data_types)
        
        for data_type in data_types:
            if data_type not in self.SUPPORTED_DATA_TYPES:
                print(f"⚠️  跳过不支持的数据类型: {data_type}")
                continue
            
            print(f"\n--- 处理 {data_type} ({self.SUPPORTED_DATA_TYPES[data_type]['description']}) ---")
            
            try:
                config = self.SUPPORTED_DATA_TYPES[data_type]
                archiver = config['archiver'](data_type=data_type)
                
                # 根据数据类型使用相应的更新方法
                if config['type'] == 'period':
                    archiver.update(lookback_months=24)  # 财务数据回溯24个月
                elif config['type'] == 'trade_date':
                    archiver.update(lookback_days=120)   # 交易数据回溯120天
                elif config['type'] == 'snapshot':
                    archiver.update()                    # 快照数据直接更新
                
                success_count += 1
                print(f"✅ {data_type} 更新完成")
                
            except Exception as e:
                print(f"❌ {data_type} 更新失败: {e}")
        
        print(f"\n📋 更新完成: {success_count}/{total_count} 个数据类型成功")
    
    def force_update_mode(self, data_types: List[str], partitions: List[str]):
        """强制更新历史数据模式（无论文件是否存在）"""
        print(f"\n💪 强制更新历史数据模式")
        print(f"📊 数据类型: {', '.join(data_types)}")
        print(f"📅 分区: {', '.join(partitions)}")
        
        total_tasks = len(data_types) * len(partitions)
        success_count = 0
        
        for data_type in data_types:
            if data_type not in self.SUPPORTED_DATA_TYPES:
                print(f"⚠️  跳过不支持的数据类型: {data_type}")
                continue
            
            print(f"\n--- 强制更新 {data_type} ({self.SUPPORTED_DATA_TYPES[data_type]['description']}) ---")
            
            try:
                config = self.SUPPORTED_DATA_TYPES[data_type]
                archiver = config['archiver'](data_type=data_type)
                
                for partition in partitions:
                    if self._process_with_retry(archiver, partition, max_retries=3, enable_pagination=True):
                        success_count += 1
                
            except Exception as e:
                print(f"❌ 创建 {data_type} 归档器失败: {e}")
        
        print(f"\n📋 强制更新完成: {success_count}/{total_tasks} 个任务成功")
    
    def backfill_mode(self, data_types: List[str], lookback_months: int = None, lookback_days: int = None):
        """补全历史数据模式（自动寻路补全缺失数据）"""
        print(f"\n🔍 补全历史数据模式")
        print(f"📊 数据类型: {', '.join(data_types)}")
        
        total_missing = 0
        total_filled = 0
        
        for data_type in data_types:
            if data_type not in self.SUPPORTED_DATA_TYPES:
                print(f"⚠️  跳过不支持的数据类型: {data_type}")
                continue
            
            config = self.SUPPORTED_DATA_TYPES[data_type]
            print(f"\n--- 补全 {data_type} ({config['description']}) ---")
            
            # 生成应该存在的分区列表
            if config['type'] == 'period':
                months = lookback_months or 24
                expected_partitions = set(self._generate_periods(months))
            elif config['type'] == 'trade_date':
                days = lookback_days or 120
                expected_partitions = set(self._generate_trade_dates(days))
            else:
                print(f"  ⚠️  快照数据不需要补全")
                continue
            
            # 获取已存在的分区
            existing_partitions = self._get_existing_partitions(data_type)
            
            # 找出缺失的分区
            missing_partitions = expected_partitions - existing_partitions
            
            print(f"  📊 应有分区: {len(expected_partitions)}")
            print(f"  📊 已有分区: {len(existing_partitions)}")
            print(f"  📊 缺失分区: {len(missing_partitions)}")
            
            if not missing_partitions:
                print(f"  ✅ 无缺失数据")
                continue
            
            total_missing += len(missing_partitions)
            
            # 补全缺失的分区
            try:
                archiver = config['archiver'](data_type=data_type)
                filled_count = 0
                
                for partition in sorted(missing_partitions):
                    print(f"  🔧 补全缺失分区: {partition}")
                    if self._process_with_retry(archiver, partition, max_retries=3, enable_pagination=True):
                        filled_count += 1
                
                total_filled += filled_count
                print(f"  📈 补全结果: {filled_count}/{len(missing_partitions)} 个分区成功")
                
            except Exception as e:
                print(f"  ❌ 创建 {data_type} 归档器失败: {e}")
        
        print(f"\n📋 补全完成: {total_filled}/{total_missing} 个缺失分区成功补全")
    
    def targeted_update_mode(self, data_type: str, partitions: List[str]):
        """定向强制更新模式"""
        print(f"\n🎯 定向强制更新模式")
        print(f"📊 数据类型: {data_type} ({self.SUPPORTED_DATA_TYPES[data_type]['description']})")
        print(f"📅 分区: {', '.join(partitions)}")
        
        if data_type not in self.SUPPORTED_DATA_TYPES:
            print(f"❌ 不支持的数据类型: {data_type}")
            return
        
        try:
            config = self.SUPPORTED_DATA_TYPES[data_type]
            archiver = config['archiver'](data_type=data_type)
            
            success_count = 0
            for partition in partitions:
                if self._process_with_retry(archiver, partition, max_retries=3, enable_pagination=True):
                    success_count += 1
            
            print(f"\n📋 定向更新完成: {success_count}/{len(partitions)} 个分区成功")
            
        except Exception as e:
            print(f"❌ 创建归档器失败: {e}")
    
    def list_supported_types(self):
        """列出支持的数据类型"""
        print(f"\n📋 支持的数据类型:")
        print(f"{'类型':<15} {'归档器':<20} {'描述':<15}")
        print("-" * 50)
        
        for data_type, config in self.SUPPORTED_DATA_TYPES.items():
            archiver_name = config['archiver'].__name__
            description = config['description']
            print(f"{data_type:<15} {archiver_name:<20} {description:<15}")

def main():
    parser = argparse.ArgumentParser(
        description='Raw数据管理CLI工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 1. update命令 - 按配置表更新
    update_parser = subparsers.add_parser('update', help='按配置表更新数据')
    update_parser.add_argument('--data-types', type=str, 
                              help='指定数据类型，用逗号分隔（不指定则处理所有）')
    
    # 2. force-update命令 - 强制更新历史数据
    force_update_parser = subparsers.add_parser('force-update', help='强制更新历史数据')
    force_update_parser.add_argument('--data-types', type=str, required=True,
                                    help='数据类型，用逗号分隔')
    force_update_parser.add_argument('--periods', type=str,
                                    help='期间/日期，用逗号分隔')
    force_update_parser.add_argument('--lookback-months', type=int, default=12,
                                    help='回溯月数（用于财务数据）')
    force_update_parser.add_argument('--lookback-days', type=int, default=60,
                                    help='回溯天数（用于交易数据）')
    
    # 3. backfill命令 - 补全历史数据
    backfill_parser = subparsers.add_parser('backfill', help='补全历史数据')
    backfill_parser.add_argument('--data-types', type=str, required=True,
                                help='数据类型，用逗号分隔')
    backfill_parser.add_argument('--lookback-months', type=int, default=24,
                                help='回溯月数（用于财务数据）')
    backfill_parser.add_argument('--lookback-days', type=int, default=120,
                                help='回溯天数（用于交易数据）')
    
    # 4. targeted-update命令 - 定向强制更新
    targeted_parser = subparsers.add_parser('targeted-update', help='定向强制更新')
    targeted_parser.add_argument('--data-type', type=str, required=True,
                                help='数据类型')
    targeted_parser.add_argument('--periods', type=str, required=True,
                                help='期间/日期，用逗号分隔')
    
    # 5. list命令 - 列出支持的数据类型
    subparsers.add_parser('list', help='列出支持的数据类型')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        cli = RawDataCLI()
        
        if args.command == 'update':
            data_types = None
            if args.data_types:
                data_types = [dt.strip() for dt in args.data_types.split(',')]
            cli.update_mode(data_types)
            
        elif args.command == 'force-update':
            data_types = [dt.strip() for dt in args.data_types.split(',')]
            
            if args.periods:
                partitions = [p.strip() for p in args.periods.split(',')]
            else:
                # 根据数据类型生成默认分区
                partitions = []
                for data_type in data_types:
                    if data_type in cli.SUPPORTED_DATA_TYPES:
                        config = cli.SUPPORTED_DATA_TYPES[data_type]
                        if config['type'] == 'period':
                            partitions.extend(cli._generate_periods(args.lookback_months))
                        elif config['type'] == 'trade_date':
                            partitions.extend(cli._generate_trade_dates(args.lookback_days))
                partitions = list(set(partitions))  # 去重
            
            cli.force_update_mode(data_types, partitions)
            
        elif args.command == 'backfill':
            data_types = [dt.strip() for dt in args.data_types.split(',')]
            cli.backfill_mode(data_types, args.lookback_months, args.lookback_days)
            
        elif args.command == 'targeted-update':
            partitions = [p.strip() for p in args.periods.split(',')]
            cli.targeted_update_mode(args.data_type, partitions)
            
        elif args.command == 'list':
            cli.list_supported_types()
        
        print(f"\n🎉 任务完成！")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
