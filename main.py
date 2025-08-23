#!/usr/bin/env python3
"""
通用Tushare数据归档器 - 主运行脚本

支持三种归档模式:
1. period: 按财报季度归档 (适用于 income, balancesheet 等)。
2. date:   按公告日期归档 (适用于 dividend 等事件驱动数据)。
3. snapshot: 按快照模式归档 (适用于 stock_basic 等日频全量更新数据)。

--- 常用命令示例 ---

# --- 1. 按季度归档 (period-based) ---

#   利润表 (income)
#   历史回填: python main.py --archiver-type period --data-type income --mode backfill
#   增量更新: python main.py --archiver-type period --data-type income --mode incremental
#   查看摘要: python main.py --archiver-type period --data-type income --mode summary

#   资产负债表 (balancesheet)
#   python main.py --archiver-type period --data-type balancesheet --mode backfill


# --- 2. 按日期归档 (date-based) ---

#   分红送股 (dividend)
#   历史回填: python main.py --archiver-type date --data-type dividend --mode backfill
#   增量更新: python main.py --archiver-type date --data-type dividend --mode incremental
#   查看摘要: python main.py --archiver-type date --data-type dividend --mode summary

# --- 3. 按快照归档 (snapshot-based) ---

#   股票基础信息 (stock_basic)
#   更新快照: python main.py --archiver-type snapshot --data-type stock_basic --mode update
#   查看摘要: python main.py --archiver-type snapshot --data-type stock_basic --mode summary

"""

#   财务审计意见 (fina_audit)
#   python main.py --archiver-type period --data-type fina_audit --mode backfill


import argparse

from period_archiver import PeriodArchiver
from date_archiver import DateArchiver
from snapshot_archiver import SnapshotArchiver
from trade_date_archiver import TradeDateArchiver
from tushare_reader import TushareReader
from stock_driven_archiver import StockDrivenArchiver

def main():
    parser = argparse.ArgumentParser(description='通用Tushare数据归档系统')
    parser.add_argument('--archiver-type', type=str, choices=['period', 'date', 'snapshot', 'trade_date', 'stock_driven'], default='period',
                        help='归档器类型: period (季度), date (公告日), snapshot (快照), trade_date (交易日), stock_driven (股票驱动)')
    parser.add_argument('--data-type', type=str, required=True,
                        help='Tushare数据类型 (例如: income, dividend)')
    parser.add_argument('--mode', choices=['backfill', 'incremental', 'summary', 'update'],
                       default='incremental', help='运行模式 (snapshot类型支持update模式)')
    parser.add_argument('--lookback', type=int, default=12,
                       help='增量更新时回溯的季度数')
    parser.add_argument('--data-path', default='./data',
                       help='数据存储路径')
    parser.add_argument('--start-date', type=str, help='回填或更新的开始日期 (格式: YYYYMMDD)')

    args = parser.parse_args()

    if args.archiver_type == 'period':
        try:
            archiver = PeriodArchiver(data_type=args.data_type, base_path=args.data_path)
            reader = TushareReader(data_type=args.data_type, base_path=args.data_path)
        except ValueError as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            archiver.backfill()
        elif args.mode == 'incremental':
            archiver.update(lookback_quarters=args.lookback)
        elif args.mode == 'summary':
            print(f"=== '{args.data_type}' 数据概要统计 ===")
            summary = reader.get_data_summary()
            if not summary.empty:
                print(summary.to_string(index=False))
            else:
                print("暂无数据")
            print(f"\n=== '{args.data_type}' 最近请求日志 ===")
            all_logs = reader.get_request_log(limit=None) # 获取全部日志以统计总数
            recent_logs = all_logs.head(10) # 只显示最近10条

            print(f"    日志总条数: {len(all_logs)}")
            if not recent_logs.empty:
                print("    最近10条日志:")
                print(recent_logs[['period', 'ingest_date', 'row_count', 'status', 'created_at']].to_string(index=False))
            else:
                print("    暂无日志")

    elif args.archiver_type == 'date':
        try:
            archiver = DateArchiver(data_type=args.data_type, base_path=args.data_path)
        except ValueError as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            kwargs = {}
            if args.start_date:
                kwargs['start_date_str'] = args.start_date
            archiver.backfill(**kwargs)
        elif args.mode == 'incremental':
            archiver.update()

    elif args.archiver_type == 'snapshot':
        try:
            archiver = SnapshotArchiver(data_type=args.data_type, base_path=args.data_path)
        except ValueError as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'update':
            archiver.update()
        elif args.mode == 'backfill': # Alias for update in snapshot mode
            archiver.backfill()
        elif args.mode == 'summary':
            print(f"'summary' mode for snapshot is not yet refactored.")

    elif args.archiver_type == 'trade_date':
        try:
            archiver = TradeDateArchiver(data_type=args.data_type, base_path=args.data_path)
        except (ValueError, FileNotFoundError) as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            kwargs = {}
            if args.start_date:
                kwargs['start_date_str'] = args.start_date
            archiver.backfill(**kwargs)
        elif args.mode == 'incremental':
            archiver.update()
        elif args.mode == 'summary':
            print(f"'summary' mode for trade_date is not yet implemented.")

    elif args.archiver_type == 'stock_driven':
        try:
            archiver = StockDrivenArchiver(data_type=args.data_type, base_path=args.data_path)
        except (ValueError, FileNotFoundError) as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            archiver.backfill()
        elif args.mode in ['incremental', 'update']:
            archiver.update()
        elif args.mode == 'summary':
            print(f"'summary' mode for stock_driven is not yet implemented.")

    print("完成!")

if __name__ == "__main__":
    main()
