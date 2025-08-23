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

    # --- Archiver Instantiation ---
    archiver_map = {
        'period': PeriodArchiver,
        'date': DateArchiver,
        'snapshot': SnapshotArchiver,
        'trade_date': TradeDateArchiver,
        'stock_driven': StockDrivenArchiver
    }

    try:
        archiver_class = archiver_map.get(args.archiver_type)
        if not archiver_class:
            print(f"错误：未知的归档器类型 '{args.archiver_type}'")
            return
        archiver = archiver_class(data_type=args.data_type, base_path=args.data_path)
        reader = TushareReader(data_type=args.data_type, base_path=args.data_path)
    except (ValueError, FileNotFoundError) as e:
        print(f"初始化错误: {e}")
        return

    # --- Mode Execution ---
    if args.mode == 'backfill':
        kwargs = {}
        if args.start_date:
            kwargs['start_date_str'] = args.start_date
        archiver.backfill(**kwargs)

    elif args.mode in ['incremental', 'update']:
        kwargs = {}
        if args.archiver_type == 'period':
            kwargs['lookback_quarters'] = args.lookback
        archiver.update(**kwargs)

    elif args.mode == 'summary':
        print(f"--- Data Summary for '{args.data_type}' ---")
        summary = reader.get_data_summary()
        if not summary.empty:
            print(summary.to_string(index=False))
        else:
            print("No data found.")

        print(f"\n--- Recent Logs for '{args.data_type}' ---")
        logs = reader.get_request_log(limit=20)
        if not logs.empty:
            print(logs.to_string(index=False))
        else:
            print("No logs found.")
    else:
        print(f"错误：归档器 '{args.archiver_type}' 不支持 '{args.mode}' 模式。")

    print("完成!")

if __name__ == "__main__":
    main()
