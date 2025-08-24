#!/usr/bin/env python3
"""
通用Tushare数据归档器 - 主运行脚本

支持三种归档模式:
1. period: 按财报季度归档 (适用于 income, balancesheet 等)。
2. date:   按公告日期归档 (适用于 dividend 等事件驱动数据)。
3. snapshot: 按快照模式归档 (适用于 stock_basic 等日频全量更新数据)。
4. trade_date: 按交易日归档 (适用于 trade_cal 等交易日数据)。
5. stock_driven: 按股票驱动模式归档 (适用于 stock_basic 等股票驱动数据)。

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



#   指数基本信息 (index_basic)
#   更新快照: python main.py --archiver-type snapshot --data-type index_basic --mode update

#   申万行业分类 (index_classify)
#   更新快照: python main.py --archiver-type snapshot --data-type index_classify --mode update




"""

#   财务审计意见 (fina_audit)
#   python main.py --archiver-type period --data-type fina_audit --mode backfill



# --- 4. 按交易日归档 (trade_date-based) ---

#   大宗交易 (block_trade)
#   python main.py --archiver-type trade_date --data-type block_trade --mode backfill

# --- 5. 按股票代码归档 (stock_driven-based) ---

#   股东人数 (stk_holdernumber)
#   python main.py --archiver-type stock_driven --data-type stk_holdernumber --mode backfill

# --- 6. 按指数+月度归档 (index_monthly-based) ---

#   指数成分和权重 (index_weight)
#   python main.py --archiver-type index_monthly --data-type index_weight --mode backfill

#   指数日线行情 (index_daily)
#   python main.py --archiver-type stock_driven --data-type index_daily --mode backfill

import argparse

from period_archiver import PeriodArchiver
from date_archiver import DateArchiver
from snapshot_archiver import SnapshotArchiver
from trade_date_archiver import TradeDateArchiver
from tushare_reader import TushareReader
from stock_driven_archiver import StockDrivenArchiver
from index_monthly_archiver import IndexMonthlyArchiver
from config import COMMON_INDEXES

def main():
    parser = argparse.ArgumentParser(description='通用Tushare数据归档系统')
    parser.add_argument('--archiver-type', type=str, choices=['period', 'date', 'snapshot', 'trade_date', 'stock_driven', 'index_monthly'], default='period',
                        help='归档器类型: period (季度), date (公告日), snapshot (快照), trade_date (交易日), stock_driven (股票驱动), index_monthly (指数月度)')
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
        'stock_driven': StockDrivenArchiver,
        'index_monthly': IndexMonthlyArchiver
    }

    try:
        archiver_class = archiver_map.get(args.archiver_type)
        if not archiver_class:
            print(f"错误：未知的归档器类型 '{args.archiver_type}'")
            return
        # --- Archiver-specific Instantiation ---
        if args.archiver_type == 'date':
            date_field = 'report_date' if args.data_type == 'report_rc' else 'ann_date'
            archiver = archiver_class(data_type=args.data_type, base_path=args.data_path, date_field=date_field)
        elif args.data_type == 'index_daily':
            archiver = archiver_class(data_type=args.data_type, base_path=args.data_path, code_list=COMMON_INDEXES)
        else:
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
