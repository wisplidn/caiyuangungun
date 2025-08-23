#!/usr/bin/env python3
"""
通用Tushare数据归档器 - 主运行脚本

支持两种归档模式:
1. period: 按财报季度归档 (适用于 income, balancesheet 等)。
2. date:   按公告日期归档 (适用于 dividend 等事件驱动数据)。

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

"""

#   财务审计意见 (fina_audit)
#   python main.py --archiver-type period --data-type fina_audit --mode backfill


import argparse

from tushare_archiver import TushareArchiver
from tushare_reader import TushareReader
from date_driven_archiver import DateDrivenArchiver

def main():
    parser = argparse.ArgumentParser(description='通用Tushare数据归档系统')
    parser.add_argument('--archiver-type', type=str, choices=['period', 'date'], default='period',
                        help='归档器类型: period (按季度) 或 date (按日期)')
    parser.add_argument('--data-type', type=str, required=True,
                        help='Tushare数据类型 (例如: income, dividend)')
    parser.add_argument('--mode', choices=['backfill', 'incremental', 'summary'],
                       default='incremental', help='运行模式')
    parser.add_argument('--lookback', type=int, default=12,
                       help='增量更新时回溯的季度数')
    parser.add_argument('--data-path', default='./data',
                       help='数据存储路径')

    args = parser.parse_args()

    if args.archiver_type == 'period':
        try:
            archiver = TushareArchiver(data_type=args.data_type, base_path=args.data_path)
            reader = TushareReader(data_type=args.data_type, base_path=args.data_path)
        except ValueError as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            print(f"开始为 '{args.data_type}' (period-based) 进行历史数据回填...")
            archiver.historical_backfill()
        elif args.mode == 'incremental':
            print(f"开始为 '{args.data_type}' (period-based) 进行增量更新（回溯{args.lookback}个季度）...")
            archiver.incremental_update(lookback_quarters=args.lookback)
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
            archiver = DateDrivenArchiver(data_type=args.data_type, base_path=args.data_path)
        except ValueError as e:
            print(f"初始化错误: {e}")
            return

        if args.mode == 'backfill':
            print(f"开始为 '{args.data_type}' (date-based) 进行历史数据回填...")
            archiver.historical_backfill()
        elif args.mode == 'incremental':
            print(f"开始为 '{args.data_type}' (date-based) 进行增量更新...")
            archiver.incremental_update()
        elif args.mode == 'summary':
            print(f"'{args.data_type}' (date-based) 的摘要信息:")
            last_date = archiver._get_last_success_date()
            if last_date:
                print(f"  最后成功同步日期: {last_date}")
            else:
                print("  尚未进行任何同步。")

    print("完成!")

if __name__ == "__main__":
    main()
