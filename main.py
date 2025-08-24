#!/usr/bin/env python3
"""
数据归档器 - 手动调试与运行脚本 (Manual Runner & Debugging Tool)

该脚本的核心价值在于 **开发与调试**。
当需要接入一个新的数据接口时，你可以使用此脚本对单个归档器进行精细化的操作，
而无需运行完整的、可能耗时很长的自动化管道。

日常的、全量的自动化数据更新，请使用 `pipeline.py`。

--- 命令示例 ---

# 1. 对单个资产进行历史回填 (从指定日期开始)
python main.py --archiver-type period --data-type income --mode backfill --start-date 20230101

# 2. 对单个资产进行增量更新 (使用默认回溯期)
python main.py --archiver-type trade_date --data-type daily --mode update

# 3. 对单个资产进行增量更新 (自定义回溯期)
#    --lookback: 对周期性数据，单位为月；对日度数据，单位为天。
python main.py --archiver-type period --data-type balancesheet --mode update --lookback 24

# 4. 查看单个资产的数据摘要和请求日志
python main.py --archiver-type snapshot --data-type stock_basic --mode summary

# 5. 为代码驱动型资产指定驱动源
python main.py --archiver-type stock_driven --data-type index_daily --driver-source COMMON_INDEXES --mode backfill

"""

import argparse
from pipeline import ARCHIVER_MAP, DRIVER_SOURCE_MAP
from tushare_reader import TushareReader

def main():
    parser = argparse.ArgumentParser(description='Manual Runner & Debugging Tool for Data Archivers')
    parser.add_argument('--archiver-type', type=str, choices=ARCHIVER_MAP.keys(), required=True, help='The type of archiver to use.')
    parser.add_argument('--data-type', type=str, required=True, help='The Tushare data type to process (e.g., income, daily).')
    parser.add_argument('--mode', choices=['backfill', 'update', 'summary'], required=True, help='The operation mode.')
    parser.add_argument('--start-date', type=str, help='Start date for backfill (YYYYMMDD).')
    parser.add_argument('--lookback', type=int, help='Lookback period in months (for period) or days (for date/trade_date).')
    parser.add_argument('--driver-source', type=str, help='For stock_driven, specify the source of the code list (e.g., COMMON_INDEXES or stock_basic).')

    args = parser.parse_args()

    try:
        archiver_class = ARCHIVER_MAP[args.archiver_type]

        # --- Archiver Instantiation ---
        init_kwargs = {'data_type': args.data_type}
        if args.driver_source:
            driver_list = DRIVER_SOURCE_MAP.get(args.driver_source)
            if driver_list:
                init_kwargs['code_list'] = driver_list
            else:
                init_kwargs['driver_data_type'] = args.driver_source

        archiver = archiver_class(**init_kwargs)

        # --- Mode Execution ---
        if args.mode == 'backfill':
            kwargs = {}
            if args.start_date:
                kwargs['start_date_str'] = args.start_date
            archiver.backfill(**kwargs)

        elif args.mode == 'update':
            kwargs = {}
            if args.lookback:
                if args.archiver_type in ['period', 'index_monthly']:
                    kwargs['lookback_months'] = args.lookback
                else:
                    kwargs['lookback_days'] = args.lookback
            archiver.update(**kwargs)

        elif args.mode == 'summary':
            reader = TushareReader(data_type=args.data_type)
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

    except Exception as e:
        print(f"An error occurred: {e}")
        return

    print("\nOperation complete.")

if __name__ == "__main__":
    main()
