#!/usr/bin/env python3
"""
自动化数据管道 - 主控脚本 (Orchestrator)

该脚本是与数据系统交互的唯一入口点。
它读取 data_manifest.py 中的数据资产清单，并根据指定的模式执行操作。

--- 命令示例 ---

# 1. 执行所有数据资产的历史回填 (自动跳过已完成部分)
python pipeline.py --mode backfill

# 2. 执行所有数据资产的日常增量更新 (根据各自策略)
python pipeline.py --mode update

"""

import argparse
from data_manifest import DATA_ASSETS
from config import COMMON_INDEXES

# 导入所有归档器
from period_archiver import PeriodArchiver
from date_archiver import DateArchiver
from snapshot_archiver import SnapshotArchiver
from trade_date_archiver import TradeDateArchiver
from stock_driven_archiver import StockDrivenArchiver
from index_monthly_archiver import IndexMonthlyArchiver


ARCHIVER_MAP = {
    'period': PeriodArchiver,
    'date': DateArchiver,
    'snapshot': SnapshotArchiver,
    'trade_date': TradeDateArchiver,
    'stock_driven': StockDrivenArchiver,
    'index_monthly': IndexMonthlyArchiver,
}

DRIVER_SOURCE_MAP = {
    'COMMON_INDEXES': COMMON_INDEXES
}

def run_backfill_pipeline():
    """执行全量历史数据回填管道"""
    print("--- Starting Full Historical Backfill Pipeline ---")
    total_assets = len(DATA_ASSETS)

    for i, asset in enumerate(DATA_ASSETS):
        asset_name = asset['name']
        archiver_type = asset['archiver']
        start_date = asset.get('backfill_start')

        print(f"\n[{i+1}/{total_assets}] Processing Asset: '{asset_name}' (Archiver: {archiver_type}) --------")

        if archiver_type == 'snapshot' and not start_date:
            print(f"  - Snapshot asset '{asset_name}' has no backfill mode. Skipping.")
            continue

        try:
            archiver_class = ARCHIVER_MAP[archiver_type]
            
            # --- Archiver-specific Instantiation ---
            init_kwargs = {'data_type': asset_name}
            if asset.get('driver_source'):
                driver_list = DRIVER_SOURCE_MAP.get(asset['driver_source'])
                if driver_list:
                    init_kwargs['code_list'] = driver_list
                else: # Fallback to file-based driver (e.g., 'stock_basic')
                    init_kwargs['driver_data_type'] = asset['driver_source']

            archiver = archiver_class(**init_kwargs)

            # --- Execute Backfill ---
            backfill_kwargs = {}
            if start_date:
                backfill_kwargs['start_date_str'] = start_date
            
            archiver.backfill(**backfill_kwargs)

        except Exception as e:
            print(f"  - FAILED to process asset '{asset_name}'. Error: {e}")

    print("\n--- Full Historical Backfill Pipeline Complete ---")

def run_update_pipeline():
    """执行日常增量更新管道"""
    from datetime import datetime
    print("--- Starting Incremental Update Pipeline ---")
    total_assets = len(DATA_ASSETS)
    today = datetime.now()

    for i, asset in enumerate(DATA_ASSETS):
        asset_name = asset['name']
        archiver_type = asset['archiver']
        policy = asset.get('policy', {})

        print(f"\n[{i+1}/{total_assets}] Processing Asset: '{asset_name}' (Archiver: {archiver_type}) --------")

        # 1. 检查运行窗口
        run_window = policy.get('run_window')
        if run_window and not (run_window['start_month'] <= today.month <= run_window['end_month']):
            print(f"  - SKIPPING: Current month {today.month} is outside the configured run window ({run_window['start_month']}-{run_window['end_month']}).")
            continue

        try:
            archiver_class = ARCHIVER_MAP[archiver_type]

            # --- Archiver-specific Instantiation ---
            init_kwargs = {'data_type': asset_name}
            if asset.get('driver_source'):
                driver_list = DRIVER_SOURCE_MAP.get(asset['driver_source'])
                if driver_list:
                    init_kwargs['code_list'] = driver_list
                else:
                    init_kwargs['driver_data_type'] = asset['driver_source']

            archiver = archiver_class(**init_kwargs)

            # --- Execute Update with policy ---
            update_kwargs = {}
            if 'lookback_days' in policy:
                update_kwargs['lookback_days'] = policy['lookback_days']
            if 'lookback_months' in policy:
                update_kwargs['lookback_months'] = policy['lookback_months']

            archiver.update(**update_kwargs)

        except Exception as e:
            print(f"  - FAILED to process asset '{asset_name}'. Error: {e}")

    print("\n--- Incremental Update Pipeline Complete ---")

def main():
    parser = argparse.ArgumentParser(description='Automated Data Pipeline Orchestrator')
    parser.add_argument('--mode', choices=['backfill', 'update'], required=True,
                        help='The pipeline mode to run: `backfill` for historical data, `update` for daily increments.')
    
    args = parser.parse_args()

    if args.mode == 'backfill':
        run_backfill_pipeline()
    elif args.mode == 'update':
        run_update_pipeline()

if __name__ == "__main__":
    main()

