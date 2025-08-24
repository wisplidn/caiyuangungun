#!/usr/bin/env python3
"""
自动化数据管道 - 主控脚本 (Orchestrator)

该脚本是与数据系统交互的核心入口点。
它读取 `data_manifest.py` 中的资产清单，并根据指定模式执行相应操作。
每次数据操作后，都会自动运行一个集成的“校验-重试-报告”工作流，以确保数据质量。

--- 命令示例 ---

# 1. 历史回填
#    对清单中的所有资产，从其 `backfill_start` 日期开始，逐个分区地下载历史数据。
#    自动跳过已存在的本地分区，支持断点续传。
python pipeline.py --mode backfill

# 2. 日常更新
#    对清单中的所有资产，根据其 `policy` 中定义的策略（如回溯期），智能地更新近期数据。
python pipeline.py --mode update

# 3. 独立质量检查
#    在不执行数据下载的情况下，独立运行“校验-重试-报告”工作流。
#    这对于手动修复数据或定期巡检非常有用。
python pipeline.py --mode quality_check

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

from quality_checker import QualityChecker

def _run_targeted_refetch(failures: list):
    """对质量检查失败的特定分区执行一次定向重新获取。"""
    print("\n--- Starting Targeted Refetch for Failed Partitions ---")
    if not failures:
        print("  - No failures to refetch.")
        return

    from itertools import groupby
    # 按资产分组以减少初始化次数
    sorted_failures = sorted(failures, key=lambda x: x['asset']['name'])
    grouped_failures = groupby(sorted_failures, key=lambda x: x['asset']['name'])

    for asset_name, asset_failures_group in grouped_failures:
        failures_list = list(asset_failures_group)
        asset_info = failures_list[0]['asset']
        archiver_type = asset_info['archiver']
        print(f"  - Refetching {len(failures_list)} partition(s) for asset: '{asset_name}'")

        try:
            archiver_class = ARCHIVER_MAP[archiver_type]
            archiver = archiver_class(data_type=asset_name)

            # 动态确定要调用的处理方法 (e.g., _process_period, _process_day)
            process_method_name = f"_process_{archiver_type.split('_')[0].replace('trade', '').replace('index', '')}"
            if 'date' in archiver_type: process_method_name = '_process_day'
            if 'period' in archiver_type: process_method_name = '_process_period'
            if 'snapshot' in archiver_type: process_method_name = 'update' # 快照没有单分区处理方法，直接调用update

            process_method = getattr(archiver, process_method_name, None)

            if not process_method:
                print(f"    - WARNING: Cannot find process method for archiver '{archiver_type}'. Skipping refetch.")
                continue

            for failure in failures_list:
                partition = failure['partition']
                print(f"    - Refetching partition: {partition}")
                if archiver_type == 'snapshot':
                    process_method()
                else:
                    process_method(partition)

        except Exception as e:
            print(f"    - FAILED to refetch for asset '{asset_name}'. Error: {e}")

def run_quality_assurance_workflow():
    """执行集成的“校验-重试-报告”工作流。"""
    checker = QualityChecker(DATA_ASSETS)

    # 1. 首次校验
    initial_failures = checker.run_checks()

    if not initial_failures:
        print("\n[QA SUCCESS] All data assets passed quality checks. No issues found.")
        return

    # 2. 定向重试
    print(f"\n[QA WARNING] Found {len(initial_failures)} data quality issue(s).")
    for f in initial_failures:
        print(f"  - Asset: {f['asset']['name']}, Partition: {f['partition']}, Reason: {f['reason']}")

    _run_targeted_refetch(initial_failures)

    # 3. 最终报告
    print("\n--- Running Final Quality Check After Refetch ---")
    final_failures = checker.run_checks()

    if not final_failures:
        print("\n[QA SUCCESS] All previously found issues have been successfully resolved.")
    else:
        print(f"\n[QA FAILED] {len(final_failures)} data quality issue(s) persist after refetch attempt:")
        for f in final_failures:
            print(f"  - Asset: {f['asset']['name']}, Partition: {f['partition']}, Reason: {f['reason']}")


def main():
    parser = argparse.ArgumentParser(description='Automated Data Pipeline Orchestrator')
    parser.add_argument('--mode', choices=['backfill', 'update', 'quality_check'], required=True,
                        help='The pipeline mode to run.')

    args = parser.parse_args()

    if args.mode == 'backfill':
        run_backfill_pipeline()
        # 在回填后自动运行质量保证
        run_quality_assurance_workflow()
    elif args.mode == 'update':
        run_update_pipeline()
        # 在更新后自动运行质量保证
        run_quality_assurance_workflow()
    elif args.mode == 'quality_check':
        # 仅独立运行质量保证
        run_quality_assurance_workflow()

if __name__ == "__main__":
    main()

