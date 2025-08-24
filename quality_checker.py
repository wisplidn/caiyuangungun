#!/usr/bin/env python3
"""
数据质量检查器 (Data Quality Checker)

该模块负责验证数据管道下载的数据的完整性。
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta

from tushare_reader import TushareReader

class QualityChecker:
    """封装所有数据质量验证规则。"""

    def __init__(self, assets_manifest: list):
        self.assets = assets_manifest
        self.reader_cache = {}

    def _get_reader(self, data_type: str) -> TushareReader:
        """缓存 TushareReader 实例以提高效率。"""
        if data_type not in self.reader_cache:
            self.reader_cache[data_type] = TushareReader(data_type=data_type)
        return self.reader_cache[data_type]

    def run_checks(self) -> list:
        """对清单中的所有资产运行数据质量检查。"""
        failure_report = []
        print("\n--- Starting Data Quality Check ---")
        for asset in self.assets:
            asset_name = asset['name']
            archiver_type = asset['archiver']
            print(f"  - Checking asset: '{asset_name}' (Type: {archiver_type})...")

            check_function_map = {
                'period': self._check_period_assets,
                'trade_date': self._check_trade_date_assets,
                'snapshot': self._check_snapshot_assets,
                # 'date' archiver is event-driven, so we don't check for completeness.
            }

            check_function = check_function_map.get(archiver_type)
            if check_function:
                failures = check_function(asset)
                if failures:
                    failure_report.extend(failures)
            else:
                print(f"    - No quality check defined for archiver type '{archiver_type}'. Skipping.")

        print("--- Data Quality Check Complete ---")
        return failure_report

    def _check_period_assets(self, asset: dict) -> list:
        """检查季度数据的完整性。"""
        failures = []
        policy = asset.get('policy', {})
        lookback_months = policy.get('lookback_months', 8)
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=lookback_months)

        from period_archiver import PeriodArchiver
        pa = PeriodArchiver(asset['name'])
        expected_partitions = pa._generate_quarters(start_date, end_date)

        reader = self._get_reader(asset['name'])
        for partition in expected_partitions:
            # Don't flag future quarters as missing
            if datetime.strptime(partition, '%Y%m%d') > end_date:
                continue
            partition_str = f"period={partition}"
            df = reader.read_latest_data(partition_str)
            if df is None or df.empty:
                failures.append({'asset': asset, 'partition': partition, 'reason': 'Missing or empty'})
        return failures

    def _check_trade_date_assets(self, asset: dict) -> list:
        """检查交易日数据的完整性。"""
        failures = []
        policy = asset.get('policy', {})
        lookback_days = policy.get('lookback_days', 30)

        trade_cal_reader = self._get_reader('trade_cal')
        all_trade_dates_df = trade_cal_reader.read_all_latest()
        if all_trade_dates_df is None or all_trade_dates_df.empty:
            print("    - WARNING: Trade calendar not found. Skipping check for trade_date assets.")
            return failures

        # Ensure 'is_open' is treated as integer
        all_trade_dates_df['is_open'] = all_trade_dates_df['is_open'].astype(int)
        all_trade_dates = all_trade_dates_df[all_trade_dates_df['is_open'] == 1]['cal_date'].tolist()

        end_date = datetime.now()
        start_date = end_date - relativedelta(days=lookback_days)
        start_date_str = start_date.strftime('%Y%m%d')

        expected_partitions = [d for d in all_trade_dates if d >= start_date_str and d <= end_date.strftime('%Y%m%d')]

        reader = self._get_reader(asset['name'])
        for partition in expected_partitions:
            partition_str = f"trade_date={partition}"
            df = reader.read_latest_data(partition_str)
            if df is None or df.empty:
                failures.append({'asset': asset, 'partition': partition, 'reason': 'Missing or empty'})
        return failures

    def _check_snapshot_assets(self, asset: dict) -> list:
        """检查快照数据的存在性。"""
        reader = self._get_reader(asset['name'])
        df = reader.read_all_latest()
        if df is None or df.empty:
            return [{'asset': asset, 'partition': 'latest', 'reason': 'Missing or empty'}]
        return []


