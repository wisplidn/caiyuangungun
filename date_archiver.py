#!/usr/bin/env python3
"""
日期归档器 (Date Archiver)

继承自 BaseArchiver，专门处理按业务日期 (如公告日) 进行归档的数据类型，
例如 dividend (分红送股) 等。
"""

import time
from datetime import datetime, timedelta

from base_archiver import BaseArchiver


class DateArchiver(BaseArchiver):
    """按业务日期进行数据归档"""

    def __init__(self, data_type: str, base_path: str = "./data", date_field: str = 'ann_date'):
        super().__init__(data_type, base_path)
        self.date_field = date_field

    def _get_processed_dates(self) -> set:
        """扫描落地路径以查找已处理的日期。"""
        if not self.landing_path.exists():
            return set()
        return {p.name.split('=')[1] for p in self.landing_path.iterdir() if p.is_dir()}

    def backfill(self, start_date_str: str = "20070101"):
        """高效地回填历史数据，仅处理缺失的日期。"""
        print(f"[{self.data_type.upper()}] Starting EFFICIENT historical backfill from {start_date_str}...")
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.now()

        # 1. 生成理论上需要处理的所有日期
        all_potential_dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        all_potential_dates_str = {d.strftime('%Y%m%d') for d in all_potential_dates}

        # 2. 扫描本地已有的日期
        processed_dates = self._get_processed_dates()

        # 3. 计算需要处理的缺失日期
        dates_to_process = sorted(list(all_potential_dates_str - processed_dates))

        print(f"  - Date range: {start_date_str} to {end_date.strftime('%Y%m%d')}")
        print(f"  - Total days in range: {len(all_potential_dates_str)}")
        print(f"  - Already processed: {len(processed_dates)}")
        print(f"  - Remaining to process: {len(dates_to_process)}")

        if not dates_to_process:
            print("  - No missing dates to process. Backfill is up-to-date.")
        else:
            for date_str in dates_to_process:
                self._process_day(date_str)

        print("Historical backfill complete.")

    def update(self, lookback_days: int = 30):
        """
        增量更新最近 N 天的数据。
        'lookback_days' 定义了从今天起回溯的天数，以重新获取和验证数据。
        """
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_days} days)...")

        # 计算需要处理的日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        dates_to_process = []
        current_date = start_date
        while current_date <= end_date:
            dates_to_process.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)

        print(f"  - Processing date range: {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}")

        for date_str in dates_to_process:
            self._process_day(date_str)

        print("Incremental update complete.")

    def _process_day(self, date_str: str):
        """处理单日数据的核心逻辑"""
        loop_start_time = time.time()
        print(f"Processing date: {date_str}...")
        params = {self.date_field: date_str}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(**{self.date_field: date_str})

            if fetch_status == 'error':
                self._log_request(date_str, ingest_date, params, 0, "error", "error", f"API fetch failed for {date_str}")
                return

            partition_path = self.landing_path / f"{self.date_field}={date_str}"
            write_status = self._save_partitioned_data(df, partition_path, date_str)

            log_status = 'no_data' if df.empty else write_status
            self._log_request(date_str, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(date_str, ingest_date, params, 0, "error", "error", str(e))
            print(f"Error processing data for {date_str}: {e}")

        loop_duration = time.time() - loop_start_time
        print(f"Finished processing for {date_str} in {loop_duration:.2f}s.")

