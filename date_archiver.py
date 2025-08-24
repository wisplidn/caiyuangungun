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

    def backfill(self, start_date_str: str = "20070101"):
        """从指定日期开始，逐日回填历史数据"""
        print(f"[{self.data_type.upper()}] Starting historical backfill from {start_date_str}...")
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.now() + timedelta(days=1)
        current_date = start_date

        while current_date < end_date:
            date_str = current_date.strftime('%Y%m%d')
            self._process_day(date_str)
            current_date += timedelta(days=1)
        print("Historical backfill complete.")

    def update(self):
        """从上次成功的位置增量更新到昨天"""
        print(f"[{self.data_type.upper()}] Starting incremental update...")

        processed_dates = []
        if self.landing_path.exists():
            for path in self.landing_path.iterdir():
                if path.is_dir() and path.name.startswith(f'{self.date_field}='):
                    processed_dates.append(path.name.split('=')[1])

        if not processed_dates:
            print("No previous data found. Starting full backfill...")
            self.backfill()
            return

        last_date_str = max(processed_dates)
        start_date = (datetime.strptime(last_date_str, '%Y%m%d') + timedelta(days=1))
        start_date_str = start_date.strftime('%Y%m%d')
        print(f"Incremental update from {start_date_str}...")
        self.backfill(start_date_str=start_date_str)

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

