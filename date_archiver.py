#!/usr/bin/env python3
"""
日期归档器 (Date Archiver)

继承自 BaseArchiver，专门处理按业务日期 (如公告日) 进行归档的数据类型，
例如 dividend (分红送股) 等。
"""

import json
import time
from datetime import datetime, timedelta

import pandas as pd

from base_archiver import BaseArchiver


class DateArchiver(BaseArchiver):
    """按业务日期进行数据归档"""

    def _save_data_for_day(self, df: pd.DataFrame, date_obj: datetime):
        """将单日数据保存到 Landing 层，按年月日分区"""
        date_str = date_obj.strftime('%Y%m%d')
        partition_path = self.landing_path / f"ann_date={date_str}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # 仅当DataFrame非空时才保存数据文件
        if not df.empty:
            data_file = partition_path / "data.parquet"
            df.to_parquet(data_file, compression='snappy', index=False)

        # 总是创建元数据文件以标记此日期已处理
        metadata = {
            "partition_key": date_str,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
        }
        with open(partition_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

    def backfill(self, start_date_str: str = "20070101"):
        """从指定日期开始，逐日回填历史数据，跳过已存在的日期"""
        print(f"[{self.data_type.upper()}] Starting historical backfill from {start_date_str}...")
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.combine((datetime.now() + timedelta(days=1)).date(), datetime.min.time()) # Process up to and including today
        current_date = start_date

        while current_date < end_date:
            date_str = current_date.strftime('%Y%m%d')
            partition_path = self.landing_path / f"ann_date={date_str}"
            if (partition_path / "metadata.json").exists():
                print(f"Date {date_str} already processed, skipping.")
            else:
                self._process_day(date_str, current_date)
            current_date += timedelta(days=1)
        print("Historical backfill complete.")

    def update(self):
        """从上次成功的位置增量更新到昨天"""
        print(f"[{self.data_type.upper()}] Starting incremental update...")

        # 通过扫描文件系统找到最后处理的日期
        processed_dates = []
        if self.landing_path.exists():
            for path in self.landing_path.iterdir():
                if path.is_dir() and path.name.startswith('ann_date='):
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

    def _process_day(self, date_str: str, date_obj: datetime):
        """处理单日数据的核心逻辑"""
        loop_start_time = time.time()
        print(f"Processing date: {date_str}...")
        params = {'ann_date': date_str}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(ann_date=date_str)

            if fetch_status == 'error':
                self._log_request(date_str, ingest_date, params, 0, "error", "error", f"API fetch failed for {date_str}")
                return

            # If API call is successful, always save metadata to mark the day as processed
            self._save_data_for_day(df, date_obj)

            # Log the result to the database
            checksum = self._calculate_checksum(df)
            log_status = 'no_data' if df.empty else 'success'
            self._log_request(date_str, ingest_date, params, len(df), checksum, log_status)

        except Exception as e:
            self._log_request(date_str, ingest_date, params, 0, "error", "error", str(e))
            print(f"Error processing data for {date_str}: {e}")
        
        loop_duration = time.time() - loop_start_time
        print(f"Finished processing for {date_str} in {loop_duration:.2f}s.")

