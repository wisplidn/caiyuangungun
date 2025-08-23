"""
Tushare日期驱动数据归档器 (DateDrivenArchiver)

用于处理按具体日期（如公告日）查询的事件驱动型数据，例如分红送股。
数据按年月分区存储，以优化查询性能并避免小文件问题。
"""

import pandas as pd
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import time
import hashlib
import json

import tushare_client

class DateDrivenArchiver:
    def __init__(self, data_type, base_path="./data"):
        self.data_type = data_type
        self.base_path = Path(base_path)
        self.landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.log_db_path = self.base_path / "logs" / "request_log.db"
        
        # 动态获取对应的数据获取函数
        self.fetch_function = getattr(tushare_client, f"get_{self.data_type}", None)
        if not self.fetch_function:
            raise ValueError(f"Unsupported data_type: '{data_type}'. Function 'get_{data_type}' not found in tushare_client.")

        self.landing_path.mkdir(parents=True, exist_ok=True)
        # 日志记录功能与TushareArchiver一致，确保request_log表存在即可
        self.landing_path.mkdir(parents=True, exist_ok=True)

    def _calculate_checksum(self, df):
        """计算DataFrame的稳定checksum"""
        if df.empty:
            return "empty"
        # 根据分红数据的特性选择稳定排序的列
        sort_columns = [col for col in ['ts_code', 'ann_date', 'ex_date', 'record_date'] if col in df.columns]
        df_sorted = df.sort_values(sort_columns)
        content = df_sorted.to_csv(index=False, float_format='%.6f')
        return hashlib.md5(content.encode()).hexdigest()

    def _log_request(self, period_date, params, row_count, checksum, status):
        """记录每日的请求日志"""
        conn = sqlite3.connect(self.log_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO request_log
            (data_type, period, ingest_date, params, row_count, checksum, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (self.data_type, period_date, datetime.now().strftime('%Y-%m-%d'), json.dumps(params), row_count, checksum, status))
        conn.commit()
        conn.close()

    def _get_last_processed_date(self, start_date_str):
        """获取在指定日期之后，最后一次成功处理的日期"""
        conn = sqlite3.connect(self.log_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(period) FROM request_log
            WHERE data_type = ? AND status IN ('success', 'no_data') AND period >= ?
        """, (self.data_type, start_date_str))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else None

    def _save_data_for_day(self, df, date_obj):
        """将单日数据保存到独立的Parquet文件，并附带metadata.json"""
        date_path = self.landing_path / f"ann_year={date_obj.year}" / f"ann_month={date_obj.month:02d}" / f"ann_day={date_obj.day:02d}"
        date_path.mkdir(parents=True, exist_ok=True)

        # 保存数据文件
        file_path = date_path / "data.parquet"
        df.to_parquet(file_path, compression='snappy', index=False)

        # 保存元数据
        metadata = {
            "data_type": self.data_type,
            "date": date_obj.strftime('%Y-%m-%d'),
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
            "fields": list(df.columns)
        }
        metadata_file = date_path / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"Saved {len(df)} records for {date_obj.strftime('%Y-%m-%d')} to {date_path}")

    def historical_backfill(self, start_date_str="20070101"):
        """历史数据回填，从指定日期开始逐日获取"""
        last_processed_date_str = self._get_last_processed_date(start_date_str)
        start_date = datetime.strptime(last_processed_date_str or start_date_str, '%Y%m%d')
        if last_processed_date_str: # 如果是断点续传，从下一天开始
            start_date += timedelta(days=1)

        end_date = datetime.now()
        current_date = start_date

        while current_date < end_date:
            date_str = current_date.strftime('%Y%m%d')
            params = {'ann_date': date_str}
            print(f"Processing {self.data_type} for {date_str}...")

            try:
                df_day = self.fetch_function(ann_date=date_str)
                checksum = self._calculate_checksum(df_day)

                if not df_day.empty:
                    self._save_data_for_day(df_day, current_date)

                self._log_request(date_str, params, len(df_day), checksum, 'no_data' if df_day.empty else 'success')

            except Exception as e:
                print(f"Error fetching data for {date_str}: {e}")
                self._log_request(date_str, params, 0, 'error', 'error')
                time.sleep(1)

            current_date += timedelta(days=1)
        print("Historical backfill complete.")

    def incremental_update(self):
        """增量更新，从上次成功的位置更新到昨天"""
        last_date_str = self._get_last_processed_date('20070101') # 从项目开始日期查找
        if not last_date_str:
            print("No previous data found. Please run historical_backfill first.")
            # 如果没有任何记录，直接从头开始回填
            self.historical_backfill()
            return

        start_date_str = (datetime.strptime(last_date_str, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        print(f"Starting incremental update from {start_date_str}...")
        self.historical_backfill(start_date_str)
        print("Incremental update complete.")

if __name__ == '__main__':
    # This block is for testing purposes only.
    # The main logic is handled by main.py.
    print("Testing DateDrivenArchiver for 'dividend'...")
    try:
        archiver = DateDrivenArchiver(data_type='dividend')
        archiver.incremental_update()
        print("\nTest complete.")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
