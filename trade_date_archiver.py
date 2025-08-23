#!/usr/bin/env python3
"""
交易日归档器 (TradeDateArchiver)

继承自 BaseArchiver，专门处理需要按交易日历进行遍历归档的数据类型。
它会自动读取本地的 'trade_cal' 快照数据作为遍历的基准。
"""

import json
import time
from datetime import datetime

import pandas as pd

from base_archiver import BaseArchiver
from tushare_reader import TushareReader


class TradeDateArchiver(BaseArchiver):
    """按交易日历进行数据归档"""

    def _get_trade_calendar(self) -> list[str]:
        """使用 TushareReader 读取本地的交易日历快照"""
        print("  - Reading local trade calendar snapshot...")
        reader = TushareReader(data_type='trade_cal', base_path=self.base_path)
        partitions = reader.get_partitions()
        if not partitions:
            raise FileNotFoundError("Trade calendar snapshot not found. Please run 'trade_cal' snapshot first.")
        
        # 读取最新的快照
        latest_partition = sorted(partitions)[-1]
        df = reader.read_latest_data(latest_partition)
        
        if df.empty or 'cal_date' not in df.columns or 'is_open' not in df.columns:
            raise ValueError("Invalid trade calendar data.")

        # 筛选出所有开市日期
        trade_dates = df[df['is_open'] == 1]['cal_date'].tolist()
        print(f"  - Found {len(trade_dates)} trading dates.")
        return sorted(trade_dates)

    def _save_data_for_day(self, df: pd.DataFrame, trade_date: str):
        """将单日数据保存到 Landing 层"""
        partition_path = self.landing_path / f"trade_date={trade_date}"
        partition_path.mkdir(parents=True, exist_ok=True)

        if not df.empty:
            df.to_parquet(partition_path / "data.parquet", compression='snappy', index=False)

        metadata = {
            "partition_key": trade_date,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
        }
        with open(partition_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

    def backfill(self, start_date_str: str = "20160101"):
        """从指定日期开始，按交易日历逐日回填"""
        print(f"[{self.data_type.upper()}] Starting historical backfill from {start_date_str}...")
        all_trade_dates = self._get_trade_calendar()
        
        # 筛选出需要处理的交易日 (不超过今天)
        today_str = datetime.now().strftime('%Y%m%d')
        dates_to_process = [d for d in all_trade_dates if d >= start_date_str and d <= today_str]

        for trade_date in dates_to_process:
            partition_path = self.landing_path / f"trade_date={trade_date}"
            if (partition_path / "metadata.json").exists():
                print(f"Date {trade_date} already processed, skipping.")
                continue
            self._process_day(trade_date)
        
        print("Historical backfill complete.")

    def update(self):
        """从上次成功的位置增量更新到最新的交易日"""
        print(f"[{self.data_type.upper()}] Starting incremental update...")
        processed_dates = []
        if self.landing_path.exists():
            for path in self.landing_path.iterdir():
                if path.is_dir() and path.name.startswith('trade_date='):
                    processed_dates.append(path.name.split('=')[1])

        if not processed_dates:
            print("No previous data found. Starting full backfill from 20160101...")
            self.backfill(start_date_str="20160101")
            return

        last_date_str = max(processed_dates)
        print(f"Last processed date is {last_date_str}. Continuing from next trading date.")
        
        all_trade_dates = self._get_trade_calendar()
        try:
            last_date_index = all_trade_dates.index(last_date_str)
            start_index = last_date_index + 1
        except ValueError:
            # 如果上次处理的日期不在日历中，从头开始
            start_index = 0

        dates_to_process = all_trade_dates[start_index:]
        for trade_date in dates_to_process:
            self._process_day(trade_date)
        print("Incremental update complete.")

    def _process_day(self, trade_date: str):
        """处理单个交易日数据的核心逻辑"""
        loop_start_time = time.time()
        print(f"Processing date: {trade_date}...")
        params = {'trade_date': trade_date}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(trade_date=trade_date)

            if fetch_status == 'error':
                self._log_request(trade_date, ingest_date, params, 0, "error", "error", f"API fetch failed for {trade_date}")
                return

            self._save_data_for_day(df, trade_date)
            checksum = self._calculate_checksum(df)
            log_status = 'no_data' if df.empty else 'success'
            self._log_request(trade_date, ingest_date, params, len(df), checksum, log_status)

        except Exception as e:
            self._log_request(trade_date, ingest_date, params, 0, "error", "error", str(e))
            print(f"Error processing data for {trade_date}: {e}")
        
        duration = time.time() - loop_start_time
        print(f"Finished processing for {trade_date} in {duration:.2f}s.")

