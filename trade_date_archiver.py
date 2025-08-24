#!/usr/bin/env python3
"""
交易日归档器 (TradeDateArchiver)

继承自 BaseArchiver，专门处理需要按交易日历进行遍历归档的数据类型。
它会自动读取本地的 'trade_cal' 快照数据作为遍历的基准。
"""

import time
from datetime import datetime

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

    def backfill(self, start_date_str: str = "20160101"):
        """从指定日期开始，按交易日历逐日回填"""
        print(f"[{self.data_type.upper()}] Starting historical backfill from {start_date_str}...")
        all_trade_dates = self._get_trade_calendar()

        today_str = datetime.now().strftime('%Y%m%d')
        dates_to_process = [d for d in all_trade_dates if d >= start_date_str and d <= today_str]

        for trade_date in dates_to_process:
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

        start_date_str = "20160101"
        if processed_dates:
            last_date_str = max(processed_dates)
            print(f"Last processed date is {last_date_str}. Continuing from next trading date.")
            all_trade_dates = self._get_trade_calendar()
            try:
                last_date_index = all_trade_dates.index(last_date_str)
                if last_date_index + 1 < len(all_trade_dates):
                    start_date_str = all_trade_dates[last_date_index + 1]
                else:
                    print("Already up to date.")
                    return
            except ValueError:
                print(f"Warning: Last processed date {last_date_str} not in trade calendar. Starting full backfill.")
                start_date_str = "20160101"
        else:
            print("No previous data found. Starting full backfill...")

        self.backfill(start_date_str=start_date_str)

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

            partition_path = self.landing_path / f"trade_date={trade_date}"
            write_status = self._save_partitioned_data(df, partition_path, trade_date)

            log_status = 'no_data' if df.empty else write_status
            self._log_request(trade_date, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(trade_date, ingest_date, params, 0, "error", "error", str(e))
            print(f"Error processing data for {trade_date}: {e}")

        duration = time.time() - loop_start_time
        print(f"Finished processing for {trade_date} in {duration:.2f}s.")

