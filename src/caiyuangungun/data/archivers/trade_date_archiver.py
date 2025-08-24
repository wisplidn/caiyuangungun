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

    def _get_processed_dates(self) -> set:
        """扫描落地路径以查找已处理的交易日。"""
        if not self.landing_path.exists():
            return set()
        return {p.name.split('=')[1] for p in self.landing_path.iterdir() if p.is_dir()}

    def backfill(self, start_date_str: str = "19901219"):
        """高效地回填历史数据，仅处理缺失的交易日。"""
        print(f"[{self.data_type.upper()}] Starting EFFICIENT historical backfill from {start_date_str}...")

        # 1. 获取理论上需要处理的所有交易日
        all_trade_dates = self._get_trade_calendar()
        potential_dates = {d for d in all_trade_dates if d >= start_date_str}

        # 2. 扫描本地已有的交易日
        processed_dates = self._get_processed_dates()

        # 3. 计算需要处理的缺失交易日
        dates_to_process = sorted(list(potential_dates - processed_dates))

        print(f"  - Date range: {start_date_str} to {all_trade_dates[-1]}")
        print(f"  - Total trade dates in range: {len(potential_dates)}")
        print(f"  - Already processed: {len(processed_dates.intersection(potential_dates))}")
        print(f"  - Remaining to process: {len(dates_to_process)}")

        if not dates_to_process:
            print("  - No missing dates to process. Backfill is up-to-date.")
        else:
            for trade_date in dates_to_process:
                self._process_day(trade_date)

        print("Historical backfill complete.")

    def update(self, lookback_days: int = 30):
        """
        增量更新最近 N 天的数据。
        'lookback_days' 定义了从今天起回溯的天数，以重新获取和验证交易日数据。
        """
        from datetime import timedelta
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_days} days)...")

        # 1. 获取所有交易日
        all_trade_dates = self._get_trade_calendar()

        # 2. 计算回溯期内的交易日
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        start_date_str = start_date.strftime('%Y%m%d')

        dates_to_process = [d for d in all_trade_dates if d >= start_date_str]

        print(f"  - Processing {len(dates_to_process)} trade dates from {start_date_str} to {end_date.strftime('%Y%m%d')}")

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

            partition_path = self.landing_path / f"trade_date={trade_date}"
            write_status = self._save_partitioned_data(df, partition_path, trade_date)

            log_status = 'no_data' if df.empty else write_status
            self._log_request(trade_date, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(trade_date, ingest_date, params, 0, "error", "error", str(e))
            print(f"Error processing data for {trade_date}: {e}")

        duration = time.time() - loop_start_time
        print(f"Finished processing for {trade_date} in {duration:.2f}s.")

