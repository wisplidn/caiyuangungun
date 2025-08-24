#!/usr/bin/env python3
"""
指数月度归档器 (IndexMonthlyArchiver)

专门处理需要按指数代码和月份进行双重遍历的月度数据，例如 index_weight。
"""


from datetime import datetime
from dateutil.relativedelta import relativedelta

from base_archiver import BaseArchiver

from config import COMMON_INDEXES

class IndexMonthlyArchiver(BaseArchiver):
    """按指数和月份进行数据归档"""

    def __init__(self, data_type: str, base_path: str = "./data", index_list: list = None):
        super().__init__(data_type, base_path)
        self.index_list = index_list if index_list is not None else COMMON_INDEXES

    def _generate_months(self, start_date_str: str) -> list[str]:
        """生成从指定日期到当前月份的所有月末日期列表"""
        months = []
        current_month = datetime.strptime(start_date_str, '%Y%m%d').date().replace(day=1)
        end_month = datetime.now().date().replace(day=1)
        while current_month <= end_month:
            last_day = current_month + relativedelta(months=1, days=-1)
            months.append(last_day.strftime('%Y%m%d'))
            current_month += relativedelta(months=1)
        return months

    def backfill(self, start_date_str: str = "20070101"):
        """对指定指数列表，从起始日期开始逐月回填"""
        print(f"[{self.data_type.upper()}] Starting historical backfill for {len(self.index_list)} indices...")
        months_to_process = self._generate_months(start_date_str)
        
        for i, index_code in enumerate(self.index_list):
            print(f"\nProcessing Index: {index_code} ({i+1}/{len(self.index_list)}) --------")
            for month_date in months_to_process:
                self._process_month(index_code, month_date)
        print("\nHistorical backfill complete.")

    def update(self, lookback_months: int = 6):
        """增量更新最近 N 个月的数据"""
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_months} months)...")
        start_date = (datetime.now() - relativedelta(months=lookback_months-1)).replace(day=1)
        months_to_process = self._generate_months(start_date.strftime('%Y%m%d'))

        for i, index_code in enumerate(self.index_list):
            print(f"\nProcessing Index: {index_code} ({i+1}/{len(self.index_list)}) --------")
            for month_date in months_to_process:
                self._process_month(index_code, month_date)
        print("\nIncremental update complete.")

    def _process_month(self, index_code: str, trade_date: str):
        """处理单个指数单个月份的核心逻辑"""
        print(f"  - Processing month: {trade_date}...")
        params = {'index_code': index_code, 'trade_date': trade_date}
        ingest_date = datetime.now().strftime('%Y-%m-%d')
        partition_key = f"{index_code}-{trade_date}"

        try:
            df, fetch_status = self.fetch_function(index_code=index_code, trade_date=trade_date)

            if fetch_status == 'error':
                self._log_request(partition_key, ingest_date, params, 0, "error", "error", f"API fetch failed for {partition_key}")
                return

            partition_path = self.landing_path / f"index_code={index_code}" / f"trade_date={trade_date}"
            write_status = self._save_partitioned_data(df, partition_path, partition_key)

            log_status = 'no_data' if df.empty else write_status
            self._log_request(partition_key, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(partition_key, ingest_date, params, 0, "error", "error", str(e))
            print(f"    - Failure: An error occurred while processing {partition_key}: {e}")

