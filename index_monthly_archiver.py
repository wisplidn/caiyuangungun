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

    def _get_processed_partitions(self) -> set:
        """扫描落地路径以查找已处理的 (index_code, trade_date) 组合。"""
        processed = set()
        if not self.landing_path.exists():
            return processed
        for index_dir in self.landing_path.iterdir():
            if index_dir.is_dir() and index_dir.name.startswith('index_code='):
                index_code = index_dir.name.split('=')[1]
                for date_dir in index_dir.iterdir():
                    if date_dir.is_dir() and date_dir.name.startswith('trade_date='):
                        trade_date = date_dir.name.split('=')[1]
                        processed.add((index_code, trade_date))
        return processed

    def backfill(self, start_date_str: str = "20070101"):
        """高效地回填历史数据，仅处理缺失的指数-月份组合。"""
        from itertools import groupby
        print(f"[{self.data_type.upper()}] Starting EFFICIENT historical backfill from {start_date_str}...")

        # 1. 生成理论上需要处理的所有分区
        all_potential_partitions = set()
        months_to_process = self._generate_months(start_date_str)
        for index_code in self.index_list:
            for month_date in months_to_process:
                all_potential_partitions.add((index_code, month_date))

        # 2. 扫描本地已有的分区
        processed_partitions = self._get_processed_partitions()

        # 3. 计算需要处理的缺失分区
        partitions_to_process = sorted(list(all_potential_partitions - processed_partitions))

        print(f"  - Total potential partitions: {len(all_potential_partitions)}")
        print(f"  - Already processed: {len(processed_partitions.intersection(all_potential_partitions))}")
        print(f"  - Remaining to process: {len(partitions_to_process)}")

        if not partitions_to_process:
            print("  - No missing partitions to process. Backfill is up-to-date.")
        else:
            # 按指数分组以优化日志输出
            grouped_tasks = groupby(partitions_to_process, key=lambda x: x[0])
            for index_code, tasks in grouped_tasks:
                print(f"\nProcessing Index: {index_code} --------")
                for _, month_date in tasks:
                    self._process_month(index_code, month_date)

        print("\nHistorical backfill complete.")

    def update(self, lookback_months: int = 12):
        """
        增量更新最近 N 个月的数据。
        'lookback_months' 定义了从今天起回溯的月数，以重新获取和验证数据。
        """
        from itertools import groupby
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_months} months)...")

        # 计算需要处理的日期范围
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=lookback_months)
        start_date_str = start_date.strftime('%Y%m%d')

        months_to_process = self._generate_months(start_date_str)
        partitions_to_process = []
        for index_code in self.index_list:
            for month_date in months_to_process:
                partitions_to_process.append((index_code, month_date))

        print(f"  - Processing {len(partitions_to_process)} partitions for {len(self.index_list)} indices...")

        # 按指数分组以优化日志输出
        grouped_tasks = groupby(partitions_to_process, key=lambda x: x[0])
        for index_code, tasks in grouped_tasks:
            print(f"\nProcessing Index: {index_code} --------")
            for _, month_date in tasks:
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

