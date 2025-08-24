#!/usr/bin/env python3
"""
季度归档器 (Period Archiver)

继承自 BaseArchiver，专门处理按财报季度 (period) 进行归档的数据类型，
例如 income, balancesheet 等。
"""

from datetime import datetime
from typing import List

from caiyuangungun.data.archivers.base_archiver import BaseArchiver


class PeriodArchiver(BaseArchiver):
    """按财报季度进行数据归档"""

    def _generate_quarters(self, start_date: datetime, end_date: datetime) -> List[str]:
        """在指定日期范围内生成所有财报季度列表。"""
        from dateutil.relativedelta import relativedelta
        quarters = set()
        current_date = start_date
        while current_date <= end_date:
            year = current_date.year
            quarter = (current_date.month - 1) // 3 + 1
            month_day = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[quarter]
            quarters.add(f"{year}{month_day}")
            current_date += relativedelta(months=3) # Move to the next quarter
        return sorted(list(quarters))

    def _get_processed_periods(self) -> set:
        """扫描落地路径以查找已处理的财报季度。"""
        if not self.landing_path.exists():
            return set()
        return {p.name.split('=')[1] for p in self.landing_path.iterdir() if p.is_dir()}

    def backfill(self, start_date_str: str = "20070101"):
        """高效地回填历史数据，仅处理缺失的财报季度。"""
        print(f"[{self.data_type.upper()}] Starting EFFICIENT historical backfill from {start_date_str}...")
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.now()

        all_potential_periods = self._generate_quarters(start_date, end_date)
        processed_periods = self._get_processed_periods()
        periods_to_process = [p for p in all_potential_periods if p not in processed_periods]

        print(f"  - Period range: {all_potential_periods[0]} to {all_potential_periods[-1]}")
        print(f"  - Total periods in range: {len(all_potential_periods)}")
        print(f"  - Already processed: {len(processed_periods)}")
        print(f"  - Remaining to process: {len(periods_to_process)}")

        if not periods_to_process:
            print("  - No missing periods to process. Backfill is up-to-date.")
        else:
            for period in periods_to_process:
                self._process_period(period)

        print("Historical backfill complete.")

    def update(self, lookback_months: int = 8):
        """
        增量更新最近 N 个月的数据。
        'lookback_months' 定义了从今天起回溯的月数，以重新获取和验证财报数据。
        """
        from dateutil.relativedelta import relativedelta
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_months} months)...")

        # 计算需要处理的日期范围
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=lookback_months)

        periods_to_process = self._generate_quarters(start_date, end_date)

        print(f"  - Processing recent periods: {', '.join(periods_to_process)}")

        for period in periods_to_process:
            self._process_period(period)

        print("Incremental update complete.")

    def _process_period(self, period: str):
        """处理单个季度的核心逻辑"""
        print(f"Processing period: {period}...")
        params = {'period': period}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(period=period)

            if fetch_status == 'error':
                self._log_request(period, ingest_date, params, 0, "error", "error", f"API fetch failed for {period}")
                return

            partition_path = self.landing_path / f"period={period}"
            write_status = self._save_partitioned_data(df, partition_path, period)

            # 如果数据为空，状态会是 'success' 或 'updated'，但我们需要记录为 'no_data'
            log_status = 'no_data' if df.empty else write_status
            self._log_request(period, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(period, ingest_date, params, 0, "error", "error", str(e))
            print(f"  - Failure: An error occurred while processing {period}: {e}")

