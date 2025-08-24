#!/usr/bin/env python3
"""
季度归档器 (Period Archiver)

继承自 BaseArchiver，专门处理按财报季度 (period) 进行归档的数据类型，
例如 income, balancesheet 等。
"""

from datetime import datetime

from base_archiver import BaseArchiver


class PeriodArchiver(BaseArchiver):
    """按财报季度进行数据归档"""

    def _generate_quarters(self, start_year: int = 2006) -> list[str]:
        """生成从指定年份到当前的所有财报季度列表"""
        quarters = []
        current_date = datetime.now()
        for year in range(start_year, current_date.year + 1):
            for quarter in range(1, 5):
                if year == current_date.year and quarter > (current_date.month - 1) // 3 + 1:
                    break
                month_day = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[quarter]
                quarters.append(f"{year}{month_day}")
        return quarters

    def backfill(self):
        """从 2006 年开始逐季度回填历史数据"""
        print(f"[{self.data_type.upper()}] Starting historical backfill...")
        for period in self._generate_quarters():
            self._process_period(period)

    def update(self, lookback_quarters: int = 12):
        """增量更新最近 N 个季度的数据"""
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_quarters} quarters)...")
        recent_quarters = self._generate_quarters()[-lookback_quarters:]
        for period in recent_quarters:
            self._process_period(period)

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

