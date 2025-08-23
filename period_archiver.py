#!/usr/bin/env python3
"""
季度归档器 (Period Archiver)

继承自 BaseArchiver，专门处理按财报季度 (period) 进行归档的数据类型，
例如 income, balancesheet 等。
"""

import json
from datetime import datetime

import pandas as pd

from base_archiver import BaseArchiver


class PeriodArchiver(BaseArchiver):
    """按财报季度进行数据归档"""

    def _get_last_checksum(self, period: str) -> str | None:
        """获取指定 period 的最新 checksum"""
        with self._get_db_connection() as conn:
            cursor = conn.execute("""
                SELECT checksum FROM request_log
                WHERE data_type = ? AND partition_key = ? AND status IN ('success', 'updated', 'no_change')
                ORDER BY ingest_date DESC LIMIT 1
            """, (self.data_type, period))
            result = cursor.fetchone()
            return result[0] if result else None

    def _save_to_landing(self, df: pd.DataFrame, period: str, ingest_date: str):
        """将数据保存到 Landing 层，按 period 和 ingest_date 分区"""
        partition_path = self.landing_path / f"period={period}" / f"ingest_date={ingest_date}"
        partition_path.mkdir(parents=True, exist_ok=True)

        data_file = partition_path / "data.parquet"
        df.to_parquet(data_file, compression='snappy', index=False)

        metadata = {
            "partition_key": period,
            "ingest_date": ingest_date,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
        }
        with open(partition_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

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
        ingest_date = datetime.now().strftime('%Y-%m-%d')
        for period in self._generate_quarters():
            period_path = self.landing_path / f"period={period}"
            if period_path.exists():
                print(f"Period {period} already exists, skipping...")
                continue
            self._process_period(period, ingest_date)

    def update(self, lookback_quarters: int = 12):
        """增量更新最近 N 个季度的数据"""
        print(f"[{self.data_type.upper()}] Starting incremental update (lookback: {lookback_quarters} quarters)...")
        ingest_date = datetime.now().strftime('%Y-%m-%d')
        recent_quarters = self._generate_quarters()[-lookback_quarters:]
        for period in recent_quarters:
            self._process_period(period, ingest_date, is_update=True)

    def _process_period(self, period: str, ingest_date: str, is_update: bool = False):
        """处理单个季度的核心逻辑"""
        print(f"Processing period: {period}...")
        params = {'period': period}
        df, fetch_status = self.fetch_function(period=period)

        if fetch_status == 'error':
            self._log_request(period, ingest_date, params, 0, "error", "error", f"API fetch failed for period {period}")
            return

        if df.empty:
            self._log_request(period, ingest_date, params, 0, "empty", "no_data")
            print(f"No data for period {period}.")
            return

        new_checksum = self._calculate_checksum(df)
        last_checksum = self._get_last_checksum(period) if is_update else None

        if is_update and new_checksum == last_checksum:
            self._log_request(period, ingest_date, params, len(df), new_checksum, "no_change")
            print(f"No changes for period {period}.")
        else:
            try:
                # 1. 保存数据文件
                self._save_to_landing(df, period, ingest_date)
                print(f"  - Saved {len(df)} records to Parquet file.")

                # 2. 记录到数据库
                status = "updated" if is_update else "success"
                self._log_request(period, ingest_date, params, len(df), new_checksum, status)
                print(f"  - Logged to DB with status: '{status}'.")

            except Exception as e:
                error_msg = f"Error saving data for period {period}: {e}"
                self._log_request(period, ingest_date, params, len(df), new_checksum, "error", str(e))
                print(f"  - {error_msg}")

