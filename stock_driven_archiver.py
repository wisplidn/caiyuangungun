#!/usr/bin/env python3
"""
股票驱动归档器 (StockDrivenArchiver)

继承自 BaseArchiver，专门处理需要按股票代码 (ts_code) 进行遍历归档的数据类型。
它会自动读取本地的 'stock_basic' 快照作为遍历的基准。
"""

import json
from datetime import datetime
import sqlite3

import pandas as pd

from base_archiver import BaseArchiver
from tushare_reader import TushareReader


class StockDrivenArchiver(BaseArchiver):
    """按股票代码进行数据归档"""

    def _get_stock_list(self) -> list[str]:
        """使用 TushareReader 读取本地的股票列表快照"""
        print("  - Reading local stock_basic snapshot...")
        reader = TushareReader(data_type='stock_basic', base_path=self.base_path)
        df = reader.read_all_latest()

        if df.empty or 'ts_code' not in df.columns:
            raise FileNotFoundError("stock_basic snapshot not found or is invalid. Please run 'stock_basic' snapshot first.")

        stock_list = df['ts_code'].unique().tolist()
        print(f"  - Found {len(stock_list)} stocks to process.")
        return sorted(stock_list)

    def _get_processed_stocks(self) -> list[str]:
        """从日志数据库获取已经成功处理的股票代码"""
        with sqlite3.connect(self.log_db_path) as conn:
            cursor = conn.execute("""
                SELECT partition_key FROM request_log
                WHERE data_type = ? AND status = 'success'
            """, (self.data_type,))
            results = cursor.fetchall()
            return [row[0] for row in results]

    def _save_data_for_stock(self, df: pd.DataFrame, ts_code: str):
        """将单个股票的数据保存到 Landing 层，按 ts_code 分区"""
        partition_path = self.landing_path / f"ts_code={ts_code}"
        partition_path.mkdir(parents=True, exist_ok=True)

        if not df.empty:
            df.to_parquet(partition_path / "data.parquet", compression='snappy', index=False)

        metadata = {
            "partition_key": ts_code,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
        }
        with open(partition_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

    def backfill(self):
        """按股票代码逐一回填历史数据，跳过已处理的股票"""
        print(f"[{self.data_type.upper()}] Starting historical backfill by stock...")
        all_stocks = self._get_stock_list()
        processed_stocks = self._get_processed_stocks()

        stocks_to_process = [s for s in all_stocks if s not in processed_stocks]
        print(f"  - Total: {len(all_stocks)}, Processed: {len(processed_stocks)}, Remaining: {len(stocks_to_process)}")

        for i, ts_code in enumerate(stocks_to_process):
            print(f"Processing {ts_code} ({i+1}/{len(stocks_to_process)})...")
            self._process_stock(ts_code)

        print("Historical backfill complete.")

    def update(self):
        """'update' 模式等同于 'backfill'，因为它会检查并处理任何未完成的股票"""
        print(f"[{self.data_type.upper()}] Update mode will check for any unprocessed stocks.")
        self.backfill()

    def _process_stock(self, ts_code: str):
        """处理单个股票数据的核心逻辑"""
        params = {'ts_code': ts_code}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(ts_code=ts_code)

            if fetch_status == 'error':
                self._log_request(ts_code, ingest_date, params, 0, "error", "error", f"API fetch failed for {ts_code}")
                return

            self._save_data_for_stock(df, ts_code)
            checksum = self._calculate_checksum(df)
            log_status = 'no_data' if df.empty else 'success'
            self._log_request(ts_code, ingest_date, params, len(df), checksum, log_status)
            print(f"  - Success: Saved {len(df)} records for {ts_code}.")

        except Exception as e:
            self._log_request(ts_code, ingest_date, params, 0, "error", "error", str(e))
            print(f"  - Failure: An error occurred while processing {ts_code}: {e}")

