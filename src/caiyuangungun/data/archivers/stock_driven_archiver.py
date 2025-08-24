#!/usr/bin/env python3
"""
股票驱动归档器 (StockDrivenArchiver)

继承自 BaseArchiver，专门处理需要按股票代码 (ts_code) 进行遍历归档的数据类型。
它会自动读取本地的 'stock_basic' 快照作为遍历的基准。
"""

from datetime import datetime
from typing import List

from caiyuangungun.data.archivers.base_archiver import BaseArchiver
from caiyuangungun.data.reader import TushareReader


class StockDrivenArchiver(BaseArchiver):
    """按代码列表进行数据归档（支持股票、指数等）"""

    def __init__(self, data_type: str, base_path: str = "./data", code_list: List = None, driver_data_type: str = 'stock_basic'):
        super().__init__(data_type, base_path)
        self.code_list = code_list
        self.driver_data_type = driver_data_type

    def _get_code_list(self) -> List[str]:
        """获取要遍历的代码列表，可以来自配置或快照文件"""
        if self.code_list is not None:
            print(f"  - Using provided list of {len(self.code_list)} codes.")
            return self.code_list

        print(f"  - Reading local {self.driver_data_type} snapshot...")
        reader = TushareReader(data_type=self.driver_data_type, base_path=self.base_path)
        df = reader.read_all_latest()

        if df.empty or 'ts_code' not in df.columns:
            raise FileNotFoundError(f"{self.driver_data_type} snapshot not found or is invalid.")

        code_list = df['ts_code'].unique().tolist()
        print(f"  - Found {len(code_list)} codes to process.")
        return sorted(code_list)

    def _get_processed_stocks(self) -> List[str]:
        """通过扫描文件系统获取已经处理过的股票代码"""
        if not self.landing_path.exists():
            return []
        return [p.name.split('=')[1] for p in self.landing_path.iterdir() if p.is_dir()]

    def backfill(self):
        """按代码列表逐一回填历史数据，跳过已存在的代码"""
        print(f"[{self.data_type.upper()}] Starting historical backfill by code...")
        all_codes = self._get_code_list()
        processed_codes = self._get_processed_stocks()

        codes_to_process = [c for c in all_codes if c not in processed_codes]
        print(f"  - Total: {len(all_codes)}, Processed: {len(processed_codes)}, Remaining: {len(codes_to_process)}")

        for i, ts_code in enumerate(codes_to_process):
            print(f"Processing {ts_code} ({i+1}/{len(codes_to_process)})..." )
            self._process_stock(ts_code)

        print("Historical backfill complete.")

    def update(self):
        """全量更新所有代码的数据，利用 checksum 跳过未变更的"""
        print(f"[{self.data_type.upper()}] Starting full update for all codes...")
        all_codes = self._get_code_list()

        for i, ts_code in enumerate(all_codes):
            print(f"Processing {ts_code} ({i+1}/{len(all_codes)})...")
            self._process_stock(ts_code)

        print("Full update complete.")

    def _process_stock(self, ts_code: str):
        """处理单个股票数据的核心逻辑"""
        params = {'ts_code': ts_code}
        ingest_date = datetime.now().strftime('%Y-%m-%d')

        try:
            df, fetch_status = self.fetch_function(ts_code=ts_code)

            if fetch_status == 'error':
                self._log_request(ts_code, ingest_date, params, 0, "error", "error", f"API fetch failed for {ts_code}")
                return

            partition_path = self.landing_path / f"ts_code={ts_code}"
            write_status = self._save_partitioned_data(df, partition_path, ts_code)

            log_status = 'no_data' if df.empty else write_status
            self._log_request(ts_code, ingest_date, params, len(df), self._calculate_checksum(df), log_status)

        except Exception as e:
            self._log_request(ts_code, ingest_date, params, 0, "error", "error", str(e))
            print(f"  - Failure: An error occurred while processing {ts_code}: {e}")

