#!/usr/bin/env python3
"""
快照归档器 (Snapshot Archiver)

继承自 BaseArchiver，用于处理日频更新、全量替换的数据类型，
例如 stock_basic (股票基础信息)。
"""

import json
import shutil
from datetime import datetime, timedelta

import pandas as pd

from base_archiver import BaseArchiver


class SnapshotArchiver(BaseArchiver):
    """对全量数据进行每日快照"""

    def backfill(self, **kwargs):
        """快照模式不支持历史回填，调用 update 即可"""
        print("Snapshot archiver does not support backfill. Please use the 'update' mode.")
        self.update(**kwargs)

    def update(self, snapshot_date: str = None, retention_days: int = 30):
        """获取并保存当日的全量数据快照"""
        if snapshot_date is None:
            snapshot_date = datetime.now().strftime('%Y%m%d')
        
        print(f"[{self.data_type.upper()}] Starting snapshot update for {snapshot_date}...")
        ingest_date = datetime.now().strftime('%Y-%m-%d')
        params = {'snapshot_date': snapshot_date}

        try:
            df, fetch_status = self.fetch_function()

            if fetch_status == 'error' or df.empty:
                status = "error" if fetch_status == 'error' else "no_data"
                error_msg = f"API fetch failed or returned empty data for {snapshot_date}"
                self._log_request(snapshot_date, ingest_date, params, 0, "", status, error_msg)
                print(error_msg)
                return

            checksum = self._calculate_checksum(df)
            self._save_snapshot(df, snapshot_date)
            self._log_request(snapshot_date, ingest_date, params, len(df), checksum, 'success')
            print(f"Successfully saved snapshot for {snapshot_date} with {len(df)} records.")

            if retention_days > 0:
                self._cleanup_old_snapshots(retention_days)

        except Exception as e:
            self._log_request(snapshot_date, ingest_date, params, 0, "", "error", str(e))
            print(f"An error occurred during snapshot update for {snapshot_date}: {e}")

    def _save_snapshot(self, df: pd.DataFrame, snapshot_date: str):
        """将快照数据保存到 Landing 层"""
        partition_path = self.landing_path / f"snapshot_date={snapshot_date}"
        partition_path.mkdir(parents=True, exist_ok=True)

        data_file = partition_path / "data.parquet"
        df.to_parquet(data_file, compression='snappy', index=False)
        print(f"  - Saved {len(df)} records to {data_file}")

        metadata = {
            "partition_key": snapshot_date,
            "row_count": len(df),
            "checksum": self._calculate_checksum(df),
            "created_at": datetime.now().isoformat(),
        }
        with open(partition_path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

    def _cleanup_old_snapshots(self, retention_days: int):
        """清理超过指定保留天数的旧快照"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0
        for partition_dir in self.landing_path.iterdir():
            if not partition_dir.is_dir() or not partition_dir.name.startswith("snapshot_date="):
                continue
            
            try:
                date_str = partition_dir.name.split('=')[1]
                snapshot_date_obj = datetime.strptime(date_str, '%Y%m%d')
                if snapshot_date_obj < cutoff_date:
                    print(f"[Cleanup] Deleting old snapshot: {partition_dir.name}")
                    shutil.rmtree(partition_dir)
                    deleted_count += 1
            except (IndexError, ValueError) as e:
                print(f"[Cleanup] Could not parse date from directory name: {partition_dir.name}, error: {e}")

        if deleted_count > 0:
            print(f"[Cleanup] Successfully deleted {deleted_count} old snapshots.")

