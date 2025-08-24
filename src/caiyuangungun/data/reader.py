#!/usr/bin/env python3
"""
Tushare 数据读取器 (Tushare Reader)

重构后的版本，能够智能地发现和读取采用统一分区方案的数据。
支持多种分区键 (e.g., period, ann_date, snapshot_date)。
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd


class TushareReader:
    def __init__(self, data_type: str, base_path: str = "./data"):
        self.data_type = data_type
        self.base_path = Path(base_path)
        self.landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.log_db_path = self.base_path / "logs" / "request_log.db"

    def get_partitions(self) -> list:
        """获取所有可用的数据分区"""
        if not self.landing_path.exists():
            return []
        return sorted([p.name for p in self.landing_path.iterdir() if p.is_dir()])

    def read_latest_data(self, partition_name: str) -> pd.DataFrame:
        """读取指定分区的最新版本数据"""
        partition_path = self.landing_path / partition_name
        data_file = partition_path / "data.parquet"
        if data_file.exists():
            return pd.read_parquet(data_file)
        return pd.DataFrame()

    def read_all_latest(self) -> pd.DataFrame:
        """读取所有分区的最新数据并合并"""
        all_data = []
        for partition in self.get_partitions():
            df = self.read_latest_data(partition)
            if not df.empty:
                all_data.append(df)

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def _get_metadata(self, partition_path: Path) -> dict:
        """安全地读取分区的元数据"""
        metadata_path = partition_path / "metadata.json"
        if metadata_path.exists():
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def get_data_summary(self) -> pd.DataFrame:
        """获取基于文件系统的数据摘要"""
        partitions = self.get_partitions()
        summary_data = []
        for part_name in partitions:
            part_path = self.landing_path / part_name
            metadata = self._get_metadata(part_path)

            # Count archived versions
            archive_partition_path = self.base_path / "raw" / "archive" / "tushare" / self.data_type / part_name
            version_count = 0
            if archive_partition_path.exists():
                version_count = len([f for f in archive_partition_path.glob('data_*.parquet')])

            summary_data.append({
                'partition': metadata.get('partition_key', part_name.split('=')[-1]),
                'archived_versions': version_count,
                'row_count': metadata.get('row_count', 0),
                'checksum': metadata.get('checksum', 'N/A'),
                'last_updated': metadata.get('updated_at', 'N/A'),
            })
        return pd.DataFrame(summary_data)

    def get_request_log(self, limit: int = 20) -> pd.DataFrame:
        """从统一日志库中获取此数据类型的请求日志"""
        if not self.log_db_path.exists():
            return pd.DataFrame()

        with sqlite3.connect(self.log_db_path) as conn:
            query = "SELECT partition_key, ingest_date, row_count, status, error_message, created_at FROM request_log WHERE data_type = ? ORDER BY created_at DESC"
            params = [self.data_type]
            if limit and limit > 0:
                query += " LIMIT ?"
                params.append(limit)

            return pd.read_sql_query(query, conn, params=params)

