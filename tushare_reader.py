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

    def _get_metadata(self, partition_path: Path) -> dict:
        """安全地读取分区或其最新版本的元数据"""
        metadata_path = partition_path / "metadata.json"
        if not metadata_path.exists() and partition_path.name.startswith('period='):
            versions = sorted([v.name for v in partition_path.iterdir() if v.is_dir() and v.name.startswith('ingest_date=')])
            if versions:
                metadata_path = partition_path / versions[-1] / "metadata.json"

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

            version_count = "N/A"
            if part_name.startswith('period='):
                versions = [v for v in part_path.iterdir() if v.is_dir() and v.name.startswith('ingest_date=')]
                version_count = len(versions)

            summary_data.append({
                'partition': metadata.get('partition_key', part_name.split('=')[-1]),
                'version_count': version_count,
                'row_count': metadata.get('row_count', 0),
                'checksum': metadata.get('checksum', 'N/A'),
                'last_updated': metadata.get('created_at', 'N/A'),
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

