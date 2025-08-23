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
        if not partition_path.exists():
            return pd.DataFrame()

        # ingest_date is only for period-based archiver
        if partition_name.startswith('period='):
            versions = sorted([v.name for v in partition_path.iterdir() if v.is_dir()])
            if not versions:
                return pd.DataFrame()
            latest_version_path = partition_path / versions[-1]
        else:
            latest_version_path = partition_path

        data_file = latest_version_path / "data.parquet"
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

    def get_request_log(self, limit: int = 100) -> pd.DataFrame:
        """从统一日志库中获取此数据类型的请求日志"""
        if not self.log_db_path.exists():
            return pd.DataFrame()

        with sqlite3.connect(self.log_db_path) as conn:
            query = "SELECT * FROM request_log WHERE data_type = ? ORDER BY created_at DESC"
            params = [self.data_type]
            if limit and limit > 0:
                query += " LIMIT ?"
                params.append(limit)
            
            return pd.read_sql_query(query, conn, params=params)

    def get_data_summary(self) -> pd.DataFrame:
        """获取数据概要统计 (基于文件系统)"""
        partitions = self.get_partitions()
        summary = []
        for part_name in partitions:
            part_path = self.landing_path / part_name
            # This logic needs to be more robust to handle different structures
            # For now, we assume a simple metadata file in the partition dir or version dir
            metadata_path = part_path / "metadata.json"
            if not metadata_path.exists() and part_name.startswith('period='):
                versions = sorted([v.name for v in part_path.iterdir() if v.is_dir()])
                if versions:
                    metadata_path = part_path / versions[-1] / "metadata.json"

            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                summary.append({
                    'partition': part_name,
                    'row_count': metadata.get('row_count', 0),
                    'checksum': metadata.get('checksum', ''),
                    'created_at': metadata.get('created_at', '')
                })
        return pd.DataFrame(summary)

