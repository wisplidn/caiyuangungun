#!/usr/bin/env python3
"""
基础归档器 (Base Archiver)

定义所有归档器的通用接口和共享逻辑，包括：
- 统一的初始化流程 (路径、数据库)
- 统一的日志系统
- 通用的数据校验和计算
- 抽象的归档方法，强制子类实现
"""

import hashlib
import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

import tushare_client


class BaseArchiver(ABC):
    """所有Tushare数据归档器的抽象基类"""

    def __init__(self, data_type: str, base_path: str = "./data"):
        self.data_type = data_type
        self.base_path = Path(base_path)
        self.landing_path = self.base_path / "raw" / "landing" / "tushare" / self.data_type
        self.log_db_path = self.base_path / "logs" / "request_log.db"

        # 动态获取数据提取函数
        self.fetch_function = self._get_fetch_function()

        # 初始化目录和日志数据库
        self._create_directories()
        self._init_log_db()

    def _get_fetch_function(self):
        """根据data_type动态获取tushare_client中的数据函数"""
        vip_func_name = f"get_{self.data_type}_vip"
        std_func_name = f"get_{self.data_type}"

        if hasattr(tushare_client, vip_func_name):
            return getattr(tushare_client, vip_func_name)
        elif hasattr(tushare_client, std_func_name):
            return getattr(tushare_client, std_func_name)
        else:
            raise ValueError(f"Unsupported data_type: '{self.data_type}'. No corresponding function found in tushare_client.")

    def _create_directories(self):
        """创建归档所需的基础目录结构"""
        self.landing_path.mkdir(parents=True, exist_ok=True)
        self.log_db_path.parent.mkdir(parents=True, exist_ok=True)

    def _init_log_db(self):
        """初始化或验证统一的日志数据库表结构"""
        with sqlite3.connect(self.log_db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,
                    partition_key TEXT NOT NULL, -- 通用分区键 (如: 20230331, 2023-08-01)
                    ingest_date TEXT NOT NULL,
                    params TEXT,
                    row_count INTEGER,
                    checksum TEXT,
                    status TEXT, -- (success, updated, no_change, no_data, error)
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(data_type, partition_key, ingest_date)
                )
            """)
            conn.commit()

    def _log_request(self, partition_key: str, ingest_date: str, params: dict, row_count: int, checksum: str, status: str, error_message: str = None):
        """向统一的日志数据库中写入一条记录"""
        conn = None
        try:
            conn = sqlite3.connect(self.log_db_path, timeout=10) # 增加超时以减少锁定冲突
            conn.execute("""
                INSERT OR REPLACE INTO request_log
                (data_type, partition_key, ingest_date, params, row_count, checksum, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.data_type, partition_key, ingest_date, json.dumps(params), row_count, checksum, status, error_message))
            conn.commit()
        except sqlite3.Error as e:
            print(f"[DB_ERROR] Failed to log request: {e}")
        finally:
            if conn:
                conn.close()

    def _calculate_checksum(self, df: pd.DataFrame) -> str:
        """计算DataFrame的稳定校验和 (MD5)"""
        if df.empty:
            return "empty"

        # 动态选择存在的列进行排序，以适应不同接口返回的字段
        potential_keys = ['ts_code', 'ann_date', 'end_date', 'trade_date']
        sort_keys = [key for key in potential_keys if key in df.columns]
        
        if not sort_keys:
            # 如果没有任何预期的排序列，则按所有列排序以确保一致性
            sort_keys = sorted(df.columns.tolist())

        df_sorted = df.sort_values(by=sort_keys).reset_index(drop=True)
        content = df_sorted.to_csv(index=False, float_format='%.6f').encode('utf-8')
        return hashlib.md5(content).hexdigest()

    @abstractmethod
    def backfill(self, **kwargs):
        """历史数据回填的抽象方法"""
        pass

    @abstractmethod
    def update(self, **kwargs):
        """增量更新的抽象方法"""
        pass

